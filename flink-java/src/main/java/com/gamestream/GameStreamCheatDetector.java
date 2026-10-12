package com.gamestream;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.lambda.LambdaClient;
import software.amazon.awssdk.services.lambda.model.InvokeRequest;
import software.amazon.awssdk.services.lambda.model.InvokeResponse;
import software.amazon.awssdk.services.lambda.model.InvocationType;

import java.util.HashMap;
import java.util.Map;

public class GameStreamCheatDetector {

    private static final ObjectMapper JSON = new ObjectMapper();

    private static final String MSK_BOOTSTRAP_SERVERS =
        requireEnv("KAFKA_BOOTSTRAP_SERVERS");

private static final String ALERT_LAMBDA_ARN =
        requireEnv("ALERT_LAMBDA_ARN");

private static String requireEnv(String name) {
    String value = System.getenv(name);
    if (value == null || value.isBlank()) {
        throw new IllegalStateException(
                "Missing required environment variable: " + name
        );
    }
    return value;
}

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(MSK_BOOTSTRAP_SERVERS)
                .setTopics("game-events")
                .setGroupId("gamestream-cheat-detector")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty("security.protocol", "SASL_SSL")
                .setProperty("sasl.mechanism", "AWS_MSK_IAM")
                .setProperty(
                        "sasl.jaas.config",
                        "software.amazon.msk.auth.iam.IAMLoginModule required;"
                )
                .setProperty(
                        "sasl.client.callback.handler.class",
                        "software.amazon.msk.auth.iam.IAMClientCallbackHandler"
                )
                .build();

        DataStream<String> events = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "MSK game-events"
        );

        events
                .keyBy(GameStreamCheatDetector::playerKey)
                .process(new CheatDetector(ALERT_LAMBDA_ARN))
                .name("detect-cheats-and-invoke-lambda")
                .print("CHEAT ALERT");

        env.execute("GameStream Cheat Detector");
    }

    private static String playerKey(String raw) {
        try {
            return JSON.readTree(raw).path("player_id").asText("unknown");
        } catch (Exception e) {
            return "invalid-event";
        }
    }

    public static class CheatDetector
            extends KeyedProcessFunction<String, String, String> {

        private final String lambdaArn;

        private transient ValueState<Integer> killsSinceLevelUp;
        private transient MapState<String, String> lootRolls;
        private transient LambdaClient lambda;

        public CheatDetector(String lambdaArn) {
            this.lambdaArn = lambdaArn;
        }

        @Override
        public void open(OpenContext context) {
            killsSinceLevelUp = getRuntimeContext().getState(
                    new ValueStateDescriptor<>(
                            "kills-since-level-up",
                            Types.INT
                    )
            );

            lootRolls = getRuntimeContext().getMapState(
                    new MapStateDescriptor<>(
                            "loot-rolls",
                            Types.STRING,
                            Types.STRING
                    )
            );

            lambda = LambdaClient.builder()
                    .region(Region.CA_CENTRAL_1)
                    .build();
        }

        @Override
        public void processElement(
                String raw,
                Context ctx,
                Collector<String> out
        ) throws Exception {

            JsonNode event = JSON.readTree(raw);

            if (!event.isObject()) {
                return;
            }

            String type = event.path("event_type").asText("");
            String player = event.path("player_id").asText("unknown");

            String rule = null;
            String message = null;

            // Count enemy kills between legitimate level-ups.
            if ("enemy_killed".equals(type)) {
                int count = killsSinceLevelUp.value() == null
                        ? 0
                        : killsSinceLevelUp.value();

                killsSinceLevelUp.update(count + 1);
            }

            // Detect suspicious level-ups.
            if ("level_up".equals(type)) {
                int previous = event.path("previous_level").asInt(-1);
                int next = event.path("new_level").asInt(-1);
                int declaredKills = event.path("enemies_killed").asInt(0);

                int observedKills = killsSinceLevelUp.value() == null
                        ? 0
                        : killsSinceLevelUp.value();

                if (previous < 0
                        || next != previous + 1
                        || declaredKills < 2
                        || observedKills < 2) {

                    rule = "suspicious_level_up";

                    message = "Level-up without two observed enemy-kill events. "
                            + "Observed kills=" + observedKills
                            + ", declared kills=" + declaredKills;
                } else {
                    // Reset the counter after a legitimate level-up.
                    killsSinceLevelUp.update(0);
                }
            }

            // Detect PvP level retention without protection.
            if ("player_defeated".equals(type)) {
                int previous = event.path("previous_level").asInt(0);
                int next = event.path("new_level").asInt(-1);

                boolean protectedLevel =
                        event.path("level_protected").asBoolean(false);

                if (previous > 0
                        && next >= previous
                        && !protectedLevel) {

                    rule = "suspicious_pvp_level_retention";

                    message =
                            "Player retained their level after PvP without protection.";
                }
            }

            // Validate loot roll IDs and item indexes.
            if ("item_collected".equals(type)) {
                String rollId = event.path("loot_roll_id").asText("");
                int index = event.path("loot_index").asInt(-1);

                if (rollId.isBlank() || (index != 1 && index != 2)) {
                    rule = "suspicious_duplicate_loot";

                    message =
                            "Loot event has a missing roll ID or invalid loot index.";
                } else {
                    String previousIndexes = lootRolls.get(rollId);

                    if (previousIndexes != null
                            && (previousIndexes.contains("," + index + ",")
                            || (index == 2
                            && !previousIndexes.contains(",1,")))) {

                        rule = "suspicious_duplicate_loot";

                        message =
                                "Duplicate or out-of-order loot index for roll "
                                + rollId;
                    } else {
                        lootRolls.put(
                                rollId,
                                previousIndexes == null
                                        ? "," + index + ","
                                        : previousIndexes + index + ","
                        );
                    }
                }
            }

            // Send an email alert through Lambda only when a rule is triggered.
            if (rule != null) {
                Map<String, Object> alert = new HashMap<>();

                alert.put("rule", rule);
                alert.put("player_id", player);
                alert.put("message", message);
                alert.put("event", JSON.convertValue(event, Map.class));

                InvokeResponse response = lambda.invoke(
                        InvokeRequest.builder()
                                .functionName(lambdaArn)
                                .invocationType(
                                        InvocationType.REQUEST_RESPONSE
                                )
                                .payload(
                                        SdkBytes.fromUtf8String(
                                                JSON.writeValueAsString(alert)
                                        )
                                )
                                .build()
                );

                if (response.functionError() != null) {
                    throw new RuntimeException(
                            "Alert Lambda failed: "
                                    + response.functionError()
                                    + " "
                                    + response.payload().asUtf8String()
                    );
                }

                if (response.statusCode() == null
                        || response.statusCode() >= 300) {
                    throw new RuntimeException(
                            "Lambda invocation failed: "
                                    + response.statusCode()
                    );
                }

                // Only detected alerts are written to the Flink sink.
                out.collect(
                        "Detected " + rule + " for player " + player
                );
            }
        }
    }
}