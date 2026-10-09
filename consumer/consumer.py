import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "game-events",
    bootstrap_servers="localhost:9092",
    group_id="gamestream-consumer",
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8"))
)


print("GameStream consumer started")


with open("game-events.jsonl", "a", encoding="utf-8") as file:

    for message in consumer:

        event = message.value

        file.write(
            json.dumps(event) + "\n"
        )

        file.flush()

        print(
            f"Saved event: "
            f"{event['player_id']} - "
            f"{event['event_type']}"
        )