import json
import signal
import time
import uuid
from datetime import datetime, timezone

import boto3
from confluent_kafka import Consumer
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider

REGION = "ca-central-1"
BUCKET = "gamestream-raw-julia-2026"
MAX_BATCH_SIZE_BYTES = 1 * 1024 * 1024  # 1 MB
FLUSH_INTERVAL_SECONDS = 300

BOOTSTRAP_SERVERS = (
    "b-1.gamestreammsk.91ihjh.c3.kafka.ca-central-1.amazonaws.com:9098,"
    "b-2.gamestreammsk.91ihjh.c3.kafka.ca-central-1.amazonaws.com:9098"
)


def oauth_cb(_oauth_config):
    token, expiry_ms = MSKAuthTokenProvider.generate_auth_token(REGION)
    return token, expiry_ms / 1000


consumer = Consumer({
    "bootstrap.servers": BOOTSTRAP_SERVERS,
    "security.protocol": "SASL_SSL",
    "sasl.mechanism": "OAUTHBEARER",
    "oauth_cb": oauth_cb,
    "client.id": "gamestream-consumer",
    "group.id": "gamestream-consumer",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

s3 = boto3.client("s3", region_name=REGION)
consumer.subscribe(["game-events"])

batch = []
batch_size_bytes = 0
last_flush = time.monotonic()
running = True


def stop_consumer(_signum, _frame):
    global running
    running = False


def upload_batch():
    global last_flush, batch_size_bytes

    if not batch:
        return

    now = datetime.now(timezone.utc)
    date_path = now.strftime("year=%Y/month=%m/day=%d")
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    batch_id = uuid.uuid4().hex[:8]
    key = f"raw/{date_path}/gamestream_events_{timestamp}_{batch_id}.jsonl"

    body = "".join(
        json.dumps(event, ensure_ascii=False) + "\n"
        for event in batch
    )

    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=body.encode("utf-8"),
        ContentType="application/x-ndjson",
    )

    print(f"Uploaded {len(batch)} events to s3://{BUCKET}/{key}", flush=True)
    batch.clear()
    batch_size_bytes = 0
    last_flush = time.monotonic()


signal.signal(signal.SIGTERM, stop_consumer)
signal.signal(signal.SIGINT, stop_consumer)

print("GameStream consumer started. Waiting for events...", flush=True)

try:
    while running:
        message = consumer.poll(1.0)

        if message is not None:
            if message.error():
                print(f"Kafka error: {message.error()}", flush=True)
            else:
                event = json.loads(message.value().decode("utf-8"))
                batch.append(event)
                batch_size_bytes += len(
                    (json.dumps(event, ensure_ascii=False) + "\n").encode("utf-8")
                )

                print(
                    f"Received: {event.get('player_id')} - "
                    f"{event.get('event_type')} "
                    f"(pending events: {len(batch)}, "
                    f"batch size: {batch_size_bytes / 1024:.1f} KB)",
                    flush=True,
                )

        elapsed = time.monotonic() - last_flush

        if batch and (
            batch_size_bytes >= MAX_BATCH_SIZE_BYTES
            or elapsed >= FLUSH_INTERVAL_SECONDS
        ):
            upload_batch()
            consumer.commit(asynchronous=False)

finally:
    try:
        upload_batch()
        consumer.commit(asynchronous=False)
    except Exception as exc:
        print(f"Final upload/commit failed: {exc}", flush=True)
    finally:
        consumer.close()
        print("Consumer closed.", flush=True)
