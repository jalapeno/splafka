#!/usr/bin/env python3
"""splafka - Subscribe to Kafka topics and write messages to a local file."""

import argparse
import json
import os
import signal
import sys
import uuid

from confluent_kafka import Consumer, KafkaError, KafkaException


def build_consumer(bootstrap_servers: str, group_id: str, from_beginning: bool) -> Consumer:
    config = {
        "bootstrap.servers": bootstrap_servers,
        "group.id": group_id,
        "auto.offset.reset": "earliest" if from_beginning else "latest",
        "enable.auto.commit": True,
    }
    return Consumer(config)


def enrich_message(msg) -> dict:
    """Parse the message value as JSON and inject Kafka metadata fields."""
    raw = msg.value()
    text = raw.decode("utf-8", errors="replace") if raw else "{}"
    try:
        record = json.loads(text)
    except json.JSONDecodeError:
        record = {"_raw_value": text}
        print(f"Warning: non-JSON message at {msg.topic()}:{msg.partition()}@{msg.offset()}", file=sys.stderr)

    record["topic"] = msg.topic()
    record["partition"] = msg.partition()
    record["offset"] = msg.offset()
    return record


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="splafka",
        description="Subscribe to Kafka topics and write messages to a local file (JSONL).",
    )
    parser.add_argument(
        "-b", "--bootstrap-servers",
        required=True,
        help="Kafka bootstrap server(s), comma-separated (e.g. host1:9092,host2:9092).",
    )
    parser.add_argument(
        "-t", "--topics",
        required=True,
        nargs="+",
        help="One or more topic names to subscribe to.",
    )
    parser.add_argument(
        "-o", "--output",
        default="output.json",
        help="Output filename (default: output.json).",
    )
    parser.add_argument(
        "-d", "--output-dir",
        default=".",
        help="Directory for the output file (default: current directory). Created if it doesn't exist.",
    )
    parser.add_argument(
        "-g", "--group-id",
        default=None,
        help="Consumer group ID (default: auto-generated unique ID).",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        default=False,
        help="Consume from the earliest available offset instead of latest.",
    )
    parser.add_argument(
        "-q", "--quiet",
        action="store_true",
        default=False,
        help="Suppress per-message output to stderr.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)

    os.makedirs(args.output_dir, exist_ok=True)
    output_path = os.path.join(args.output_dir, args.output)

    group_id = args.group_id or f"splafka-{uuid.uuid4().hex[:8]}"
    consumer = build_consumer(args.bootstrap_servers, group_id, args.from_beginning)

    shutdown = False

    def handle_signal(signum, frame):
        nonlocal shutdown
        shutdown = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print(f"Connecting to {args.bootstrap_servers}", file=sys.stderr)
    print(f"Subscribing to topics: {', '.join(args.topics)}", file=sys.stderr)
    print(f"Writing to {output_path}", file=sys.stderr)
    print(f"Consumer group: {group_id}", file=sys.stderr)
    print("Press Ctrl+C to stop.\n", file=sys.stderr)

    consumer.subscribe(args.topics)

    count = 0
    try:
        with open(output_path, "a") as f:
            while not shutdown:
                msg = consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise KafkaException(msg.error())

                record = enrich_message(msg)
                f.write(json.dumps(record) + "\n")
                f.flush()
                count += 1

                if not args.quiet:
                    preview = json.dumps(record, separators=(",", ":"))[:120]
                    print(
                        f"{record['topic']}:{record['partition']}@{record['offset']}  {preview}",
                        file=sys.stderr,
                    )
    except KafkaException as e:
        print(f"Kafka error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        consumer.close()
        print(f"\nConsumed {count} message(s). Output written to {output_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
