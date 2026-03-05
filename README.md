# splafka

Subscribe to Kafka topics and write every message to a local file in JSONL format.

## Setup

```bash
pip install -r requirements.txt
```

> **Note:** `confluent-kafka` requires `librdkafka`. On macOS: `brew install librdkafka`. On Debian/Ubuntu: `apt install librdkafka-dev`.

## Usage

```bash
python splafka.py -b <bootstrap-servers> -t <topic1> [topic2 ...] [options]
```

### Required arguments

| Flag | Description |
|------|-------------|
| `-b`, `--bootstrap-servers` | Kafka broker address(es), comma-separated (e.g. `host:9092,host2:9092`) |
| `-t`, `--topics` | One or more topic names to subscribe to |

### Optional arguments

| Flag | Default | Description |
|------|---------|-------------|
| `-o`, `--output` | `output.jsonl` | Output file path (messages are appended) |
| `-g`, `--group-id` | auto-generated | Kafka consumer group ID |
| `--from-beginning` | off | Consume from the earliest available offset |
| `-q`, `--quiet` | off | Suppress per-message logging to stderr |

### Examples

Consume two topics from a local broker and write to the default output file:

```bash
python splafka.py -b 198.18.201.39:30092 -t topic-1 topic-2
```

Consume from the beginning, writing to a specific file:

```bash
python splafka.py -b 198.18.201.39:30092 -t my-events --from-beginning -o events.jsonl
```

Press **Ctrl+C** to stop. The consumer will commit offsets and print a summary.

## Output format

Each line in the output file is a JSON object:

```json
{
  "topic": "topic-1",
  "partition": 0,
  "offset": 42,
  "timestamp": 1709500000000,
  "key": "some-key",
  "value": "{\"event\":\"example\"}",
  "received_at": "2026-03-04T20:30:00.000000+00:00"
}
```
