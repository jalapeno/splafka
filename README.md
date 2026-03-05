# splafka

Subscribe to Kafka topics and write enriched JSON messages to a local file.

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
| `-o`, `--output` | `output.json` | Output filename (messages are appended) |
| `-d`, `--output-dir` | `.` (current dir) | Directory for the output file (created if missing) |
| `-g`, `--group-id` | auto-generated | Kafka consumer group ID |
| `--from-beginning` | off | Consume from the earliest available offset |
| `-q`, `--quiet` | off | Suppress per-message logging to stderr |

### Examples

Consume two topics from a local broker and write to the default output file:

```bash
python splafka.py -b 198.18.201.39:30092 -t topic-1 topic-2
```

Consume from the beginning, writing to a specific directory and filename:

```bash
python splafka.py -b 198.18.201.39:30092 -t my-events --from-beginning -d /var/lib/splafka -o events.json
```

Press **Ctrl+C** to stop. The consumer will commit offsets and print a summary.

## Running as a systemd service

A unit file is included at `splafka.service`. To deploy as a daemon on Ubuntu:

```bash
# Create a service user and data directory
sudo useradd -r -s /usr/sbin/nologin splafka
sudo mkdir -p /opt/splafka /var/lib/splafka
sudo chown splafka:splafka /var/lib/splafka

# Copy the app and set up a virtualenv
sudo cp splafka.py requirements.txt /opt/splafka/
sudo python3 -m venv /opt/splafka/venv
sudo /opt/splafka/venv/bin/pip install -r /opt/splafka/requirements.txt

# Install and edit the unit file (adjust ExecStart topics/broker as needed)
sudo cp splafka.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now splafka
```

Manage with standard systemd commands:

```bash
sudo systemctl status splafka      # check status
sudo journalctl -u splafka -f      # follow logs
sudo systemctl restart splafka     # restart
sudo systemctl stop splafka        # stop
```

## Output format

Each line in the output file is the original JSON message from Kafka with `topic`, `partition`, and `offset` injected:

```json
{"event": "user_signup", "user_id": 1234, "email": "alice@example.com", "topic": "user-events", "partition": 0, "offset": 42}
{"event": "page_view", "url": "/dashboard", "topic": "analytics", "partition": 1, "offset": 108}
```

If a message isn't valid JSON, it is wrapped in a `_raw_value` field with a warning logged to stderr.
