import base64
import csv
import json
from datetime import timedelta

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import BaseCliTestWithDatabase


class TestTopicArchive(BaseCliTestWithDatabase):
    def test_export_resumes_from_committed_offset(self, tmp_path):
        topic = "orders"
        topic_path = f"{self.root_dir}/{topic}"
        consumer = "archive-exporter"
        producer = "order-import"
        cli = self.execute_ydb_cli_command

        cli(["topic", "create", topic, "--retention-period", "12h", "--supported-codecs", "raw,gzip"])
        cli(["topic", "consumer", "add", topic, "--consumer", consumer])

        # Read one base64-encoded message per line of the input archive and publish
        # the original bytes with compression. The first two records exercise CSV quoting.
        messages = [b'42,"placed"', b"43,ship to:\nMain Street", b"44,shipped", b"45,delivered"]
        archive = tmp_path / "orders.b64"
        archive.write_bytes(b"\n".join(base64.b64encode(message) for message in messages) + b"\n")
        cli([
            "topic", "write", topic_path, "--file", str(archive), "--format", "newline-delimited",
            "--transform", "base64", "--codec", "gzip", "--message-group-id", producer,
        ])

        # Keep the archive available for longer while exporting it.
        cli(["topic", "alter", topic, "--retention-period", "24h", "--partition-write-speed-kbps", "512"])
        description = self.driver.topic_client.describe_topic(topic_path)
        assert description.retention_period == timedelta(hours=24)
        assert description.partition_write_speed_bytes_per_second == 512 * 1024

        read = ["topic", "read", topic, "--consumer", consumer, "--limit", "2"]
        fields = "body,offset,partition_id,producer_id,seq_no"
        first_batch = tmp_path / "first-batch.csv"
        output = cli(read + [
            "--file", str(first_batch), "--format", "csv", "--metadata-fields", fields, "--commit", "1",
        ])
        assert output.stdout == ""
        with first_batch.open(encoding="utf-8", newline="") as stream:
            assert list(csv.DictReader(stream)) == [
                {"body": message.decode(), "offset": str(i), "partition_id": "0", "producer_id": producer, "seq_no": str(i + 1)}
                for i, message in enumerate(messages[:2])
            ]

        # The next process resumes from the saved offset. This time checkpoint only
        # after validating the exported JSON batch.
        second_batch = cli(read + [
            "--format", "json-array", "--transform", "base64", "--metadata-fields", fields, "--commit", "0",
        ])
        assert json.loads(second_batch.stdout) == [
            {"body": base64.b64encode(message).decode(), "offset": i, "partition_id": 0, "producer_id": producer, "seq_no": i + 1}
            for i, message in enumerate(messages[2:], start=2)
        ]

        stats = cli(["topic", "consumer", "describe", topic, "--consumer", consumer, "--partition-stats"])
        assert f"Consumer {consumer}:" in stats.stdout
        rows = [
            [cell.strip() for cell in line.split("│")[1:-1]]
            for line in stats.stdout.splitlines() if line.startswith("│")
        ]
        stats_columns = {"#", "End offset", "Committed offset"}
        stats_header = next((i for i, row in enumerate(rows) if stats_columns.issubset(row)), None)
        assert stats_header is not None and stats_header + 1 < len(rows), stats.stdout
        header, partition = rows[stats_header:stats_header + 2]
        offsets = dict(zip(header, partition))
        assert offsets["#"] == "0"
        assert offsets["End offset"] == "4"
        assert offsets["Committed offset"] == "2"
        cli([
            "topic", "consumer", "offset", "commit", topic, "--consumer", consumer,
            "--partition", "0", "--offset", "4",
        ])
        assert json.loads(cli(read + ["--format", "json-array"]).stdout) == []

        # A support engineer can still inspect the last message by offset without
        # affecting the exporter's checkpoint.
        replay = cli([
            "topic", "read", topic, "--partitions", "0", "--start-offset", "3", "--limit", "1",
            "--format", "pretty", "--metadata-fields", fields,
        ])
        header, message = [
            [cell.strip() for cell in line.split("│")[1:-1]]
            for line in replay.stdout.splitlines() if line.startswith("│")
        ]
        assert dict(zip(header, message)) == {
            "body": messages[-1].decode(), "offset": "3", "partition_id": "0", "producer_id": producer, "seq_no": "4",
        }

        cli(["topic", "consumer", "drop", topic, "--consumer", consumer])
        assert not self.driver.topic_client.describe_topic(topic_path).consumers
        cli(["topic", "drop", topic])
        assert topic not in {entry.name for entry in self.driver.scheme_client.list_directory(self.root_dir).children}
