import copy
import json
import unittest
from unittest.mock import Mock

from .yt_in_docker.yt_client import YtClient


class TestQueueProducerSerialization(unittest.TestCase):
    def setUp(self):
        self.client = YtClient.__new__(YtClient)
        self.client._run_yt_cli = Mock()

    def test_rows_escape_reserved_json_keys(self):
        rows = [
            {"data": "$unchanged", "$sequence_number": 1, "$tablet_index": 0, "$$literal": "value"},
            {"data": "second", "$sequence_number": 2},
        ]
        original = copy.deepcopy(rows)
        self.client.push_queue_producer("//producer", "//queue", "session", rows=rows)

        self.client._run_yt_cli.assert_called_once()
        args, kwargs = self.client._run_yt_cli.call_args
        self.assertEqual(args[0], [
            "push-queue-producer", "//producer", "//queue",
            "--session-id", "session", "--epoch", "0", "--input-format", "json",
        ])
        self.assertEqual([json.loads(line) for line in kwargs["input_data"].splitlines()], [
            {"data": "$unchanged", "$$sequence_number": 1, "$$tablet_index": 0, "$$$literal": "value"},
            {"data": "second", "$$sequence_number": 2},
        ])
        self.assertEqual(rows, original)

    def test_raw_input_is_preserved(self):
        raw = '{data="value";"$sequence_number"=1};\n'
        self.client.push_queue_producer("//producer", "//queue", "session", input_data=raw, input_format="yson")

        self.client._run_yt_cli.assert_called_once()
        args, kwargs = self.client._run_yt_cli.call_args
        self.assertEqual(args[0][-2:], ["--input-format", "yson"])
        self.assertEqual(kwargs["input_data"], raw)
