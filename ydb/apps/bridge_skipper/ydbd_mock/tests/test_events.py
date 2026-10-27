import json
import os
import tempfile
import unittest

from ydb.apps.bridge_skipper.ydbd_mock.lib.events import EventLog


def read_lines(path):
    with open(path) as f:
        return f.read().splitlines()


class EventLogTest(unittest.TestCase):
    def make(self, **kw):
        self.now = 100.0

        def clock():
            self.now += 1.0
            return self.now

        return EventLog("pile1", clock=clock, **kw)

    def test_common_fields_and_numbering(self):
        log = self.make()
        a = log.append("start", generation=1)
        b = log.append("commit", cause="admin")
        self.assertEqual((a["seq"], a["ts"], a["pile"], a["kind"], a["generation"]), (1, 101.0, "pile1", "start", 1))
        self.assertEqual((b["seq"], b["ts"]), (2, 102.0))
        self.assertEqual(log.last_seq, 2)
        self.assertEqual(log.kinds(), ["start", "commit"])

    def test_a_field_never_replaces_a_common_field(self):
        log = self.make()
        e = log.append("scenario", pile="pile2", seq=99, ts=1, kind_of="x")
        self.assertEqual((e["pile"], e["seq"], e["ts"]), ("pile1", 1, 101.0))
        self.assertEqual((e["field_pile"], e["field_seq"], e["field_ts"], e["kind_of"]), ("pile2", 99, 1, "x"))

    def test_query_filters(self):
        log = self.make()
        log.append("rpc.start", method="UpdateClusterState")
        log.append("rpc.end", method="UpdateClusterState")
        log.append("commit", cause="admin")
        log.append("rpc.start", method="GetClusterState")
        self.assertEqual([e["seq"] for e in log.query(kind="rpc")], [1, 2, 4])
        self.assertEqual([e["seq"] for e in log.query(kind="rpc.start")], [1, 4])
        self.assertEqual([e["seq"] for e in log.query(grep="UpdateClusterState")], [1, 2])
        self.assertEqual([e["seq"] for e in log.query(since=2)], [3, 4])
        self.assertEqual([e["seq"] for e in log.query(after_ts=102.0)], [3, 4])
        self.assertEqual([e["seq"] for e in log.query(kind="rpc", limit=2)], [1, 2])
        self.assertEqual(log.query(kind="nothing"), [])

    def test_capacity_drops_the_oldest(self):
        log = self.make(capacity=3)
        for i in range(5):
            log.append("x", i=i)
        self.assertEqual([e["i"] for e in log.query()], [2, 3, 4])
        self.assertEqual(log.last_seq, 5)

    def test_sink_gets_one_json_line_per_event_and_survives_reopen(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "events.jsonl")
            log = self.make(sink_path=path)
            log.append("start", generation=1)
            log.append("odd", value=object)
            log.close()
            log.append("after-close")  # must not raise
            lines = read_lines(path)
            self.assertEqual([json.loads(x)["kind"] for x in lines], ["start", "odd"])
            second = self.make(sink_path=path)
            second.append("start", generation=2)
            second.close()
            self.assertEqual(len(read_lines(path)), 3, "a restart appends")
            seqs = [json.loads(x)["seq"] for x in read_lines(path)]
            self.assertEqual(seqs, [1, 2, 3], "seq keeps growing across a restart")
            seqs = [json.loads(x)["seq"] for x in read_lines(path)]
            self.assertEqual(seqs, [1, 2, 3], "seq keeps growing across a restart")


if __name__ == "__main__":
    unittest.main()
