import copy
import unittest

from prepare_timeline import prepare


def capture(node=1):
    args = dict(query="query", task_id="9", actor=f"actor-{node}")
    return dict(dqTrace=dict(query="query", node_id=node, started_unix_us=1000,
                            schema_version=1, completed=True, truncated=False), traceEvents=[
        dict(name="task", ph="i", ts=1000, args=dict(args, stage_id=5, callables={"GraceJoin": 1})),
        dict(name="state", ph="i", ts=1010, args=dict(args, revision=1, state="ready")),
        dict(name="state", ph="i", ts=1020, args=dict(args, revision=2, state="running")),
        dict(name="runner", ph="X", ts=1021, dur=10,
             args=dict(args, channels_before=[dict(channel_id="42", buffered_bytes=None)],
                       progress=dict(cpu_us=6, elapsed_us=10, input_rows=3, output_rows=2,
                                     observed_progress=True))),
        dict(name="state", ph="i", ts=1032, args=dict(args, revision=3, state="blocked", waits=[])),
        dict(name="state", ph="i", ts=1040, args=dict(args, revision=4, state="finished")),
    ])


class PrepareTimelineTest(unittest.TestCase):
    def test_merges_nodes_without_changing_intervals_or_unknown_fields(self):
        first = capture()
        original = copy.deepcopy(first)
        trace, rows = prepare([first, capture(2)])
        self.assertEqual(first, original)
        runs = [event for event in trace["traceEvents"] if event["name"] == "runner"]
        self.assertEqual(len(runs), 2)
        self.assertNotEqual(runs[0]["pid"], runs[1]["pid"])
        self.assertEqual(runs[0]["ts"], 21)
        self.assertEqual(runs[0]["dur"], 10)
        self.assertIsNone(runs[0]["args"]["channels_before"][0]["buffered_bytes"])
        self.assertEqual(rows[0]["input_rows_per_run"], 3)
        waits = {event["name"]: event for event in trace["traceEvents"] if event.get("cat") == "dq.wait"}
        self.assertEqual(waits["queue_wait"]["dur"], 10)
        self.assertEqual(waits["dependency_wait"]["dur"], 8)

    def test_rejects_duplicate_snapshots_instead_of_double_counting_work(self):
        with self.assertRaises(ValueError):
            prepare([capture(), capture()])

    def test_rejects_listing_and_missing_trace(self):
        for value in (dict(queries=[]), dict(error="trace_not_found")):
            with self.assertRaises(ValueError):
                prepare([value])

    def test_preserves_loss_and_completion_markers(self):
        value = capture()
        value["dqTrace"].update(truncated=True, dropped_events=20, completed=False)
        trace, _ = prepare([value])
        self.assertEqual(trace["dqCaptures"][0]["dropped_events"], 20)
        self.assertFalse(trace["dqCaptures"][0]["completed"])


if __name__ == "__main__":
    unittest.main()
