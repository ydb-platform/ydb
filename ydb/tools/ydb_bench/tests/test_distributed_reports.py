import json
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest

from ydb.tools.ydb_bench.lib import web
from ydb.tools.ydb_bench.lib.common import BenchmarkError, atomic_write_json, atomic_write_text
from ydb.tools.ydb_bench.lib.distributed_reports import attempt_counters


class DistributedReportsTest(unittest.TestCase):
    def setUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.root = Path(directory.name)

    def counters(self, root, context, host, timestamp):
        relative = "hosts/" + host
        record = {
            "timestamp_unix": timestamp,
            "context": context,
            "nodes": [{"role": "static", "index": 1, "pools": {"System": {"CurrentThreadCountPercent": 800}}}],
        }
        atomic_write_text(root / relative / "ydb-metrics.jsonl", json.dumps(record) + "\n")
        return relative

    def test_attempt_counters_keep_host_identity_and_clock_domains(self):
        directory = self.root / "dynamic-nodes-01/load-00000002/repeat-001"
        context = {"attempt": 7, "repetition": 1}
        hosts = {
            host: self.counters(directory, context, host, timestamp) for host, timestamp in (("a", 100), ("b", 200))
        }
        atomic_write_json(directory / "host-metrics.json", {"artifact_directories": hosts})
        profile = {"attempts": [{"attempt": 7, "dynamic_nodes": 1, "load": 2}]}
        value = attempt_counters(self.root, profile, "7")
        self.assertEqual(["a", "b"], [item["host_id"] for item in value["samples"]])
        self.assertEqual([100, 200], [item["timestamp_unix"] for item in value["samples"]])
        self.assertEqual([1, 1], [item["nodes"][0]["index"] for item in value["samples"]])
        self.assertEqual(2, len(value["artifacts"]))
        self.assertFalse(value["truncated"])
        self.assertEqual([], attempt_counters(self.root, profile, "8")["samples"])

    def test_verification_does_not_read_rejected_verification(self):
        for name in ("verification", "verification-rejected-001"):
            directory = self.root / name / "repeat-001"
            relative = self.counters(directory, {"verification": True, "repetition": 1}, "a", 100)
            atomic_write_json(directory / "host-metrics.json", {"artifact_directories": {"a": relative}})
        value = attempt_counters(self.root, {}, "verification")
        self.assertEqual(1, len(value["samples"]))
        self.assertEqual(1, len(value["artifacts"]))

    def test_bounded_counter_history_retains_both_hosts(self):
        directory = self.root / "verification/repeat-001"
        hosts = {}
        for host in ("a", "b"):
            relative = self.counters(directory, {"verification": True, "repetition": 1}, host, 100)
            path = directory / relative / "ydb-metrics.jsonl"
            path.write_text(path.read_text() * 300)
            hosts[host] = relative
        atomic_write_json(directory / "host-metrics.json", {"artifact_directories": hosts})
        value = attempt_counters(self.root, {}, "verification")
        self.assertTrue(value["truncated"])
        self.assertEqual(300, len(value["samples"]))
        self.assertEqual({"a", "b"}, {item["host_id"] for item in value["samples"]})

    def test_malformed_counter_index_is_a_report_error(self):
        directory = self.root / "verification/repeat-001"
        for value in (None, [], "invalid", {"artifact_directories": []}):
            atomic_write_json(directory / "host-metrics.json", value)
            with self.subTest(value=value), self.assertRaises(BenchmarkError):
                attempt_counters(self.root, {}, "verification")

    def test_counter_index_cannot_escape_attempt_directory(self):
        directory = self.root / "verification/repeat-001"
        atomic_write_json(directory / "host-metrics.json", {"artifact_directories": {"a": "../../outside"}})
        with self.assertRaisesRegex(BenchmarkError, "path"):
            attempt_counters(self.root, {}, "verification")

    def test_profile_api_separates_same_named_benchmarks(self):
        run = self.root / "run"
        steps, records = [], []
        for benchmark in ("local-ydb", "distributed-ydb"):
            relative = benchmark + "/same"
            steps.append({"id": benchmark, "benchmark": benchmark, "profile": "same", "state": "passed"})
            records.append({"benchmark": benchmark, "profile": "same", "directory": relative, "status": "completed"})
            atomic_write_json(
                run / relative / "run.json",
                {
                    "schema_version": 4,
                    "benchmark": benchmark,
                    "profile": "same",
                    "state": "passed",
                    "status": "completed",
                    "distributed": {"clusters": []} if benchmark == "distributed-ydb" else None,
                },
            )
        atomic_write_json(
            run / "run.json",
            {
                "schema_version": 4,
                "state": "completed",
                "status": "completed",
                "runs": records,
                "steps": steps,
            },
        )
        atomic_write_text(
            run / "events.jsonl",
            "".join(
                json.dumps(
                    {
                        "type": "step-finished",
                        "sequence": index,
                        "step_id": step["id"],
                        "state": "passed",
                    }
                )
                + "\n"
                for index, step in enumerate(steps, 1)
            ),
        )
        service = web.RunService(self.root)
        self.addCleanup(service.shutdown)
        self.assertEqual("local-ydb", service.local_ydb_profile("run", "same")["benchmark"])
        value = service.local_ydb_profile("run", "same", "distributed-ydb")
        self.assertEqual("distributed-ydb", value["benchmark"])
        self.assertEqual({"clusters": []}, value["distributed"])
        self.assertEqual([1], [item["sequence"] for item in service.local_ydb_activity("run", "same")["events"]])
        activity = service.local_ydb_activity("run", "same", benchmark="distributed-ydb")
        self.assertEqual([2], [item["sequence"] for item in activity["events"]])
        with self.assertRaisesRegex(BenchmarkError, "unsupported"):
            service.local_ydb_profile("run", "same", "ping-bench")
        counters = run / "distributed-ydb/same/verification/repeat-001"
        relative = self.counters(counters, {"verification": True, "repetition": 1}, "a", 100)
        atomic_write_json(counters / "host-metrics.json", {"artifact_directories": {"a": relative}})
        value = service.local_ydb_metrics("run", "same", "verification", "distributed-ydb")
        self.assertEqual(1, len(value["samples"]))
        self.assertIn("/distributed-ydb/same/", value["artifacts"][0]["url"])
        self.assertEqual([], service.local_ydb_metrics("run", "same", "verification")["samples"])
        entries = service.local_ydb_comparison(["run"])["entries"]
        self.assertEqual(
            {("local-ydb", "same"), ("distributed-ydb", "same")},
            {(item["benchmark"], item["profile"]) for item in entries},
        )
        pairs = [["run", "same"], ["run", "same", "distributed-ydb"]]
        saved = service.save_comparison({"name": "Mixed benchmarks", "profiles": pairs, "baseline": pairs[1]})
        self.assertEqual(pairs, saved["profiles"])
        self.assertEqual(pairs[1], service.saved_comparisons()[0]["baseline"])

    @unittest.skipUnless(shutil.which("node"), "node is required for report route tests")
    def test_report_routes_preserve_benchmark_and_nested_profile(self):
        def function(start, end):
            return web._JS[web._JS.index(start) : web._JS.index(end, web._JS.index(start))]

        script = (
            "const enc=encodeURIComponent;"
            + function("function parseLocalYdbProfileSelection", "function profileGroups")
            + function("function localYdbViewHref", "function localYdbViewTabs")
            + function("function localAttemptHref", "function localCounterCharts")
            + """
        const groups={'local-ydb/same':[{}], 'distributed-ydb/nested/same':[{}]};
        const container={dataset:{localYdbRunId:'host:run',localYdbProfile:'nested/same',ydbBenchmark:'distributed-ydb'}};
        process.stdout.write(JSON.stringify({
          selection:parseLocalYdbProfileSelection(groups,'distributed-ydb/nested/same/view/discovery'),
          tab:localYdbViewHref(container,'result'),
          attempt:localAttemptHref('host:run','nested/same',7,'distributed-ydb'),
          legacy:localAttemptHref('run','same',7)
        }));
        """
        )
        value = json.loads(subprocess.check_output(["node", "-e", script], text=True))
        self.assertEqual({"profile": "distributed-ydb/nested/same", "view": "discovery"}, value["selection"])
        self.assertEqual("#run/host%3Arun/profile/distributed-ydb%2Fnested%2Fsame%2Fview%2Fresult", value["tab"])
        self.assertEqual("#distributed-attempt/host%3Arun/nested%2Fsame/7", value["attempt"])
        self.assertEqual("#attempt/run/same/7", value["legacy"])
