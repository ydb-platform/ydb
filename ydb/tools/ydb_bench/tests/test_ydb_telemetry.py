import json
import gzip
import io
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from ydb.tools.ydb_bench.lib import web, ydb_telemetry, distributed_artifacts
from ydb.tools.ydb_bench.lib.common import BenchmarkError
from ydb.tools.ydb_bench.lib.results import SCHEMA_VERSION


class YdbTelemetryTest(unittest.TestCase):
    def test_archive_delta_roundtrip(self):
        def sensor(value):
            return {"kind": "RATE", "labels": {"sensor": "a"}, "value": value}

        meta = {"host": "host", "role": "static", "index": 1, "port": 1234, "context": {"attempt": 1}}
        payloads = [
            [sensor(0), sensor(7), {"kind": "HIST", "hist": {"buckets": [0, 3]}}],
            [sensor(0), sensor(7), {"kind": "HIST", "hist": {"buckets": [0, 3]}}],
            [sensor(2), sensor(0)],
            [],
            [sensor(0)],
            [sensor(-1.5)],
            [sensor(0.0)],
        ]
        records = [dict(meta, timestamp_unix=i, counters={"sensors": s, "extra": i}) for i, s in enumerate(payloads)]
        records.insert(2, dict(meta, timestamp_unix=1.5, error="offline"))
        records.insert(1, dict(records[0], role="dynamic"))
        encoder = ydb_telemetry.CountersArchiveEncoder()
        encoded = [encoder.encode(r) for r in records]
        self.assertEqual(encoded[2]["changes"], [])
        self.assertNotIn("present", encoded[2])
        self.assertNotIn("counters", encoded[3])
        self.assertTrue(any(value == {"value": 0} for _, value in encoded[4]["changes"]))
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "part.gz"
            path.write_bytes(gzip.compress("".join(json.dumps(r) + "\n" for r in encoded).encode()))
            self.assertEqual(list(ydb_telemetry.read_counters_archive(path)), records)
            encoded.pop(2)
            path.write_bytes(gzip.compress("".join(json.dumps(r) + "\n" for r in encoded).encode()))
            with self.assertRaisesRegex(ValueError, "missing or reordered"):
                list(ydb_telemetry.read_counters_archive(path))
            path.write_bytes(gzip.compress((json.dumps(records[0]) + "\n").encode()))
            self.assertEqual(list(ydb_telemetry.read_counters_archive(path)), records[:1])

    def test_full_archives_preserve_all_sensors_and_rotate(self):
        payload = {
            "sensors": [
                {
                    "labels": {"counters": "tablets", "sensor": "Histogram", "tablet": "42"},
                    "hist": {"bounds": [1, 10], "buckets": [2, 3], "inf": 4},
                },
                {"labels": {"counters": "private", "sensor": "Gauge"}, "value": -7},
            ]
        }
        with tempfile.TemporaryDirectory() as directory:
            archive = ydb_telemetry.YdbCountersArchive(
                Path(directory),
                lambda: [("static", 1, 1234), ("dynamic", 2, 1235)],
                {"attempt": 3, "repetition": 2},
                interval=5,
            )
            with mock.patch.object(
                archive, "_fetch_payload", return_value=json.dumps(payload).encode()
            ) as fetch, mock.patch.object(archive._stop, "wait", return_value=True) as wait, mock.patch.object(
                ydb_telemetry, "ARCHIVE_PART_BYTES", 1
            ):
                archive._run()
            self.assertIsNone(archive.error)
            files = sorted(Path(directory).glob("*.jsonl.gz"))
            self.assertEqual(len(files), 2)
            records = [next(ydb_telemetry.read_counters_archive(path)) for path in files]
            self.assertEqual([row["role"] for row in records], ["static", "dynamic"])
            self.assertTrue(all(row["counters"] == payload for row in records))
            self.assertEqual(records[0]["context"], {"attempt": 3, "repetition": 2})
            self.assertEqual(fetch.call_args_list[0].args[1], "/counters/json?@private=1")
            self.assertLessEqual(wait.call_args.args[0], 5)
            self.assertEqual(list(Path(directory).glob("*.jsonl")), [])

    def test_archive_checkpoints_after_rotation(self):
        payload = {"sensors": [{"labels": {"sensor": "x"}, "value": 9}]}
        for limit in ("ARCHIVE_PART_BYTES", "ARCHIVE_STATE_BYTES"):
            with self.subTest(limit=limit), tempfile.TemporaryDirectory() as directory:
                archive = ydb_telemetry.YdbCountersArchive(Path(directory), lambda: [("static", 1, 1234)], {})
                with mock.patch.object(
                    archive, "_fetch_payload", return_value=json.dumps(payload).encode()
                ), mock.patch.object(archive._stop, "wait", side_effect=[False, True]), mock.patch.object(
                    ydb_telemetry, limit, 1
                ):
                    archive._run()
                self.assertIsNone(archive.error)
                files = sorted(Path(directory).glob("*.gz"))
                self.assertEqual(len(files), 2)
                for path in files:
                    self.assertEqual(next(ydb_telemetry.read_counters_archive(path))["counters"], payload)

    def test_archive_errors_and_response_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = ydb_telemetry.YdbCountersArchive(Path(directory), lambda: [("static", 1, 1234)], {})
            with mock.patch.object(archive, "_fetch_payload", side_effect=OSError("offline")), mock.patch.object(
                archive._stop, "wait", return_value=True
            ):
                archive._run()
            record = json.loads(gzip.decompress(next(Path(directory).glob("*.gz")).read_bytes()))
            self.assertEqual(record["error"], "offline")
            self.assertNotIn("counters", record)
            with mock.patch.object(archive._opener, "open", return_value=io.BytesIO(b"12345")):
                with self.assertRaisesRegex(ValueError, "too large"):
                    archive._fetch_payload(1234, "/counters/json", 4)

    def test_archive_transfer_budget_does_not_change_regular_artifacts(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "telemetry" / "sample" / "ydb-counters"
            source.mkdir(parents=True)
            data = gzip.compress(b'{"counters":{}}\n')
            (source / "part.jsonl.gz").write_bytes(data)
            with mock.patch.object(distributed_artifacts, "MAX_RESULT_BYTES", 1):
                with self.assertRaises(BenchmarkError):
                    distributed_artifacts.snapshot_results(root, source.parent)
                artifacts = distributed_artifacts.snapshot_results(root, source.parent, telemetry=True)
                import base64

                def read(operation, request):
                    return {"data": base64.b64encode(data[request["offset"] :]).decode()}

                distributed_artifacts.copy_results(
                    read, {}, "job", artifacts, "telemetry/sample", root / "copy", telemetry=True
                )
            self.assertEqual((root / "copy" / "ydb-counters" / "part.jsonl.gz").read_bytes(), data)

    def test_parse_exact_executor_counters(self):
        sensors = [
            {"labels": {"execpool": "User", "sensor": "CurrentThreadCountPercent"}, "value": 250},
            {"labels": {"execpool": "User", "sensor": "CpuMicrosec"}, "value": 12000000},
            {"labels": {"sensor": "CpuMicrosec"}, "value": 1},
            {"labels": {"execpool": "User", "sensor": "Other"}, "value": 7},
        ]
        self.assertEqual(
            ydb_telemetry.parse_counters({"sensors": sensors}),
            {
                "User": {"CurrentThreadCountPercent": 250, "CpuMicrosec": 12000000},
            },
        )
        with self.assertRaisesRegex(ValueError, "ambiguous"):
            ydb_telemetry.parse_counters({"sensors": sensors + sensors[:1]})
        with self.assertRaises(ValueError):
            ydb_telemetry.parse_counters({})

    def test_invalid_numbers_are_ignored(self):
        for value in (True, -1, float("inf"), float("nan"), "12", 10**500):
            with self.subTest(value=str(value)[:30]):
                self.assertEqual(
                    ydb_telemetry.parse_counters(
                        {
                            "sensors": [
                                {
                                    "labels": {"execpool": "User", "sensor": "CpuMicrosec"},
                                    "value": value,
                                }
                            ]
                        }
                    ),
                    {},
                )

    def test_rates_reset_after_errors_and_counter_restart(self):
        monitor = ydb_telemetry.YdbCountersMonitor(None, lambda: [("dynamic", 1, 31000)], {"attempt": 1})
        values = [{"User": {"CpuMicrosec": value}} for value in (100, 500, 10, 30, 60)]
        values.insert(3, OSError("offline"))
        with mock.patch.object(monitor, "_fetch", side_effect=values), mock.patch.object(
            ydb_telemetry.time, "monotonic", side_effect=(1, 3, 5, 9, 11)
        ):
            nodes = [monitor._sample()["nodes"][0] for _ in values]
        self.assertEqual(nodes[0]["rates"], {})
        self.assertEqual(nodes[1]["rates"]["User"]["CpuMicrosec"], 200)
        self.assertNotIn("CpuMicrosec", nodes[2]["rates"]["User"])
        self.assertEqual(nodes[3]["error"], "offline")
        self.assertEqual(nodes[4]["rates"], {})
        self.assertEqual(nodes[5]["rates"]["User"]["CpuMicrosec"], 15)

    @staticmethod
    def sample(attempt=1, repetition=1):
        return {
            "timestamp_unix": 100,
            "context": {"attempt": attempt, "repetition": repetition},
            "nodes": [
                {
                    "role": "dynamic",
                    "index": 1,
                    "timestamp_unix": 100,
                    "pools": {"User": {"CurrentThreadCountPercent": 250, "CpuMicrosec": 100}},
                    "rates": {},
                }
            ],
        }

    def test_reader_selects_early_attempt_and_verification(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.jsonl"
            samples = [self.sample(1)] + [self.sample(2)] * 400
            verification = self.sample()
            verification["context"]["verification"] = True
            samples.append(verification)
            path.write_text("".join(json.dumps(value) + "\n" for value in samples) + '{"partial":')
            self.assertEqual(len(ydb_telemetry.read_metrics(path, "1")["samples"]), 1)
            self.assertEqual(len(ydb_telemetry.read_metrics(path, "verification")["samples"]), 1)
            limited = ydb_telemetry.read_metrics(path, "2")
            self.assertEqual(len(limited["samples"]), 300)
            self.assertTrue(limited["truncated"])

    def test_reader_sanitizes_imported_values_and_reports_errors(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.jsonl"
            sample = self.sample()
            sample["nodes"][0]["pools"]["User"]["CpuMicrosec"] = float("nan")
            sample["nodes"].append("bad node")
            path.write_text("invalid json\n" + json.dumps(sample) + "\n")
            value = ydb_telemetry.read_metrics(path, 1)
            self.assertEqual(value["invalid_records"], 1)
            self.assertIsNone(value["samples"][0]["nodes"][0]["pools"]["User"]["CpuMicrosec"])
            json.dumps(value, allow_nan=False)

    def test_collection_errors_are_best_effort(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.jsonl"
            monitor = ydb_telemetry.YdbCountersMonitor(path, lambda: None, {})
            monitor._run()
            self.assertIn("node list", monitor.error)

    def test_storage_limit_is_not_repeated_for_later_attempts(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "metrics.jsonl"
            with mock.patch.object(ydb_telemetry, "MAX_FILE_BYTES", 1200):
                for _ in range(3):
                    monitor = ydb_telemetry.YdbCountersMonitor(path, lambda: [], {})
                    with mock.patch.object(monitor, "_sample", return_value=self.sample()):
                        monitor._run()
            self.assertEqual(len(path.read_text().splitlines()), 1)
            self.assertTrue(ydb_telemetry.read_metrics(path, 1)["truncated"])

    def test_service_scopes_metrics_and_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / "run-1"
            profile = run / "local-ydb" / "profile"
            profile.mkdir(parents=True)
            (run / "run.json").write_text(
                json.dumps(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "state": "passed",
                        "runs": [
                            {
                                "benchmark": "local-ydb",
                                "profile": "profile",
                                "manifest": "local-ydb/profile/run.json",
                            }
                        ],
                    }
                )
            )
            (profile / "run.json").write_text(
                json.dumps(
                    {
                        "schema_version": SCHEMA_VERSION,
                        "benchmark": "local-ydb",
                        "state": "passed",
                    }
                )
            )
            path = profile / "ydb-metrics.jsonl"
            path.write_text(json.dumps(self.sample()) + "\n")
            service = web.RunService(root)
            try:
                value = service.local_ydb_metrics("run-1", "profile", "1")
                self.assertEqual(len(value["samples"]), 1)
                self.assertIn("/artifact/", value["artifact"])
                for attempt in ("0", "../1", "", "-2"):
                    with self.assertRaises(BenchmarkError):
                        service.local_ydb_metrics("run-1", "profile", attempt)
                with self.assertRaises(BenchmarkError):
                    service.local_ydb_metrics("run-1", "other", "1")
                path.unlink()
                path.symlink_to(root / "outside")
                with self.assertRaisesRegex(BenchmarkError, "escape"):
                    service.local_ydb_metrics("run-1", "profile", "1")
            finally:
                service.shutdown()

    @unittest.skipUnless(shutil.which("node"), "node is required for chart behavior checks")
    def test_attempt_charts_keep_repetitions_separate_and_convert_thread_units(self):
        script = web._JS[
            web._JS.index("function localCounterCharts(") : web._JS.index("async function renderLocalYdbAttempt(")
        ]
        first = self.sample()
        first["nodes"][0]["pools"]["User"]["CurrentThreadCountPercent"] = 253
        first["nodes"][0]["pools"]["System"] = {
            "CurrentThreadCountPercent": 175,
            "CpuMicrosec": 500,
            "ElapsedMicrosec": 600,
        }
        first["nodes"][0]["rates"] = {"System": {"CpuMicrosec": 25, "ElapsedMicrosec": 30}}
        second = self.sample(repetition=2)
        second["nodes"][0]["pools"]["User"]["CurrentThreadCountPercent"] = 900
        script += "const samples=" + json.dumps([first, second]) + ";"
        script += """
        const result=localCounterCharts(samples,1,'dynamic 1',true);
        const row=result.series.Current.find(item=>item.label==='User').rows.get('0');
        if(row.Current!==2.53||result.xValues.length!==1)throw Error('wrong samples');
        for(const name of ['Current','Default','Max','PossibleMax','PotentialMax','ElapsedMicrosec','CpuMicrosec']){
          if(result.series[name].length!==2)throw Error('missing pool');
        }
        if(result.series.Current[0].rows.get('0').Current!==1.75)throw Error('wrong System threads');
        if(result.series.CpuMicrosec[1].rows.get('0').CpuMicrosec!==100)throw Error('wrong raw counter');
        if(result.series.CpuMicrosec[0].rows.get('0').CpuMicrosec!==500)throw Error('wrong System counter');
        const rate=localCounterCharts(samples,1,'dynamic 1',false);
        if(rate.series.CpuMicrosec[1].rows.get('0').CpuMicrosec!==null)throw Error('raw treated as rate');
        if(rate.series.CpuMicrosec[0].rows.get('0').CpuMicrosec!==25)throw Error('wrong System rate');
        if(rate.series.ElapsedMicrosec[0].rows.get('0').ElapsedMicrosec!==30)throw Error('wrong elapsed rate');
        """
        subprocess.run([shutil.which("node"), "-e", script], check=True, capture_output=True, text=True)
