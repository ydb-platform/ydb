import json
import sys
import tempfile
import threading
import unittest
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).resolve().parent))

import transform_build_results as reports


class ParallelArchivesTest(unittest.TestCase):
    def test_no_archives_does_not_start_workers(self):
        with mock.patch.object(reports, "ThreadPoolExecutor") as executor:
            self.assertEqual(reports.save_zips({}, "/unused", "https://example.com/"), {})
        executor.assert_not_called()

    def test_worker_count_is_bounded_by_cpus_and_suites(self):
        for cpus, suites, workers in ((None, 10, 1), (2, 10, 2), (64, 2, 2), (64, 8, 4)):
            with self.subTest(cpus=cpus, suites=suites):
                jobs = {f"suite/{i}": {f"logs/{i}"} for i in range(suites)}
                with mock.patch.object(reports.os, "cpu_count", return_value=cpus), \
                        mock.patch.object(reports, "save_zip", side_effect=lambda suite, *args: suite), \
                        mock.patch.object(reports, "ThreadPoolExecutor", wraps=ThreadPoolExecutor) as executor:
                    urls = reports.save_zips(jobs, "/unused", "https://example.com/")
                executor.assert_called_once_with(max_workers=workers)
                self.assertEqual(urls, {suite: suite for suite in jobs})

    def test_four_archives_can_be_compressed_concurrently(self):
        barrier = threading.Barrier(4, timeout=10)
        jobs = {f"suite/{i}": {f"logs/{i}"} for i in range(8)}

        def pack(suite, out_dir, url_prefix, logs_dir):
            self.assertEqual(logs_dir, jobs[suite])
            # Would time out if ZIP creation were serialized. Eight jobs exercise
            # two batches with the same four-worker limit, without timing sleeps.
            barrier.wait()
            return url_prefix + suite

        with mock.patch.object(reports.os, "cpu_count", return_value=64), \
                mock.patch.object(reports, "save_zip", side_effect=pack):
            urls = reports.save_zips(jobs, "/unused", "https://example.com/")
        self.assertEqual(urls, {suite: "https://example.com/" + suite for suite in jobs})

    def test_filename_collisions_are_rejected_before_writing(self):
        with mock.patch.object(reports, "save_zip") as pack:
            with self.assertRaisesRegex(ValueError, "same ZIP filename"):
                reports.save_zips({"a/b-c": {"one"}, "a-b/c": {"two"}}, "/unused", "")
        pack.assert_not_called()

    def test_worker_errors_are_propagated(self):
        with mock.patch.object(reports, "save_zip", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                reports.save_zips({"suite/a": {"logs"}, "suite/b": {"logs"}}, "/unused", "")


class ArchiveReportTest(unittest.TestCase):
    def setUp(self):
        temp = tempfile.TemporaryDirectory()
        self.addCleanup(temp.cleanup)
        self.root = Path(temp.name)
        self.out = self.root / "out"
        self.archives = self.root / "archives"
        self.archives.mkdir()
        self.report = self.root / "report.json"
        self.url_prefix = "https://example.com/archives/"

    def result(self, suite, name, status):
        logs = self.out / suite / "test-results/unittest/testing_out_stuff"
        logs.mkdir(parents=True, exist_ok=True)
        log = logs / f"{name}.log"
        log.write_text(name)
        return {
            "type": "test", "path": suite, "name": name, "status": status,
            "links": {"logsdir": [str(logs)], "stdout": [str(log)]},
        }

    def transform(self):
        reports.transform(
            str(self.report), lambda suite, name: suite == "ydb/muted", str(self.out),
            "https://example.com/logs/", str(self.root / "logs"), 1024,
            str(self.archives), self.url_prefix, None,
        )

    def test_zip_contents_links_and_compression_level(self):
        result = self.result("ydb/example suite", "failed", "FAILED")
        log_dir = Path(result["links"]["logsdir"][0])
        (log_dir / "passed.log").write_text("keep passed-test logs too")
        (log_dir / "server.core").write_text("excluded core")
        (log_dir / "server.DMP").write_text("excluded dump")
        (log_dir / "server.backtrace").write_text("keep backtrace")
        with mock.patch.object(reports.zipfile, "ZipFile", wraps=zipfile.ZipFile) as factory:
            url = reports.save_zip(result["path"], str(self.archives), self.url_prefix, {str(log_dir)})
        factory.assert_called_once_with(
            str(self.archives / "ydb-example suite.zip"), mode="w",
            compression=zipfile.ZIP_DEFLATED, compresslevel=6,
        )
        self.assertEqual(url, self.url_prefix + "ydb-example%20suite.zip")
        with zipfile.ZipFile(self.archives / "ydb-example suite.zip") as archive:
            self.assertCountEqual(archive.namelist(), [
                "unittest/failed.log", "unittest/passed.log", "unittest/server.backtrace",
            ])
            self.assertEqual(archive.read("unittest/passed.log"), b"keep passed-test logs too")
            self.assertIsNone(archive.testzip())

    def test_transform_preserves_suite_selection_and_links(self):
        results = [
            self.result("ydb/mixed", "failed", "FAILED"),
            self.result("ydb/mixed", "passed", "OK"),
            self.result("ydb/mixed", "not_launched", "NOT_LAUNCHED"),
            self.result("ydb/muted", "muted", "FAILED"),
            self.result("ydb/healthy", "passed", "OK"),
        ]
        self.report.write_text(json.dumps({"results": results}))
        self.transform()
        results = json.loads(self.report.read_text())["results"]
        self.assertCountEqual([p.name for p in self.archives.iterdir()], ["ydb-mixed.zip", "ydb-muted.zip"])
        mixed_url = [self.url_prefix + "ydb-mixed.zip"]
        for result in results[:3]:
            self.assertEqual(result["links"]["logsdir"], mixed_url)
        self.assertIn("stdout", results[0]["links"])
        self.assertEqual(results[1]["links"], {"logsdir": mixed_url})
        self.assertEqual(results[2]["error_type"], "NOT_LAUNCHED")
        self.assertNotIn("stdout", results[2]["links"])
        self.assertEqual(results[3]["status"], "MUTE")
        self.assertIn("stdout", results[3]["links"])
        self.assertEqual(results[3]["links"]["logsdir"], [self.url_prefix + "ydb-muted.zip"])
        self.assertEqual(results[4]["links"], {})
        with zipfile.ZipFile(self.archives / "ydb-mixed.zip") as archive:
            self.assertEqual(archive.read("unittest/passed.log"), b"passed")

    def test_failed_archiving_does_not_overwrite_report(self):
        self.report.write_text(json.dumps({"results": [self.result("ydb/fail", "test", "FAILED")]}))
        original = self.report.read_bytes()
        with mock.patch.object(reports, "save_zip", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                self.transform()
        self.assertEqual(self.report.read_bytes(), original)


if __name__ == "__main__":
    unittest.main()
