import sys
import tempfile
import unittest
import zipfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from transform_build_results import save_zip


class SaveZipTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp_dir.cleanup)
        self.root = Path(self.temp_dir.name)
        self.ya_out = self.root / "out"
        self.suite = "ydb/tests/example"
        self.results = self.ya_out / self.suite / "test-results"
        self.archive_dir = self.root / "archives"
        self.archive_dir.mkdir()

    def write_log(self, relative_path, content="log"):
        path = self.results / relative_path
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
        return path

    def make_archive(self, logs_dirs):
        url = save_zip(
            self.suite,
            str(self.archive_dir),
            "https://example.com/artifacts/",
            {str(path) for path in logs_dirs},
            str(self.ya_out),
        )
        self.assertEqual(url, "https://example.com/artifacts/ydb-tests-example.zip")
        return zipfile.ZipFile(self.archive_dir / "ydb-tests-example.zip")

    def test_merged_or_unsplit_results(self):
        log = self.write_log("py3test/testing_out_stuff/server.log")
        with self.make_archive({log.parent}) as archive:
            self.assertEqual(archive.namelist(), ["py3test/testing_out_stuff/server.log"])
            self.assertEqual(archive.read(archive.namelist()[0]), b"log")

    def test_split_results_keep_file_chunk_type_and_retry_paths(self):
        paths = [
            "py3test/testing_out_stuff/test_a.py/chunk0/testing_out_stuff/server.log",
            "py3test/testing_out_stuff/test_b.py/chunk0/testing_out_stuff/server.log",
            "py3test/testing_out_stuff/test_a.py/chunk1/testing_out_stuff/server.log",
            "unittest/testing_out_stuff/chunk0/testing_out_stuff/server.log",
            "py3test/run1/testing_out_stuff/chunk0/testing_out_stuff/server.log",
            "py3test/run2/testing_out_stuff/chunk0/testing_out_stuff/server.log",
        ]
        logs = [self.write_log(path, str(index)) for index, path in enumerate(paths)]
        with self.make_archive({log.parent for log in logs}) as archive:
            self.assertCountEqual(archive.namelist(), paths)
            for index, path in enumerate(paths):
                self.assertEqual(archive.read(path), str(index).encode())

    def test_overlapping_logs_dirs_do_not_duplicate_files(self):
        log = self.write_log("py3test/testing_out_stuff/chunk0/testing_out_stuff/server.log")
        with self.make_archive({self.results / "py3test/testing_out_stuff", log.parent}) as archive:
            self.assertEqual(len(archive.namelist()), 1)
            self.assertEqual(archive.read(archive.namelist()[0]), b"log")

    def test_coredumps_are_still_excluded(self):
        log = self.write_log("py3test/testing_out_stuff/server.log")
        self.write_log("py3test/testing_out_stuff/server.core")
        self.write_log("py3test/testing_out_stuff/server.DMP")
        with self.make_archive({log.parent}) as archive:
            self.assertEqual(archive.namelist(), ["py3test/testing_out_stuff/server.log"])

    def test_logs_outside_suite_results_are_rejected(self):
        outside = self.ya_out / "another-suite"
        outside.mkdir(parents=True)
        (outside / "server.log").write_text("outside")
        with self.assertRaisesRegex(ValueError, "outside suite results"):
            self.make_archive({outside})


if __name__ == "__main__":
    unittest.main()
