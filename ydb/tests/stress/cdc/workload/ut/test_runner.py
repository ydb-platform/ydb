import unittest
from unittest.mock import Mock, patch

from ydb.tests.stress.cdc.workload import WorkloadRunner


class TestWorkloadRunner(unittest.TestCase):
    def test_default_paths_are_unique(self):
        client = Mock(database="/Root/db/")
        with patch("ydb.tests.stress.cdc.workload.random.randint", return_value=100):
            paths = [WorkloadRunner(client, duration=0).table_path for _ in range(1000)]

        self.assertEqual(len(paths), len(set(paths)))
        self.assertTrue(all(path.startswith("/Root/db/table/") for path in paths))

    def test_explicit_paths(self):
        for database in ("/Root/db", "/Root/db/"):
            for path, expected in (("table/cdc", "/Root/db/table/cdc"),
                                   ("/Root/other/cdc", "/Root/other/cdc")):
                with self.subTest(database=database, path=path):
                    runner = WorkloadRunner(Mock(database=database), duration=0, path=path)
                    self.assertEqual(runner.table_path, expected)
