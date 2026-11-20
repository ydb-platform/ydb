import threading
import unittest
from unittest.mock import patch

import ydb

from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.stress.common.common import YdbClient
from ydb.tests.stress.oltp_workload.workload.type.json_index import WorkloadJsonIndex


class TestJsonIndexSelect(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cluster = KiKiMR(KikimrConfigGenerator(
            nodes=1,
            extra_feature_flags={"enable_json_index": True},
        ))
        cls.addClassCleanup(cls.cluster.stop)
        cls.cluster.start()
        cls.client = YdbClient(f"grpc://localhost:{cls.cluster.nodes[1].port}", "/Root", True)
        cls.addClassCleanup(cls.client.close)
        cls.client.wait_connection()

    def test_select_rows(self):
        workload = WorkloadJsonIndex(self.client, "json_index_select", threading.Event())
        predicates = (
            ("JSON_EXISTS(`json`, 'strict $')", 2),
            ("JSON_EXISTS(`json`, 'strict $.*')", 2),
            ("JSON_EXISTS(`json`, 'strict $.common_version ? (@ > 0 && @ < 6)') "
             "OR JSON_EXISTS(`json`, 'strict $.*.size() ? (@ >= 0)')", 2),
            ("JSON_EXISTS(`json`, 'strict $.common_type ? (@ == \"base\")')", 1),
        )
        for json_type in ("Json", "JsonDocument"):
            table_path = workload.get_table_path(json_type)
            index_name = "json_idx"
            workload._create_table(table_path, json_type == "JsonDocument")
            self.client.query(f"""
                INSERT INTO `{table_path}` (pk, json) VALUES
                    (1, {json_type}('{{"common_type":"base","common_version":1}}')),
                    (2, {json_type}('{{"common_type":"compact","common_version":3}}'));
            """, False)
            workload._create_index(index_name, table_path)
            workload._wait_model_index_ready(table_path, index_name)

            for predicate, expected in predicates:
                with self.subTest(json_type=json_type, predicate=predicate):
                    with patch(
                        "ydb.tests.stress.oltp_workload.workload.type.json_index.json.get_random_predicate",
                        return_value=predicate,
                    ):
                        self.assertEqual(workload._select_rows(table_path, index_name), expected)

            with self.subTest(json_type=json_type, predicate="pk > 0"):
                with patch(
                    "ydb.tests.stress.oltp_workload.workload.type.json_index.json.get_random_predicate",
                    return_value="pk > 0",
                ):
                    with self.assertRaises(ydb.issues.GenericError):
                        workload._select_rows(table_path, index_name)
