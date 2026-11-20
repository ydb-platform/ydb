import random
import threading
import unittest
from unittest.mock import patch

import ydb

from ydb.tests.library.fixtures import json
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
        cls.workload = WorkloadJsonIndex(cls.client, "json_index_select", threading.Event())
        cls.tables = []
        for json_type in ("Json", "JsonDocument"):
            table_path = cls.workload.get_table_path(json_type)
            index_name = "json_idx"
            cls.workload._create_table(table_path, json_type == "JsonDocument")
            cls.client.query(f"""
                INSERT INTO `{table_path}` (pk, json) VALUES
                    (1, {json_type}('{{"common_type":"base","common_version":1}}')),
                    (2, {json_type}('{{"common_type":"compact","common_version":3}}'));
            """, False)
            cls.workload._create_index(index_name, table_path)
            cls.workload._wait_model_index_ready(table_path, index_name)
            cls.tables.append((table_path, index_name))

    def test_predicate_pool_uses_index(self):
        failures = []
        for table_path, index_name in self.tables:
            for mode in ("strict", "lax"):
                generator = json.JsonPredicateGenerator(mode=mode)
                predicates = generator._atoms + generator._and_pairs + generator._or_pairs
                with patch.object(json, "random", random.Random(56476)):
                    predicates += [generator.random_predicate() for _ in range(100)]
                for predicate in predicates:
                    try:
                        self.client.query(f"""
                            SELECT pk FROM `{table_path}` VIEW `{index_name}`
                            WHERE {predicate};
                        """, False, log_error=False)
                    except ydb.issues.GenericError as error:
                        failures.append(f"{table_path}: {predicate}: {error}")
        self.assertFalse(failures, "\n".join(failures))

    def test_full_range_error_propagates(self):
        for table_path, index_name in self.tables:
            with patch.object(json, "get_random_predicate", return_value="JSON_EXISTS(`json`, 'strict $')"):
                with self.assertRaisesRegex(ydb.issues.GenericError, "full-range search"):
                    self.workload._select_rows(table_path, index_name)
