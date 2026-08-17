# -*- coding: utf-8 -*-
import os
import pytest
import yatest
from ydb.tests.library.common.types import Erasure

from ydb.tests.library.stress.fixtures import StressFixture


class TestYdbWorkload(StressFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.NONE,
            extra_feature_flags={
                "enable_move_column_table": True,
                "enable_columnshard_bool": True,
                "enable_cs_dictionary_encoding": True,
                "enable_cut_history": True,
                "enable_columnshard_interval": True,
                "enable_columnshard_uuid": True,
                "enable_columnshard_dy_number": True,
            },
            column_shard_config={
                "allow_nullable_columns_in_pk": True,
                "generate_internal_path_id": True,
                "cut_history_enabled": True,
            },
            # The deny list is Hive's, not ColumnShard's, and ColumnShard is on it by
            # default — which would disable the cutter for the tablets under test.
            hive_config={
                "cut_history_deny_list": "KeyValue,PersQueue,BlobDepot",
            },
        )

    def test(self):
        yatest.common.execute([
            yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
            "--endpoint", self.endpoint,
            "--database", self.database,
            "--duration", self.base_duration,
        ])
