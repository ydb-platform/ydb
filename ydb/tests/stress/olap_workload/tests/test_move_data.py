# -*- coding: utf-8 -*-
import os

import pytest
import yatest
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.fixtures import ydb_database_ctx
from ydb.tests.library.stress.fixtures import StressFixture


class TestOlapWorkloadMoveData(StressFixture):
    """Run the olap workload against a tenant so the MoveData leg actually engages.

    The pool shrink that triggers a decommission is a CMS operation on a database,
    and a domain path such as /Root reports no storage units at all — the workload
    detects that and disables itself. A tenant with two units gives it something to
    remove, so this test covers what test_workload.py cannot.
    """

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            erasure=Erasure.NONE,
            extra_feature_flags={
                "enable_move_column_table": True,
                "enable_columnshard_move_data": True,
            },
            column_shard_config={
                "generate_internal_path_id": True,
            },
        )

    def test_move_data(self):
        # Two units: removing one has to leave a unit behind.
        with ydb_database_ctx(
            self.cluster, "/Root/olap_move_data", node_count=1, storage_pools={"hdd": 2}
        ) as database_path:
            yatest.common.execute(
                [
                    yatest.common.binary_path(os.environ["YDB_WORKLOAD_PATH"]),
                    "--endpoint", self.endpoint,
                    "--database", database_path,
                    "--duration", self.base_duration,
                ]
            )
