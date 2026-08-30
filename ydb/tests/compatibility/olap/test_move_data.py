import logging
import time

import pytest
from google.protobuf import text_format

import ydb.public.api.protos.ydb_cms_pb2 as cms_tenants_pb
from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.protobuf_console import AlterTenantRequest, GetTenantStatusRequest
from ydb.tests.library.compatibility.fixtures import RollingUpgradeAndDowngradeFixture
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


class TestMoveDataDormant(RollingUpgradeAndDowngradeFixture):
    """NEW code rolled against OLD config. EnableColumnshardMoveData is deliberately
    absent — naming it in static YAML would fail config parsing on every pre-26.4
    binary (unknown proto field), so old-code+new-config is impossible by
    construction for text config and the safe rollout is: roll the binary first,
    then enable the flag through CMS. This test covers the first half: a
    mixed-version cluster with the MoveData machinery compiled in but dormant.
    Runs on any version matrix.
    """

    rows_count = 100

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster(
            tenant_db="move_data_dormant",
            column_shard_config={
                "alter_object_enabled": True,
            },
        )

    def test_dormant_roll(self):
        table_name = "olap_move_data_dormant"
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{table_name}` (
                    ts Timestamp NOT NULL,
                    id Uint64 NOT NULL,
                    payload Utf8,
                    PRIMARY KEY (ts, id)
                )
                PARTITION BY HASH(ts, id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4)
                """
            )
            values = ",".join(
                f'(Timestamp("2024-01-01T00:{i // 60 % 60:02d}:{i % 60:02d}.000000Z"), {i}, "p{i}")'
                for i in range(self.rows_count)
            )
            session_pool.execute_with_retries(f"INSERT INTO `{table_name}` (ts, id, payload) VALUES {values};")

        def assert_readable():
            with ydb.QuerySessionPool(self.driver) as session_pool:
                result = session_pool.execute_with_retries(
                    f"SELECT COUNT(*) AS cnt FROM `{table_name}`;",
                    retry_settings=ydb.RetrySettings(idempotent=True),
                )
                assert result[0].rows[0]["cnt"] == self.rows_count

        for _ in self.roll():
            assert_readable()
        assert_readable()


class TestMoveData(RollingUpgradeAndDowngradeFixture):
    """Roll the cluster while a group decommission (MoveData) is in flight.

    Removing storage units from the tenant is what makes Hive ask ColumnShard to
    move its blobs. The move then has to survive every binary version the roll
    walks through: data stays readable and complete at each step, and the pool
    converges to the expected size afterwards.
    """

    rows_count = 200
    converge_timeout = 300

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if min(self.versions) < (26, 4):
            pytest.skip("MoveData is available starting from 26.4")

        yield from self.setup_cluster(
            tenant_db="move_data",
            extra_feature_flags=["enable_columnshard_move_data"],
            column_shard_config={
                "alter_object_enabled": True,
            },
        )

    # ---- CMS helpers: the pool size is the only MoveData trigger available ----

    def _console_client(self):
        return kikimr_client_factory("localhost", self.cluster.nodes[1].port)

    def _storage_units(self):
        request = GetTenantStatusRequest(self.database_path)
        response = self._console_client().console_request(text_format.MessageToString(request.protobuf))
        result = cms_tenants_pb.GetDatabaseStatusResult()
        response.GetTenantStatusResponse.Response.operation.result.Unpack(result)
        return result.required_resources.storage_units[0]

    def _alter_units(self, unit_kind, delta):
        request = AlterTenantRequest(self.database_path)
        if delta < 0:
            request.add_storage_groups_to_remove(unit_kind, -delta)
        else:
            request.add_storage_groups_to_add(unit_kind, delta)
        self._console_client().console_request(text_format.MessageToString(request.protobuf))

    def _wait_units(self, expected):
        deadline = time.time() + self.converge_timeout
        while time.time() < deadline:
            if self._storage_units().count == expected:
                return True
            time.sleep(2)
        return False


    def _create_table(self, table_name):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f"""
                CREATE TABLE `{table_name}` (
                    ts Timestamp NOT NULL,
                    id Uint64 NOT NULL,
                    payload Utf8,
                    PRIMARY KEY (ts, id)
                )
                PARTITION BY HASH(ts, id)
                WITH (STORE = COLUMN, PARTITION_COUNT = 4)
                """
            )

    def _write_data(self, table_name):
        values = []
        for i in range(self.rows_count):
            ts = f"2024-01-01T00:{i // 60:02d}:{i % 60:02d}.000000Z"
            values.append(f'(Timestamp("{ts}"), {i}, "payload_{i}")')
        with ydb.QuerySessionPool(self.driver) as session_pool:
            session_pool.execute_with_retries(
                f'INSERT INTO `{table_name}` (ts, id, payload) VALUES {",".join(values)};'
            )

    def _assert_readable(self, table_name, expected_rows):
        with ydb.QuerySessionPool(self.driver) as session_pool:
            result = session_pool.execute_with_retries(
                f"SELECT COUNT(*) AS cnt FROM `{table_name}`;",
                retry_settings=ydb.RetrySettings(idempotent=True),
            )
            assert result[0].rows[0]["cnt"] == expected_rows, (
                f"`{table_name}` returned {result[0].rows[0]['cnt']} rows, expected {expected_rows}"
            )
            # Reading a real column proves the portions themselves are readable, not
            # just that the shard answers a metadata-only count.
            result = session_pool.execute_with_retries(
                f"SELECT payload FROM `{table_name}` ORDER BY ts, id LIMIT 1;",
                retry_settings=ydb.RetrySettings(idempotent=True),
            )
            assert result[0].rows[0]["payload"] == "payload_0"

    def test_move_data_during_roll(self):
        table_name = "olap_move_data"
        self._create_table(table_name)
        self._write_data(table_name)
        self._assert_readable(table_name, self.rows_count)

        units = self._storage_units()
        unit_kind, initial_count = units.unit_kind, units.count
        logger.info("tenant pool: kind=%s count=%s", unit_kind, initial_count)

        # A removal has to leave a unit behind, so grow first when the pool is minimal.
        if initial_count < 2:
            self._alter_units(unit_kind, 2 - initial_count)
            assert self._wait_units(2), "pool did not grow to 2 units before the test"
            initial_count = 2

        # Start the decommission and deliberately do NOT wait for it: the roll below
        # should happen while portions are still being rewritten.
        self._alter_units(unit_kind, -1)

        for _ in self.roll():
            self._assert_readable(table_name, self.rows_count)

        assert self._wait_units(initial_count - 1), (
            f"pool did not converge to {initial_count - 1} units after the roll"
        )

        self._alter_units(unit_kind, 1)
        assert self._wait_units(initial_count), "pool did not return to its initial size"
        self._assert_readable(table_name, self.rows_count)
