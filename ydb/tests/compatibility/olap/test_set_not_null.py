import pytest

from ydb.tests.library.common.protobuf_ss import SchemeDescribeRequest
from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture, current_binary_path
from ydb.tests.oss.ydb_sdk_import import ydb


class ColumnTableSetNotNullBase(RestartToAnotherVersionFixture):
    feature_flag = "enable_column_store_set_not_null"
    enable_feature = False

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        if current_binary_path not in self.all_binary_paths:
            pytest.skip("SET NOT NULL on column tables requires the current binary")

        flags = []
        disabled_flags = []
        if self.all_binary_paths[0] == current_binary_path:
            flags.append("enable_set_column_constraint")
            (flags if self.enable_feature else disabled_flags).append(self.feature_flag)
        yield from self.setup_cluster(
            extra_feature_flags=flags,
            disabled_feature_flags=disabled_flags,
            table_service_config={"enable_olap_sink": True},
        )

    def _execute(self, query):
        with ydb.QuerySessionPool(self.driver) as pool:
            return pool.execute_with_retries(query)

    def _change_version(self):
        next_index = (self.current_binary_paths_index + 1) % len(self.all_binary_paths)
        flags = self.config.yaml_config.setdefault("feature_flags", {})
        # The initial binary does not know this feature flag. The persisted schema
        # must remain readable and enforce the constraint without the flag.
        if self.all_binary_paths[next_index] == current_binary_path:
            flags[self.feature_flag] = self.enable_feature
            flags["enable_set_column_constraint"] = True
        else:
            flags.pop(self.feature_flag, None)
            flags.pop("enable_set_column_constraint", None)
        self.change_cluster_version()

    def _upgrade_if_needed(self):
        if self.all_binary_paths[self.current_binary_paths_index] != current_binary_path:
            self._change_version()

    def _create_table(self):
        self._execute("""
            CREATE TABLE `set_not_null` (
                id Uint64 NOT NULL,
                value Int64,
                PRIMARY KEY (id)
            ) WITH (STORE = COLUMN);
        """)

    def _assert_nullable(self, nullable):
        response = self.cluster.client.send(
            SchemeDescribeRequest(f"{self.database_path}/set_not_null").protobuf,
            "SchemeDescribe",
        )
        columns = response.PathDescription.ColumnTableDescription.Schema.Columns
        value_column = next(column for column in columns if column.Name == "value")
        assert value_column.NotNull == (not nullable)

    def _assert_rows(self, expected):
        result = self._execute("SELECT id, value FROM `set_not_null` ORDER BY id;")
        assert [(row["id"], row["value"]) for row in result[0].rows] == expected

    def _assert_null_write_rejected(self):
        column_types = ydb.BulkUpsertColumns()
        column_types.add_column("id", ydb.PrimitiveType.Uint64)
        column_types.add_column("value", ydb.OptionalType(ydb.PrimitiveType.Int64))
        with pytest.raises((ydb.issues.BadRequest, ydb.issues.PreconditionFailed), match="(?i)null"):
            self.driver.table_client.bulk_upsert(
                f"{self.database_path}/set_not_null",
                [{"id": 100, "value": None}],
                column_types,
            )


class TestColumnTableSetNotNullDisabled(ColumnTableSetNotNullBase):
    def test_disabled_constraint_preserves_nullable_data_across_versions(self):
        self._create_table()
        self._execute("UPSERT INTO `set_not_null` (id, value) VALUES (1u, 10), (2u, NULL);")
        expected = [(1, 10), (2, None)]

        for stage in range(2):
            self._assert_nullable(True)
            self._assert_rows(expected)
            if self.all_binary_paths[self.current_binary_paths_index] == current_binary_path:
                with pytest.raises(
                    ydb.issues.GenericError,
                    match="EnableColumnStoreSetNotNull",
                ):
                    self._execute("ALTER TABLE `set_not_null` ALTER COLUMN value SET NOT NULL;")
                self._assert_nullable(True)

            key = stage + 3
            self._execute(f"UPSERT INTO `set_not_null` (id, value) VALUES ({key}u, NULL);")
            expected.append((key, None))
            self._assert_rows(expected)
            if stage == 0:
                self._change_version()


@pytest.mark.skip(
    reason="Requires ColumnShard SET NOT NULL validation protocol; see ydb/core/tx/columnshard/SET_NOT_NULL_DESIGN.md"
)
class TestColumnTableSetNotNull(ColumnTableSetNotNullBase):
    enable_feature = True

    def test_constraint_survives_upgrade_and_downgrade(self):
        self._create_table()
        self._execute("UPSERT INTO `set_not_null` (id, value) VALUES (1u, 10), (2u, 20);")
        self._assert_nullable(True)

        # In the upgrade scenario the old binary writes nullable column data;
        # in the downgrade scenario the current binary installs the constraint.
        self._upgrade_if_needed()
        self._execute("ALTER TABLE `set_not_null` ALTER COLUMN value SET NOT NULL;")
        # A client may retry after losing the response to a completed operation.
        self._execute("ALTER TABLE `set_not_null` ALTER COLUMN value SET NOT NULL;")
        self._assert_nullable(False)
        self._assert_rows([(1, 10), (2, 20)])
        self._assert_null_write_rejected()

        self._change_version()
        self._assert_nullable(False)
        self._assert_rows([(1, 10), (2, 20)])
        self._assert_null_write_rejected()
        self._execute("UPSERT INTO `set_not_null` (id, value) VALUES (3u, 30);")
        self._assert_rows([(1, 10), (2, 20), (3, 30)])

        self._change_version()
        self._assert_nullable(False)
        self._assert_rows([(1, 10), (2, 20), (3, 30)])
        self._assert_null_write_rejected()

    def test_failed_validation_keeps_nullable_schema_across_versions(self):
        self._create_table()
        self._execute("UPSERT INTO `set_not_null` (id, value) VALUES (1u, 10), (2u, NULL);")

        self._upgrade_if_needed()
        with pytest.raises(
            (ydb.issues.BadRequest, ydb.issues.PreconditionFailed, ydb.issues.GenericError, ydb.issues.SchemeError),
            match="(?i)null",
        ):
            self._execute("ALTER TABLE `set_not_null` ALTER COLUMN value SET NOT NULL;")
        self._assert_nullable(True)
        self._assert_rows([(1, 10), (2, None)])

        self._change_version()
        self._assert_nullable(True)
        self._execute("UPSERT INTO `set_not_null` (id, value) VALUES (3u, NULL);")
        self._assert_rows([(1, 10), (2, None), (3, None)])
