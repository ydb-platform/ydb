import pytest

from ydb.tests.library.compatibility.fixtures import RestartToAnotherVersionFixture
from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.datashard.lib.create_table import create_table_sql_request
from ydb.tests.datashard.lib.types_of_variables import (
    pk_types,
    non_pk_types,
    cleanup_type_name,
    format_sql_value,
    types_not_supported_yet_in_columnshard
)


def kb_to_b(n):
    return n * 1024


def mb_to_b(n):
    return kb_to_b(n) * 1024


class TestResultSetValue(RestartToAnotherVersionFixture):

    @pytest.fixture()
    def store_type(self, request):
        return request.param

    @pytest.fixture()
    def channel_buffer_size(self, request):
        return getattr(request, 'param', mb_to_b(8))

    @pytest.fixture(autouse=True, scope="function")
    def setup(self, store_type, channel_buffer_size):
        self.store_type = store_type
        self.channel_buffer_size = channel_buffer_size

        if min(self.versions) < (25, 3, 2):
            pytest.skip("Result set formats are not supported in <= 25.3.1")

        supported_pk_types = pk_types if store_type == "ROW" else {k: v for k, v in pk_types.items() if k not in types_not_supported_yet_in_columnshard}
        supported_non_pk_types = non_pk_types if store_type == "ROW" else {k: v for k, v in non_pk_types.items() if k not in types_not_supported_yet_in_columnshard}
        self.all_types = {**supported_pk_types, **supported_non_pk_types}

        yield from self.setup_cluster(
            extra_feature_flags={
                "enable_arrow_result_set_format": True,
                "enable_columnshard_bool": True,
            },
            table_service_config={
                "resource_manager": {
                    "channel_buffer_size": channel_buffer_size
                }
            },
            column_shard_config={
                "disabled_on_scheme_shard": False,
            }
        )

    # ------------------------------------- Tests -------------------------------------

    @pytest.mark.parametrize("store_type", ["ROW", "COLUMN"])
    @pytest.mark.parametrize("channel_buffer_size", [kb_to_b(2), mb_to_b(8)])
    @pytest.mark.parametrize("schema_inclusion_mode", [
        ydb.QuerySchemaInclusionMode.UNSPECIFIED,  # UNSPECIFIED is the same as ALWAYS
        ydb.QuerySchemaInclusionMode.ALWAYS,
        ydb.QuerySchemaInclusionMode.FIRST_ONLY,
    ])
    def test_schema_inclusion_mode(self, store_type, schema_inclusion_mode):
        table_name = "test_value"
        rows_count = 500

        self._create_table(table_name, store_type)
        self._fill_table(table_name, rows_count, store_type)

        self._validate_schema_inclusion_mode(
            self._read_table(table_name, schema_inclusion_mode=schema_inclusion_mode),
            rows_count,
            schema_inclusion_mode,
            stmt_cnt=1
        )

        self.change_cluster_version()

        self._validate_schema_inclusion_mode(
            self._read_table(table_name, schema_inclusion_mode=schema_inclusion_mode),
            rows_count,
            schema_inclusion_mode,
            stmt_cnt=1
        )

        self._drop_table(table_name)

    @pytest.mark.parametrize("store_type", ["ROW"])  # without COLUMN because DML
    @pytest.mark.parametrize("channel_buffer_size", [kb_to_b(2)])
    @pytest.mark.parametrize("concurrent_result_sets", [True, False])
    @pytest.mark.parametrize("schema_inclusion_mode", [
        ydb.QuerySchemaInclusionMode.ALWAYS,
        ydb.QuerySchemaInclusionMode.FIRST_ONLY,
    ])
    def test_multistatement(self, store_type, schema_inclusion_mode, concurrent_result_sets):
        table_name = "test_value"
        rows_count = 500

        self._create_table(table_name, store_type)
        self._fill_table(table_name, rows_count, store_type)

        self._validate_schema_inclusion_mode(
            self._multistatement_read_table(
                table_name,
                schema_inclusion_mode=schema_inclusion_mode,
                concurrent_result_sets=concurrent_result_sets
            ),
            rows_count,
            schema_inclusion_mode,
            stmt_cnt=3
        )

        self.change_cluster_version()

        self._validate_schema_inclusion_mode(
            self._multistatement_read_table(
                table_name,
                schema_inclusion_mode=schema_inclusion_mode,
                concurrent_result_sets=concurrent_result_sets
            ),
            rows_count,
            schema_inclusion_mode,
            stmt_cnt=3
        )

        self._drop_table(table_name)

    @pytest.mark.parametrize("store_type", ["ROW"])  # without COLUMN because DML
    def test_empty_result(self, store_type):
        table_name = "test_value"
        rows_count = 500

        self._create_table(table_name, store_type)
        self._fill_table(table_name, rows_count, store_type)

        self._validate_empty_result(table_name)
        self.change_cluster_version()
        self._validate_empty_result(table_name)

        self._drop_table(table_name)

    @pytest.mark.parametrize("store_type", ["ROW", "COLUMN"])
    def test_limit_ordered_columns(self, store_type):
        table_name = "test_value"
        rows_count = 500
        limit = 100

        assert limit < rows_count

        self._create_table(table_name, store_type)
        self._fill_table(table_name, rows_count, store_type)

        self._validate_limit_ordered_columns(self._read_table(table_name, limit=limit, pragmas=["OrderedColumns"], ordered=False), limit)
        self.change_cluster_version()
        self._validate_limit_ordered_columns(self._read_table(table_name, limit=limit, pragmas=["OrderedColumns"], ordered=False), limit)

        self._drop_table(table_name)

    @pytest.mark.parametrize("store_type", ["ROW", "COLUMN"])
    def test_column_alias(self, store_type):
        table_name = "test_value"
        rows_count = 500

        self._create_table(table_name, store_type)
        self._fill_table(table_name, rows_count, store_type)

        self._validate_column_alias(table_name)
        self.change_cluster_version()
        self._validate_column_alias(table_name)

        self._drop_table(table_name)

    # -------------------------- Methods to execute queries ---------------------------

    def _try_execute(
        self,
        query,
        schema_inclusion_mode=None,
        concurrent_result_sets=False,
    ):
        with ydb.QuerySessionPool(self.driver) as pool:
            try:
                return pool.execute_with_retries(
                    query,
                    result_set_format=ydb.QueryResultSetFormat.VALUE,
                    schema_inclusion_mode=schema_inclusion_mode,
                    concurrent_result_sets=concurrent_result_sets
                )
            except Exception as e:
                assert False, f"Failed query `{query}`, error: {e}"

    def _create_table(self, table_name, store_type):
        query = create_table_sql_request(
            table_name,
            columns={"pk_": {"Uint64": None}, "col_": self.all_types.keys()},
            pk_columns={"pk_": {"Uint64": None}},
            index_columns={},
            unique="",
            sync="",
            column_table=store_type == "COLUMN"
        )
        self._try_execute(query)

    def _drop_table(self, table_name):
        query = f"DROP TABLE {table_name};"
        self._try_execute(query)

    def _fill_table(self, table_name, rows_count, store_type, batch_size=100, offset=0):
        type_names = list(self.all_types.keys())

        columns = ["pk_Uint64"]
        for type_name in type_names:
            columns.append(f"col_{cleanup_type_name(type_name)}")

        values = []
        for i in range(rows_count):
            value = [format_sql_value(i + offset, "Uint64", store_type == "COLUMN")]
            for type_name in type_names:
                if i % 4 != 3:
                    value.append(format_sql_value(self.all_types[type_name](i % 128), type_name))
                else:
                    value.append('NULL')
            values.append("(" + ", ".join(value) + ")")

        query = ""
        for batch_start in range(0, rows_count, batch_size):
            batch_rows = values[batch_start:batch_start + batch_size]
            if not batch_rows:
                continue
            query += f"UPSERT INTO {table_name} ({", ".join(columns)}) VALUES {", ".join(batch_rows)};\n"

        assert len(query) != 0
        self._try_execute(query)

    def _read_table(
        self,
        table_name,
        schema_inclusion_mode=None,
        limit=None,
        pragmas=[],
        ordered=True
    ):
        all_columns = ", ".join(["pk_Uint64", *[f"col_{cleanup_type_name(type_name)}" for type_name in self.all_types.keys()]]) if ordered else "*"
        query = f"SELECT {all_columns} FROM {table_name}"

        if limit is not None:
            query += f" LIMIT {limit}"

        if len(pragmas) != 0:
            pragmas_stmt = "\n".join(f"PRAGMA {pragma};" for pragma in pragmas) + "\n"
            query = pragmas_stmt + query

        query += ";"
        return self._try_execute(query, schema_inclusion_mode)

    def _multistatement_read_table(
        self,
        table_name,
        schema_inclusion_mode=None,
        concurrent_result_sets=False,
    ):
        all_columns = ["pk_Uint64", *[f"col_{cleanup_type_name(type_name)}" for type_name in self.all_types.keys()]]
        queries = [
            f"SELECT {", ".join(all_columns)} FROM {table_name};",
            f"UPDATE {table_name} SET {all_columns[1]} = {all_columns[1]} + 1 WHERE {all_columns[0]} >= 0 RETURNING {", ".join(all_columns)};",
            f"SELECT * FROM {table_name} ORDER BY {all_columns[0]};"
        ]
        return self._try_execute(
            "\n".join(queries),
            schema_inclusion_mode=schema_inclusion_mode,
            concurrent_result_sets=concurrent_result_sets
        )

    # --------------------- Methods to validate results for tests ---------------------

    @staticmethod
    def validate_format_value(result_set):
        assert result_set.format == ydb.QueryResultSetFormat.VALUE
        # These fields must be empty if the format is VALUE
        assert result_set.arrow_format_meta is None or len(result_set.arrow_format_meta.schema) == 0
        assert result_set.data is None or len(result_set.data) == 0

    def _validate_schema_inclusion_mode(self, result_sets, rows_count, schema_inclusion_mode, stmt_cnt):
        assert result_sets is not None and len(result_sets) != 0

        # To detect the schema inclusion mode
        if self.channel_buffer_size == kb_to_b(2):
            assert len(result_sets) > stmt_cnt

        result_set_indexes = set()
        result_rows_count = {}

        for result_set in result_sets:
            self.validate_format_value(result_set)

            if schema_inclusion_mode in [ydb.QuerySchemaInclusionMode.UNSPECIFIED, ydb.QuerySchemaInclusionMode.ALWAYS]:
                assert len(result_set.columns) == len(self.all_types) + 1
            elif schema_inclusion_mode == ydb.QuerySchemaInclusionMode.FIRST_ONLY:
                if result_set.index not in result_set_indexes:
                    assert len(result_set.columns) == len(self.all_types) + 1
                else:
                    assert len(result_set.columns) == 0
            else:
                assert False, f"Unsupported schema inclusion mode: {schema_inclusion_mode}"

            result_set_indexes.add(result_set.index)
            result_rows_count[result_set.index] = result_rows_count.get(result_set.index, 0) + len(result_set.rows)

        assert len(result_set_indexes) == stmt_cnt
        for cnt in result_rows_count.values():
            assert cnt == rows_count

    def _validate_empty_result(self, table_name):
        empty_response_queries = [
            f"ALTER TABLE {table_name} ADD COLUMN col_NewCol Uint32;",
            f"UPSERT INTO {table_name} (pk_Uint64) VALUES (1234), (5678);",
            f"ALTER TABLE {table_name} DROP COLUMN col_NewCol;",
        ]

        for query in empty_response_queries:
            result_sets = self._try_execute(query)
            assert result_sets is not None and len(result_sets) == 0

        empty_result_queries = [
            f"SELECT * FROM {table_name} WHERE 1 = 0;",
            f"UPDATE {table_name} SET col_Int64 = 1234 WHERE pk_Uint64 > 1234567 RETURNING *;"
        ]

        for query in empty_result_queries:
            result_sets = self._try_execute(query)
            assert result_sets is not None and len(result_sets) == 1

            result = result_sets[0]
            self.validate_format_value(result)
            assert len(result.columns) == len(self.all_types) + 1
            assert len(result.rows) == 0

    def _validate_limit_ordered_columns(self, result_sets, rows_count):
        assert result_sets is not None and len(result_sets) != 0

        result_rows_count = 0
        all_columns = ["pk_Uint64", *[f"col_{cleanup_type_name(type_name)}" for type_name in self.all_types.keys()]]

        for result_set in result_sets:
            self.validate_format_value(result_set)

            result_rows_count += len(result_set.rows)
            assert len(result_set.columns) == len(self.all_types) + 1
            for result_column, col_name in zip(result_set.columns, all_columns):
                assert result_column.name == col_name

        assert result_rows_count == rows_count

    def _validate_column_alias(self, table_name):
        first_alias = "alias_col_Int64"
        second_alias = "alias_col_String"

        query = f"SELECT col_Int64 AS {first_alias}, col_String AS {second_alias} FROM {table_name} LIMIT 1;"
        result_sets = self._try_execute(query)
        assert result_sets is not None and len(result_sets) == 1

        result = result_sets[0]
        self.validate_format_value(result)
        assert len(result.rows) == 1
        assert len(result.columns) == 2

        for column, alias in zip(result.columns, [first_alias, second_alias]):
            assert column.name == alias
