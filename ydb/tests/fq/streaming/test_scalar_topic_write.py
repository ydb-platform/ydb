import logging
import pytest
from typing import Callable, Iterable, Optional

import ydb

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase, YdbClient
from ydb.tests.library.test_meta import link_test_case
from ydb.tests.tools.datastreams_helpers.control_plane import create_stream, create_read_rule

logger = logging.getLogger(__name__)


class TestScalarTopicWriteInYdb(StreamingTestBase):
    """
    Functional and stability tests for writing scalar data into a topic via ``INSERT INTO <topic>``.
    """

    def _read(self, path: str, endpoint, count: int, timeout=None) -> list:
        return self.read_stream(count, topic_path=path, endpoint=endpoint, timeout=timeout)

    def _assert_topic(self, path: str, endpoint, expected: list) -> None:
        actual = self._read(path, endpoint, len(expected))
        assert actual == expected, f"topic {path}: expected {expected}, got {actual}"

    def _expect_error(self, kikimr: Kikimr, sql: str, substrings: Iterable[str] = (), client: Optional[YdbClient] = None) -> str:
        """Run ``sql`` expecting it to fail and return the error string. ``max_retries=0`` keeps the
        negative case fast even if the underlying error is retriable (e.g. unavailable source)."""
        with pytest.raises(ydb.issues.Error) as exc_info:
            if client is None:
                client = kikimr.ydb_client
            client.session_pool.execute_with_retries(
                sql, retry_settings=ydb.RetrySettings(max_retries=0)
            )
        message = str(exc_info.value)
        if isinstance(substrings, str):
            substrings = (substrings,)
        for substring in substrings:
            assert substring in message, f"expected '{substring}' in error, got: {message}"
        return message

    @link_test_case("#39419")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_write_via_select_values_as_table(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        endpoint, ref, path = self.get_write_topic(kikimr, "write_scalar", local_topics, entity_name)

        kikimr.ydb_client.query(f'INSERT INTO {ref} SELECT "my_data";')
        self._assert_topic(path, endpoint, ["my_data"])

        kikimr.ydb_client.query(
            f'INSERT INTO {ref} (Data) VALUES ("my_data1"), ("my_data2"), ("my_data3");'
        )
        self._assert_topic(path, endpoint, ["my_data1", "my_data2", "my_data3"])

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref}
                SELECT * FROM AS_TABLE([
                    <|Data: "my_data1"|>,
                    <|Data: "my_data2"|>,
                    <|Data: "my_data3"|>,
                ]);"""
        )
        self._assert_topic(path, endpoint, ["my_data1", "my_data2", "my_data3"])

    @link_test_case("#39421")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_write_precompute_agg(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        endpoint, ref, path = self.get_write_topic(kikimr, "write_agg", local_topics, entity_name)

        table = entity_name("my_row_table")
        kikimr.ydb_client.query(
            f"""CREATE TABLE `{table}` (id Int32, Data String, PRIMARY KEY (id));"""
        )
        try:
            kikimr.ydb_client.query(
                f"""UPSERT INTO `{table}` (id, Data) VALUES
                    (1, "data_a"), (2, "data_c"), (3, "data_b");"""
            )

            # MAX returns Optional<String>; topic write requires a non-optional data column.
            kikimr.ydb_client.query(f"INSERT INTO {ref} SELECT Unwrap(MAX(Data)) FROM `{table}`;")
            self._assert_topic(path, endpoint, ["data_c"])
        finally:
            kikimr.ydb_client.query(f"DROP TABLE `{table}`;")

    @link_test_case("#39423")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_write_same_data_multiple_topics(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        endpoint, refs, paths = self.get_write_topics(
            kikimr, "write_multi", local_topics, entity_name, topics_count=2
        )

        kikimr.ydb_client.query(
            f"""$my_data = SELECT * FROM AS_TABLE([
                    <|Data: "data1"|>,
                    <|Data: "data2"|>,
                    <|Data: "data3"|>,
                    <|Data: "data4"|>
                ]);

                INSERT INTO {refs[0]} SELECT * FROM $my_data;
                INSERT INTO {refs[1]} SELECT * FROM $my_data;"""
        )
        self._assert_topic(paths[0], endpoint, ["data1", "data2", "data3", "data4"])
        self._assert_topic(paths[1], endpoint, ["data1", "data2", "data3", "data4"])

        # Second query: each topic receives a different filtered subset.
        endpoint2, refs2, paths2 = self.get_write_topics(
            kikimr, "write_multi_filtered", local_topics, entity_name, topics_count=2
        )
        kikimr.ydb_client.query(
            f"""$my_data = SELECT * FROM AS_TABLE([
                    <|Data: "data1"|>,
                    <|Data: "data2"|>,
                    <|Data: "data3"|>,
                    <|Data: "data4"|>
                ]);

                INSERT INTO {refs2[0]} SELECT * FROM $my_data WHERE Data >= "data2";
                INSERT INTO {refs2[1]} SELECT * FROM $my_data WHERE Data <= "data3";"""
        )
        self._assert_topic(paths2[0], endpoint2, ["data2", "data3", "data4"])
        self._assert_topic(paths2[1], endpoint2, ["data1", "data2", "data3"])

    @link_test_case("#39425")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_write_multiple_times_same_topic(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        pytest.skip("Write ordering is not guaranteed now: YQ-5387")

        endpoint, ref, path = self.get_write_topic(kikimr, "write_repeat", local_topics, entity_name)

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref} (Data) VALUES ("data0");
                INSERT INTO {ref} (Data) VALUES ("data1");
                INSERT INTO {ref} (Data) VALUES ("data2");
                INSERT INTO {ref} (Data) VALUES ("data3");"""
        )
        self._assert_topic(path, endpoint, ["data0", "data1", "data2", "data3"])

        endpoint2, ref2, path2 = self.get_write_topic(
            kikimr, "write_repeat_mixed", local_topics, entity_name
        )
        kikimr.ydb_client.query(
            f"""INSERT INTO {ref2} (Data) VALUES ("my_data1"), ("my_data2"), ("my_data3");

                INSERT INTO {ref2}
                SELECT * FROM AS_TABLE([
                    <|Data: "my_data4"|>,
                    <|Data: "my_data5"|>,
                    <|Data: "my_data6"|>,
                ]);

                INSERT INTO {ref2}
                SELECT a.Data || b.Data FROM AS_TABLE([<|Data: "1-X", Id: 1|>, <|Data: "2-X", Id: 2|>]) AS a
                LEFT JOIN AS_TABLE([<|Data: "2-Y", Id: 2|>, <|Data: "1-Y", Id: 1|>]) AS b
                ON a.Id = b.Id;"""
        )
        expected = ["my_data1", "my_data2", "my_data3", "my_data4", "my_data5", "my_data6", "j1-j2"]
        self._assert_topic(path2, endpoint2, expected)

    @link_test_case("#39434")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_write_joined_data(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        endpoint, ref, path = self.get_write_topic(kikimr, "write_join", local_topics, entity_name)

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref}
                SELECT Unwrap(a.Data || b.Data) AS Data
                FROM AS_TABLE([<|Data: "1-X", Id: 1|>, <|Data: "2-X", Id: 2|>]) AS a
                LEFT JOIN AS_TABLE([<|Data: "2-Y", Id: 2|>, <|Data: "1-Y", Id: 1|>]) AS b
                ON a.Id = b.Id;"""
        )
        assert sorted(self._read(path, endpoint, 2)) == ['1-X1-Y', '2-X2-Y']

    @link_test_case("#39429")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_write_and_read_single_query(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        pytest.skip("reading own writes within a single query is not supported: YQ-5388")

        _, ref, _ = self.get_write_topic(kikimr, "read_own_Write", local_topics, entity_name)

        result_sets = kikimr.ydb_client.query(
            f"""INSERT INTO {ref}(Data) VALUES("data0");
                INSERT INTO {ref}(Data) VALUES("data1");
                INSERT INTO {ref}(Data) VALUES("data2");
                INSERT INTO {ref}(Data) VALUES("data3");

                SELECT
                    SystemMetadata('offset') as offset,
                    SystemMetadata('seq_no') as seq_no,
                    SystemMetadata("write_time") as write_time,
                    Data
                FROM {ref};"""
        )

        assert len(result_sets) == 1
        assert len(result_sets[0].rows) == 4

        rows = result_sets[0].rows
        assert rows[0]["Data"] == "data0"
        assert rows[1]["Data"] == "data1"
        assert rows[2]["Data"] == "data2"
        assert rows[3]["Data"] == "data3"

        assert rows[0]["offset"] == 0
        assert rows[1]["offset"] == 1
        assert rows[2]["offset"] == 2
        assert rows[3]["offset"] == 3

        assert rows[0]["seq_no"] == 0
        assert rows[1]["seq_no"] == 1
        assert rows[2]["seq_no"] == 2
        assert rows[3]["seq_no"] == 3

    @link_test_case("#39440")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_data_types_validation(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        endpoint, ref, path = self.get_write_topic(kikimr, "write_types", local_topics, entity_name)

        kikimr.ydb_client.query(
            f"""INSERT INTO {ref} (Data) VALUES ("string");"""
        )
        kikimr.ydb_client.query(
            f"""INSERT INTO {ref} (Data) VALUES (Unwrap(CAST('{{"my_json": true}}' AS Json)));"""
        )
        kikimr.ydb_client.query(
            f"""INSERT INTO {ref} (Data) VALUES (Unwrap(CAST('{{abc=123; def=456}}' AS Yson)));"""
        )
        self._assert_topic(path, endpoint, ["string", "{\"my_json\": true}", "{abc=123; def=456}"])

        # Unsupported scalar types must fail.
        self._expect_error(
            kikimr, f'INSERT INTO {ref} (Data) VALUES (42);', ["is not a string, yson or json"]
        )
        self._expect_error(
            kikimr, f'INSERT INTO {ref} (Data) VALUES ("utf8"u);', ["is not a string, yson or json"]
        )
        self._expect_error(
            kikimr, f'INSERT INTO {ref} (Data) VALUES (CurrentUtcTimestamp());', ["is not a string, yson or json"]
        )

        # Unsupported nested types must fail.
        self._expect_error(
            kikimr, f'INSERT INTO {ref} (Data) VALUES ("data"), (Nothing(String?));', ["must have a data type, but has Optional"]
        )
        self._expect_error(
            kikimr, f'INSERT INTO {ref} (Data) VALUES (AsStruct("data" AS Data, 1 AS Id));', ["must have a data type, but has Struct"]
        )
        self._expect_error(
            kikimr, f'INSERT INTO {ref} (Data) VALUES (AsTuple("data", 1));', ["must have a data type, but has Tuple"]
        )

        # Common type inference over incompatible literals must fail.
        self._expect_error(
            kikimr, f'INSERT INTO {ref} (Data) VALUES ("string"), (42), (CurrentUtcTimestamp());', ["is not a string, yson or json"]
        )

        # Writing more than one column must fail.
        self._expect_error(
            kikimr, f'INSERT INTO {ref} (Data, Other) VALUES ("string", "other");', ["Only struct with single string, yson or json field is accepted, but has struct with 2 members"]
        )

    @link_test_case("#39446")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_with_options_validation(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        pytest.skip("unknown WITH options are not validated: YQ-5389")

        _, ref, _ = self.get_write_topic(kikimr, "write_types", local_topics, entity_name)

        self._expect_error(
            kikimr,
            f"""INSERT INTO {ref} WITH (unknown = feature)
                SELECT 'MyData'""",
            ["unknown option 'unknown'"]
        )

    @link_test_case("#39451")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_write_target_validation(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        endpoint = self.get_endpoint(kikimr, local_topics)
        source_name = entity_name("target_source")
        self.create_source(kikimr, source_name, endpoint=endpoint)

        # Write into a non-existent topic (under an existing source / locally).
        bad_topic_ref = "`non_existent_topic`" if local_topics else f"`{source_name}`.`non_existent_topic`"
        if local_topics:
            self._expect_error(kikimr, f'INSERT INTO {bad_topic_ref} SELECT "Data";', ["Cannot find table", "non_existent_topic", "because it does not exist or you do not have access permissions"])
        else:
            self._expect_error(
                kikimr,
                f'INSERT INTO {bad_topic_ref} SELECT "Data";',
                ["determine external YDB entity type", "Describe path", "non_existent_topic", "in external YDB database", "with endpoint"],
            )

        # Write into a non-existent external source.
        if not local_topics:
            self._expect_error(
                kikimr,
                'INSERT INTO `non_existent_source`.`my_topic` SELECT "Data";',
                ["Cannot find table", "/Root/non_existent_source", "my_topic", "because it does not exist or you do not have access permissions"],
            )

        # Write into an unavailable external source (valid metadata, unreachable location).
        if not local_topics:
            unavailable_source = entity_name("unavailable_source")
            kikimr.ydb_client.query(
                f"""CREATE EXTERNAL DATA SOURCE `{unavailable_source}` WITH (
                        SOURCE_TYPE = "Ydb",
                        LOCATION = "localhost:1",
                        DATABASE_NAME = "/Root",
                        AUTH_METHOD = "NONE"
                    );"""
            )
            self._expect_error(
                kikimr,
                f'INSERT INTO `{unavailable_source}`.`my_topic` SELECT "Data";',
                ["Describe path", "/Root/my_topic", "in external YDB database", "/Root", "with endpoint", "localhost:1", "failed"],
            )

        try:
            external_client = YdbClient(endpoint.endpoint, endpoint.database)
            test_client = YdbClient(kikimr.endpoint.endpoint, kikimr.endpoint.database, "test@builtin")
            external_client.wait_connection()
            test_client.wait_connection()

            test_secret_name = entity_name("test_secret")
            test_source_name = entity_name("test_target_source")
            kikimr.ydb_client.query(f"""
                CREATE SECRET `{test_secret_name}` WITH (value = "test@builtin");
                CREATE EXTERNAL DATA SOURCE `{test_source_name}` WITH (
                    SOURCE_TYPE = "Ydb",
                    LOCATION = "{endpoint.endpoint}",
                    DATABASE_NAME = "{endpoint.database}",
                    AUTH_METHOD = "TOKEN",
                    TOKEN_SECRET_PATH = "{test_secret_name}"
                );
            """)

            path = f"{test_source_name}_test_topic"
            create_stream(path, partitions_count=1, default_endpoint=endpoint)
            kikimr.ydb_client.query(
                f"""GRANT DESCRIBE SCHEMA ON `{test_source_name}` TO `test@builtin`;
                    GRANT USE ON `{test_secret_name}` TO `test@builtin`;"""
            )
            external_client.query(
                f"""GRANT DESCRIBE SCHEMA ON `{path}` TO `test@builtin`;"""
            )

            ref = f"`{path}`" if local_topics else f"`{test_source_name}`.`{path}`"
            self._expect_error(
                kikimr, f'INSERT INTO {ref} SELECT "Data";', ["access to topic", "test_topic in database:", "denied for", "test@builtin", "no WriteTopic rights"], client=test_client
            )

            if not local_topics:
                self._expect_error(
                    kikimr,
                    f'INSERT INTO `{unavailable_source}`.my_topic SELECT "Data";',
                    ["Cannot find table", "/Root/unavailable_source", "my_topic", "because it does not exist or you do not have access permissions."],
                    client=test_client,
                )
        finally:
            test_client.stop()
            external_client.stop()

    @link_test_case("#39632")
    @pytest.mark.parametrize("local_topics", [True, False])
    def test_mixed_valid_invalid_statements(
        self, kikimr: Kikimr, entity_name: Callable[[str], str], local_topics: bool
    ) -> None:
        endpoint = self.get_endpoint(kikimr, local_topics)
        source_name = entity_name("mixed_source")
        self.create_source(kikimr, source_name, endpoint=endpoint)
        self.consumer_name = f"{source_name}_consumer"

        ok_path = f"{source_name}_ok"
        create_stream(ok_path, default_endpoint=endpoint)
        create_read_rule(ok_path, self.consumer_name, default_endpoint=endpoint)
        ok_topic = f"`{ok_path}`" if local_topics else f"`{source_name}`.`{ok_path}`"
        bad_topic = "`non_existent_topic`" if local_topics else f"`{source_name}`.`non_existent_topic`"

        sql = f"""
            -- Ok
            INSERT INTO {ok_topic}
            SELECT a.Data || b.Data FROM AS_TABLE([<|Data: "1-X", Id: 1|>, <|Data: "2-X", Id: 2|>]) AS a
            LEFT JOIN AS_TABLE([<|Data: "2-Y", Id: 2|>, <|Data: "1-Y", Id: 1|>]) AS b
            ON a.Id = b.Id;

            -- Fail
            INSERT INTO `non_existent_source`.`my_topic` SELECT "Data";

            -- Ok
            INSERT INTO {ok_topic} (Data) VALUES ("my_data1"), ("my_data2"), ("my_data3");

            -- Fail
            INSERT INTO {bad_topic} SELECT "Data";

            -- Ok
            INSERT INTO {ok_topic}
            SELECT * FROM AS_TABLE([
                <|Data: "my_data1"|>,
                <|Data: "my_data2"|>,
                <|Data: "my_data3"|>,
            ]);

            -- Fail
            INSERT INTO {ok_topic} WITH (unknown = feature) SELECT "MyData";
        """

        # The whole query must fail atomically: none of the "Ok" statements are committed.
        self._expect_error(kikimr, sql)
        assert self._read(ok_path, endpoint, 1, timeout=5) == []
