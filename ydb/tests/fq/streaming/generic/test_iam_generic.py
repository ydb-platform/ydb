import json
import pytest
import logging
import time
import datetime
from typing import Callable

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase
from ydb.tests.tools.datastreams_helpers.control_plane import Endpoint
import ydb.issues

logger = logging.getLogger(__name__)

# Random but stable service-account-id placeholder; real validation is done server-side.
FAKE_SERVICE_ACCOUNT_ID = "aje00000000000000000"

# The token that the IAM emulator returns by default.
USER_TOKEN = "root@builtin"


class TestIamAuthGeneric(StreamingTestBase):
    """Verify that a generic source works with AUTH_METHOD=IAM works end-to-end with connector."""

    def create_iam_secret(self, kikimr: Kikimr, secret_name: str) -> None:
        kikimr.ydb_client.query(f"""
            CREATE SECRET `{secret_name}` WITH (value="{USER_TOKEN}");
        """)

    def set_cloud_id(self, kikimr: Kikimr, cloud_id: str = "test-cloud-id") -> None:
        """Set cloud_id user attribute on the database root path (/Root).

        DescribeResourceId reads GetAttributes() of the database root, so we must
        use ESchemeOpAlterUserAttributes rather than ALTER TABLE which only supports
        table-level settings.
        """
        kikimr.cluster.client.add_attr("/", "Root", {"cloud_id": cloud_id}, token="root@builtin")

    def create_iam_source(
        self,
        kikimr: Kikimr,
        source_name: str,
        secret_path: str,
        endpoint: Endpoint,
        shared_reading: bool = False,
        service_account_id: str = FAKE_SERVICE_ACCOUNT_ID
    ) -> None:
        """Create an External Data Source that authenticates via IAM."""
        kikimr.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{endpoint.endpoint}",
                DATABASE_NAME = "{endpoint.database}",
                USE_TLS = "FALSE",
                AUTH_METHOD = "IAM",
                INITIAL_TOKEN_SECRET_PATH = "{secret_path}",
                SERVICE_ACCOUNT_ID = "{service_account_id}",
                SHARED_READING="{shared_reading}"
            );
        """)

    def create_table(
        self,
        kikimr: Kikimr,
        table_name: str,
    ) -> None:
        kikimr.ydb_client.query(f"CREATE TABLE `{table_name}` (a INT, b STRING, c Bool, d Timestamp, e Interval, PRIMARY KEY(a, b))")
        kikimr.ydb_client.query(f"UPSERT INTO `{table_name}` (a, b, c, d, e) VALUES (1, 'abc', false, Timestamp('2025-08-21T11:22:33.456789Z'), Interval('PT1M'))")
        kikimr.ydb_client.query(f"UPSERT INTO `{table_name}` (a, b, c, d, e) VALUES (2, 'abcdefghijklmnoprstuvwxyz', true, Timestamp('2025-08-21T22:33:44.567Z'), Interval('PT10S'))")

    @pytest.mark.parametrize(
        "service_account_id", [FAKE_SERVICE_ACCOUNT_ID, "bad", "bad-token", "bad-skip-1", "bad-token-skip-1"]
    )
    def test_generic_read_iam_auth(
        self,
        kikimr: Kikimr,
        service_account_id: str,
        entity_name: Callable[[str], str],
    ) -> None:
        """Creates and populates local table, and read it from query
        via an IAM-auth external data source."""

        endpoint = self.get_endpoint(kikimr, local_topics=True)
        source_name = entity_name("iam_source")
        table_name = entity_name("iam_table")

        # 1. Create the secret and set cloud_id on the database root.
        secret_name = entity_name("iam_secret")
        self.create_iam_secret(kikimr, secret_name)
        self.set_cloud_id(kikimr)
        time.sleep(1)

        # 2. Create and populate local table
        self.create_table(kikimr, table_name)

        # 3. Create IAM-auth external data source.
        self.create_iam_source(kikimr, source_name, secret_name, endpoint, service_account_id=service_account_id)

        tab = f"`{source_name}`.`{table_name}`"

        # 3. Read from IAM-auth source, verify results
        try:
            result = kikimr.ydb_client.query(f"SELECT * FROM {tab} ORDER BY a, b")
        except ydb.issues.Error as ex:
            assert service_account_id != FAKE_SERVICE_ACCOUNT_ID, ex
            if service_account_id == 'bad':
                assert 'Reject bad SA' in ex.message
            if service_account_id == 'bad-token':
                assert 'Access denied' in ex.message
            logger.debug(ex)
            kikimr.ydb_client.query(f"DROP EXTERNAL DATA SOURCE `{source_name}`;")
            return

        assert service_account_id == FAKE_SERVICE_ACCOUNT_ID, "Unexpected success"

        assert result
        rows = result[0].rows
        logger.debug(rows)
        # note: Interval type is currently not supported by fq connector and silently ignored
        expected = [
            {'a': 1, 'b': b'abc', 'c': False, 'd': datetime.datetime(2025, 8, 21, 11, 22, 33, 456789)},
            {'a': 2, 'b': b'abcdefghijklmnoprstuvwxyz', 'c': True, 'd': datetime.datetime(2025, 8, 21, 22, 33, 44, 567000)},
        ]
        assert rows == expected

        result = kikimr.ydb_client.query(f"SELECT * FROM {tab} WHERE a = 1 ORDER BY a, b")
        expected = [*filter(lambda x: x["a"] == 1, expected)]
        assert result
        rows = result[0].rows
        logger.debug(rows)
        assert rows == expected

        kikimr.ydb_client.query(f"DROP TABLE `{table_name}`;")
        kikimr.ydb_client.query(f"DROP EXTERNAL DATA SOURCE `{source_name}`;")

    @pytest.mark.parametrize(
        "kikimr",
        [
            {"enable_dq_source_stream_lookup_join": True},
        ],
        indirect=["kikimr"],
    )
    @pytest.mark.parametrize(
        "service_account_id", [FAKE_SERVICE_ACCOUNT_ID, "bad", "bad-token", "bad-skip-1", "bad-token-skip-1"]
    )
    def test_generic_lookup_iam_auth(
        self,
        kikimr: Kikimr,
        service_account_id: str,
        entity_name: Callable[[str], str],
    ) -> None:
        """Creates and populates local table, and read it from query
        via an IAM-auth external data source."""

        endpoint = self.get_endpoint(kikimr, local_topics=True)
        source_name = entity_name("iam_source")
        table_name = entity_name("iam_table")
        query_name = entity_name("iam_query")
        ttl = 1

        # 1. Create the secret and set cloud_id on the database root.
        secret_name = entity_name("iam_secret")
        self.create_iam_secret(kikimr, secret_name)
        self.set_cloud_id(kikimr)
        time.sleep(1)
        # 2. Create and populate local table
        self.create_table(kikimr, table_name)
        # 3. Create topics
        self.init_topics(source_name, create_output=True, partitions_count=2, endpoint=endpoint)
        # 4. Create IAM-auth external data source.
        self.create_iam_source(kikimr, source_name, secret_name, endpoint, service_account_id=service_account_id)
        tab = f"`{source_name}`.`{table_name}`"
        inp = f"`{source_name}`.`{self.input_topic}`"
        out = f"`{source_name}`.`{self.output_topic}`"

        # 5. Join data from topic and lookup using external source with IAM-auth
        if service_account_id != FAKE_SERVICE_ACCOUNT_ID:
            try:
                messages = ['1', '2', '1', '2', '3']
                self.write_stream(messages, endpoint=endpoint)
                kikimr.ydb_client.query(Rf"""
                    SELECT Data, a, b, c, CAST(d AS String) AS d
                      FROM {inp} AS i
                      LEFT JOIN /*+streamlookup(TTL {ttl} FullscanLimit 0)*/ ANY {tab} AS db
                             ON CAST(i.Data AS Int) = db.a
                     LIMIT 1
                """)
            except ydb.issues.Error as ex:
                if service_account_id == 'bad':
                    assert 'Reject bad SA' in ex.message
                if service_account_id == 'bad-token':
                    assert 'Access denied' in ex.message
                logger.info(ex)
                logger.info(type(ex))
                logger.info(ex.args)
                kikimr.ydb_client.query(f"DROP TABLE `{table_name}`;")
                kikimr.ydb_client.query(f"DROP EXTERNAL DATA SOURCE `{source_name}`;")
                return
            assert False, "Unexpected success"

        kikimr.ydb_client.query(Rf"""
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO {out} SELECT UNWRAP(Yson2::SerializeJson(Yson2::From(TableRow())))
                FROM (
                    SELECT Data, a, b, c, CAST(d AS String) AS d
                      FROM {inp} AS i
                      LEFT JOIN /*+streamlookup(TTL {ttl} FullscanLimit 0)*/ ANY {tab} AS db
                             ON CAST(i.Data AS Int) = db.a
                )
            END DO
        """)

        path = f"/Root/{query_name}"
        self.wait_completed_checkpoints(kikimr, path)
        messages = ['1', '2', '1', '2', '3']
        self.write_stream(messages, endpoint=endpoint)
        expected = [
            '{"Data":"1","a":1,"b":"abc","c":false,"d":"2025-08-21T11:22:33.456789Z"}',
            '{"Data":"2","a":2,"b":"abcdefghijklmnoprstuvwxyz","c":true,"d":"2025-08-21T22:33:44.567000Z"}',
        ] * 2 + [
            '{"Data":"3","a":null,"b":null,"c":null,"d":null}',
        ]
        result = self.read_stream(len(expected), topic_path=self.output_topic, endpoint=endpoint)
        logger.debug([*map(json.loads, sorted(result))])
        assert sorted(result) == sorted(expected)

        time.sleep(ttl)  # at least TTL

        kikimr.ydb_client.query(f"UPDATE `{table_name}` SET c = not c")
        expected = [
            '{"Data":"1","a":1,"b":"abc","c":true,"d":"2025-08-21T11:22:33.456789Z"}',
            '{"Data":"2","a":2,"b":"abcdefghijklmnoprstuvwxyz","c":false,"d":"2025-08-21T22:33:44.567000Z"}',
        ] * 2 + [
            '{"Data":"3","a":null,"b":null,"c":null,"d":null}',
        ]
        self.write_stream(messages, endpoint=endpoint)
        result = self.read_stream(len(expected), topic_path=self.output_topic, endpoint=endpoint)
        logger.debug([*map(json.loads, sorted(result))])
        assert sorted(result) == sorted(expected)
        logger.debug(kikimr.ydb_client.query("SELECT * FROM `.sys/streaming_queries`")[0].rows)

        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`;")
        kikimr.ydb_client.query(f"DROP TABLE `{table_name}`;")
        kikimr.ydb_client.query(f"DROP EXTERNAL DATA SOURCE `{source_name}`;")
