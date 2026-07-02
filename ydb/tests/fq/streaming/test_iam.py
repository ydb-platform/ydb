import json
import logging
import time
from typing import Callable

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase
from ydb.tests.tools.datastreams_helpers.control_plane import Endpoint

logger = logging.getLogger(__name__)

# Random but stable service-account-id placeholder; real validation is done server-side.
FAKE_SERVICE_ACCOUNT_ID = "aje00000000000000000"

# The token that the IAM emulator returns by default.
USER_TOKEN = "root@builtin"


class TestIamAuth(StreamingTestBase):
    """Verify that a topic source with AUTH_METHOD=IAM works end-to-end in a CREATE STREAMING QUERY."""

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
    ) -> None:
        """Create an External Data Source that authenticates via IAM."""
        kikimr.ydb_client.query(f"""
            CREATE EXTERNAL DATA SOURCE `{source_name}` WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{endpoint.endpoint}",
                DATABASE_NAME = "{endpoint.database}",
                AUTH_METHOD = "IAM",
                INITIAL_TOKEN_SECRET_PATH = "{secret_path}",
                SERVICE_ACCOUNT_ID = "{FAKE_SERVICE_ACCOUNT_ID}"
            );
        """)

    def test_read_topic_iam_auth(
        self,
        kikimr: Kikimr,
        entity_name: Callable[[str], str],
    ) -> None:
        """Write a message to a local topic, process it through a CREATE STREAMING QUERY
        that reads via an IAM-auth external data source, and verify the output topic
        receives the transformed message."""

        endpoint = self.get_endpoint(kikimr, local_topics=True)
        source_name = entity_name("iam_source")
        query_name = entity_name("iam_query")

        # 1. Create the secret and set cloud_id on the database root.
        secret_name = entity_name("iam_secret")
        self.create_iam_secret(kikimr, secret_name)
        self.set_cloud_id(kikimr)
        time.sleep(1)

        # 2. Create input + output topics and an IAM-auth external data source.
        self.init_topics(source_name, create_output=True, partitions_count=1, endpoint=endpoint)
        self.create_iam_source(kikimr, source_name, secret_name, endpoint)

        inp = f"`{source_name}`.`{self.input_topic}`"
        out = f"`{self.output_topic}`"

        # 3. Start a persistent streaming query: read from IAM-auth source, write to output topic.
        sql = R'''
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                INSERT INTO {out} SELECT UNWRAP(Yson::SerializeJson(Yson::From(TableRow())))
                FROM (
                    SELECT time FROM {inp}
                    WITH (
                        FORMAT = "json_each_row",
                        SCHEMA = (time String NOT NULL)
                    )
                );
            END DO;'''

        kikimr.ydb_client.query(sql.format(query_name=query_name, inp=inp, out=out))
        path = f"/Root/{query_name}"
        self.wait_completed_checkpoints(kikimr, path)

        # 4. Write a message to the input topic and read the result from the output topic.
        self.write_stream(['{"time": "iam lunch time"}'], endpoint=endpoint)
        result = self.read_stream(1, topic_path=self.output_topic, endpoint=endpoint)[0]
        assert json.loads(result) == {"time": "iam lunch time"}

        kikimr.ydb_client.query(f"DROP STREAMING QUERY `{query_name}`;")
