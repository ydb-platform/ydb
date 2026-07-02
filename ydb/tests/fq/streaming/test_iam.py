import logging
import os
import time
from typing import Callable

import pytest

from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase
from ydb.tests.tools.datastreams_helpers.control_plane import Endpoint

logger = logging.getLogger(__name__)

# Random but stable service-account-id placeholder; real validation is done server-side.
FAKE_SERVICE_ACCOUNT_ID = "aje00000000000000000"

# The token that the IAM emulator (bin/main.py --token) returns by default.
IAM_TOKEN = "test-iam-token"
USER_TOKEN = "root@builtin"

class TestIamAuth(StreamingTestBase):
    """Verify that a topic source with AUTH_METHOD=IAM can be read via
    TIAMCredentialsProvider talking to the local IAM emulator."""

    def create_iam_secret(self, kikimr: Kikimr, secret_name: str):
        kikimr.ydb_client.query(f"""
            CREATE SECRET `{secret_name}` WITH (value="{USER_TOKEN}");
        """)

    def set_cloud_id(self, kikimr: Kikimr, cloud_id: str = "test-cloud-id") -> None:
        """Set cloud_id user attribute on the database root path (/Root).

        DescribeResourceId reads GetAttributes() of the database root, so we must
        use ESchemeOpAlterUserAttributes rather than ALTER TABLE which only supports
        table-level settings.
        """
        # kikimr.cluster.client is a KiKiMRMessageBusClient with add_attr(working_dir, name, attrs)
        # For database root "/Root": working_dir="/", name="Root"
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
        """Write one message to a local topic and read it back through an IAM-auth source."""
        # IAM gRPC emulator is started by the recipe and sets IAM_ENDPOINT env var:
        iam_endpoint = os.environ.get("IAM_ENDPOINT")
        assert iam_endpoint, "IAM_ENDPOINT is not set - is the iam_grpc_emulator recipe included?"
        logger.info(f"IAM gRPC emulator is at {iam_endpoint}")

        endpoint = self.get_endpoint(kikimr, local_topics=True)
        source_name = entity_name("iam_source")

        # 1. Create the secret that holds the token the emulator will return.
        secret_name = entity_name("iam_secret")
        self.create_iam_secret(kikimr, secret_name)
        self.set_cloud_id(kikimr)
        time.sleep(1)

        # 2. Create topics and an IAM-auth external data source.
        self.init_topics(source_name, create_output=False, partitions_count=1, endpoint=endpoint)
        self.create_iam_source(kikimr, source_name, secret_name, endpoint)

        input_name = f"`{source_name}`.`{self.input_topic}`"

        # 3. Run a streaming SELECT that reads one message from the topic.
        sql = f"""SELECT time FROM {input_name}
            WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA = (time String NOT NULL)
            )
            LIMIT 1"""

        future = kikimr.ydb_client.query_async(sql)
       # time.sleep(200)
        self.write_stream(['{"time": "iam lunch time"}'], endpoint=endpoint)
        result_sets = future.result()
        assert result_sets[0].rows[0]["time"] == b"iam lunch time"
