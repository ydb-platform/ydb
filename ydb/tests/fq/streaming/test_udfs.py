import logging
import json
import time
import ydb

from typing import Optional
from ydb.tests.fq.streaming_common.common import Kikimr, StreamingTestBase, YdbClient, set_test_env
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import pytest
import yatest.common

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def kikimr_udfs(request):
    set_test_env(request)
    config = KikimrConfigGenerator(
        erasure=Erasure.MIRROR_3_DC,
        extra_feature_flags=[
            "enable_external_data_sources",
            "enable_streaming_queries_counters",
            "enable_topics_sql_io_operations",
            "enable_streaming_queries",
        ],
        query_service_config={"available_external_data_sources": ["Ydb", "YdbTopics"]},
        pq_client_service_types=["yandex-query"],
        table_service_config={},
        default_clusteradmin="root@builtin",
        use_in_memory_pdisks=False,
        udfs_path=yatest.common.build_path("yql/essentials/udfs/common/python/python3_small")
    )
    config.yaml_config["log_config"] = {
        "default_level": 4,
        "entry": [
            {"component": "KQP_PROXY", "level": 7}
        ]
    }

    kikimr = Kikimr(config, timeout_seconds=30, enable_discovery=False)
    yield kikimr
    kikimr.stop()


class TestUdfsUsage(StreamingTestBase):
    def test_dynamic_udf(self, kikimr_udfs, entity_name):
        source_name = entity_name("test_udfs")
        self.init_topics(source_name, create_output=False)
        self.create_source(kikimr_udfs, source_name)

        future = kikimr_udfs.ydb_client.query_async(f"""
            $script = @@#py
import urllib.parse

def get_all_cgi_params(url):
    params = urllib.parse.parse_qs(urllib.parse.urlparse(url).query)
    return {{k: v[0] for k, v in params.items()}}
@@;

            $callable = Python3::get_all_cgi_params(
                Callable<(Utf8?)->Dict<Utf8,Utf8>>,
                $script
            );

            SELECT $callable(url)["name"] AS name FROM `{source_name}`.`{self.input_topic}`
            WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                SCHEMA (
                    url Utf8 NOT NULL
                )
            )
            LIMIT 1
        """)

        time.sleep(1)
        self.write_stream(['{"url": "http://kuibyshevsky.sam.sudrf.ru/modules.php?name=information"}'])

        assert future.result()[0].rows[0]["name"] == "information"

    @pytest.mark.parametrize("local_topics", [True, False])
    def test_precompute_recovery(self, kikimr_udfs, local_topics, entity_name):
        inp, out, endpoint = self.get_io_names(kikimr_udfs, "test_precompute_recovery", local_topics, entity_name)
        path = "/Root/test_precompute_recovery_query"

        test_table = "test_table"
        kikimr_udfs.ydb_client.query(f"""
            CREATE TABLE `{test_table}` (
                Key Uint64,
                Payload String,
                PRIMARY KEY (Key)
            );
        """)
        kikimr_udfs.ydb_client.query(f"""
            UPSERT INTO `{test_table}` (Key, Payload) VALUES (1, "test-")
        """)

        def validate_query(text: str, previous_ids: int, status: str = "RUNNING", check_issues: bool = True, suffix: Optional[str] = None):
            result_sets = kikimr_udfs.ydb_client.query("""
                SELECT
                    Path,
                    Status,
                    Issues,
                    Text,
                    Run,
                    ResourcePool,
                    RetryCount,
                    LastFailAt,
                    LastExecutionId,
                    PreviousExecutionIds
                FROM `.sys/streaming_queries`
            """)
            assert len(result_sets) == 1

            result_set_rows = result_sets[0].rows
            assert len(result_set_rows) == 1

            row = result_set_rows[0]

            if check_issues:
                assert row.Issues == "{}"

            assert row.Path == path
            assert row.Status == status
            assert row.Text.strip() in text.strip()
            assert row.Run
            assert row.ResourcePool == "default"
            assert row.RetryCount == 0
            assert row.LastFailAt is None
            assert row.LastExecutionId is not None
            assert len(json.loads(row.PreviousExecutionIds)) == min(previous_ids, 3)

            if suffix is not None:
                self.wait_completed_checkpoints(kikimr_udfs, path)
                self.write_stream_with_message_metadata(kikimr_udfs, [("test_data", {"msg_id": "id-1"})], endpoint=endpoint)
                assert self.read_stream(1, topic_path=self.output_topic, endpoint=endpoint)[0] == f"test_data{suffix}"

        tests_count = 20
        for i in range(20):
            sql = f"""
                CREATE OR REPLACE STREAMING QUERY `{path}` AS DO BEGIN
                    -- Revision {i}
                    INSERT INTO {out} SELECT Data || "{i}" FROM {inp}
                END DO
            """
            kikimr_udfs.ydb_client.query(sql)

            validate_query(sql, i, suffix=str(i))

        self.wait_completed_checkpoints(kikimr_udfs, path)
        kikimr_udfs.ydb_client.query(f"""
            ALTER STREAMING QUERY `{path}` SET (RUN = FALSE)
        """)
        self.write_stream_with_message_metadata(kikimr_udfs, [("test_data", {"msg_id": "id-1"})], endpoint=endpoint)
        logger.info("Stopped simple query")

        # Start query with heavy precompute
        precompute_sql = f"""
CREATE OR REPLACE STREAMING QUERY `{path}` AS DO BEGIN
$script = @@#py
import time

def hang():
    time.sleep(20)
    return "result"
@@;

$callable = Python3::hang(Callable<()->String>, $script);

$precompute = SELECT Payload || $callable() FROM `{test_table}` LIMIT 1;

INSERT INTO {out} SELECT * FROM {inp}
WHERE Data LIKE Unwrap($precompute);
END DO
        """
        future = kikimr_udfs.ydb_client.query_async(precompute_sql, timeout=10)
        logger.info("Started hanging query")

        time.sleep(5)
        validate_query(precompute_sql, tests_count, status="STARTING")
        logger.info("Hanging query validation finished, waiting for query to exit")

        with pytest.raises(ydb.issues.Error) as exc_info:
            future.result()
        logger.info(f"Hanging query finished: {exc_info.value}")

        kikimr_udfs.ydb_client.stop()
        kikimr_udfs.first_node.kill()
        kikimr_udfs.first_node.start()
        logger.info("Node with query restarted")

        time.sleep(5)
        second_node = list(kikimr_udfs.cluster.nodes.values())[1]
        kikimr_udfs.ydb_client = YdbClient(database=kikimr_udfs.endpoint.database, endpoint=f"grpc://{second_node.host}:{second_node.port}")
        logger.info("Checking query state after restart")

        validate_query(precompute_sql, tests_count, status="STARTING", check_issues=False)
        logger.info("Hanging query validated after restart")

        sql = f"""
            CREATE OR REPLACE STREAMING QUERY `{path}` AS DO BEGIN
                -- Revision FINAL
                INSERT INTO {out} SELECT Data || "_final" FROM {inp}
            END DO
        """
        kikimr_udfs.ydb_client.query(sql)

        time.sleep(1)
        validate_query(sql, tests_count)
        logger.info("Checked final query info")

        assert self.read_stream(1, topic_path=self.output_topic, endpoint=endpoint)[0] == "test_data_final"
        logger.info("Checked checkpoint recovery")

        time.sleep(5)
        validate_query(sql, tests_count, suffix="_final")
        logger.info("Checked final status")
