import time

from ydb.tests.fq.streaming.common import Kikimr, StreamingTestBase
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator

import pytest
import yatest.common


@pytest.fixture(scope="module")
def kikimr_udfs(request):
    config = KikimrConfigGenerator(
        extra_feature_flags={"enable_external_data_sources": True},
        query_service_config={"available_external_data_sources": ["Ydb", "YdbTopics"]},
        table_service_config={},
        default_clusteradmin="root@builtin",
        use_in_memory_pdisks=True,
        udfs_path=yatest.common.build_path("yql/essentials/udfs/common/python/python3_small")
    )
    config.yaml_config["log_config"]["default_level"] = 8

    kikimr = Kikimr(config)
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

