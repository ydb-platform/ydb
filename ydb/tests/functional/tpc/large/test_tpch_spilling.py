import ydb.tests.olap.load.lib.tpch as tpch
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpchSpillingS10(tpch.TestTpch10, FunctionalTestBase):
    iterations: int = 1
    query_settings = {i : tpch.TestTpch10.QuerySettings(query_prefix='pragma ydb.UseGraceJoinCoreForMap = "true";') for i in range(1, 23)}
    # temporary exclude q5: https://github.com/ydb-platform/ydb/issues/15359
    skip_tests: list = [5]

    table_service_config = {
        'enable_spilling_nodes': 'All',
        'spilling_service_config': {
            'local_file_config': {
                'enable': True,
                'max_total_size': 536870912000,
                'max_file_size': 107374182400,
            }
        },
        'resource_manager': {
            'verbose_memory_limit_exception': True
        },
    }

    memory_controller_config = {
        'activities_limit_percent': 60,
        'query_execution_limit_percent': 50,
        'hard_limit_bytes': 6 * 1073741824,
    }

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster(table_service_config=cls.table_service_config, memory_controller_config=cls.memory_controller_config)
        cls.run_cli(['workload', 'tpch', '-p', 'olap_yatests/tpch/s10', 'init', '--store=column'])
        cls.run_cli(['workload', 'tpch', '-p', 'olap_yatests/tpch/s10', 'import', 'generator', '--scale=10'])

        tpch.TestTpch10.setup_class()
