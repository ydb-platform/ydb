import yatest.common
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper


class FunctionalTestBase:
    cluster = None

    table_service_config_for_spilling = {
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
    }

    @classmethod
    def setup_cluster(cls, with_spilling: bool = False) -> None:
        table_service_config = {}
        if with_spilling:
            table_service_config = cls.table_service_config_for_spilling
        memory_controller_config = {}
        if with_spilling:
            memory_controller_config = cls.memory_controller_config
        print('MISHA', table_service_config)
        cls.cluster = KiKiMR(configurator=KikimrConfigGenerator(
            domain_name='local',
            extra_feature_flags=["enable_resource_pools"],
            use_in_memory_pdisks=True,
            table_service_config=table_service_config,
            memory_controller_config=memory_controller_config,
        ))
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        YdbCluster.reset(
            ydb_endpoint=f'grpc://{node.host}:{node.grpc_port}',
            ydb_database=f'{cls.cluster.domain_name}/test_db',
            ydb_mon_port=node.mon_port,
            dyn_nodes_count=1
        )
        db = f'/{YdbCluster.ydb_database}'
        cls.cluster.create_database(
            db,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        cls.cluster.register_and_start_slots(db, count=YdbCluster.get_dyn_nodes_count())
        cls.cluster.wait_tenant_up(db)

    @classmethod
    def run_cli(cls, argv: list[str]) -> yatest.common.process._Execution:
        return yatest.common.execute(YdbCliHelper.get_cli_command() + argv)
