import yatest.common
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper


class FunctionalTestBase:
    cluster = None

    @classmethod
    def setup_cluster(cls) -> None:
        cls.cluster = KiKiMR(configurator=KikimrConfigGenerator(
            domain_name='local',
            extra_feature_flags=["enable_resource_pools"],
            use_in_memory_pdisks=True,
        ))
        cls.cluster.start()
        node = cls.cluster.nodes[1]
        YdbCluster.reset(
            ydb_endpoint=f'grpc://{node.host}:{node.grpc_port}',
            ydb_database=f'{cls.cluster.domain_name}/test_db',
            ydb_mon_port=node.mon_port
        )
        db = f'/{YdbCluster.ydb_database}'
        cls.cluster.create_database(
            db,
            storage_pool_units_count={
                'hdd': 1
            }
        )
        cls.cluster.register_and_start_slots(db, count=1)
        cls.cluster.wait_tenant_up(db)

    @classmethod
    def run_cli(cls, argv: list[str]) -> yatest.common.process._Execution:
        return yatest.common.execute(YdbCliHelper.get_cli_command() + argv)
