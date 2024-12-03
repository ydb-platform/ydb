import yatest.common
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


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
        YdbCluster.ydb_endpoint = cls.get_endpoint()
        YdbCluster.ydb_database = cls.get_database()
        YdbCluster.ydb_mon_port = node.mon_port

    @classmethod
    def get_endpoint(cls) -> str:
        node = cls.cluster.nodes[1]
        return f'grpc://{node.host}:{node.grpc_port}'

    @classmethod
    def get_database(cls) -> str:
        return cls.cluster.domain_name

    @classmethod
    def run_cli(cls, argv: list[str]) -> yatest.common.process._Execution:
        return yatest.common.execute(
            [
                yatest.common.binary_path('ydb/apps/ydb/ydb'),
                '--endpoint',
                cls.get_endpoint(),
                '--database',
                f'/{cls.get_database()}',
            ] + argv
        )
