from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TestClean(FunctionalTestBase):
    def test(self):
        def _exists(path: str) -> bool:
            try:
                YdbCluster.get_ydb_driver().scheme_client.describe_path(f'/{YdbCluster.ydb_database}/{path}')
            except BaseException:
                return False
            return True

        self.run_cli(['workload', 'tpch', '-p', 'custom/tpch/s1', 'init', '--store=column'])
        assert _exists('custom/tpch/s1/orders')

        self.run_cli(['workload', 'tpcds', '-p', 'custom/tpcds/s1', 'init', '--store=column'])
        assert _exists('custom/tpcds/s1/call_center')

        self.run_cli(['workload', 'tpcds', 'init', '--store=column'])
        assert _exists('call_center')

        self.run_cli(['workload', 'clickbench', '-p', 'custom/clickbench/hits', 'init', '--store=column'])
        assert _exists('custom/clickbench/hits')

        self.run_cli(['workload', 'clickbench', 'init', '--store=column'])
        assert _exists('clickbench/hits')

        self.run_cli(['workload', 'stock', 'init', '--store=column', '-o', '0'])
        assert _exists('orderLines')
        assert _exists('orders')
        assert _exists('stock')

        self.run_cli(['workload', 'kv', 'init', '--init-upserts=0', '--store=column'])
        assert _exists('kv_test')

        self.run_cli(['workload', 'kv', '-p', 'custom/kv/table', 'init', '--init-upserts=0', '--store=column'])
        assert _exists('custom/kv/table')

        self.run_cli(['workload', 'kv', 'clean'])
        assert not _exists('kv_test')
        assert _exists('custom')

        self.run_cli(['workload', 'stock', 'clean'])
        assert not _exists('orderLines')
        assert not _exists('orders')
        assert not _exists('stock')
        assert _exists('custom')

        self.run_cli(['workload', 'clickbench', 'clean'])
        assert not _exists('clickbench')
        assert _exists('custom')

        self.run_cli(['workload', 'tpcds', 'clean'])
        assert not _exists('call_center')
        assert _exists('custom')

        self.run_cli(['workload', 'kv', '-p', 'custom/kv/table', 'clean'])
        assert not _exists('custom/kv')
        assert _exists('custom')

        self.run_cli(['workload', 'clickbench', '-p', 'custom/clickbench/hits', 'clean'])
        assert not _exists('custom/clickbench')
        assert _exists('custom')

        self.run_cli(['workload', 'tpcds', '-p', 'custom/tpcds/s1', 'clean'])
        assert not _exists('custom/tpcds')
        assert _exists('custom')

        self.run_cli(['workload', 'tpch', '-p', 'custom/tpch/s1', 'clean'])
        assert not _exists('custom')
        children = list(filter(lambda x: not x.startswith('.'), [e.name for e in YdbCluster.get_ydb_driver().scheme_client.list_directory(f'/{YdbCluster.ydb_database}').children]))
        assert len(children) == 0

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
