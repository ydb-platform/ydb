import pytest
import ydb.tests.olap.load.lib.upload as upload
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib import fill_tables
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper


class TestUploadTpchS0_1(upload.UploadTpchBase, FunctionalTestBase):
    scale: float = 0.1

    @classmethod
    def setup_class(cls) -> None:
        YdbCluster._tables_path = ''
        cls.setup_cluster()
        super().setup_class()

    @classmethod
    def do_teardown_class(cls) -> None:
        try:
            super().do_teardown_class()
        finally:
            cls.cluster.stop()


class TestDataShardCompaction:
    @pytest.mark.parametrize('method', ['sdk', 'legacy'])
    def test_datashard_compaction(self, method, monkeypatch):
        class ClusterOwner(FunctionalTestBase):
            pass

        calls = []
        name = f'force_datashard_compact_{method}'
        compact = getattr(fill_tables, name)

        def record_compaction(*args):
            calls.extend(args[-2])
            return compact(*args)

        monkeypatch.setattr(fill_tables, name, record_compaction)
        path = 'upload/row_tpch'
        try:
            ClusterOwner.setup_cluster()
            with fill_tables.TableFiller(YdbCliHelper.get_cli_path(), attempts=1) as filler:
                options = {} if method == 'sdk' else {'datashard_compaction': method}
                filler.fill([fill_tables.Dataset('tpch', path, scale='0.01', store='row')],
                            compaction_timeout=120, **options)
            assert f'/{YdbCluster.ydb_database}/{path}/customer' in calls
            assert len(calls) == 8
        finally:
            if ClusterOwner.cluster is not None:
                if YdbCluster._ydb_driver is not None:
                    YdbCluster.get_ydb_driver().stop()
                ClusterOwner.cluster.stop()
