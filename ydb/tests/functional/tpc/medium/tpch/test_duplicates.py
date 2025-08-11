import ydb.tests.olap.load.lib.tpch as tpch
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase
from ydb.tests.olap.lib.ydb_cluster import YdbCluster


class TestTpchDoubleImportBase(tpch.TestTpch1, FunctionalTestBase):
    """https://github.com/ydb-platform/ydb/issues/22253"""
    iterations: int = 1

    @classmethod
    def addition_init_params(cls) -> list[str]:
        if cls.float_mode:
            return ['--float-mode', cls.float_mode]
        return []

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls._get_path()}', 'init', '--store=column', '--datetime-types=dt64'] + cls.addition_init_params())

        for table in cls.get_tables():
            cls._alter_compaction()

        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls._get_path()}', 'import', 'generator', f'--scale={cls.scale}'])
        cls.run_cli(['workload', 'tpch', '-p', f'olap_yatests/{cls._get_path()}', 'import', 'generator', f'--scale={cls.scale}'])
        super().setup_class()


class TestTpchDuplicatesZeroLevel(TestTpchDoubleImportBase):
    """https://github.com/ydb-platform/ydb/issues/22253"""

    @classmethod
    def _alter_compaction(cls) -> str:
        set_compaction_query = f"""
            ALTER OBJECT `/{YdbCluster.ydb_database}/olap_yatests/{cls._get_path()}/{table}` (TYPE TABLE)
                SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                    {{"levels" : [{{"class_name" : "Zero", "expected_blobs_size" : 2048000}},
                                    {{"class_name" : "Zero"}}]}}`);
        """
        cls.run_cli(['table', 'query', 'execute', '-t', 'scheme', '-q', set_compaction_query])


class TestTpchDuplicatesNoCompaction(TestTpchDoubleImportBase):
    """https://github.com/ydb-platform/ydb/issues/22252"""

    @classmethod
    def _alter_compaction(cls) -> str:
        set_compaction_query = f"""
            ALTER OBJECT `/{YdbCluster.ydb_database}/olap_yatests/{cls._get_path()}/{table}` (TYPE TABLE)
                SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`lc-buckets`, `COMPACTION_PLANNER.FEATURES`=`
                    {{"levels" : [{{"class_name" : "Zero"}}]}}`);
        """
        cls.run_cli(['table', 'query', 'execute', '-t', 'scheme', '-q', set_compaction_query])
