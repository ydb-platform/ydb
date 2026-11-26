from ydb.tests.olap.lib.fill_tables import Dataset
import ydb.tests.olap.load.lib.tpch as tpch
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpchS1(tpch.TestTpch1, FunctionalTestBase):
    iterations: int = 1

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster()
        import_options = ('--upload-threads', '4', '--max-in-flight', '4') if cls.scale == 1 else ()
        cls.fill_tables(Dataset('tpch', f'olap_yatests/{cls._get_path()}', scale=cls.scale,
                                float_mode=cls.float_mode, import_options=import_options))
        super().setup_class()
