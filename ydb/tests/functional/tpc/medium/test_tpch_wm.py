from ydb.tests.olap.lib.fill_tables import Dataset
import ydb.tests.workload_manager.common.workload_manager as wm
from ydb.tests.functional.tpc.lib.conftest import FunctionalTestBase


class TestTpchWMS0_1(wm.WorkloadManagerTpchBase, wm.WorkloadManagerConcurrentQueryLimit, FunctionalTestBase):
    tables_size: dict[str, int] = {
        'lineitem': 600572,
    }
    scale: float = 0.1
    iterations: int = 1

    @classmethod
    def setup_class(cls) -> None:
        cls.setup_cluster_ext(extra_feature_flags=['enable_composite_conveyor_scheduling'])
        cls.fill_tables(Dataset('tpch', f'olap_yatests/{cls.get_path()}', scale=cls.scale, float_mode=cls.float_mode))
        super().setup_class()
