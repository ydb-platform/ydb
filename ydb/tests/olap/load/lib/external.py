from __future__ import annotations
import os
from .conftest import LoadSuiteBase
from ydb.tests.olap.lib.ydb_cli import WorkloadType, CheckCanonicalPolicy


class ExternalSuiteBase(LoadSuiteBase):
    workload_type: WorkloadType = WorkloadType.EXTERNAL
    iterations: int = 1
    check_canonical: CheckCanonicalPolicy = CheckCanonicalPolicy.ERROR
    __query_list: list[str] = None

    @staticmethod
    def __get_query_list(path: str, name_prefix: list[str]) -> list[str]:
        if os.path.isfile(path):
            return [f"{'.'.join(name_prefix)}"] if path.endswith('.sql') or path.endswith('.yql') else []
        if os.path.isdir(path):
            result = []
            for child in sorted(os.listdir(path)):
                result += ExternalSuiteBase.__get_query_list(os.path.join(path, child), name_prefix + [child])
            return result
        return []

    @classmethod
    def get_query_list(cls) -> list[str]:
        if cls.__query_list is None:
            cls.__query_list = ExternalSuiteBase.__get_query_list(os.path.join(cls.get_external_path(), 'run'), [])
        return cls.__query_list

    @classmethod
    def do_setup_class(cls):
        if not cls.verify_data or os.getenv('NO_VERIFY_DATA', '0') == '1':
            return
        cls.check_tables_size(folder=cls.external_folder, tables={})

    def test(self, query_name: str):
        self.run_workload_test(self.external_folder, query_name=query_name)


def pytest_generate_tests(metafunc):
    if issubclass(metafunc.cls, ExternalSuiteBase):
        metafunc.parametrize("query_name", metafunc.cls.get_query_list())


class TestExternalA1(ExternalSuiteBase):
    external_folder: str = 'a1'


class TestExternalX1(ExternalSuiteBase):
    external_folder: str = 'x1'


class TestExternalM1(ExternalSuiteBase):
    external_folder: str = 'm1'
    iterations: int = 5


class TestExternalB1(ExternalSuiteBase):
    external_folder: str = 'b1'
    iterations: int = 5
