# from .conftest import integrations
import typing

import pytest
import yatest

from ydb.tests.postgres_integrations import library as tl


def filter_formatter(test_names: typing.List[str]) -> str:
    test_names = [item[len("golang-lib-pq/"):] for item in test_names]
    return "^(" + "|".join(test_names) + ")$"


def setup_module(module: pytest.Module):
    tl.setup_module(module)


def teardown_module(module: pytest.Module):
    tl.teardown_module(module)


def test_pg_generated(testname):
    tl.execute_test(testname)


def pytest_generate_tests(metafunc: pytest.Metafunc):
    if metafunc.definition.name == "test_pg_generated":
        tl.pytest_generate_tests(metafunc)


tl.set_filter_formatter(filter_formatter)
tl.set_tests_folder(yatest.common.source_path("ydb/tests/postgres_integrations/go-libpq/data"))
