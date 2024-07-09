# from .conftest import integrations



from os import path
from typing import Sequence

import yatest

import pytest


from ydb.tests.postgres_integrations.library import IntegrationTests

integrations=IntegrationTests(yatest.common.source_path("ydb/tests/postgres_integrations/go-libpq/data"))

def pytest_generate_tests(metafunc: pytest.Metafunc):
    integrations.pytest_generate_tests(metafunc)


def pytest_deselected(items: Sequence[pytest.Item]):
    integrations.pytest_deselected(items)


def test(testname):
    integrations.execute_test(testname)
