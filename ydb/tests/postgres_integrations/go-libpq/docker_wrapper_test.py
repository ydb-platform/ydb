# from .conftest import integrations
import typing


from os import path
from typing import Sequence

import pytest

import yatest

from ydb.tests.postgres_integrations.library import IntegrationTests,PgTestWrapper

from . import conftest

integrations = IntegrationTests(yatest.common.source_path("ydb/tests/postgres_integrations/go-libpq/data"))

class TestWrapper(PgTestWrapper):
    @classmethod
    def setup_class(cls):
        print('"rekby setup class')
        cls.initialize(integrations, conftest.selected_items)

    @classmethod
    def filter_format(cls, test_names: typing.List[str])->str:
        test_names = [item[len("golang-lib-pq/"):] for item in test_names]
        return "^(" + "|".join(test_names) + ")$"

def pytest_generate_tests(metafunc: pytest.Metafunc):
    if metafunc.definition.name == "test_pg_generated":
        integrations.pytest_generate_tests(metafunc)
