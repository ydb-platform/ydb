import ydb.tests.postgres_integrations.library
import pytest


def pytest_collection_finish(session: pytest.Session):
    ydb.tests.postgres_integrations.library.pytest_collection_finish(session)
