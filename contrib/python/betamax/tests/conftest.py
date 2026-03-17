import betamax
import pytest

import yatest.common


@pytest.fixture(scope="session", autouse=True)
def betamax_config():
    with betamax.Betamax.configure() as config:
        config.cassette_library_dir = yatest.common.source_path("contrib/python/betamax/tests/cassettes/")
