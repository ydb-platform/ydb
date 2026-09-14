import pytest
import yatest.common

from oidc_proxy_env import started_oidc_proxy_env, started_oidc_proxy_full_flow_env


@pytest.fixture(autouse=True)
def skip_oidc_proxy_functional_tests_under_asan():
    if yatest.common.context.sanitize == "address":
        pytest.skip("mvp_oidc_proxy functional suite is not supported under address sanitizer")


@pytest.fixture
def oidc_proxy_env():
    with started_oidc_proxy_env() as env:
        yield env


@pytest.fixture
def oidc_proxy_full_flow_env():
    with started_oidc_proxy_full_flow_env() as env:
        yield env
