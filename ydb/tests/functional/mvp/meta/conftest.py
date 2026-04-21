import pytest
import yatest.common
from support_links_env import started_meta_support_links_env


@pytest.fixture(autouse=True)
def skip_meta_functional_tests_under_asan():
    if yatest.common.context.sanitize == "address":
        pytest.skip("KiKiMR startup via ydbd is not supported under address sanitizer in this suite")


@pytest.fixture
def meta_support_links_env():
    with started_meta_support_links_env() as env:
        yield env
