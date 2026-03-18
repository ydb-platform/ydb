import pytest
from support_links_env import started_meta_support_links_env


@pytest.fixture
def meta_support_links_env():
    with started_meta_support_links_env() as env:
        yield env
