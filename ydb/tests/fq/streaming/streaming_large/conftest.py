import pytest
import random
import string

from ydb.tests.fq.streaming_common.common import Kikimr
from ydb.tests.fq.streaming_common.common import get_ydb_config
from ydb.tests.fq.streaming_common.common import set_test_env


@pytest.fixture(scope="module")
def kikimr(request):
    set_test_env(request)
    kikimr = Kikimr(get_ydb_config(request))
    yield kikimr
    kikimr.stop()


@pytest.fixture
def entity_name(request):
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    def entity_name_wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return entity_name_wrapper
