import pytest
import random
import string
from typing import Final

from ydb.tests.fq.streaming_common.common import Kikimr
from ydb.tests.fq.streaming_common.common import get_ydb_config
from ydb.tests.fq.streaming_common.common import set_test_env
from ydb.tests.fq.generic.utils.settings import Settings

docker_compose_file_path: Final = "ydb/tests/fq/streaming/generic/docker-compose.yml"


@pytest.fixture
def settings() -> Settings:
    return Settings.from_env(docker_compose_file_path=docker_compose_file_path)


@pytest.fixture
def kikimr(request, settings: Settings):
    set_test_env(request)
    kikimr = Kikimr(get_ydb_config(request, enable_fq_connector=settings))
    yield kikimr
    kikimr.stop()


@pytest.fixture
def entity_name(request):
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    def entity_name_wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return entity_name_wrapper
