import logging
import pytest
import random
import string

from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config, set_test_env
from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.compatibility.fixtures import inter_stable_binary_path

logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def kikimr(request):
    param = getattr(request, "param", {})
    set_test_env(request)

    main_binary_path = kikimr_driver_path()

    logger.info(f"Main binary path: {main_binary_path}")
    logger.info(f"Stable binary path: {inter_stable_binary_path}")

    config = get_ydb_config(request)
    config.set_binary_paths([main_binary_path])

    kikimr = Kikimr(
        config,
        main_binary_path=main_binary_path,
        stable_binary_path=inter_stable_binary_path,
        enable_discovery=param.get("enable_discovery", True),
        tenant_database="/Root/my_tenant",
    )
    yield kikimr
    kikimr.stop()


@pytest.fixture
def entity_name(request):
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    def entity_name_wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return entity_name_wrapper
