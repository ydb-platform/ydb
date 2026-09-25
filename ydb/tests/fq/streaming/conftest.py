import logging
import pytest
import random
import string

from ydb.tests.fq.streaming_common.common import Kikimr, get_ydb_config, set_test_env


logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def kikimr(request):
    param = getattr(request, "param", {})
    set_test_env(request)
    config = get_ydb_config(request)
    if "enable_htap_tx" in param:
        config.yaml_config["table_service_config"]["enable_htap_tx"] = param["enable_htap_tx"]
    kikimr = Kikimr(
        config,
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
