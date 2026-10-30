import pytest
import random
import string

from ydb.tests.fq.streaming_common.common import Kikimr
from ydb.tests.fq.streaming_common.common import get_ydb_config
from ydb.tests.fq.streaming_common.common import set_test_env


@pytest.fixture(scope="module")
def kikimr(request):
    param = getattr(request, "param", {})
    set_test_env(request)
<<<<<<< HEAD
    kikimr = Kikimr(get_ydb_config(request), enable_discovery=param.get("enable_discovery", True))
=======
    config = get_ydb_config(request)
    if max_message_size := param.get("max_message_size"):
        config.yaml_config["grpc_config"]["max_message_size"] = max_message_size
        config.yaml_config["pqconfig"]["max_message_size_bytes"] = max_message_size
    kikimr = Kikimr(
        config,
        enable_discovery=param.get("enable_discovery", True),
        tenant_database="/Root/my_tenant",
    )
>>>>>>> 95446e4dcbc (YQ-5796 fix big message topic processing (#57672))
    yield kikimr
    kikimr.stop()


@pytest.fixture
def entity_name(request):
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    def entity_name_wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return entity_name_wrapper
