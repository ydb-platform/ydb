import logging
import pytest
import random
import string

from ydb.tests.library.harness.param_constants import kikimr_driver_path
from ydb.tests.library.compatibility.fixtures import inter_stable_binary_path
from ydb.tests.fq.streaming_common.common import Kikimr, counter_nodes, get_ydb_config, set_test_env

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

    if max_message_size := param.get("max_message_size"):
        config.yaml_config["grpc_config"]["max_message_size"] = max_message_size
        config.yaml_config["pqconfig"]["max_message_size_bytes"] = max_message_size
    kikimr = Kikimr(
        config,
        main_binary_path=main_binary_path,
        stable_binary_path=inter_stable_binary_path,
        enable_discovery=param.get("enable_discovery", True),
        tenant_database="/Root/my_tenant",
        is_compatibility_tests=param.get("is_compatibility_tests", False),
    )
    yield kikimr
    kikimr.stop()


@pytest.fixture
def wait_initial_resources(kikimr: Kikimr) -> None:
    # A partial planner snapshot can place all readers on one node, causing
    # the health check to restart the query during test assertions.
    kikimr.wait_kqp_node_count(expected_count=len(counter_nodes(kikimr.cluster)), timeout_seconds=120)


@pytest.fixture
def entity_name(request):
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    def entity_name_wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return entity_name_wrapper
