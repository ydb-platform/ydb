import logging
import os
import pytest
import random
import string
import time

from ydb.tests.fq.streaming_common.common import Kikimr, YdbClient, get_ydb_config, set_test_env
from ydb.tests.tools.datastreams_helpers.control_plane import Endpoint
from ydb.tests.library.harness.param_constants import kikimr_driver_path


logger = logging.getLogger(__name__)


@pytest.fixture(scope="module")
def kikimr(request):
    param = getattr(request, "param", {})
    set_test_env(request)
    kikimr = Kikimr(get_ydb_config(request), enable_discovery=param.get("enable_discovery", True),
        tenant_database="/Root/romashka")
    yield kikimr
    kikimr.stop()


@pytest.fixture
def entity_name(request):
    suffix = ''.join(random.choices(string.ascii_letters + string.digits, k=8))

    def entity_name_wrapper(name: str) -> str:
        return f"{name}_{suffix}"

    return entity_name_wrapper
