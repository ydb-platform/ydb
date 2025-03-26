import os

import yatest
import pytest

FLAVOUR_TO_PATH = {}

_PREFIX = "YDB_FLAVOUR_"

for key, value in os.environ.items():
    if key.startswith(_PREFIX):
        new_key = key[len(_PREFIX):]
        FLAVOUR_TO_PATH[new_key] = value

if not FLAVOUR_TO_PATH:
    raise RuntimeError("Flavours not found. Did you forget to include 'tests/library/flavours/flavours_deps.inc' in ya.make?")


@pytest.fixture(scope='module', params=FLAVOUR_TO_PATH.values(), ids=FLAVOUR_TO_PATH.keys())
def ydb_flavour_path(request):
    """
    This fixture is a way to test different ydb flavours in one parametrized test
    """
    return yatest.common.binary_path(request.param)
