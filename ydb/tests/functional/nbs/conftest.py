# -*- coding: utf-8 -*-
import os

import pytest

_DEFAULT_TIMEOUT = int(os.environ.get('PYTEST_TIMEOUT') or 60)


def pytest_collection_modifyitems(items):
    for item in items:
        if item.get_closest_marker('timeout') is None:
            item.add_marker(pytest.mark.timeout(_DEFAULT_TIMEOUT, func_only=True))
