# -*- coding: utf-8 -*-
"""Apply PYTEST_TIMEOUT to the test function only.

Setup and teardown stay unbounded so undo/delete can finish after a hang.
"""

import pytest


def pytest_collection_modifyitems(items):
    for item in items:
        if item.get_closest_marker('timeout') is None:
            item.add_marker(pytest.mark.timeout(60, func_only=True))
