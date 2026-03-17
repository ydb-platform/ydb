import os

import pytest

from pytest_codeblock.constants import CODEBLOCK_MARK

__author__ = "Artur Barseghyan <artur.barseghyan@gmail.com>"
__copyright__ = "2025 Artur Barseghyan"
__license__ = "MIT"
__all__ = (
    "pytest_collection_modifyitems",
    "pytest_runtest_setup",
    "pytest_runtest_teardown",
)


# Modify test item during collection
def pytest_collection_modifyitems(config, items):
    for item in items:
        ...


# Setup before test runs
def pytest_runtest_setup(item):
    if item.get_closest_marker("openai"):
        ...


# Teardown after the test ends
def pytest_runtest_teardown(item, nextitem):
    ...
