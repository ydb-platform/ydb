import os
import sys
import time
from contextlib import contextmanager
from unittest.mock import patch

import pytest

__all__ = [
    "ZoneInfo",
    "AlwaysEqual",
    "NeverEqual",
    "AlwaysLarger",
    "AlwaysSmaller",
]

IS_WINDOWS = sys.platform == "win32"


if sys.version_info >= (3, 9):
    from zoneinfo import ZoneInfo
else:
    from backports.zoneinfo import ZoneInfo


class AlwaysEqual:
    def __eq__(self, other):
        return True


class NeverEqual:
    def __eq__(self, other):
        return False


class AlwaysLarger:
    def __lt__(self, other):
        return False

    def __le__(self, other):
        return False

    def __gt__(self, other):
        return True

    def __ge__(self, other):
        return True


class AlwaysSmaller:
    def __lt__(self, other):
        return True

    def __le__(self, other):
        return True

    def __gt__(self, other):
        return False

    def __ge__(self, other):
        return False


@contextmanager
def system_tz_ams():
    if IS_WINDOWS:
        pytest.skip("tzset is not available on Windows")
    try:
        with patch.dict(os.environ, {"TZ": "Europe/Amsterdam"}):
            time.tzset()
            yield
    finally:
        time.tzset()


@contextmanager
def system_tz(name):
    if IS_WINDOWS:
        pytest.skip("tzset is not available on Windows")
    try:
        with patch.dict(os.environ, {"TZ": name}):
            time.tzset()
            yield
    finally:
        time.tzset()


@contextmanager
def system_tz_nyc():
    if IS_WINDOWS:
        pytest.skip("tzset is not available on Windows")
    try:
        with patch.dict(os.environ, {"TZ": "America/New_York"}):
            time.tzset()
            yield
    finally:
        time.tzset()
