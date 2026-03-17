"""
Async exceptions handler.

Handle excpetions in background coroutines.
"""

import pytest

from testsuite.utils import traceback


class BaseError(Exception):
    pass


class BackgroundExceptionError(BaseError):
    pass


__tracebackhide__ = traceback.hide(BaseError)


@pytest.fixture
def _asyncexc():
    errors: list[Exception] = []
    try:
        yield errors
    finally:
        _raise_if_any(errors)


@pytest.fixture
def asyncexc_append(_asyncexc):
    """Register background exception."""
    return _asyncexc.append


@pytest.fixture
def asyncexc_check(_asyncexc):
    """Raise in case there are background exceptions."""

    def check():
        _raise_if_any(_asyncexc)

    return check


def _raise_if_any(errors):
    if not errors:
        return
    errors = _clear_and_copy(errors)
    for exc in errors:
        raise BackgroundExceptionError(
            f'There were {len(errors)} background exceptions'
            f', showing the first one',
        ) from exc


def _clear_and_copy(errors):
    copy = errors[:]
    errors.clear()
    return copy
