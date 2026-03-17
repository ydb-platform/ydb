"""Tests for josepy.magic_typing."""

import sys
import warnings
from unittest import mock

import pytest


def test_import_success() -> None:
    import typing as temp_typing

    typing_class_mock = mock.MagicMock()
    text_mock = mock.MagicMock()
    typing_class_mock.Text = text_mock
    sys.modules["typing"] = typing_class_mock
    if "josepy.magic_typing" in sys.modules:
        del sys.modules["josepy.magic_typing"]  # pragma: no cover
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        from josepy.magic_typing import Text
    assert Text == text_mock
    del sys.modules["josepy.magic_typing"]
    sys.modules["typing"] = temp_typing


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] + [__file__]))  # pragma: no cover
