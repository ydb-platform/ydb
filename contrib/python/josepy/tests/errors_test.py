"""Tests for josepy.errors."""

import sys
import unittest

import pytest


class UnrecognizedTypeErrorTest(unittest.TestCase):
    def setUp(self) -> None:
        from josepy.errors import UnrecognizedTypeError

        self.error = UnrecognizedTypeError("foo", {"type": "foo"})

    def test_str(self) -> None:
        assert "foo was not recognized, full message: {'type': 'foo'}" == str(self.error)


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] + [__file__]))  # pragma: no cover
