# Python module tests

"""Tests of the snuggs module."""

import pytest  # type: ignore

from fiona._vendor import snuggs


@pytest.mark.parametrize("arg", ["''", "null", "false", 0])
def test_truth_false(arg):
    """Expression is not true."""
    assert not snuggs.eval(f"(truth {arg})")


@pytest.mark.parametrize("arg", ["'hi'", "true", 1])
def test_truth(arg):
    """Expression is true."""
    assert snuggs.eval(f"(truth {arg})")


@pytest.mark.parametrize("arg", ["''", "null", "false", 0])
def test_not(arg):
    """Expression is true."""
    assert snuggs.eval(f"(not {arg})")
