import pytest

from flex.constants import (
    EMPTY,
)
from flex.decorators import (
    skip_if_any_kwargs_empty,
)


def test_skips_single_kwarg():
    @skip_if_any_kwargs_empty('value')
    def test_fn(value):
        assert False, "Should be skipped"

    test_fn(value=EMPTY)

    with pytest.raises(AssertionError):
        test_fn(value=1)

    with pytest.raises(AssertionError):
        # not a kwarg
        test_fn(EMPTY)


def test_skips_multiple_kwargs():
    @skip_if_any_kwargs_empty('value_a', 'value_b')
    def test_fn(value_a, value_b):
        assert False, "Should be skipped"

    with pytest.raises(AssertionError):
        # not kwargs
        test_fn(EMPTY, EMPTY)

    test_fn(value_a=1, value_b=EMPTY)
    test_fn(value_a=EMPTY, value_b=EMPTY)
    test_fn(value_a=EMPTY, value_b=2)

    with pytest.raises(AssertionError):
        test_fn(1, 2)

    with pytest.raises(AssertionError):
        test_fn(value_a=1, value_b=2)


def test_ignores_undeclared_kwargs():
    @skip_if_any_kwargs_empty('value_a', 'value_b')
    def test_fn(value_a, value_b, value_c):
        assert False, "Should be skipped"

    with pytest.raises(AssertionError):
        # not kwargs
        test_fn(value_a=1, value_b=2, value_c=EMPTY)
