import pytest

from flex.constants import (
    EMPTY,
)
from flex.decorators import (
    skip_if_empty,
)


def test_skips_single_arg():
    @skip_if_empty
    def test_fn(value):
        assert False, "Should be skipped"

    test_fn(EMPTY)

    with pytest.raises(AssertionError):
        test_fn(1)
