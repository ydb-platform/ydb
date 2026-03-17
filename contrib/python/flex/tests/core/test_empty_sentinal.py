import pytest

from flex.constants import EMPTY


def test_empty_not_comparable():
    with pytest.raises(TypeError):
        EMPTY < 10
