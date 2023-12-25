import pytest

from yarl._url import cached_property


class A:
    def __init__(self):
        self._cache = {}

    @cached_property
    def prop(self):
        """Docstring."""
        return 1


def test_reify():
    a = A()
    assert 1 == a.prop


def test_reify_class():
    assert isinstance(A.prop, cached_property)
    assert "Docstring." == A.prop.__doc__


def test_reify_assignment():
    a = A()

    with pytest.raises(AttributeError):
        a.prop = 123
