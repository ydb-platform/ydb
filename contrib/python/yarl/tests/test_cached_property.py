import pytest

from yarl._url import cached_property  # type: ignore[attr-defined]


class A:
    def __init__(self) -> None:
        self._cache: dict[str, int] = {}

    @cached_property
    def prop(self) -> int:
        """Docstring."""
        return 1


def test_reify() -> None:
    a = A()
    assert 1 == a.prop


def test_reify_class() -> None:
    assert isinstance(A.prop, cached_property)
    assert "Docstring." == A.prop.__doc__


def test_reify_assignment() -> None:
    a = A()

    with pytest.raises(AttributeError):
        a.prop = 123
