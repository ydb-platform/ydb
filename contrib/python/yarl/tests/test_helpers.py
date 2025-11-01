import platform

import pytest

from yarl import _helpers, _helpers_py

IS_PYPY = platform.python_implementation() == "PyPy"


class CachedPropertyMixin:
    cached_property = NotImplemented

    def test_cached_property(self) -> None:
        class A:
            def __init__(self):
                self._cache = {}

            @self.cached_property  # type: ignore[misc]
            def prop(self):
                return 1

        a = A()
        assert a.prop == 1

    def test_cached_property_class(self) -> None:
        class A:
            def __init__(self):
                """Init."""
                # self._cache not set because its never accessed in this test

            @self.cached_property  # type: ignore[misc]
            def prop(self):
                """Docstring."""

        assert isinstance(A.prop, self.cached_property)
        assert A.prop.__doc__ == "Docstring."

    def test_cached_property_assignment(self) -> None:
        class A:
            def __init__(self):
                self._cache = {}

            @self.cached_property  # type: ignore[misc]
            def prop(self):
                """Mock property."""

        a = A()

        with pytest.raises(AttributeError):
            a.prop = 123

    def test_cached_property_without_cache(self) -> None:
        class A:
            def __init__(self):
                pass

            @self.cached_property  # type: ignore[misc]
            def prop(self):
                """Mock property."""

        a = A()

        with pytest.raises(AttributeError):
            a.prop = 123

    def test_cached_property_check_without_cache(self) -> None:
        class A:
            def __init__(self):
                pass

            @self.cached_property  # type: ignore[misc]
            def prop(self):
                """Mock property."""

        a = A()
        with pytest.raises(AttributeError):
            assert a.prop == 1


class TestPyCachedProperty(CachedPropertyMixin):
    cached_property = _helpers_py.cached_property  # type: ignore[assignment]


if (
    not _helpers.NO_EXTENSIONS
    and not IS_PYPY
    and hasattr(_helpers, "cached_property_c")
):

    class TestCCachedProperty(CachedPropertyMixin):
        cached_property = _helpers.cached_property_c  # type: ignore[assignment, attr-defined, unused-ignore] # noqa: E501
