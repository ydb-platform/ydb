import gc
import sys
from collections.abc import Callable
from operator import not_
from typing import TYPE_CHECKING, Any, Protocol, TypeVar

import pytest

from propcache.api import cached_property

IS_PYPY = hasattr(sys, "pypy_version_info")

if sys.version_info >= (3, 11):
    from typing import assert_type

_T_co = TypeVar("_T_co", covariant=True)


class APIProtocol(Protocol):
    def cached_property(
        self, func: Callable[[Any], _T_co]
    ) -> cached_property[_T_co]: ...


def test_cached_property(propcache_module: APIProtocol) -> None:
    class A:
        def __init__(self) -> None:
            """Init."""

        @propcache_module.cached_property
        def prop(self) -> int:
            return 1

    a = A()
    if sys.version_info >= (3, 11):
        assert_type(a.prop, int)
    assert a.prop == 1


def test_cached_property_without_cache(propcache_module: APIProtocol) -> None:
    class A:

        __slots__ = ()

        def __init__(self) -> None:
            pass

        @propcache_module.cached_property
        def prop(self) -> None:
            """Mock property."""

    a = A()

    with pytest.raises(AttributeError):
        a.prop = 123  # type: ignore[assignment]


def test_cached_property_check_without_cache(propcache_module: APIProtocol) -> None:
    class A:

        __slots__ = ()

        def __init__(self) -> None:
            """Init."""

        @propcache_module.cached_property
        def prop(self) -> None:
            """Mock property."""

    a = A()
    with pytest.raises((TypeError, AttributeError)):
        assert a.prop == 1


def test_cached_property_caching(propcache_module: APIProtocol) -> None:
    class A:
        def __init__(self) -> None:
            """Init."""

        @propcache_module.cached_property
        def prop(self) -> int:
            """Docstring."""
            return 1

    a = A()
    assert a.prop == 1


def test_cached_property_class_docstring(propcache_module: APIProtocol) -> None:
    class A:
        def __init__(self) -> None:
            """Init."""

        @propcache_module.cached_property
        def prop(self) -> None:
            """Docstring."""

    if TYPE_CHECKING:
        assert isinstance(A.prop, cached_property)
    else:
        assert isinstance(A.prop, propcache_module.cached_property)
    assert "Docstring." == A.prop.__doc__


def test_set_name(propcache_module: APIProtocol) -> None:
    """Test that the __set_name__ method is called and checked."""

    class A:

        @propcache_module.cached_property
        def prop(self) -> None:
            """Docstring."""

    A.prop.__set_name__(A, "prop")

    match = r"Cannot assign the same cached_property to two "
    with pytest.raises(TypeError, match=match):
        A.prop.__set_name__(A, "something_else")


def test_get_without_set_name(propcache_module: APIProtocol) -> None:
    """Test that get without __set_name__ fails."""
    cp = propcache_module.cached_property(not_)

    class A:
        """A class."""

    A.cp = cp  # type: ignore[attr-defined]
    match = r"Cannot use cached_property instance "
    with pytest.raises(TypeError, match=match):
        _ = A().cp  # type: ignore[attr-defined]


def test_ensured_wrapped_function_is_accessible(propcache_module: APIProtocol) -> None:
    """Test that the wrapped function can be accessed from python."""

    class A:
        def __init__(self) -> None:
            """Init."""

        @propcache_module.cached_property
        def prop(self) -> int:
            """Docstring."""
            return 1

    a = A()
    assert A.prop.func(a) == 1


@pytest.mark.c_extension
@pytest.mark.skipif(IS_PYPY, reason="PyPy has no C extension")
def test_cached_property_no_refcount_leak(propcache_module: APIProtocol) -> None:
    """Test that cached_property does not leak references."""

    class CachedPropertySentinel:
        """A unique object we can track."""

    def count_sentinels() -> int:
        """Count the number of CachedPropertySentinel instances in gc."""
        gc.collect()
        return sum(
            1 for obj in gc.get_objects() if isinstance(obj, CachedPropertySentinel)
        )

    class A:
        def __init__(self) -> None:
            """Init."""

        @propcache_module.cached_property
        def prop(self) -> CachedPropertySentinel:
            """Return a sentinel object."""
            return CachedPropertySentinel()

    initial_sentinel_count = count_sentinels()

    a = A()

    # First access - creates and caches the object
    result = a.prop
    # sys.getrefcount returns 1 higher than actual (the temp ref from the call)
    # After first access: result owns 1, __dict__ owns 1, getrefcount call owns 1 = 3
    initial_refcount = sys.getrefcount(result)

    # Should have exactly 1 Sentinel instance now
    assert count_sentinels() == initial_sentinel_count + 1

    # Second access - should return the cached object without creating new refs
    result2 = a.prop
    assert result is result2
    # After second access: result owns 1, result2 owns 1, __dict__ owns 1, getrefcount call owns 1 = 4
    # Only result2 should add 1
    second_refcount = sys.getrefcount(result)
    assert second_refcount == initial_refcount + 1

    # Still should have exactly 1 Sentinel instance
    assert count_sentinels() == initial_sentinel_count + 1

    # Third access
    result3 = a.prop
    assert result is result3
    # result2 and result3 each add 1
    third_refcount = sys.getrefcount(result)
    assert third_refcount == initial_refcount + 2

    # Clean up local refs - should be back to just result and __dict__
    del result2
    del result3
    after_cleanup_refcount = sys.getrefcount(result)
    assert after_cleanup_refcount == initial_refcount

    # Clear the cache and verify no leak when re-fetching
    # After clearing: only result owns it
    del a.__dict__["prop"]
    cleared_refcount = sys.getrefcount(result)
    assert cleared_refcount == initial_refcount - 1  # No longer in __dict__

    # Re-fetch - this should create a new object, not affect old one
    result4 = a.prop
    assert result4 is not result  # Should be a new object
    refetch_refcount = sys.getrefcount(result)
    assert refetch_refcount == cleared_refcount  # Original object refcount unchanged

    # Now we should have 2 Sentinel instances:
    # - original in `result`
    # - new one in `result4`
    assert count_sentinels() == initial_sentinel_count + 2
