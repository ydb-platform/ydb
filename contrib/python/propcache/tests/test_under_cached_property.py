import gc
import sys
from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Protocol, TypedDict, TypeVar

import pytest

from propcache.api import under_cached_property

IS_PYPY = hasattr(sys, "pypy_version_info")

if sys.version_info >= (3, 11):
    from typing import assert_type

_T_co = TypeVar("_T_co", covariant=True)


class APIProtocol(Protocol):
    def under_cached_property(
        self, func: Callable[[Any], _T_co]
    ) -> under_cached_property[_T_co]: ...


def test_under_cached_property(propcache_module: APIProtocol) -> None:
    class A:
        def __init__(self) -> None:
            self._cache: dict[str, int] = {}

        @propcache_module.under_cached_property
        def prop(self) -> int:
            return 1

        @propcache_module.under_cached_property
        def prop2(self) -> str:
            return "foo"

    a = A()
    if sys.version_info >= (3, 11):
        assert_type(a.prop, int)
    assert a.prop == 1
    if sys.version_info >= (3, 11):
        assert_type(a.prop2, str)
    assert a.prop2 == "foo"


def test_under_cached_property_typeddict(propcache_module: APIProtocol) -> None:
    """Test static typing passes with TypedDict."""

    class _Cache(TypedDict, total=False):
        prop: int
        prop2: str

    class A:
        def __init__(self) -> None:
            self._cache: _Cache = {}

        @propcache_module.under_cached_property
        def prop(self) -> int:
            return 1

        @propcache_module.under_cached_property
        def prop2(self) -> str:
            return "foo"

    a = A()
    if sys.version_info >= (3, 11):
        assert_type(a.prop, int)
    assert a.prop == 1
    if sys.version_info >= (3, 11):
        assert_type(a.prop2, str)
    assert a.prop2 == "foo"


def test_under_cached_property_assignment(propcache_module: APIProtocol) -> None:
    class A:
        def __init__(self) -> None:
            self._cache: dict[str, Any] = {}

        @propcache_module.under_cached_property
        def prop(self) -> None:
            """Mock property."""

    a = A()

    with pytest.raises(AttributeError):
        a.prop = 123  # type: ignore[assignment]


def test_under_cached_property_without_cache(propcache_module: APIProtocol) -> None:
    class A:
        def __init__(self) -> None:
            """Init."""
            self._cache: dict[str, int] = {}

        @propcache_module.under_cached_property
        def prop(self) -> None:
            """Mock property."""

    a = A()

    with pytest.raises(AttributeError):
        a.prop = 123  # type: ignore[assignment]


def test_under_cached_property_check_without_cache(
    propcache_module: APIProtocol,
) -> None:
    class A:
        def __init__(self) -> None:
            """Init."""
            # Note that self._cache is intentionally missing
            # here to verify AttributeError

        @propcache_module.under_cached_property
        def prop(self) -> None:
            """Mock property."""

    a = A()
    with pytest.raises(AttributeError):
        _ = a.prop  # type: ignore[call-overload]


def test_under_cached_property_caching(propcache_module: APIProtocol) -> None:
    class A:
        def __init__(self) -> None:
            self._cache: dict[str, int] = {}

        @propcache_module.under_cached_property
        def prop(self) -> int:
            """Docstring."""
            return 1

    a = A()
    assert a.prop == 1


def test_under_cached_property_class_docstring(propcache_module: APIProtocol) -> None:
    class A:
        def __init__(self) -> None:
            """Init."""

        @propcache_module.under_cached_property
        def prop(self) -> None:
            """Docstring."""

    if TYPE_CHECKING:
        # At type checking, the fixture doesn't represent the real module, so
        # we use the global-level imported module to verify the isinstance() check here
        # matches the behaviour users would see in real code.
        assert isinstance(A.prop, under_cached_property)
    else:
        assert isinstance(A.prop, propcache_module.under_cached_property)
    assert "Docstring." == A.prop.__doc__


def test_ensured_wrapped_function_is_accessible(propcache_module: APIProtocol) -> None:
    """Test that the wrapped function can be accessed from python."""

    class A:
        def __init__(self) -> None:
            """Init."""
            self._cache: dict[str, int] = {}

        @propcache_module.under_cached_property
        def prop(self) -> int:
            """Docstring."""
            return 1

    a = A()
    assert A.prop.wrapped(a) == 1


@pytest.mark.c_extension
@pytest.mark.skipif(IS_PYPY, reason="PyPy has no C extension")
def test_under_cached_property_no_refcount_leak(propcache_module: APIProtocol) -> None:
    """Test that under_cached_property does not leak references."""

    class UnderCachedPropertySentinel:
        """A unique object we can track."""

    def count_sentinels() -> int:
        """Count the number of UnderCachedPropertySentinel instances in gc."""
        gc.collect()
        return sum(
            1
            for obj in gc.get_objects()
            if isinstance(obj, UnderCachedPropertySentinel)
        )

    class A:
        def __init__(self) -> None:
            """Init."""
            self._cache: dict[str, Any] = {}

        @propcache_module.under_cached_property
        def prop(self) -> UnderCachedPropertySentinel:
            """Return a sentinel object."""
            return UnderCachedPropertySentinel()

    initial_sentinel_count = count_sentinels()

    a = A()

    # First access - creates and caches the object
    result = a.prop
    # sys.getrefcount returns 1 higher than actual (the temp ref from the call)
    # After first access: result owns 1, _cache owns 1, getrefcount call owns 1 = 3
    initial_refcount = sys.getrefcount(result)

    # Should have exactly 1 Sentinel instance now
    assert count_sentinels() == initial_sentinel_count + 1

    # Second access - should return the cached object without creating new refs
    result2 = a.prop
    assert result is result2
    # After second access: result owns 1, result2 owns 1, _cache owns 1, getrefcount call owns 1 = 4
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

    # Clean up local refs - should be back to just result and _cache
    del result2
    del result3
    after_cleanup_refcount = sys.getrefcount(result)
    assert after_cleanup_refcount == initial_refcount

    # Clear the cache and verify no leak when re-fetching
    # After clearing: only result owns it
    del a._cache["prop"]
    cleared_refcount = sys.getrefcount(result)
    assert cleared_refcount == initial_refcount - 1  # No longer in _cache

    # Re-fetch - this should create a new object, not affect old one
    result4 = a.prop
    assert result4 is not result  # Should be a new object
    refetch_refcount = sys.getrefcount(result)
    assert refetch_refcount == cleared_refcount  # Original object refcount unchanged

    # Now we should have 2 Sentinel instances:
    # - original in `result`
    # - new one in `result4`
    assert count_sentinels() == initial_sentinel_count + 2
