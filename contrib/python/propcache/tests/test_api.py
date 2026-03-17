"""Test we do not break the public API."""

from propcache import _helpers, api


def test_api() -> None:
    """Verify the public API is accessible."""
    assert api.cached_property is not None
    assert api.under_cached_property is not None
    assert api.cached_property is _helpers.cached_property
    assert api.under_cached_property is _helpers.under_cached_property
