import sys

import pytest

import markupsafe

try:
    from markupsafe import _speedups
except ImportError:
    _speedups = None  # type: ignore[assignment]


@pytest.mark.thread_unsafe(reason="Tampers with sys.modules")
@pytest.mark.skipif(_speedups is None, reason="speedups unavailable")
def test_ext_init() -> None:
    """Test that the extension module uses multi-phase init by checking that
    uncached imports result in different module objects.
    """
    if markupsafe._escape_inner is not _speedups._escape_inner:  # type: ignore[attr-defined]
        pytest.skip("speedups not active")

    for k in [k for k in sys.modules if k.startswith("markupsafe")]:
        del sys.modules[k]

    import markupsafe._speedups as new

    assert _speedups.__dict__ != new.__dict__
    assert _speedups._escape_inner is not new._escape_inner
