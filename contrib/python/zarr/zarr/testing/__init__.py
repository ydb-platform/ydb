import importlib.util
import warnings

from zarr.errors import ZarrUserWarning

if importlib.util.find_spec("pytest") is not None:
    from zarr.testing.store import StoreTests
else:
    warnings.warn(
        "pytest not installed, skipping test suite", category=ZarrUserWarning, stacklevel=2
    )

from zarr.testing.utils import assert_bytes_equal

# TODO: import public buffer tests?

__all__ = ["StoreTests", "assert_bytes_equal"]
