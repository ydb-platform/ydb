"""
Helpers for creating arrays.

!!! warning "Deprecated"
    This sub-module is deprecated. All functions here are defined in the top level zarr namespace instead.

"""

import warnings

from zarr.api.synchronous import (
    array,
    create,
    empty,
    empty_like,
    full,
    full_like,
    ones,
    ones_like,
    open_array,
    open_like,
    zeros,
    zeros_like,
)
from zarr.errors import ZarrDeprecationWarning

__all__ = [
    "array",
    "create",
    "empty",
    "empty_like",
    "full",
    "full_like",
    "ones",
    "ones_like",
    "open_array",
    "open_like",
    "zeros",
    "zeros_like",
]

warnings.warn(
    "zarr.creation is deprecated. "
    "Import these functions from the top level zarr. namespace instead.",
    ZarrDeprecationWarning,
    stacklevel=2,
)
