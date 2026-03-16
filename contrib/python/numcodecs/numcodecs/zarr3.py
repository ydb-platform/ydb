"""
This module is DEPRECATED. It will may be removed entirely in a future release of Numcodecs.
The codecs exported here are available in Zarr Python >= 3.1.3
"""

from __future__ import annotations

import importlib
import warnings
from importlib.metadata import version
from typing import Any

from packaging.version import Version


def __getattr__(name: str) -> Any:
    """
    Emit a warning when someone imports from this module
    """
    if name in __all__:
        msg = (
            "The numcodecs.zarr3 module is deprecated and will be removed in a future release of numcodecs. "
            f"Import {name} via zarr.codecs.numcodecs.{name} instead. This requires Zarr Python >= 3.1.3. "
        )

        warnings.warn(msg, DeprecationWarning, stacklevel=2)
        module = importlib.import_module("zarr.codecs.numcodecs")
        obj = getattr(module, name)
        globals()[name] = obj  # cache so subsequent lookups skip __getattr__
        return obj
    raise AttributeError(f"module {__name__} has no attribute {name}")


try:
    import zarr  # noqa: F401

    zarr_version = version('zarr')
    if Version(zarr_version) < Version("3.1.3"):  # pragma: no cover
        msg = f"Zarr 3.1.3 or later is required to use the numcodecs zarr integration. Got {zarr_version}."
        raise ImportError(msg)
except ImportError as e:  # pragma: no cover
    msg = "zarr could not be imported. Zarr 3.1.3 or later is required to use the numcodecs zarr integration."
    raise ImportError(msg) from e

__all__ = [
    "BZ2",  # noqa: F822
    "CRC32",  # noqa: F822
    "CRC32C",  # noqa: F822
    "LZ4",  # noqa: F822
    "LZMA",  # noqa: F822
    "ZFPY",  # noqa: F822
    "Adler32",  # noqa: F822
    "AsType",  # noqa: F822
    "BitRound",  # noqa: F822
    "Blosc",  # noqa: F822
    "Delta",  # noqa: F822
    "FixedScaleOffset",  # noqa: F822
    "Fletcher32",  # noqa: F822
    "GZip",  # noqa: F822
    "JenkinsLookup3",  # noqa: F822
    "PCodec",  # noqa: F822
    "PackBits",  # noqa: F822
    "Quantize",  # noqa: F822
    "Shuffle",  # noqa: F822
    "Zlib",  # noqa: F822
    "Zstd",  # noqa: F822
]
