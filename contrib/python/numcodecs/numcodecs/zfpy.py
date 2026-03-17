import warnings
from contextlib import suppress
from importlib.metadata import PackageNotFoundError, version
from types import ModuleType

_zfpy: ModuleType | None = None

_zfpy_version: tuple = ()
with suppress(PackageNotFoundError):
    _zfpy_version = tuple(map(int, version("zfpy").split(".")))

if _zfpy_version:
    # Check NumPy version
    _numpy_version: tuple = tuple(map(int, version("numpy").split('.')))
    if _numpy_version >= (2, 0, 0) and _zfpy_version < (1, 0, 1):  # pragma: no cover
        _zfpy_version = ()
        warnings.warn(
            "NumPy version >= 2.0.0 detected. The zfpy library is incompatible with this version of NumPy. "
            "Please downgrade to NumPy < 2.0.0 or wait for an update from zfpy.",
            UserWarning,
            stacklevel=2,
        )
    else:
        with suppress(ImportError):
            import zfpy as _zfpy  # type: ignore[no-redef]

if _zfpy:
    import numpy as np

    from .abc import Codec
    from .compat import ensure_bytes, ensure_contiguous_ndarray, ndarray_copy

    # noinspection PyShadowingBuiltins
    class ZFPY(Codec):
        """Codec providing compression using zfpy via the Python standard
        library.

        Parameters
        ----------
        mode : integer
            One of the zfpy mode choice, e.g., ``zfpy.mode_fixed_accuracy``.
        tolerance : double, optional
            A double-precision number, specifying the compression accuracy needed.
        rate : double, optional
            A double-precision number, specifying the compression rate needed.
        precision : int, optional
            A integer number, specifying the compression precision needed.

        """

        codec_id = "zfpy"

        def __init__(
            self,
            mode=_zfpy.mode_fixed_accuracy,
            tolerance=-1,
            rate=-1,
            precision=-1,
            compression_kwargs=None,
        ):
            self.mode = mode
            if mode == _zfpy.mode_fixed_accuracy:
                self.compression_kwargs = {"tolerance": tolerance}
            elif mode == _zfpy.mode_fixed_rate:
                self.compression_kwargs = {"rate": rate}
            elif mode == _zfpy.mode_fixed_precision:
                self.compression_kwargs = {"precision": precision}

            self.tolerance = tolerance
            self.rate = rate
            self.precision = precision

        def encode(self, buf):
            # not flatten c-order array and raise exception for f-order array
            if not isinstance(buf, np.ndarray):
                raise TypeError(
                    "The zfp codec does not support none numpy arrays."
                    f" Your buffers were {type(buf)}."
                )
            if buf.flags.c_contiguous:
                flatten = False
            else:
                raise ValueError(
                    "The zfp codec does not support F order arrays. "
                    f"Your arrays flags were {buf.flags}."
                )
            buf = ensure_contiguous_ndarray(buf, flatten=flatten)

            # do compression
            return _zfpy.compress_numpy(buf, write_header=True, **self.compression_kwargs)

        def decode(self, buf, out=None):
            # normalise inputs
            buf = ensure_bytes(buf)
            if out is not None:
                out = ensure_contiguous_ndarray(out)

            # do decompression
            dec = _zfpy.decompress_numpy(buf)

            # handle destination
            if out is not None:
                return ndarray_copy(dec, out)
            else:
                return dec

        def __repr__(self):
            return (
                f"{type(self).__name__}(mode={self.mode!r}, "
                f"tolerance={self.tolerance}, rate={self.rate}, "
                f"precision={self.precision})"
            )
