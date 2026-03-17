from typing import Final

import cffi

import numpy as np

try:
    # Note: IDE might complain as this import comes from a cffi C extension
    from timezonefinder import inside_polygon_ext  # type: ignore

    clang_extension_loaded = True
    ffi = cffi.FFI()

except ImportError:
    clang_extension_loaded = False
    inside_polygon_ext = None
    ffi = None

INT_LIST_REP: Final[str] = "int []"


def pt_in_poly_clang(x: int, y: int, coords: np.ndarray) -> bool:
    """wrapper of the point in polygon test algorithm C extension

    ATTENTION: the input numpy arrays must have a C_CONTIGUOUS memory layout
    https://numpy.org/doc/stable/reference/generated/numpy.ascontiguousarray.html?highlight=ascontiguousarray#numpy.ascontiguousarray
    """
    if not clang_extension_loaded:
        raise ValueError(
            "Trying to use the clang implementation of the point in polygon algorithm "
            "while the C extension in not loaded."
        )
    x_coords = coords[0]
    y_coords = coords[1]
    nr_coords = len(x_coords)

    y_coords = np.ascontiguousarray(y_coords)
    x_coords = np.ascontiguousarray(x_coords)
    x_coords_ffi = ffi.from_buffer(INT_LIST_REP, x_coords)
    y_coords_ffi = ffi.from_buffer(INT_LIST_REP, y_coords)
    contained = inside_polygon_ext.lib.inside_polygon_int(
        x, y, nr_coords, x_coords_ffi, y_coords_ffi
    )
    return contained
