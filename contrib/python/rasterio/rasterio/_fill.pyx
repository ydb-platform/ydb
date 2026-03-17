# distutils: language = c++
# cython: c_string_type=unicode, c_string_encoding=utf8

"""Raster fill."""

include "gdal.pxi"

from contextlib import ExitStack

import numpy as np
from rasterio._err cimport exc_wrap_int
from rasterio._io cimport MemoryDataset


def _fillnodata(
    image,
    mask,
    double max_search_distance=100.0,
    int smoothing_iterations=0,
    **filloptions
):
    cdef GDALRasterBandH image_band = NULL
    cdef GDALRasterBandH mask_band = NULL
    cdef char **alg_options = NULL
    cdef MemoryDataset image_dataset = None
    cdef MemoryDataset mask_dataset = None

    with ExitStack() as exit_stack:
        # copy numpy ndarray into an in-memory dataset.
        image_dataset = exit_stack.enter_context(MemoryDataset(image))
        image_band = image_dataset.band(1)

        if mask is not None:
            mask_cast = mask.astype(np.uint8)
            mask_dataset = exit_stack.enter_context(MemoryDataset(mask_cast))
            mask_band = mask_dataset.band(1)

        try:
            for k, v in filloptions.items():
                k = k.upper()
                v = str(v)
                alg_options = CSLSetNameValue(alg_options, k, v)

            if CSLFindName(alg_options, "TEMP_FILE_DRIVER") < 0:
                alg_options = CSLSetNameValue(alg_options, "TEMP_FILE_DRIVER", "MEM")

            exc_wrap_int(
                GDALFillNodata(
                    image_band,
                    mask_band,
                    max_search_distance,
                    0,
                    smoothing_iterations,
                    alg_options,
                    NULL,
                    NULL
                )
            )
            return np.asarray(image_dataset)
        finally:
            CSLDestroy(alg_options)
