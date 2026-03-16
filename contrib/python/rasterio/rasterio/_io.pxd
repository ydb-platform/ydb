include "gdal.pxi"

cimport numpy as np

from rasterio._base cimport DatasetBase


cdef class DatasetReaderBase(DatasetBase):
    pass


cdef class DatasetWriterBase(DatasetReaderBase):
    cdef readonly object _init_dtype
    cdef readonly object _init_nodata
    cdef readonly object _init_gcps
    cdef readonly object _init_rpcs
    cdef readonly object _options


cdef class BufferedDatasetWriterBase(DatasetWriterBase):
    pass


cdef class MemoryDataset(DatasetWriterBase):
    cdef np.ndarray _array


cdef class MemoryFileBase:
    cdef VSILFILE * _vsif
    cdef public object _env


ctypedef np.uint8_t DTYPE_UBYTE_t
ctypedef np.uint16_t DTYPE_UINT16_t
ctypedef np.int16_t DTYPE_INT16_t
ctypedef np.uint32_t DTYPE_UINT32_t
ctypedef np.int32_t DTYPE_INT32_t
ctypedef np.float32_t DTYPE_FLOAT32_t
ctypedef np.float64_t DTYPE_FLOAT64_t


cdef bint in_dtype_range(value, dtype)

cdef int io_auto(image, GDALRasterBandH band, bint write, int resampling=*) except -1
cdef int io_band(GDALRasterBandH band, int mode, double x0, double y0, double width, double height, object data, int resampling=*) except -1
cdef int io_multi_band(GDALDatasetH hds, int mode, double x0, double y0, double width, double height, object data, Py_ssize_t[:] indexes, int resampling=*) except -1
