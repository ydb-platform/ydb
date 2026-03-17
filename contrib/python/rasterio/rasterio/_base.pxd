include "gdal.pxi"


cdef class DatasetBase:

    cdef GDALDatasetH _hds
    cdef readonly object name
    cdef readonly object mode
    cdef readonly object options
    cdef readonly object width
    cdef readonly object height
    cdef readonly object shape
    cdef public object driver
    cdef public object _count
    cdef public object _dtypes
    cdef public object _crs
    cdef public object _crs_wkt
    cdef public object _transform
    cdef public object _block_shapes
    cdef public object _nodatavals
    cdef public object _units
    cdef public object _descriptions
    cdef public object _scales
    cdef public object _offsets
    cdef public object _read
    cdef public object _gcps
    cdef public object _rpcs
    cdef public object _env
    cdef GDALDatasetH handle(self) except NULL
    cdef GDALRasterBandH band(self, int bidx) except NULL


cdef const char *get_driver_name(GDALDriverH driver)

cdef void osr_set_traditional_axis_mapping_strategy(OGRSpatialReferenceH hSrs)
cdef GDALDatasetH open_dataset(object filename, int flags, object allowed_drivers, object open_options, bint sharing, object siblings) except NULL
