include "gdal.pxi"


cdef class CRS:
    cdef OGRSpatialReferenceH _osr
    cdef object _data
    cdef object _epsg
    cdef object _wkt


cdef void osr_set_traditional_axis_mapping_strategy(OGRSpatialReferenceH hSrs)
