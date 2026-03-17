include "gdal.pxi"


cdef class CRS:

    cdef OGRSpatialReferenceH _osr
    cdef object _data
    cdef public object _epsg
    cdef public object _wkt
    cdef object _geodetic_crs
