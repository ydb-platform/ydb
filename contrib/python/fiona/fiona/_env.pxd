include "gdal.pxi"


cdef class ConfigEnv(object):
    cdef public object options


cdef class GDALEnv(ConfigEnv):
    cdef public object _have_registered_drivers


cdef _safe_osr_release(OGRSpatialReferenceH srs)
