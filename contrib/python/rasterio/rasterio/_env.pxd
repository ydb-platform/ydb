include "gdal.pxi"


cdef class ConfigEnv:
    cdef public object options


cdef class GDALEnv(ConfigEnv):
    pass
