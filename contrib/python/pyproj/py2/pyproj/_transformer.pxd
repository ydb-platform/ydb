include "proj.pxi"

from pyproj._crs cimport Base


cdef class _Transformer(Base):
    cdef PJ_PROJ_INFO proj_info
    cdef public object input_geographic
    cdef public object output_geographic
    cdef object _input_radians
    cdef object _output_radians
    cdef public object is_pipeline
    cdef public object skip_equivalent
    cdef public object projections_equivalent
    cdef public object projections_exact_same
    cdef public object type_name
