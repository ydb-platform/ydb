include "proj.pxi"

from pyproj._crs cimport _CRS, Base


cdef class _TransformerGroup:
    cdef PJ_CONTEXT* context
    cdef readonly object _context_manager
    cdef readonly list _transformers
    cdef readonly list _unavailable_operations
    cdef readonly list _best_available

cdef class _Transformer(Base):
    cdef PJ_PROJ_INFO proj_info
    cdef readonly _area_of_use
    cdef readonly str type_name
    cdef readonly tuple _operations
    cdef readonly _CRS _source_crs
    cdef readonly _CRS _target_crs

    @staticmethod
    cdef _Transformer _from_pj(
        PJ_CONTEXT* context,
        PJ *transform_pj,
        bint always_xy,
    )
