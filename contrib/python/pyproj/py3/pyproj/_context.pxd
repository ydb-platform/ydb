include "proj.pxi"

cpdef str _get_proj_error()
cpdef void _clear_proj_error() noexcept
cdef PJ_CONTEXT* pyproj_context_create() except *
