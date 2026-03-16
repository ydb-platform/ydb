include "proj.pxi"

cdef class Proj:
    cdef PJ * projpj
    cdef PJ_CONTEXT * projctx
    cdef PJ_PROJ_INFO projpj_info
    cdef char *pjinitstring
    cdef object _proj_version

