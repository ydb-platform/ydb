cdef extern from "stdarg.h":
    ctypedef void *va_list
    void va_start(va_list ap, void *last) nogil
    void va_end(va_list ap) nogil

cdef extern from "includes/etree_defs.h":
    cdef int va_int(va_list ap) nogil
    cdef char *va_charptr(va_list ap) nogil
