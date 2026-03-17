# cython: language_level=2

cdef object ET
cdef object partial
cdef type _QName

cdef class ElementMaker:
    cdef readonly dict _nsmap
    cdef readonly dict _typemap
    cdef readonly object _namespace
    cdef readonly object _makeelement
