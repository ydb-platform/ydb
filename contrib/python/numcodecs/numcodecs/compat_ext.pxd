# cython: language_level=3


cdef extern from *:
    """
    #ifndef PyBytes_RESIZE
    #define PyBytes_RESIZE(b, n) _PyBytes_Resize(&b, n)
    #endif
    """
    int PyBytes_RESIZE(object b, Py_ssize_t n) except -1


cpdef memoryview ensure_continguous_memoryview(obj)
