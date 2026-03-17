cdef struct Writer:
    boolean (*reserve)(Writer &writer, size_t amount) except False
    boolean (*append_c)(Writer &writer, char datum) except False
    boolean (*append_s)(Writer &writer, const char *s, Py_ssize_t length) except False
    PyObject *options


ctypedef Writer &WriterRef
