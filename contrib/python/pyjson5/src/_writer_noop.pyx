cdef struct WriterNoop:
    Writer base


cdef boolean _WriterNoop_reserve(WriterRef writer_, size_t amount) except False:
    return True


cdef boolean _WriterNoop_append_c(Writer &writer_, char datum) except False:
    return True


cdef boolean _WriterNoop_append_s(Writer &writer_, const char *s,
                                  Py_ssize_t length) except False:
    return True
