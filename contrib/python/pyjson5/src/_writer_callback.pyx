cdef struct WriterCallback:
    Writer base
    PyObject *callback


cdef boolean _WriterCbBytes_append_c(Writer &writer_, char datum) except False:
    cdef WriterCallback *writer = <WriterCallback*> &writer_

    CallFunction(writer.callback, b'c', datum)

    return True


cdef boolean _WriterCbBytes_append_s(Writer &writer_, const char *s, Py_ssize_t length) except False:
    cdef WriterCallback *writer = <WriterCallback*> &writer_

    if expect(length <= 0, False):
        return True

    CallFunction(writer.callback, b'y#', s, <int> length)

    return True


cdef boolean _WriterCbStr_append_c(Writer &writer_, char datum) except False:
    cdef WriterCallback *writer = <WriterCallback*> &writer_

    CallFunction(writer.callback, b'C', datum)

    return True


cdef boolean _WriterCbStr_append_s(Writer &writer_, const char *s, Py_ssize_t length) except False:
    cdef WriterCallback *writer = <WriterCallback*> &writer_

    if expect(length <= 0, False):
        return True

    CallFunction(writer.callback, b'U#', s, <int> length)

    return True
