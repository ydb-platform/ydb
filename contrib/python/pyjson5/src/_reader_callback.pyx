cdef struct ReaderCallbackBase:
    Py_ssize_t position
    Py_ssize_t maxdepth
    

cdef struct ReaderCallback:
    ReaderCallbackBase base
    PyObject *callback
    PyObject *args
    int32_t lookahead

ctypedef ReaderCallback &ReaderCallbackRef


cdef inline uint32_t _reader_Callback_get(ReaderCallbackRef self):
    cdef int32_t c = self.lookahead

    self.lookahead = -1
    self.base.position += 1

    return cast_to_uint32(c)


cdef int32_t _reader_Callback_good(ReaderCallbackRef self) except -1:
    cdef Py_ssize_t c = -1

    if self.lookahead >= 0:
        return True

    cdef object value = CallObject(self.callback, self.args)
    if (value is None) or (value is False):
        return False

    if isinstance(value, int):
        c = value
    elif isinstance(value, ORD_CLASSES):
        if not value:
            return False
        c = ord(value)
    else:
        _raise_not_ord(value, self.base.position)

    if c < 0:
        return False
    elif c > 0x10ffff:
        _raise_not_ord(value, self.base.position)

    self.lookahead = c

    return True
