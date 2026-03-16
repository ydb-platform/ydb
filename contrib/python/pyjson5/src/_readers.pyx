ctypedef fused ReaderRef:
    ReaderUCSRef
    ReaderCallbackRef


cdef boolean _reader_enter(ReaderRef self) except False:
    if self.base.maxdepth == 0:
        _raise_nesting(_reader_tell(self))

    Py_EnterRecursiveCall(' while decoding nested JSON5 object')

    self.base.maxdepth -= 1

    return True


cdef void _reader_leave(ReaderRef self):
    Py_LeaveRecursiveCall()
    self.base.maxdepth += 1


cdef inline Py_ssize_t _reader_tell(ReaderRef self):
    return self.base.position


cdef inline uint32_t _reader_get(ReaderRef self):
    cdef uint32_t c0
    if ReaderRef is ReaderUTF8Ref:
        c0 = _reader_utf8_get(self)
    elif ReaderRef in ReaderUCSRef:
        c0 = _reader_ucs_get(self)
    elif ReaderRef is ReaderCallbackRef:
        c0 = _reader_Callback_get(self)
    return c0


cdef int32_t _reader_good(ReaderRef self) except -1:
    if ReaderRef in ReaderUCSRef:
        return _reader_ucs_good(self)
    elif ReaderRef is ReaderCallbackRef:
        return _reader_Callback_good(self)
