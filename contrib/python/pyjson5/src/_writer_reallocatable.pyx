cdef struct WriterReallocatable:
    Writer base
    size_t position
    size_t length
    void *obj


cdef boolean _WriterReallocatable_reserve(WriterRef writer_, size_t amount) except False:
    cdef size_t current_size
    cdef size_t needed_size
    cdef size_t new_size
    cdef void *temp
    cdef WriterReallocatable *writer = <WriterReallocatable*> &writer_

    if expect(amount <= 0, False):
        return True

    needed_size = writer.position + amount
    current_size = writer.length
    if expect(needed_size < current_size, True):
        return True

    new_size = current_size
    while new_size <= needed_size:
        new_size = (new_size + 32) + (new_size // 4)
        if expect(new_size < current_size, False):
            ErrNoMemory()

    temp = ObjectRealloc(writer.obj, new_size + 1)
    if temp is NULL:
        ErrNoMemory()

    writer.obj = temp
    writer.length = new_size

    return True


cdef boolean _WriterReallocatable_append_c(Writer &writer_, char datum) except False:
    cdef WriterReallocatable *writer = <WriterReallocatable*> &writer_

    _WriterReallocatable_reserve(writer.base, 1)
    (<char*> writer.obj)[writer.position] = datum
    writer.position += 1

    return True


cdef boolean _WriterReallocatable_append_s(Writer &writer_, const char *s, Py_ssize_t length) except False:
    cdef WriterReallocatable *writer = <WriterReallocatable*> &writer_

    if expect(length <= 0, False):
        return True

    _WriterReallocatable_reserve(writer.base, length)
    memcpy(&(<char*> writer.obj)[writer.position], s, length)
    writer.position += length

    return True


