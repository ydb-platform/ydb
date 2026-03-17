cdef struct ReaderUCS:
    Py_ssize_t remaining
    Py_ssize_t position
    Py_ssize_t maxdepth


cdef struct ReaderUCS1:
    ReaderUCS base
    const Py_UCS1 *string


cdef struct ReaderUCS2:
    ReaderUCS base
    const Py_UCS2 *string


cdef struct ReaderUCS4:
    ReaderUCS base
    const Py_UCS4 *string


cdef struct ReaderUTF8:
    ReaderUCS base
    const Py_UCS1 *string


ctypedef ReaderUCS1 &ReaderUCS1Ref
ctypedef ReaderUCS2 &ReaderUCS2Ref
ctypedef ReaderUCS4 &ReaderUCS4Ref
ctypedef ReaderUTF8 &ReaderUTF8Ref

ctypedef Py_UCS1 *UCS1String
ctypedef Py_UCS2 *UCS2String
ctypedef Py_UCS4 *UCS4String

ctypedef fused ReaderUCSRef:
    ReaderUCS1Ref
    ReaderUCS2Ref
    ReaderUCS4Ref
    ReaderUTF8Ref

ctypedef fused UCSString:
    UCS1String
    UCS2String
    UCS4String


cdef inline int32_t _reader_ucs_good(ReaderUCSRef self):
    return self.base.remaining > 0


cdef inline uint32_t _reader_ucs_get(ReaderUCSRef self):
    cdef int32_t c = self.string[0]

    self.string += 1
    self.base.remaining -= 1
    self.base.position += 1

    return cast_to_uint32(c)


cdef inline uint32_t _reader_utf8_get(ReaderUCSRef self):
    cdef uint32_t c0 = _reader_ucs_get(self)
    cdef unsigned int n

    if (c0 & 0b1_0000000) == 0b0_0000000:    # ASCII
        return c0
    elif (c0 & 0b11_000000) == 0b10_000000:  # broken continuation
        return c0
    elif (c0 & 0b111_00000) == 0b110_00000:  # 2 bytes
        c0 = (c0 & 0b000_11111)
        n = 1
    elif (c0 & 0b1111_0000) == 0b1110_0000:  # 3 bytes
        c0 = (c0 & 0b0000_1111)
        n = 2
    elif (c0 & 0b11111_000) == 0b11110_000:  # 4 bytes
        c0 = (c0 & 0b00000_111)
        n = 3
    else:  # 5+ bytes, invalid
        return c0

    for n in range(n, 0, -1):
        if not _reader_ucs_good(self):
            return c0

        c0 = (c0 << 6) | (_reader_ucs_get(self) & 0b00_111111)

    return c0
