cdef boolean _is_line_terminator(uint32_t c) nogil:
    # https://www.ecma-international.org/ecma-262/5.1/#sec-7.3
    return c in (
        0x000A,  # Line Feed <LF>
        0x000D,  # Carriage Return <CR>
        0x2028,  # Line separator <LS>
        0x2029,  # Paragraph separator <PS>
    )

cdef boolean _is_ws_zs(uint32_t c) nogil:
    # https://spec.json5.org/#white-space
    return unicode_cat_of(c) == 1

cdef boolean _is_identifier_start(uint32_t c) nogil:
    # https://www.ecma-international.org/ecma-262/5.1/#sec-7.6
    return unicode_cat_of(c) == 2

cdef boolean _is_identifier_part(uint32_t c) nogil:
    # https://www.ecma-international.org/ecma-262/5.1/#sec-7.6
    return unicode_cat_of(c) >= 2

cdef inline boolean _is_x(uint32_t c) nogil:
    return (c | 0x20) == b'x'

cdef inline boolean _is_e(uint32_t c) nogil:
    return (c | 0x20) == b'e'

cdef inline boolean _is_decimal(uint32_t c) nogil:
    return b'0' <= c <= b'9'

cdef inline boolean _is_hex(uint32_t c) nogil:
    return b'a' <= (c | 0x20) <= b'f'

cdef inline boolean _is_hexadecimal(uint32_t c) nogil:
    return _is_decimal(c) or _is_hex(c)

cdef boolean _is_in_float_representation(uint32_t c) nogil:
    if _is_decimal(c):
        return True
    if _is_e(c):
        return True
    elif c in b'.+-':
        return True
    else:
        return False
