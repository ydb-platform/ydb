cdef AlwaysTrue _raise_decoder(cls, msg, extra=None, result=None) except True:
    raise _DecoderException(cls, msg, extra, result)


cdef AlwaysTrue _raise_unclosed(const char *what, Py_ssize_t start) except True:
    return _raise_decoder(
        Json5EOF,
        f'Unclosed {what} starting near {start}',
    )


cdef AlwaysTrue _raise_no_data(Py_ssize_t where) except True:
    return _raise_decoder(
        Json5EOF,
        f'No JSON data found near {where}',
    )


cdef AlwaysTrue _raise_stray_character(const char *what, Py_ssize_t where) except True:
    return _raise_decoder(
        Json5IllegalCharacter,
        f'Stray {what} near {where}',
        what,
    )


cdef AlwaysTrue _raise_expected_sc(const char *char_a, uint32_t char_b, Py_ssize_t near, uint32_t found) except True:
    return _raise_decoder(
        Json5IllegalCharacter,
        f'Expected {char_a} or U+{char_b:04x} near {near}, found U+{found:04x}',
        f'{found:c}',
    )


cdef AlwaysTrue _raise_expected_s(const char *char_a, Py_ssize_t near, uint32_t found) except True:
    return _raise_decoder(
        Json5IllegalCharacter,
        f'Expected {char_a} near {near}, found U+{found:04x}',
        f'{found:c}',
    )


cdef AlwaysTrue _raise_expected_c(uint32_t char_a, Py_ssize_t near, uint32_t found) except True:
    return _raise_decoder(
        Json5IllegalCharacter,
        f'Expected U+{char_a:04x} near {near}, found U+{found:04x}',
        f'{found:c}',
    )


cdef AlwaysTrue _raise_extra_data(uint32_t found, Py_ssize_t where) except True:
    return _raise_decoder(
        Json5ExtraData,
        f'Extra data U+{found:04X} near {where}',
        f'{found:c}',
    )


cdef AlwaysTrue _raise_unframed_data(uint32_t found, Py_ssize_t where) except True:
    return _raise_decoder(
        Json5ExtraData,
        f'Lost unframed data near {where}',
        f'{found:c}',
    )


cdef AlwaysTrue _raise_nesting(Py_ssize_t where, object result=None) except True:
    return _raise_decoder(
        Json5NestingTooDeep,
        f'Maximum nesting level exceeded near {where}',
        None,
        result,
    )


cdef AlwaysTrue _raise_not_ord(object value, Py_ssize_t where) except True:
    return _raise_decoder(
        Json5IllegalType,
        f'type(value)=={type(value)!r} not in (int, str, bytes) near {where} or the value is not valid.',
        value,
    )
