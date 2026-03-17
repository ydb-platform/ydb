cdef enum:
    NO_EXTRA_DATA = 0x0011_0000


cdef boolean _skip_single_line(ReaderRef reader) except False:
    cdef uint32_t c0
    while _reader_good(reader):
        c0 = _reader_get(reader)
        if _is_line_terminator(c0):
            break

    return True


cdef boolean _skip_multiline_comment(ReaderRef reader) except False:
    cdef uint32_t c0
    cdef boolean seen_asterisk = False
    cdef Py_ssize_t comment_start = _reader_tell(reader)

    while True:
        if expect(not _reader_good(reader), False):
            break

        c0 = _reader_get(reader)
        if c0 == b'*':
            seen_asterisk = True
        elif seen_asterisk:
            if c0 == b'/':
                return True
            seen_asterisk = False

    _raise_unclosed(b'comment', comment_start)
    return False


#     data found
# -1: exhausted
# -2: exception
cdef int32_t _skip_to_data_sub(ReaderRef reader, uint32_t c0) except -2:
    cdef int32_t c1 = 0  # silence warning
    cdef boolean seen_slash

    seen_slash = False
    while True:
        if c0 == b'/':
            if seen_slash:
                _skip_single_line(reader)
                seen_slash = False
            else:
                seen_slash = True
        elif c0 == b'*':
            if expect(not seen_slash, False):
                _raise_stray_character('asterisk', _reader_tell(reader))

            _skip_multiline_comment(reader)
            seen_slash = False
        elif not _is_ws_zs(c0):
            c1 = cast_to_int32(c0)
            break
        elif expect(seen_slash, False):
            _raise_stray_character('slash', _reader_tell(reader))

        if not _reader_good(reader):
            c1 = -1
            break

        c0 = _reader_get(reader)

    if expect(seen_slash, False):
        _raise_stray_character('slash', _reader_tell(reader))

    return c1


#    data found
# -1 exhausted
# -2 exception
cdef int32_t _skip_to_data(ReaderRef reader) except -2:
    cdef uint32_t c0
    cdef int32_t c1
    if _reader_good(reader):
        c0 = _reader_get(reader)
        c1 = _skip_to_data_sub(reader, c0)
    else:
        c1 = -1
    return c1


cdef int32_t _get_hex_character(ReaderRef reader, Py_ssize_t length) except -1:
    cdef Py_ssize_t start
    cdef uint32_t c0
    cdef uint32_t result
    cdef Py_ssize_t index

    start = _reader_tell(reader)
    result = 0
    for index in range(length):
        result <<= 4
        if expect(not _reader_good(reader), False):
            _raise_unclosed(b'escape sequence', start)

        c0 = _reader_get(reader)
        if b'0' <= c0 <= b'9':
            result |= c0 - <uint32_t> b'0'
        elif b'a' <= c0 <= b'f':
            result |= c0 - <uint32_t> b'a' + 10
        elif b'A' <= c0 <= b'F':
            result |= c0 - <uint32_t> b'A' + 10
        else:
            _raise_expected_s('hexadecimal character', start, c0)

    if expect(result > 0x10ffff, False):
        _raise_expected_s('Unicode code point', start, result)

    return cast_to_int32(result)


# >=  0: character to append
cdef int32_t _get_escaped_unicode_maybe_surrogate(ReaderRef reader, Py_ssize_t start) except -1:
    cdef uint32_t c0
    cdef uint32_t c1

    c0 = cast_to_uint32(_get_hex_character(reader, 4))
    if expect(unicode_is_lo_surrogate(c0), False):
        _raise_expected_s('high surrogate before low surrogate', start, c0)
    elif not unicode_is_hi_surrogate(c0):
        return c0

    _accept_string(reader, b'\\u')

    c1 = cast_to_uint32(_get_hex_character(reader, 4))
    if expect(not unicode_is_lo_surrogate(c1), False):
        _raise_expected_s('low surrogate', start, c1)

    return unicode_join_surrogates(c0, c1)


# >=  0: character to append
#    -1: skip
# <  -1: -(next character + 1)
cdef int32_t _get_escape_sequence(ReaderRef reader,
                                  Py_ssize_t start) except 0x7ffffff:
    cdef uint32_t c0

    c0 = _reader_get(reader)
    if expect(not _reader_good(reader), False):
        _raise_unclosed(b'string', start)

    if c0 == b'b':
        return 0x0008
    elif c0 == b'f':
        return 0x000c
    elif c0 == b'n':
        return 0x000a
    elif c0 == b'r':
        return 0x000d
    elif c0 == b't':
        return 0x0009
    elif c0 == b'v':
        return 0x000b
    elif c0 == b'0':
        return 0x0000
    elif c0 == b'x':
        return _get_hex_character(reader, 2)
    elif c0 == b'u':
        return _get_escaped_unicode_maybe_surrogate(reader, start)
    elif c0 == b'U':
        return _get_hex_character(reader, 8)
    elif expect(b'1' <= c0 <= b'9', False):
        _raise_expected_s('escape sequence', start, c0)
        return -2
    elif _is_line_terminator(c0):
        if c0 != 0x000D:
            return -1

        c0 = _reader_get(reader)
        if c0 == 0x000A:
            return -1

        return -cast_to_int32(c0 + 1)
    else:
        return cast_to_int32(c0)


cdef object _decode_string_sub(ReaderRef reader, uint32_t delim,
                               Py_ssize_t start, uint32_t c0):
    cdef int32_t c1
    cdef StackHeapString[uint32_t] buf

    while True:
        if expect(c0 == delim, False):
            break

        if expect(not _reader_good(reader), False):
            _raise_unclosed(b'string', start)

        if expect(c0 != b'\\', True):
            if expect(c0 in (0xA, 0xD), False):
                _raise_unclosed(b'string', start)

            buf.push_back(c0)
            c0 = _reader_get(reader)
            continue

        c1 = _get_escape_sequence(reader, start)
        if c1 >= -1:
            if expect(not _reader_good(reader), False):
                _raise_unclosed(b'string', start)

            if c1 >= 0:
                c0 = cast_to_uint32(c1)
                buf.push_back(c0)

            c0 = _reader_get(reader)
        else:
            c0 = cast_to_uint32(-(c1 + 1))

    return PyUnicode_FromKindAndData(
        PyUnicode_4BYTE_KIND, buf.data(), buf.size(),
    )


cdef object _decode_string(ReaderRef reader, int32_t *c_in_out):
    cdef uint32_t delim
    cdef uint32_t c0
    cdef int32_t c1
    cdef Py_ssize_t start
    cdef object result

    c1 = c_in_out[0]
    delim = cast_to_uint32(c1)
    start = _reader_tell(reader)

    if expect(not _reader_good(reader), False):
        _raise_unclosed(b'string', start)

    c0 = _reader_get(reader)
    result = _decode_string_sub(reader, delim, start, c0)

    c_in_out[0] = NO_EXTRA_DATA
    return result


cdef object _decode_double(StackHeapString[char] &buf, Py_ssize_t start):
    cdef double d0
    cdef const char *end_of_double

    d0 = 0.0  # silence warning
    end_of_double = parse_number(buf.data(), &d0)
    if end_of_double != NULL and end_of_double[0] == b'\0':
        return PyFloat_FromDouble(d0)

    _raise_unclosed('NumericLiteral', start)


cdef object _decode_number_leading_zero(ReaderRef reader, StackHeapString[char] &buf,
                                        int32_t *c_in_out, Py_ssize_t start):
    cdef uint32_t c0
    cdef int32_t c1 = 0  # silence warning

    if not _reader_good(reader):
        c_in_out[0] = -1
        return 0

    c0 = _reader_get(reader)
    if _is_x(c0):
        while True:
            if not _reader_good(reader):
                c1 = -1
                break

            c0 = _reader_get(reader)
            if _is_hexadecimal(c0):
                buf.push_back(<char> <unsigned char> c0)
            elif c0 != b'_':
                c1 = cast_to_int32(c0)
                break

        c_in_out[0] = c1

        buf.push_back(b'\0')
        try:
            return PyLong_FromString(buf.data(), NULL, 16)
        except Exception:
            _raise_unclosed('NumericLiteral', start)
    elif c0 == b'.':
        buf.push_back(b'0')
        buf.push_back(b'.')

        while True:
            if not _reader_good(reader):
                c1 = -1
                break

            c0 = _reader_get(reader)
            if _is_in_float_representation(c0):
                buf.push_back(<char> <unsigned char> c0)
            elif c0 != b'_':
                c1 = cast_to_int32(c0)
                break

        c_in_out[0] = c1

        if buf.data()[buf.size() - 1] == b'.':
            (<char*> buf.data())[buf.size() - 1] = b'\0'
        else:
            buf.push_back(b'\0')

        return _decode_double(buf, start)
    elif _is_e(c0):
        while True:
            if not _reader_good(reader):
                c1 = -1
                break

            c0 = _reader_get(reader)
            if _is_in_float_representation(c0):
                pass
            elif c0 == b'_':
                pass
            else:
                c1 = cast_to_int32(c0)
                break

        c_in_out[0] = c1
        return 0.0
    else:
        c1 = cast_to_int32(c0)
        c_in_out[0] = c1
        return 0


cdef object _decode_number_any(ReaderRef reader, StackHeapString[char] &buf,
                               int32_t *c_in_out, Py_ssize_t start):
    cdef uint32_t c0
    cdef int32_t c1
    cdef boolean is_float = False
    cdef boolean was_point = False
    cdef boolean leading_point = False

    c1 = c_in_out[0]
    c0 = cast_to_uint32(c1)

    if c0 == b'.':
        buf.push_back(b'0')
        is_float = True
        leading_point = True

    while True:
        if _is_decimal(c0):
            pass
        elif _is_in_float_representation(c0):
            is_float = True
        elif c0 != b'_':
            c1 = cast_to_int32(c0)
            break

        if c0 == b'_':
            pass
        elif c0 != b'.':
            if was_point:
                was_point = False
                if not _is_e(c0):
                    buf.push_back(b'.')
            buf.push_back(<char> <unsigned char> c0)
        elif not was_point:
            was_point = True
        else:
            _raise_unclosed('NumericLiteral', start)

        if not _reader_good(reader):
            c1 = -1
            break

        c0 = _reader_get(reader)

    c_in_out[0] = c1

    if leading_point and buf.size() == 1:  # single '.'
        _raise_unclosed('NumericLiteral', start)

    buf.push_back(b'\0')

    if not is_float:
        try:
            return PyLong_FromString(buf.data(), NULL, 10)
        except Exception:
            pass
        _raise_unclosed('NumericLiteral', start)
    else:
        return _decode_double(buf, start)


cdef object _decode_number(ReaderRef reader, int32_t *c_in_out):
    cdef uint32_t c0
    cdef int32_t c1
    cdef Py_ssize_t start = _reader_tell(reader)
    cdef StackHeapString[char] buf

    c1 = c_in_out[0]
    c0 = cast_to_uint32(c1)

    if c0 == b'+':
        if expect(not _reader_good(reader), False):
            _raise_unclosed(b'number', start)

        c0 = _reader_get(reader)
        if c0 == b'I':
            _accept_string(reader, b'nfinity')
            c_in_out[0] = NO_EXTRA_DATA
            return CONST_POS_INF
        elif c0 == b'N':
            _accept_string(reader, b'aN')
            c_in_out[0] = NO_EXTRA_DATA
            return CONST_POS_NAN
    elif c0 == b'-':
        if expect(not _reader_good(reader), False):
            _raise_unclosed(b'number', start)

        c0 = _reader_get(reader)
        if c0 == b'I':
            _accept_string(reader, b'nfinity')
            c_in_out[0] = NO_EXTRA_DATA
            return CONST_NEG_INF
        elif c0 == b'N':
            _accept_string(reader, b'aN')
            c_in_out[0] = NO_EXTRA_DATA
            return CONST_NEG_NAN

        buf.push_back(b'-')

    if c0 == b'0':
        return _decode_number_leading_zero(reader, buf, c_in_out, start)
    else:
        c1 = cast_to_int32(c0)
        c_in_out[0] = c1
        return _decode_number_any(reader, buf, c_in_out, start)


#  1: done
#  0: data found
# -1: exception (exhausted)
cdef uint32_t _skip_comma(ReaderRef reader, Py_ssize_t start,
                          uint32_t terminator, const char *what,
                          int32_t *c_in_out) except -1:
    cdef int32_t c0
    cdef uint32_t c1
    cdef boolean needs_comma
    cdef uint32_t done

    c0 = c_in_out[0]
    c1 = cast_to_uint32(c0)

    needs_comma = True
    while True:
        c0 = _skip_to_data_sub(reader, c1)
        if c0 < 0:
            break

        c1 = cast_to_uint32(c0)
        if c1 == terminator:
            c_in_out[0] = NO_EXTRA_DATA
            return 1

        if c1 != b',':
            if expect(needs_comma, False):
                _raise_expected_sc(
                    'comma', terminator, _reader_tell(reader), c1,
                )
            c_in_out[0] = c0
            return 0

        if expect(not needs_comma, False):
            _raise_stray_character('comma', _reader_tell(reader))

        if expect(not _reader_good(reader), False):
            break

        c1 = _reader_get(reader)
        needs_comma = False

    _raise_unclosed(what, start)
    return -1


cdef unicode _decode_identifier_name(ReaderRef reader, int32_t *c_in_out):
    cdef int32_t c0
    cdef uint32_t c1
    cdef Py_ssize_t start
    cdef StackHeapString[uint32_t] buf

    start = _reader_tell(reader)

    c0 = c_in_out[0]
    c1 = cast_to_uint32(c0)
    if expect(not _is_identifier_start(c1), False):
        _raise_expected_s('IdentifierStart', _reader_tell(reader), c1)

    while True:
        if expect(c1 == b'\\', False):
            if not _reader_good(reader):
                _raise_unclosed('IdentifierName', start)
                break

            c1 = _reader_get(reader)
            if c1 == b'u':
                c1 = cast_to_uint32(_get_escaped_unicode_maybe_surrogate(reader, _reader_tell(reader)))
            elif c1 == b'U':
                c1 = cast_to_uint32(_get_hex_character(reader, 8))
            else:
                _raise_expected_s('UnicodeEscapeSequence', _reader_tell(reader), c1)

        buf.push_back(c1)

        if not _reader_good(reader):
            c0 = -1
            break

        c1 = _reader_get(reader)
        if not _is_identifier_part(c1):
            c0 = cast_to_int32(c1)
            break

    c_in_out[0] = c0
    return PyUnicode_FromKindAndData(
        PyUnicode_4BYTE_KIND, buf.data(), buf.size(),
    )


cdef boolean _decode_object(ReaderRef reader, object result) except False:
    cdef int32_t c0
    cdef uint32_t c1
    cdef Py_ssize_t start
    cdef boolean done
    cdef object key
    cdef object value
    cdef object ex

    start = _reader_tell(reader)

    c0 = _skip_to_data(reader)
    if expect(c0 >= 0, True):
        c1 = cast_to_uint32(c0)
        if c1 == b'}':
            return True

        while True:
            if c1 in b'"\'':
                key = _decode_string(reader, &c0)
            else:
                key = _decode_identifier_name(reader, &c0)
            if expect(c0 < 0, False):
                break

            c1 = cast_to_uint32(c0)
            c0 = _skip_to_data_sub(reader, c1)
            if expect(c0 < 0, False):
                break

            c1 = cast_to_uint32(c0)
            if expect(c1 != b':', False):
                _raise_expected_s('colon', _reader_tell(reader), c1)

            if expect(not _reader_good(reader), False):
                break

            c0 = _skip_to_data(reader)
            if expect(c0 < 0, False):
                break

            try:
                value = _decode_recursive(reader, &c0)
            except _DecoderException as ex:
                PyDict_SetItem(result, key, (<_DecoderException> ex).result)
                raise

            if expect(c0 < 0, False):
                break

            PyDict_SetItem(result, key, value)

            done = _skip_comma(
                reader, start, <unsigned char>b'}', b'object', &c0,
            )
            if done:
                return True

            c1 = cast_to_uint32(c0)

    _raise_unclosed(b'object', start)
    return False


cdef boolean _decode_array(ReaderRef reader, object result) except False:
    cdef int32_t c0
    cdef uint32_t c1
    cdef Py_ssize_t start
    cdef boolean done
    cdef object value
    cdef object ex

    start = _reader_tell(reader)

    c0 = _skip_to_data(reader)
    if expect(c0 >= 0, True):
        c1 = cast_to_uint32(c0)
        if c1 == b']':
            return True

        while True:
            try:
                value = _decode_recursive(reader, &c0)
            except _DecoderException as ex:
                PyList_Append(result, (<_DecoderException> ex).result)
                raise

            if expect(c0 < 0, False):
                break

            PyList_Append(result, value)

            done = _skip_comma(
                reader, start, <unsigned char>b']', b'array', &c0,
            )
            if done:
                return True

    _raise_unclosed(b'array', start)


cdef boolean _accept_string(ReaderRef reader, const char *string) except False:
    cdef uint32_t c0
    cdef uint32_t c1
    cdef Py_ssize_t start

    start = _reader_tell(reader)
    while True:
        c0 = string[0]
        string += 1
        if not c0:
            break

        if expect(not _reader_good(reader), False):
            _raise_unclosed(b'literal', start)

        c1 = _reader_get(reader)
        if expect(c0 != c1, False):
            _raise_expected_c(c0, start, c1)

    return True


cdef object _decode_null(ReaderRef reader, int32_t *c_in_out):
    #                       n
    _accept_string(reader, b'ull')
    c_in_out[0] = NO_EXTRA_DATA
    return None


cdef object _decode_true(ReaderRef reader, int32_t *c_in_out):
    #                       t
    _accept_string(reader, b'rue')
    c_in_out[0] = NO_EXTRA_DATA
    return True


cdef object _decode_false(ReaderRef reader, int32_t *c_in_out):
    #                      f
    _accept_string(reader, b'alse')
    c_in_out[0] = NO_EXTRA_DATA
    return False


cdef object _decode_inf(ReaderRef reader, int32_t *c_in_out):
    #                       I
    _accept_string(reader, b'nfinity')
    c_in_out[0] = NO_EXTRA_DATA
    return CONST_POS_INF


cdef object _decode_nan(ReaderRef reader, int32_t *c_in_out):
    #                       N
    _accept_string(reader, b'aN')
    c_in_out[0] = NO_EXTRA_DATA
    return CONST_POS_NAN


cdef object _decode_recursive_enter(ReaderRef reader, int32_t *c_in_out):
    cdef boolean (*fn)(ReaderRef reader, object result) except False
    cdef object result
    cdef int32_t c0
    cdef uint32_t c1
    cdef object ex

    c0 = c_in_out[0]
    c1 = cast_to_uint32(c0)

    if c1 == b'{':
        result = {}
        fn = _decode_object
    else:
        result = []
        fn = _decode_array

    _reader_enter(reader)
    try:
        fn(reader, result)
    except RecursionError:
        _raise_nesting(_reader_tell(reader), result)
    except _DecoderException as ex:
        (<_DecoderException> ex).result = result
        raise
    finally:
        _reader_leave(reader)

    c_in_out[0] = NO_EXTRA_DATA
    return result


cdef object _decoder_unknown(ReaderRef reader, int32_t *c_in_out):
    cdef int32_t c0
    cdef uint32_t c1
    cdef Py_ssize_t start

    c0 = c_in_out[0]
    c1 = cast_to_uint32(c0)
    start = _reader_tell(reader)

    _raise_expected_s('JSON5Value', start, c1)


cdef object _decode_recursive(ReaderRef reader, int32_t *c_in_out):
    cdef int32_t c0
    cdef uint32_t c1
    cdef Py_ssize_t start
    cdef DrsKind kind
    cdef object (*decoder)(ReaderRef, int32_t*)

    c0 = c_in_out[0]
    c1 = cast_to_uint32(c0)
    if c1 >= 128:
        start = _reader_tell(reader)
        _raise_expected_s('JSON5Value', start, c1)

    kind = drs_lookup[c1]
    if kind == DRS_fail:
        decoder = _decoder_unknown
    elif kind == DRS_null:
        decoder = _decode_null
    elif kind == DRS_true:
        decoder = _decode_true
    elif kind == DRS_false:
        decoder = _decode_false
    elif kind == DRS_inf:
        decoder = _decode_inf
    elif kind == DRS_nan:
        decoder = _decode_nan
    elif kind == DRS_string:
        decoder = _decode_string
    elif kind == DRS_number:
        decoder = _decode_number
    elif kind == DRS_recursive:
        decoder = _decode_recursive_enter
    else:
        unreachable()
        decoder = _decoder_unknown

    return decoder(reader, c_in_out)


cdef object _decode_all_sub(ReaderRef reader, boolean some):
    cdef Py_ssize_t start
    cdef int32_t c0
    cdef uint32_t c1
    cdef object result
    cdef object ex

    start = _reader_tell(reader)
    c0 = _skip_to_data(reader)
    if expect(c0 < 0, False):
        _raise_no_data(start)

    result = _decode_recursive(reader, &c0)
    try:
        if c0 < 0:
            pass
        elif not some:
            start = _reader_tell(reader)
            c1 = cast_to_uint32(c0)
            c0 = _skip_to_data_sub(reader, c1)
            if expect(c0 >= 0, False):
                c1 = cast_to_uint32(c0)
                _raise_extra_data(c1, start)
        elif expect(not _is_ws_zs(c0), False):
            start = _reader_tell(reader)
            c1 = cast_to_uint32(c0)
            _raise_unframed_data(c1, start)
    except _DecoderException as ex:
        (<_DecoderException> ex).result = result
        raise

    return result


cdef object _decode_all(ReaderRef reader, boolean some):
    cdef object ex, ex2
    try:
        return _decode_all_sub(reader, some)
    except _DecoderException as ex:
        ex2 = (<_DecoderException> ex).cls(
            (<_DecoderException> ex).msg,
            (<_DecoderException> ex).result,
            (<_DecoderException> ex).extra,
        )
    raise ex2


cdef object _decode_ucs1(const void *string, Py_ssize_t length,
                         Py_ssize_t maxdepth, boolean some):
    cdef ReaderUCS1 reader = ReaderUCS1(
        ReaderUCS(length, 0, maxdepth),
        <const Py_UCS1*> string,
    )
    return _decode_all(reader, some)


cdef object _decode_ucs2(const void *string, Py_ssize_t length,
                         Py_ssize_t maxdepth, boolean some):
    cdef ReaderUCS2 reader = ReaderUCS2(
        ReaderUCS(length, 0, maxdepth),
        <const Py_UCS2*> string,
    )
    return _decode_all(reader, some)


cdef object _decode_ucs4(const void *string, Py_ssize_t length,
                         Py_ssize_t maxdepth, boolean some):
    cdef ReaderUCS4 reader = ReaderUCS4(
        ReaderUCS(length, 0, maxdepth),
        <const Py_UCS4*> string,
    )
    return _decode_all(reader, some)


cdef object _decode_utf8(const void *string, Py_ssize_t length,
                         Py_ssize_t maxdepth, boolean some):
    cdef ReaderUTF8 reader = ReaderUTF8(
        ReaderUCS(length, 0, maxdepth),
        <const Py_UCS1*> string,
    )
    return _decode_all(reader, some)


cdef object _decode_unicode(object data, Py_ssize_t maxdepth, boolean some):
    cdef Py_ssize_t length
    cdef int kind
    cdef const char *s

    PyUnicode_READY(data)

    if CYTHON_COMPILING_IN_PYPY:
        length = 0
        s = PyUnicode_AsUTF8AndSize(data, &length)
        return _decode_utf8(s, length, maxdepth, some)

    length = PyUnicode_GET_LENGTH(data)
    kind = PyUnicode_KIND(data)

    if kind == PyUnicode_1BYTE_KIND:
        return _decode_ucs1(PyUnicode_1BYTE_DATA(data), length, maxdepth, some)
    elif kind == PyUnicode_2BYTE_KIND:
        return _decode_ucs2(PyUnicode_2BYTE_DATA(data), length, maxdepth, some)
    elif kind == PyUnicode_4BYTE_KIND:
        return _decode_ucs4(PyUnicode_4BYTE_DATA(data), length, maxdepth, some)
    else:
        unreachable()


cdef object _decode_buffer(Py_buffer &view, int32_t wordlength,
                           Py_ssize_t maxdepth, boolean some):
    cdef object (*decoder)(const void*, Py_ssize_t, Py_ssize_t, boolean)
    cdef Py_ssize_t length = 0

    if wordlength == 0:
        decoder = _decode_utf8
        length = view.len // 1
    elif wordlength == 1:
        decoder = _decode_ucs1
        length = view.len // 1
    elif wordlength == 2:
        decoder = _decode_ucs2
        length = view.len // 2
    elif wordlength == 4:
        decoder = _decode_ucs4
        length = view.len // 4
    else:
        _raise_illegal_wordlength(wordlength)
        unreachable()
        length = 0
        decoder = NULL

    return decoder(view.buf, length, maxdepth, some)


cdef object _decode_callback(object cb, object args, Py_ssize_t maxdepth,
                             boolean some):
    cdef ReaderCallback reader = ReaderCallback(
        ReaderCallbackBase(0, maxdepth),
        <PyObject*> cb,
        <PyObject*> args,
        -1,
    )
    return _decode_all(reader, some)
