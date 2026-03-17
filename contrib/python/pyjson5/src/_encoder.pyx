cdef int _encode_unicode_impl(WriterRef writer, UCSString data, Py_ssize_t length) except -1:
    cdef char buf[32]
    cdef uint32_t c
    cdef uint32_t s1, s2
    cdef const char *escaped_string
    cdef Py_ssize_t escaped_length
    cdef size_t unescaped_length, index
    cdef Py_ssize_t sublength

    if length > 0:
        writer.reserve(writer, 2 + length)
        writer.append_c(writer, <char> PyUnicode_1BYTE_DATA((<Options> writer.options).quotationmark)[0])
        while True:
            if UCSString is UCS1String:
                sublength = length
            else:
                sublength = min(length, <Py_ssize_t> sizeof(buf))

            unescaped_length = ESCAPE_DCT.find_unescaped_range(data, sublength)
            if unescaped_length > 0:
                if UCSString is UCS1String:
                    writer.append_s(writer, <const char*> data, unescaped_length)
                else:
                    for index in range(unescaped_length):
                        buf[index] = <const char> data[index]
                    writer.append_s(writer, buf, unescaped_length)

                data += unescaped_length
                length -= unescaped_length
                if length <= 0:
                    break

                if UCSString is not UCS1String:
                    continue

            c = data[0]
            if (UCSString is UCS1String) or (c < 0x100):
                escaped_length = ESCAPE_DCT.items[c][0]
                escaped_string = &ESCAPE_DCT.items[c][1]
                writer.append_s(writer, escaped_string, escaped_length)
            elif (UCSString is UCS2String) or (c <= 0xffff):
                buf[0] = b'\\';
                buf[1] = b'u';
                buf[2] = HEX[(c >> (4*3)) & 0xf];
                buf[3] = HEX[(c >> (4*2)) & 0xf];
                buf[4] = HEX[(c >> (4*1)) & 0xf];
                buf[5] = HEX[(c >> (4*0)) & 0xf];
                buf[6] = 0;

                writer.append_s(writer, buf, 6);
            else:
                # surrogate pair
                c -= 0x10000
                s1 = 0xd800 | ((c >> 10) & 0x3ff)
                s2 = 0xdc00 | (c & 0x3ff)

                buf[0x0] = b'\\';
                buf[0x1] = b'u';
                buf[0x2] = HEX[(s1 >> (4*3)) & 0xf];
                buf[0x3] = HEX[(s1 >> (4*2)) & 0xf];
                buf[0x4] = HEX[(s1 >> (4*1)) & 0xf];
                buf[0x5] = HEX[(s1 >> (4*0)) & 0xf];

                buf[0x6] = b'\\';
                buf[0x7] = b'u';
                buf[0x8] = HEX[(s2 >> (4*3)) & 0xf];
                buf[0x9] = HEX[(s2 >> (4*2)) & 0xf];
                buf[0xa] = HEX[(s2 >> (4*1)) & 0xf];
                buf[0xb] = HEX[(s2 >> (4*0)) & 0xf];

                buf[0xc] = 0;

                writer.append_s(writer, buf, 12);

            data += 1
            length -= 1
            if length <= 0:
                break
        writer.append_c(writer, <char> PyUnicode_1BYTE_DATA((<Options> writer.options).quotationmark)[0])
    else:
        writer.append_s(writer, b'""', 2)

    return True


cdef int _encode_unicode(WriterRef writer, object data) except -1:
    cdef Py_ssize_t length
    cdef int kind

    PyUnicode_READY(data)

    length = PyUnicode_GET_LENGTH(data)
    kind = PyUnicode_KIND(data)

    if kind == PyUnicode_1BYTE_KIND:
        _encode_unicode_impl(writer, PyUnicode_1BYTE_DATA(data), length)
    elif kind == PyUnicode_2BYTE_KIND:
        _encode_unicode_impl(writer, PyUnicode_2BYTE_DATA(data), length)
    elif kind == PyUnicode_4BYTE_KIND:
        _encode_unicode_impl(writer, PyUnicode_4BYTE_DATA(data), length)
    else:
        pass  # impossible

    return True


cdef int _encode_nested_key(WriterRef writer, object data) except -1:
    cdef const char *string
    cdef char c
    cdef Py_ssize_t index, length
    cdef int result

    cdef WriterReallocatable sub_writer = WriterReallocatable(
        Writer(
            _WriterReallocatable_reserve,
            _WriterReallocatable_append_c,
            _WriterReallocatable_append_s,
            writer.options,
        ),
        0, 0, NULL,
    )
    try:
        result = _encode(sub_writer.base, data)
        if expect(result < 0, False):
            return result

        length = sub_writer.position
        string = <char*> sub_writer.obj

        writer.reserve(writer, 2 + length)
        writer.append_c(writer, <char> PyUnicode_1BYTE_DATA((<Options> writer.options).quotationmark)[0])
        for index in range(length):
            c = string[index]
            if c in b'\\"':
                writer.append_c(writer, b'\\')
            writer.append_c(writer, c)
        writer.append_c(writer, <char> PyUnicode_1BYTE_DATA((<Options> writer.options).quotationmark)[0])
    finally:
        if sub_writer.obj is not NULL:
            ObjectFree(sub_writer.obj)

    return True


cdef int _append_ascii(WriterRef writer, object data) except -1:
    cdef Py_buffer view
    cdef const char *buf
    cdef Py_ssize_t index
    cdef unsigned char c

    if PyUnicode_Check(data):
        PyUnicode_READY(data)
        if not PyUnicode_IS_ASCII(data):
            raise TypeError('Expected ASCII data')
        writer.append_s(writer, <const char*> PyUnicode_1BYTE_DATA(data), PyUnicode_GET_LENGTH(data))
    else:
        PyObject_GetBuffer(data, &view, PyBUF_CONTIG_RO)
        try:
            buf = <const char*> view.buf
            for index in range(view.len):
                c = <unsigned char> buf[index]
                if c & ~0x7f:
                    raise TypeError('Expected ASCII data')

            writer.append_s(writer, buf, view.len)
        finally:
            PyBuffer_Release(&view)

    return True


cdef int _encode_tojson(WriterRef writer, object data) except -1:
    cdef object value = getattr(data, (<Options> writer.options).tojson, None)
    if value is None:
        return False

    if callable(value):
        Py_EnterRecursiveCall(' while encoding nested JSON5 object')
        try:
            value = value()
        finally:
            Py_LeaveRecursiveCall()

    _append_ascii(writer, value)
    return True


cdef int _encode_sequence(WriterRef writer, object data) except -1:
    cdef boolean first
    cdef object iterator
    cdef object value
    cdef int result

    try:
        iterator = PyObject_GetIter(data)
    except TypeError:
        return False

    Py_EnterRecursiveCall(' while encoding nested JSON5 object')
    try:
        writer.append_c(writer, <char> b'[')
        first = True
        value = None
        while iter_next(iterator, &<PyObject*&> value):
            if not first:
                writer.append_c(writer, <char> b',')
            else:
                first = False

            result = _encode(writer, value)
            if expect(result < 0, False):
                return result
        writer.append_c(writer, <char> b']')
    finally:
        Py_LeaveRecursiveCall()

    return True


cdef int _encode_mapping(WriterRef writer, object data) except -1:
    cdef boolean first
    cdef object iterator, key, value
    cdef int result

    if not isinstance(data, (<Options> writer.options).mappingtypes):
        return False

    iterator = PyObject_GetIter(data)

    Py_EnterRecursiveCall(' while encoding nested JSON5 object')
    try:
        writer.append_c(writer, <char> b'{')
        first = True
        key = None
        while iter_next(iterator, &<PyObject*&> key):
            if not first:
                writer.append_c(writer, <char> b',')
            else:
                first = False
            value = data[key]

            if PyUnicode_Check(key):
                _encode_unicode(writer, key)
            else:
                _encode_nested_key(writer, key)

            writer.append_c(writer, <char> b':')

            result = _encode(writer, value)
            if expect(result < 0, False):
                return result
        writer.append_c(writer, <char> b'}')
    finally:
        Py_LeaveRecursiveCall()

    return True


cdef int _encode_none(WriterRef writer, object data) except -1:
    writer.append_s(writer, b'null', 4)
    return True


cdef int _encode_bytes(WriterRef writer, object data) except -1:
    _encode_unicode(writer, PyUnicode_FromEncodedObject(data, 'UTF-8', 'strict'))
    return True


cdef int _encode_datetime(WriterRef writer, object data) except -1:
    cdef object stringified
    cdef Py_ssize_t length
    cdef const char *string

    if not isinstance(data, DATETIME_CLASSES):
        return False

    stringified = data.isoformat()
    length = 0
    string = PyUnicode_AsUTF8AndSize(stringified, &length)

    writer.reserve(writer, 2 + length)
    writer.append_c(writer, <char> PyUnicode_1BYTE_DATA((<Options> writer.options).quotationmark)[0])
    writer.append_s(writer, string, length)
    writer.append_c(writer, <char> PyUnicode_1BYTE_DATA((<Options> writer.options).quotationmark)[0])

    return True


cdef int _encode_format_string(WriterRef writer, object data, object fmt) except -1:
    cdef object formatted
    cdef const char *string
    cdef Py_ssize_t length = 0  # silence warning

    formatted = PyUnicode_Format(fmt, data)
    string = PyUnicode_AsUTF8AndSize(formatted, &length)
    writer.append_s(writer, string, length)

    return True


cdef int _encode_float(WriterRef writer, object data) except -1:
    cdef double value = PyFloat_AsDouble(data)
    cdef int classification = fpclassify(value)
    cdef char buf[64]
    cdef char *end
    cdef char *string
    cdef Py_ssize_t length

    if classification == FP_NORMAL:
        end = Dtoa(buf, PyFloat_AsDouble(data))
        length = end - buf
        string = buf
    elif classification in (FP_SUBNORMAL, FP_ZERO):
        string = b'0.0'
        length = 3
    elif classification == FP_NAN:
        string = b'NaN'
        length = 3
    else:
        # classification == FP_INFINITE
        if value > 0.0:
            string = b'Infinity'
            length = 8
        else:
            string = b'-Infinity'
            length = 9

    writer.append_s(writer, string, length)
    return True


cdef int _encode_long(WriterRef writer, object data) except -1:
    if PyBool_Check(data):
        if data is True:
            writer.append_s(writer, 'true', 4)
        else:
            writer.append_s(writer, 'false', 5)
    else:
        _encode_format_string(writer, data, DEFAULT_INTFORMAT)
    return True


cdef int _encode_decimal(WriterRef writer, object data) except -1:
    if not isinstance(data, Decimal):
        return False

    _encode_format_string(writer, data, DEFAULT_DECIMALFORMAT)
    return True


cdef int _encode_unstringifiable(WriterRef writer, object data) except -1:
    if not data:
        writer.append_s(writer, b'none', 4)
        return True

    Py_EnterRecursiveCall(' while encoding JSON5 object with vars(obj) fallback')
    try:
        try:
            data = PyObject_GenericGetDict(data, NULL)
        except:
            pass
        else:
            if _encode_mapping(writer, data):
                return True
    finally:
        Py_LeaveRecursiveCall()

    _raise_unstringifiable(data)


cdef int _encode_other(WriterRef writer, object data):
    cdef int result = 0

    while True:
        if (<Options> writer.options).tojson is not None:
            result = (<int(*)(WriterRef, object)> _encode_tojson)(writer, data)
            if result != 0:
                break

        if obj_has_iter(data):
            result = (<int(*)(WriterRef, object)> _encode_mapping)(writer, data)
            if result != 0:
                break

            result = (<int(*)(WriterRef, object)> _encode_sequence)(writer, data)
            if result != 0:
                break

        result = (<int(*)(WriterRef, object)> _encode_decimal)(writer, data)
        if result != 0:
            break

        result = (<int(*)(WriterRef, object)> _encode_datetime)(writer, data)
        if result != 0:
            break

        result = (<int(*)(WriterRef, object)> _encode_unstringifiable)(writer, data)
        if result != 0:
            break

        break

    return result


cdef int _encode(WriterRef writer, object data):
    cdef int (*encoder)(WriterRef, object)

    if data is None:
        encoder = <int(*)(WriterRef, object)> _encode_none
    elif PyUnicode_Check(data):
        encoder = <int(*)(WriterRef, object)> _encode_unicode
    elif PyLong_Check(data):
        encoder = <int(*)(WriterRef, object)> _encode_long
    elif PyFloat_Check(data):
        encoder = <int(*)(WriterRef, object)> _encode_float
    elif PyBytes_Check(data):
        encoder = <int(*)(WriterRef, object)> _encode_bytes
    else:
        encoder = <int(*)(WriterRef, object)> _encode_other

    return encoder(writer, data)


cdef int _encode_callback_bytes(object data, object cb, object options) except -1:
    cdef WriterCallback writer = WriterCallback(
        Writer(
            _WriterNoop_reserve,
            _WriterCbBytes_append_c,
            _WriterCbBytes_append_s,
            <PyObject*> options,
        ),
        <PyObject*> cb,
    )

    if expect(not callable(cb), False):
        raise TypeError(f'type(cb)=={type(cb)!r} is not callable')

    return _encode(writer.base, data)


cdef int _encode_callback_str(object data, object cb, object options) except -1:
    cdef WriterCallback writer = WriterCallback(
        Writer(
            _WriterNoop_reserve,
            _WriterCbStr_append_c,
            _WriterCbStr_append_s,
            <PyObject*> options,
        ),
        <PyObject*> cb,
    )

    if expect(not callable(cb), False):
        raise TypeError(f'type(cb)=={type(cb)!r} is not callable')

    return _encode(writer.base, data)
