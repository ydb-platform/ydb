global DEFAULT_MAX_NESTING_LEVEL, __all__, __author__, __doc__, __license__, __version__

DEFAULT_MAX_NESTING_LEVEL = 32
'''
Maximum nesting level of data to decode if no ``maxdepth`` argument is specified.
'''

__version__ = PyUnicode_FromKindAndData(PyUnicode_1BYTE_KIND, VERSION, VERSION_LENGTH)
'''
Current library version.
'''


def decode(object data, object maxdepth=None, object some=False):
    '''
    Decodes JSON5 serialized data from an :class:`str` object.

    .. code:: python

        decode('["Hello", "world!"]') == ['Hello', 'world!']

    Parameters
    ----------
    data : str
        JSON5 serialized data
    maxdepth : Optional[int]
        Maximum nesting level before are the parsing is aborted.

        * If ``None`` is supplied, then the value of the global variable \
        ``DEFAULT_MAX_NESTING_LEVEL`` is used instead.
        * If the value is ``0``, then only literals are accepted, e.g. ``false``, \
        ``47.11``, or ``"string"``.
        * If the value is negative, then the any nesting level is allowed until \
        Python's recursion limit is hit.
    some : bool
        Allow trailing junk.

    Raises
    ------
    Json5DecoderException
        An exception occured while decoding.
    TypeError
        An argument had a wrong type.

    Returns
    -------
    object
        Deserialized data.
    '''
    if maxdepth is None:
        maxdepth = DEFAULT_MAX_NESTING_LEVEL

    if isinstance(data, unicode):
        return _decode_unicode(data, maxdepth, bool(some))
    else:
        raise TypeError(f'type(data) == {type(data)!r} not supported')


def decode_latin1(object data, object maxdepth=None, object some=False):
    '''
    Decodes JSON5 serialized data from a :class:`bytes` object.

    .. code:: python

        decode_latin1(b'["Hello", "world!"]') == ['Hello', 'world!']

    Parameters
    ----------
    data : bytes
        JSON5 serialized data, encoded as Latin-1 or ASCII.
    maxdepth : Optional[int]
        see :func:`decode(…) <pyjson5.decode>`
    some : bool
        see :func:`decode(…) <pyjson5.decode>`

    Raises
    ------
    Json5DecoderException
        An exception occured while decoding.
    TypeError
        An argument had a wrong type.

    Returns
    -------
    object
        see :func:`decode(…) <pyjson5.decode>`
    '''
    return decode_buffer(data, maxdepth, bool(some), 1)


def decode_utf8(object data, object maxdepth=None, object some=False):
    '''
    Decodes JSON5 serialized data from a :class:`bytes` object.

    .. code:: python

        decode_utf8(b'["H\\xe2\\x82\\xacllo", "w\\xc3\\xb6rld!"]') == ['H€llo', 'wörld!']

    Parameters
    ----------
    data : bytes
        JSON5 serialized data, encoded as UTF-8 or ASCII.
    maxdepth : Optional[int]
        see :func:`decode(…) <pyjson5.decode>`
    some : bool
        see :func:`decode(…) <pyjson5.decode>`

    Raises
    ------
    Json5DecoderException
        An exception occured while decoding.
    TypeError
        An argument had a wrong type.

    Returns
    -------
    object
        see :func:`decode(…) <pyjson5.decode>`
    '''
    return decode_buffer(data, maxdepth, bool(some), 0)


def decode_buffer(object obj, object maxdepth=None, object some=False,
                  object wordlength=None):
    '''
    Decodes JSON5 serialized data from an object that supports the buffer protocol,
    e.g. :class:`bytearray`.

    .. code:: python

        obj = memoryview(b'["Hello", "world!"]')

        decode_buffer(obj) == ['Hello', 'world!']

    Parameters
    ----------
    data : object
        JSON5 serialized data.
        The argument must support Python's buffer protocol, i.e.
        :class:`memoryview(…) <memoryview>` must work. The buffer must be contigious.
    maxdepth : Optional[int]
        see :func:`decode(…) <pyjson5.decode>`
    some : bool
        see :func:`decode(…) <pyjson5.decode>`
    wordlength : Optional[int]
        Must be 0, 1, 2, 4 to denote UTF-8, UCS1, USC2 or USC4 data, resp.
        Surrogates are not supported. Decode the data to an :class:`str` if need be.
        If ``None`` is supplied, then the buffer's ``itemsize`` is used.

    Raises
    ------
    Json5DecoderException
        An exception occured while decoding.
    TypeError
        An argument had a wrong type.
    ValueError
        The value of ``wordlength`` was invalid.

    Returns
    -------
    object
        see :func:`decode(…) <pyjson5.decode>`
    '''
    cdef Py_buffer view

    if maxdepth is None:
        maxdepth = DEFAULT_MAX_NESTING_LEVEL

    PyObject_GetBuffer(obj, &view, PyBUF_CONTIG_RO)
    try:
        if wordlength is None:
            wordlength = view.itemsize
        return _decode_buffer(view, wordlength, maxdepth, bool(some))
    finally:
        PyBuffer_Release(&view)


def decode_callback(object cb, object maxdepth=None, object some=False,
                    object args=None):
    '''
    Decodes JSON5 serialized data by invoking a callback.

    .. code:: python

        cb = iter('["Hello","world!"]').__next__

        decode_callback(cb) == ['Hello', 'world!']

    Parameters
    ----------
    cb : Callable[Any, Union[str|bytes|bytearray|int|None]]
        A function to get values from.
        The functions is called like ``cb(*args)``, and it returns:

        * **str, bytes, bytearray:** \
            ``len(…) == 0`` denotes exhausted input. \
            ``len(…) == 1`` is the next character.
        * **int:** \
            ``< 0`` denotes exhausted input. \
           ``>= 0`` is the ordinal value of the next character.
        * **None:** \
            input exhausted
    maxdepth : Optional[int]
        see :func:`decode(…) <pyjson5.decode>`
    some : bool
        see :func:`decode(…) <pyjson5.decode>`
    args : Optional[Iterable[Any]]
        Arguments to call ``cb`` with.

    Raises
    ------
    Json5DecoderException
        An exception occured while decoding.
    TypeError
        An argument had a wrong type.

    Returns
    -------
    object
        see :func:`decode(…) <pyjson5.decode>`
    '''
    if not callable(cb):
        raise TypeError(f'type(cb)=={type(cb)!r} is not callable')

    if maxdepth is None:
        maxdepth = DEFAULT_MAX_NESTING_LEVEL

    if args:
        args = tuple(args)
    else:
        args = ()

    return _decode_callback(cb, args, maxdepth, bool(some))


def decode_io(object fp, object maxdepth=None, object some=True):
    '''
    Decodes JSON5 serialized data from a file-like object.

    .. code:: python

        fp = io.StringIO("""
            ['Hello', /* TODO look into specs whom to greet */]
            'Wolrd' // FIXME: look for typos
        """)

        decode_io(fp) == ['Hello']
        decode_io(fp) == 'Wolrd'

        fp.seek(0)

        decode_io(fp, some=False)
        # raises Json5ExtraData('Extra data U+0027 near 56', ['Hello'], "'")

    Parameters
    ----------
    fp : IOBase
        A file-like object to parse from.
    maxdepth : Optional[int] = None
        see :func:`decode(…) <pyjson5.decode>`
    some : bool
        see :func:`decode(…) <pyjson5.decode>`

    Raises
    ------
    Json5DecoderException
        An exception occured while decoding.
    TypeError
        An argument had a wrong type.

    Returns
    -------
    object
        see :func:`decode(…) <pyjson5.decode>`
    '''
    if not isinstance(fp, IOBase):
        raise TypeError(f'type(fp)=={type(fp)!r} is not IOBase compatible')
    elif not fp.readable():
        raise TypeError(f'fp is not readable')
    elif fp.closed:
        raise TypeError(f'fp is closed')

    if maxdepth is None:
        maxdepth = DEFAULT_MAX_NESTING_LEVEL

    return _decode_callback(fp.read, (1,), maxdepth, bool(some))


def encode(object data, *, options=None, **options_kw):
    '''
    Serializes a Python object as a JSON5 compatible string.

    .. code:: python

        encode(['Hello', 'world!']) == '["Hello","world!"]'

    Parameters
    ----------
    data : object
        Python object to serialize.
    options : Optional[Options]
        Extra options for the encoder.
        If ``options`` **and** ``options_kw`` are specified, then ``options.update(**options_kw)`` is used.
    options_kw
        See Option's arguments.

    Raises
    ------
    Json5EncoderException
        An exception occured while encoding.
    TypeError
        An argument had a wrong type.

    Returns
    -------
    str
        Unless ``float('inf')`` or ``float('nan')`` is encountered, the result
        will be valid JSON data (as of RFC8259).

        The result is always ASCII. All characters outside of the ASCII range
        are escaped.

        The result safe to use in an HTML template, e.g.
        ``<a onclick='alert({{ encode(url) }})'>show message</a>``.
        Apostrophes ``"'"`` are encoded as ``"\\u0027"``, less-than,
        greater-than, and ampersand likewise.
    '''
    cdef void *temp
    cdef object result
    cdef Py_ssize_t start = (
        <Py_ssize_t> <void*> &(<AsciiObject*> NULL).data[0]
    )
    cdef Py_ssize_t length
    cdef object opts = _to_options(options, options_kw)
    cdef WriterReallocatable writer = WriterReallocatable(
        Writer(
            _WriterReallocatable_reserve,
            _WriterReallocatable_append_c,
            _WriterReallocatable_append_s,
            <PyObject*> opts,
        ),
        start, 0, NULL,
    )

    try:
        if expect(_encode(writer.base, data) < 0, False):
            exception_thrown()

        length = writer.position - start
        if length <= 0:
            # impossible
            return u''

        temp = ObjectRealloc(writer.obj, writer.position + 1)
        if temp is not NULL:
            writer.obj = temp
        (<char*> writer.obj)[writer.position] = 0

        result = ObjectInit(<PyObject*> writer.obj, unicode)
        writer.obj = NULL

        (<PyASCIIObject*> result).length = length
        reset_hash(<PyASCIIObject*> result)
        reset_wstr(<PyASCIIObject*> result)
        (<PyASCIIObject*> result).state.interned = SSTATE_NOT_INTERNED
        (<PyASCIIObject*> result).state.kind = PyUnicode_1BYTE_KIND
        (<PyASCIIObject*> result).state.compact = True
        set_ready(<PyASCIIObject*> result)
        (<PyASCIIObject*> result).state.ascii = True

        return result
    finally:
        if writer.obj is not NULL:
            ObjectFree(writer.obj)


def encode_bytes(object data, *, options=None, **options_kw):
    '''
    Serializes a Python object to a JSON5 compatible bytes string.

    .. code:: python

        encode_bytes(['Hello', 'world!']) == b'["Hello","world!"]'

    Parameters
    ----------
    data : object
        see :func:`encode(…) <pyjson5.encode>`
    options : Optional[Options]
        see :func:`encode(…) <pyjson5.encode>`
    options_kw
        see :func:`encode(…) <pyjson5.encode>`

    Raises
    ------
    Json5EncoderException
        An exception occured while encoding.
    TypeError
        An argument had a wrong type.

    Returns
    -------
    bytes
        see :func:`encode(…) <pyjson5.encode>`
    '''
    cdef void *temp
    cdef object result
    cdef Py_ssize_t start = (
        <Py_ssize_t> <void*> &(<PyBytesObject*> NULL).ob_sval[0]
    )
    cdef Py_ssize_t length
    cdef object opts = _to_options(options, options_kw)
    cdef WriterReallocatable writer = WriterReallocatable(
        Writer(
            _WriterReallocatable_reserve,
            _WriterReallocatable_append_c,
            _WriterReallocatable_append_s,
            <PyObject*> opts,
        ),
        start, 0, NULL,
    )

    try:
        if expect(_encode(writer.base, data) < 0, False):
            exception_thrown()

        length = writer.position - start
        if length <= 0:
            # impossible
            return b''

        temp = ObjectRealloc(writer.obj, writer.position + 1)
        if temp is not NULL:
            writer.obj = temp
        (<char*> writer.obj)[writer.position] = 0

        result = <object> <PyObject*> ObjectInitVar(
            (<PyVarObject*> writer.obj), bytes, length,
        )
        writer.obj = NULL

        reset_hash(<PyBytesObject*> result)

        return result
    finally:
        if writer.obj is not NULL:
            ObjectFree(writer.obj)


def encode_callback(object data, object cb, object supply_bytes=False, *,
                    options=None, **options_kw):
    '''
    Serializes a Python object into a callback function.

    The callback function ``cb`` gets called with single characters and strings
    until the input ``data`` is fully serialized.

    .. code:: python

        encode_callback(['Hello', 'world!'], print)
        #prints:
        # [
        # "
        # Hello
        # "
        # ,
        # "
        # world!
        # "
        " ]

    Parameters
    ----------
    data : object
        see :func:`encode(…) <pyjson5.encode>`
    cb : Callable[[Union[bytes|str]], None]
        A callback function.
        Depending on the truthyness of ``supply_bytes`` either :class:`bytes` or
        :class:`str` is supplied.
    supply_bytes : bool
        Call ``cb(…)`` with a :class:`bytes` argument if true,
        otherwise :class:`str`.
    options : Optional[Options]
        see :func:`encode(…) <pyjson5.encode>`
    options_kw
        see :func:`encode(…) <pyjson5.encode>`

    Raises
    ------
    Json5EncoderException
        An exception occured while encoding.
    TypeError
        An argument had a wrong type.

    Returns
    -------
    Callable[[Union[bytes|str]], None]
        The supplied argument ``cb``.
    '''
    cdef int (*encoder)(object obj, object cb, object options) except -1
    cdef Options opts = _to_options(options, options_kw)

    if supply_bytes:
        encoder = _encode_callback_bytes
    else:
        encoder = _encode_callback_str

    encoder(data, cb, options=opts)

    return cb


def encode_io(object data, object fp, object supply_bytes=True, *,
              options=None, **options_kw):
    '''
    Serializes a Python object into a file-object.

    The return value of :meth:`fp.write(…) <io.BufferedWriter.write>` is not checked.
    If ``fp`` is unbuffered, then the result will be garbage!

    Parameters
    ----------
    data : object
        see :func:`encode(…) <pyjson5.encode>`
    fp : IOBase
        A file-like object to serialize into.
    supply_bytes : bool
        Call :meth:`fp.write(…) <io.BufferedWriter.write>` with a :class:`bytes` argument if true,
        otherwise :class:`str`.
    options : Optional[Options]
        see :func:`encode(…) <pyjson5.encode>`
    options_kw
        see :func:`encode(…) <pyjson5.encode>`

    Raises
    ------
    Json5EncoderException
        An exception occured while encoding.
    TypeError
        An argument had a wrong type.

    Returns
    -------
    IOBase
        The supplied argument ``fp``.
    '''
    cdef int (*encoder)(object obj, object cb, object options) except -1
    cdef object opts = _to_options(options, options_kw)

    if not isinstance(fp, IOBase):
        raise TypeError(f'type(fp)=={type(fp)!r} is not IOBase compatible')
    elif not fp.writable():
        raise TypeError(f'fp is not writable')
    elif fp.closed:
        raise TypeError(f'fp is closed')

    if supply_bytes:
        encoder = _encode_callback_bytes
    else:
        encoder = _encode_callback_str

    encoder(data, fp.write, options=opts)

    return fp


def encode_noop(object data, *, options=None, **options_kw):
    '''
    Test if the input is serializable.

    Most likely you want to serialize ``data`` directly, and catch exceptions
    instead of using this function!

    .. code:: python

        encode_noop({47: 11}) == True
        encode_noop({47: object()}) == False

    Parameters
    ----------
    data : object
        see :func:`encode(…) <pyjson5.encode>`
    options : Optional[Options]
        see :func:`encode(…) <pyjson5.encode>`
    options_kw
        see :func:`encode(…) <pyjson5.encode>`

    Returns
    -------
    bool
        ``True`` iff ``data`` is serializable.
    '''
    cdef object opts = _to_options(options, options_kw)
    cdef Writer writer = Writer(
        _WriterNoop_reserve,
        _WriterNoop_append_c,
        _WriterNoop_append_s,
        <PyObject*> opts,
    )

    if expect(_encode(writer, data) < 0, False):
        exception_thrown()

    return True


__all__ = (
    # DECODE
    'decode', 'decode_utf8', 'decode_latin1', 'decode_buffer', 'decode_callback', 'decode_io',
    # ENCODE
    'encode', 'encode_bytes', 'encode_callback', 'encode_io', 'encode_noop', 'Options',
    # LEGACY
    'loads', 'load', 'dumps', 'dump',
    # EXCEPTIONS
    'Json5Exception',
    'Json5EncoderException', 'Json5UnstringifiableType',
    'Json5DecoderException', 'Json5NestingTooDeep', 'Json5EOF', 'Json5IllegalCharacter', 'Json5ExtraData', 'Json5IllegalType',
)

__doc__ = PyUnicode_FromKindAndData(PyUnicode_1BYTE_KIND, LONGDESCRIPTION, LONGDESCRIPTION_LENGTH)

__license__ = 'MIT OR Apache-2.0'

__author__ = '2018-2025 René Kijewski <pypi.org@k6i.de>'
