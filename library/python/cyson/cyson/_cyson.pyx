# -*- coding: utf-8 -*-
#cython: embedsignature=True
#cython: infer_types=True

cimport cython
cimport libc.stdio
cimport libc.string
cimport libcyson as C

from libc.stdint cimport uint64_t, int64_t
from libc.stddef cimport size_t

cimport cpython.pycapsule

from cpython.bytearray cimport (
    PyByteArray_FromStringAndSize,
    PyByteArray_Resize,
    PyByteArray_AS_STRING,
    PyByteArray_GET_SIZE,
)
from cpython.bytes cimport (
    PyBytes_Check,
    PyBytes_FromStringAndSize,
    PyBytes_AS_STRING,
    PyBytes_GET_SIZE,
)
from cpython.dict cimport PyDict_Next, PyDict_Copy, PyDict_Check
from cpython.int cimport PyInt_FromLong
from cpython.long cimport PyLong_AsUnsignedLong
from cpython.list cimport PyList_GET_SIZE, PyList_GET_ITEM
from cpython.object cimport (
    PyObject, PyTypeObject,
    PyObject_GetIter, PyObject_GetItem, PyObject_CallMethod,
)
from cpython.ref cimport Py_DECREF
from cpython.tuple cimport PyTuple_GET_SIZE, PyTuple_GET_ITEM
from cpython.unicode cimport (
    PyUnicode_AsUTF8String, PyUnicode_DecodeUTF8, PyUnicode_Check
)


cdef extern from "Python.h":
    # Actually returns a new reference (if an iterator isn't exhausted), but
    # returns NULL without setting an exception for exhausted iterator (so
    # return type isn't `object`). This declaration requires manual `Py_DECREF`
    # calls (cython thinks the return value is a borrowed reference).
    PyObject* PyIter_Next(object) except? NULL


cdef extern from "library/python/cyson/cyson/helpers.h":
    libc.stdio.FILE* PyFile_AsFile(object) except NULL
    bint PyFile_CheckExact(object)
    bint GenericCheckBuffer(object)


cdef extern from "library/python/cyson/cyson/helpers.h" namespace "NCYson":
    bint PY3
    bint PY2

    object ConvertPyStringToPyNativeString(object)
    bytes ConvertPyStringToPyBytes(object)
    object GetCharBufferAndOwner(object, const char**, size_t*)
    bytes ConvertPyLongToPyBytes(object)


cdef extern from "library/python/cyson/cyson/unsigned_long.h" namespace "NCYson":
    PyTypeObject PyUnsignedLong_Type

    type PreparePyUIntType(object)
    object ConstructPyNumberFromUint(uint64_t)


cdef struct PycStringIO_CAPI:
    int (*cread)(object, char **, Py_ssize_t) except -1
    int (*creadline)(object, char **) except -1
    int (*cwrite)(object, const char *, Py_ssize_t) except -1
    bytes (*cgetvalue)(object)
    object (*NewOutput)(int)
    object (*NewInput)(object)
    void* InputType
    void* OutputType


# set numpy aliases for possible use
cdef np = None
cdef np_import_failed = None
cdef npy_generic = None
cdef npy_integers = None
cdef npy_uintegers = None
cdef npy_floats = None


cdef PycStringIO_CAPI* cStringIO_CAPI
if PY2:
    cStringIO_CAPI = <PycStringIO_CAPI*>cpython.pycapsule.PyCapsule_Import(
        "cStringIO.cStringIO_CAPI", 0
    )


class UnableToSerializeError(TypeError):
    def __init__(self, value):
        super(UnableToSerializeError, self).__init__(value)
        self.value = value

    def __str__(self):
        return (
            "Unable to serialize an object of type {!r}: {!r}"
            .format(type(self.value), self.value)
        )


def _yson_repr(obj):
    return ConvertPyStringToPyNativeString(dumps(obj))


UInt = PreparePyUIntType(_yson_repr)


class YsonEntity(object):
    def __init__(self, attributes=None):
        self.attributes = attributes

    def __repr__(self):
        return _yson_repr(self)

    def __eq__(self, other):
        return type(other) is YsonEntity and self.attributes == other.attributes


class YsonString(bytes):
    def __new__(cls, value, attributes=None):
        self = bytes.__new__(cls, ConvertPyStringToPyBytes(value))
        self.attributes = attributes
        return self

    def __repr__(self):
        return _yson_repr(self)


class YsonInt64(int):
    def __new__(cls, value, attributes=None):
        self = int.__new__(cls, value)
        self.attributes = attributes
        return self

    def __repr__(self):
        return _yson_repr(self)


class YsonUInt64(long):
    def __new__(cls, value, attributes=None):
        self = long.__new__(cls, value)
        self.attributes = attributes
        return self

    def __repr__(self):
        return _yson_repr(self)


class YsonFloat64(float):
    def __new__(cls, value, attributes=None):
        self = float.__new__(cls, value)
        self.attributes = attributes
        return self

    def __repr__(self):
        return _yson_repr(self)


class YsonBoolean(int):
    def __new__(cls, value, attributes=None):
        self = int.__new__(cls, bool(value))
        self.attributes = attributes
        return self

    def __repr__(self):
        return _yson_repr(self)


class YsonList(list):
    def __new__(cls, value, attributes=None):
        self = list.__new__(cls, value)
        self.attributes = attributes
        return self

    def __repr__(self):
        return _yson_repr(self)


class YsonMap(dict):
    def __new__(cls, value, attributes=None):
        self = dict.__new__(cls, value)
        self.attributes = attributes
        return self

    def __repr__(self):
        return _yson_repr(self)


# Input

@cython.freelist(16)
cdef class InputStream:
    """YSON input stream adaptor.

    Provides means for YSON Reader to read raw bytes from some stream.

    A proper way to construct an InputStream instance is to use
    one of its static constructor methods
    (:meth:`from_string`, :meth:`from_file`, etc).

    """

    cdef object capsule
    cdef object data

    def __cinit__(self, capsule, data=None):
        self.capsule = capsule
        self.data = data

    cdef C.yson_input_stream* ptr(self):
        return _c_open_yson_input_stream_capsule(self.capsule)

    @staticmethod
    def from_string(value):
        """Read from a contiguous memory buffer.

        :param value: Input memory buffer: any object implementing char buffer protocol (i.e. str/bytes).

        """

        cdef const char* data
        cdef size_t size
        cdef object holder = GetCharBufferAndOwner(value, &data, &size)

        capsule = _c_make_yson_input_stream_capsule(
            C.yson_input_stream_from_string(data, size)
        )

        return InputStream.__new__(InputStream, capsule, holder)

    @staticmethod
    def from_file(file_obj, int buffer_size=65536):
        """Read from an arbitrary file-like object.

        Two special cases are available within CPython:
        - Builtin python file objects (effectively ``FILE*`` wrappers)
        - :class:`cStringIO.StringIO` objects

        In other cases ``file_obj`` is required to have a read() method
        accepting integer argument.

        """

        cdef libc.stdio.FILE* c_file

        # Special case for cStringIO streams, both types support reading
        t = type(file_obj)
        if cStringIO_CAPI and (
            t is <object>cStringIO_CAPI.InputType or
            t is <object>cStringIO_CAPI.OutputType
        ):
            yson_input_stream = C.yson_input_stream_new(
                <void*>file_obj,
                _c_yson_input_stream_cstringio_read,
            )
        # Special case for building file objects (effectively FILE* wrappers)
        elif PyFile_CheckExact(file_obj):
            yson_input_stream = C.yson_input_stream_from_file(
                PyFile_AsFile(file_obj), buffer_size
            )
        # General case: use Python read(<size>) method
        else:
            file_obj = _ReadableHolder.__new__(
                _ReadableHolder, file_obj, buffer_size
            )
            yson_input_stream = C.yson_input_stream_new(
                <void*>file_obj,
                _c_yson_input_stream_generic_read,
            )

        return InputStream.__new__(
            InputStream,
            _c_make_yson_input_stream_capsule(yson_input_stream),
            file_obj,
        )

    @staticmethod
    def from_fd(int fd, int buffer_size=65536):
        """Read from a POSIX file descriptor.

        :param fd: File descriptor number.
        :param buffer_size: InputStream internal buffer size.

        """

        capsule = _c_make_yson_input_stream_capsule(
            C.yson_input_stream_from_fd(fd, buffer_size)
        )
        return InputStream.__new__(InputStream, capsule)

    @staticmethod
    def from_iter(iter_obj):
        """Read from a chunked input stream.

        :param iter_obj: An iterator which yields a series
        of contiguous memory buffer objects.

        >>> from cyson import InputStream, Reader
        >>> def stream():
        ...     yield '['
        ...     yield '123;'
        ...     yield 'hel'
        ...     yield 'lo;'
        ...     yield 'world;'
        ...     yield '#;'
        ...     yield ']'
        ...
        >>> Reader(InputStream.from_iter(stream()), mode='node').node()
        [123, 'hello', 'world', None]

        """

        holder = _IteratorHolder.__new__(_IteratorHolder, iter_obj)
        capsule = _c_make_yson_input_stream_capsule(
            C.yson_input_stream_new(<void*> holder, _c_yson_input_stream_next)
        )
        return InputStream.__new__(InputStream, capsule, holder)


cdef void _c_destroy_yson_input_stream_capsule(object capsule):
    cdef C.yson_input_stream* c_stream
    c_stream = _c_open_yson_input_stream_capsule(capsule)
    C.yson_input_stream_delete(c_stream)


cdef inline C.yson_input_stream* _c_open_yson_input_stream_capsule(object capsule):
    return <C.yson_input_stream*> cpython.pycapsule.PyCapsule_GetPointer(
        capsule,
        "yson_input_stream"
    )


cdef inline object _c_make_yson_input_stream_capsule(C.yson_input_stream* c_stream):
    return cpython.pycapsule.PyCapsule_New(
        c_stream,
        "yson_input_stream",
        _c_destroy_yson_input_stream_capsule,
    )


cdef class _IteratorHolder:
    cdef object iter_
    cdef object last_buffer_holder_

    def __cinit__(self, obj):
        self.iter_ = obj
        self.last_buffer_holder_ = None

    cdef inline get_next_buffer(self, const char** data, size_t* size):
        self.last_buffer_holder_ = GetCharBufferAndOwner(next(self.iter_), data, size)


cdef class _ReadableHolder:
    cdef object fileobj_
    cdef object chunk_size_
    cdef size_t c_chunk_size_
    cdef object last_buffer_holder_

    def __cinit__(self, fileobj, chunk_size):
        self.fileobj_ = fileobj
        self.chunk_size_ = chunk_size
        self.c_chunk_size_ = <size_t>PyLong_AsUnsignedLong(chunk_size)

    cdef inline read_chunk(self, const char** data, size_t* size):
        self.last_buffer_holder_ = GetCharBufferAndOwner(
            self.fileobj_.read(self.chunk_size_), data, size
        )


cdef C.yson_input_stream_result _c_yson_input_stream_next(
    void* ctx, const char** ptr, size_t* length
) except C.YSON_INPUT_STREAM_RESULT_ERROR:
    try:
        (<_IteratorHolder>ctx).get_next_buffer(ptr, length)
    except StopIteration:
        return C.YSON_INPUT_STREAM_RESULT_EOF
    else:
        return C.YSON_INPUT_STREAM_RESULT_OK


cdef C.yson_input_stream_result _c_yson_input_stream_cstringio_read(
    void* ctx, const char** ptr, size_t* length
) except C.YSON_INPUT_STREAM_RESULT_ERROR:
    """Callback for reading from cStringIO.StringIO objects."""

    nread = cStringIO_CAPI.cread(<object>ctx, <char**>ptr, -1)
    length[0] = nread

    if nread > 0:
        return C.YSON_INPUT_STREAM_RESULT_OK
    if nread == 0:
        return C.YSON_INPUT_STREAM_RESULT_EOF
    else:
        return C.YSON_INPUT_STREAM_RESULT_ERROR


cdef C.yson_input_stream_result _c_yson_input_stream_generic_read(
    void* ctx, const char** ptr, size_t* length
) except C.YSON_INPUT_STREAM_RESULT_ERROR:
    """Callback for reading from arbitrary Python file-like objects."""

    cdef _ReadableHolder holder = <_ReadableHolder>ctx

    holder.read_chunk(ptr, length)

    if length[0] > holder.c_chunk_size_:
        raise RuntimeError(
            'reading inconsistency: {} bytes were read, but only {} requested'
            .format(length[0], holder.chunk_size_)
        )

    if length[0] > 0:
        return C.YSON_INPUT_STREAM_RESULT_OK
    elif length[0] == 0:
        return C.YSON_INPUT_STREAM_RESULT_EOF
    else:
        return C.YSON_INPUT_STREAM_RESULT_ERROR


# Output

@cython.freelist(16)
cdef class OutputStream:
    """YSON output stream adaptor.

    Provides means for YSON Writer to write raw bytes to some stream.

    A proper way to construct an InputStream instance is to use
    one of its static constructor methods
    (:meth:`from_file`, :meth:`from_fd`).

    To write into a string, use :meth:`to_file` with
    a :class:`cStringIO.StringIO` instance.

    To write into a custom stream, provide your object
    with ``write()`` method.

    """

    cdef object capsule
    cdef object data

    def __cinit__(self, capsule, data = None):
        self.capsule = capsule
        self.data = data

    cdef C.yson_output_stream* ptr(self):
        return _c_open_yson_output_stream_capsule(self.capsule)

    @staticmethod
    def from_file(file_obj, int buffer_size=65536):
        """Write to an arbitrary file-like object.

        A file object is required to have a write() method accepting str/bytes
        arguments.

        Two special cases are available within CPython:
        - Builtin python file objects (effectively ``FILE*`` wrappers).
        - :class:`cStringIO.StringIO` objects.

        For these cases, special optimized implementations are used.

        """

        cdef libc.stdio.FILE* c_file

        # Special case for cStringIO streams
        if cStringIO_CAPI and type(file_obj) is <object> cStringIO_CAPI.OutputType:
            capsule = _c_make_yson_output_stream_capsule(
                C.yson_output_stream_new(
                    <void*> file_obj,
                    _c_yson_output_stream_cstringio_write,
                    buffer_size
                )
            )
        # Special case for builting file objects (effectively FILE* wrappers)
        elif PyFile_CheckExact(file_obj):
            c_file = PyFile_AsFile(file_obj)
            capsule = _c_make_yson_output_stream_capsule(
                C.yson_output_stream_from_file(
                    c_file,
                    buffer_size
                )
            )
        # General case: use python write() method
        else:
            capsule = _c_make_yson_output_stream_capsule(
                C.yson_output_stream_new(
                    <void*> file_obj,
                    _c_yson_output_stream_write,
                    buffer_size
                )
            )

        return OutputStream.__new__(OutputStream, capsule, file_obj)

    @staticmethod
    def from_fd(int fd, int buffer_size=65536):
        """Write to a POSIX file descriptor.

        :param fd: File descriptor number.
        :param buffer_size: OutputStream internal buffer size.

        """

        capsule = _c_make_yson_output_stream_capsule(
            C.yson_output_stream_from_fd(fd, buffer_size)
        )
        return OutputStream.__new__(OutputStream, capsule)

    @staticmethod
    def from_bytearray(bytearray dest, int buffer_size=0):
        capsule = _c_make_yson_output_stream_capsule(
            C.yson_output_stream_new(
                <void*> dest,
                _c_yson_output_stream_bytearray_write,
                buffer_size
            )
        )
        return OutputStream.__new__(OutputStream, capsule, dest)


cdef void _c_destroy_yson_output_stream_capsule(object capsule):
    cdef C.yson_output_stream* c_stream
    c_stream = _c_open_yson_output_stream_capsule(capsule)
    C.yson_output_stream_delete(c_stream)


cdef inline C.yson_output_stream* _c_open_yson_output_stream_capsule(object capsule):
    return <C.yson_output_stream*> cpython.pycapsule.PyCapsule_GetPointer(
        capsule,
        "yson_output_stream"
    )


cdef inline object _c_make_yson_output_stream_capsule(C.yson_output_stream* c_stream):
    return cpython.pycapsule.PyCapsule_New(
        c_stream,
        "yson_output_stream",
        _c_destroy_yson_output_stream_capsule,
    )


cdef C.yson_output_stream_result _c_yson_output_stream_write(
    void* ctx,
    const char* ptr,
    size_t length
) except C.YSON_OUTPUT_STREAM_RESULT_ERROR:
    """Callback for writing into arbitrary Python file objects."""

    obj = <object> ctx
    data = PyBytes_FromStringAndSize(ptr, length)
    obj.write(data)
    return C.YSON_OUTPUT_STREAM_RESULT_OK


cdef C.yson_output_stream_result _c_yson_output_stream_cstringio_write(
    void* ctx,
    const char* ptr,
    size_t length
) except C.YSON_OUTPUT_STREAM_RESULT_ERROR:
    """Callback for writing into cStringIO.StringIO objects."""

    obj = <object> ctx
    cStringIO_CAPI.cwrite(obj, ptr, length);
    return C.YSON_OUTPUT_STREAM_RESULT_OK


cdef C.yson_output_stream_result _c_yson_output_stream_bytearray_write(
    void* ctx,
    const char* ptr,
    size_t length
) except C.YSON_OUTPUT_STREAM_RESULT_ERROR:
    """Callback for writing into bytearray objects."""

    cdef bytearray obj = <bytearray>ctx
    cdef size_t old_length = PyByteArray_GET_SIZE(obj)

    PyByteArray_Resize(obj, old_length + length)
    libc.string.memcpy(PyByteArray_AS_STRING(obj) + old_length, ptr, length)

    return C.YSON_OUTPUT_STREAM_RESULT_OK


# Reader

cdef class ListFragmentIterator

@cython.freelist(16)
cdef class Reader:
    cdef C.yson_reader* c_reader
    cdef InputStream stream

    def __cinit__(self, InputStream stream not None, mode=b'node'):
        """Create a YSON Reader for reading from ``stream``.

        Attributes on values are ignored.

        :param stream: Input stream object.
        :param mode: Input stream shape: 'node', 'list_frament', 'map_fragment'.

        """
        cdef C.yson_stream_type stream_type

        cdef bytes bytes_mode = ConvertPyStringToPyBytes(mode)

        if bytes_mode == b'node':
            stream_type = C.YSON_STREAM_TYPE_NODE
        elif bytes_mode == b'list_fragment':
            stream_type = C.YSON_STREAM_TYPE_LIST_FRAGMENT
        elif bytes_mode == b'map_fragment':
            stream_type = C.YSON_STREAM_TYPE_MAP_FRAGMENT
        else:
            raise ValueError("Invalid reader mode {!r}".format(bytes_mode))

        self.c_reader = C.yson_reader_new(
            stream.ptr(),
            stream_type
        )
        self.stream = stream

    def __dealloc__(self):
        C.yson_reader_delete(self.c_reader)

    cdef _scalar_handler(self, value, dict attributes):
        if value is None:
            return YsonEntity(attributes)
        return value

    cdef _list_handler(self, ListFragmentIterator items, dict attributes):
        return YsonList(items)

    cdef _map_handler(self, MapFragmentIterator items, dict attributes):
        return YsonMap(items)

    cdef _read_object(self, C.yson_event_type event_type):
        return _reader_read_object(self, event_type)

    def node(self):
        """Read whole input stream as a single YSON node.

        >>> from cyson import InputStream, Reader

        >>> s = '{key=1;value=foo}'
        >>> r = Reader(InputStream.from_string(s), mode='node')
        >>> r.node() == {'value': 'foo', 'key': 1}
        True

        Invalid stream shape results in exception:

        >>> s = '{key=1;value=foo}; {key=2; value=bar}'
        >>> r = Reader(InputStream.from_string(s), mode='node')
        >>> r.node()
        Traceback (most recent call last):
            ...
        ValueError: Invalid YSON at offset 17: Expected stream end, but found ";"

        """

        cdef C.yson_event_type event_type

        event_type = _c_yson_reader_get_next_event(self.c_reader)
        assert event_type == C.YSON_EVENT_BEGIN_STREAM

        event_type = _c_yson_reader_get_next_event(self.c_reader)
        result = self._read_object(event_type)

        event_type = _c_yson_reader_get_next_event(self.c_reader)
        assert event_type == C.YSON_EVENT_END_STREAM

        return result

    def list_fragments(
        self, process_table_index=False, process_attributes=False,
        stop_at_key_switch=False, keep_control_records=False
    ):
        """Iterate over input stream as a sequence of objects.

        >>> from cyson import InputStream, Reader

        >>> s = '{key=1;value=foo}; {key=2; value=bar}'
        >>> r = Reader(InputStream.from_string(s), mode='list_fragment')
        >>> l = list(r.list_fragments())
        >>> l == [{'value': 'foo', 'key': 1}, {'value': 'bar', 'key': 2}]
        True

        """

        cdef C.yson_event_type event_type

        event_type = _c_yson_reader_get_next_event(self.c_reader)
        assert event_type == C.YSON_EVENT_BEGIN_STREAM

        # NOTE: for backward compatibility purpose
        process_attributes = process_attributes or process_table_index

        return ListFragmentIterator(
            self,
            C.YSON_EVENT_END_STREAM,
            process_attributes,
            stop_at_key_switch,
            keep_control_records,
        )

    def map_fragments(self):
        """Iterate over input stream as a sequence of (key, value) pairs.

        >>> from cyson import InputStream, Reader

        >>> s = 'a=b; c=d; e=[1;3;4]'
        >>> r = Reader(InputStream.from_string(s), mode='map_fragment')
        >>> list(r.map_fragments())
        [('a', 'b'), ('c', 'd'), ('e', [1, 3, 4])]

        """

        cdef C.yson_event_type event_type

        event_type = _c_yson_reader_get_next_event(self.c_reader)
        assert event_type == C.YSON_EVENT_BEGIN_STREAM

        return MapFragmentIterator(self, C.YSON_EVENT_END_STREAM)


@cython.freelist(16)
cdef class ListFragmentIterator:
    cdef Reader reader
    cdef readonly bint at_begin
    cdef readonly bint at_end
    cdef bint stop_at_key_switch
    cdef C.yson_event_type end_event
    cdef bint process_attributes
    cdef bint keep_control_records
    cdef readonly int table_index
    cdef int64_t row_index_base
    cdef int64_t row_index_offset
    cdef int64_t range_index
    cdef readonly bint is_key_switched

    def __cinit__(
        self,
        Reader reader not None,
        C.yson_event_type end_event,
        bint process_attributes,
        bint stop_at_key_switch,
        bint keep_control_records
    ):
        self.reader = reader
        self.at_begin = True
        self.at_end = False
        self.end_event = end_event
        self.process_attributes = process_attributes
        self.stop_at_key_switch = stop_at_key_switch
        self.keep_control_records = keep_control_records
        self.table_index = 0
        self.row_index_base = -1
        self.row_index_offset = -1
        self.range_index = -1
        self.is_key_switched = False

    def __iter__(self):
        return self

    cpdef close(self):
        if not self.at_end:
            for _ in self:
                pass
            self.at_end = True

    def __next__(self):
        cdef C.yson_event_type event_type
        cdef bint was_at_begin = self.at_begin

        if self.at_end:
            raise StopIteration

        self.at_begin = False
        self.is_key_switched = False

        event_type = _c_yson_reader_get_next_event(self.reader.c_reader)
        if event_type == self.end_event:
            self.at_end = True
            raise StopIteration

        value = self.reader._read_object(event_type)
        if self.process_attributes and isinstance(value, YsonEntity):
            self.do_process_attributes(value.attributes)

            if not was_at_begin and self.stop_at_key_switch and self.is_key_switched:
                raise StopIteration

            if self.keep_control_records:
                return value

            return self.__next__()
        else:
            self.row_index_offset += 1
            return value

    property row_index:
        def __get__(self):
            if self.row_index_base >= 0:
                return self.row_index_base + self.row_index_offset

    property range_index:
        def __get__(self):
            if self.range_index >= 0:
                return self.range_index

    cdef do_process_attributes(self, dict attributes):
        table_index = attributes.get(b'table_index')
        if table_index is not None:
            self.table_index = table_index

        row_index = attributes.get(b'row_index')
        if row_index is not None:
            self.row_index_base = row_index
            self.row_index_offset = -1

        range_index = attributes.get(b'range_index')
        if range_index is not None:
            self.range_index = range_index

        key_switch = attributes.get(b'key_switch')
        if key_switch is not None:
            self.is_key_switched = key_switch


cdef class MapFragmentIterator:
    cdef Reader reader
    cdef bint at_end
    cdef C.yson_event_type end_event

    def __cinit__(self,
                  Reader reader not None,
                  C.yson_event_type end_event):
        self.reader = reader
        self.at_end = False
        self.end_event = end_event

    def __iter__(self):
        return self

    cpdef close(self):
        if not self.at_end:
            for _ in self:
                pass
            self.at_end = True

    def __next__(self):
        cdef C.yson_event_type event_type

        if self.at_end:
            raise StopIteration

        event_type = _c_yson_reader_get_next_event(self.reader.c_reader)
        if event_type == self.end_event:
            self.at_end = True
            raise StopIteration

        key = _c_yson_reader_get_byte_string(self.reader.c_reader)
        value = self.reader._read_object(
            _c_yson_reader_get_next_event(self.reader.c_reader)
        )
        return key, value


cdef inline bytes _c_yson_reader_get_error(C.yson_reader* c_reader):
    return <bytes> C.yson_reader_get_error_message(c_reader)


cdef inline C.yson_event_type _c_yson_reader_get_next_event(
    C.yson_reader* c_reader
) except C.YSON_EVENT_ERROR:
    cdef C.yson_event_type event_type

    event_type = C.yson_reader_get_next_event(c_reader)
    # A propagated exception would have fired earlier
    if event_type == C.YSON_EVENT_ERROR:
        raise ValueError(_c_yson_reader_get_error(c_reader))
    return event_type


cdef object _c_yson_reader_get_scalar(C.yson_reader* c_reader):
    cdef C.yson_scalar_type scalar_type

    scalar_type = C.yson_reader_get_scalar_type(c_reader)
    if scalar_type == C.YSON_SCALAR_ENTITY:
        return None
    elif scalar_type == C.YSON_SCALAR_BOOLEAN:
        return C.yson_reader_get_boolean(c_reader)
    elif scalar_type == C.YSON_SCALAR_INT64:
        return PyInt_FromLong(C.yson_reader_get_int64(c_reader))
    elif scalar_type == C.YSON_SCALAR_UINT64:
        return ConstructPyNumberFromUint(C.yson_reader_get_uint64(c_reader))
    elif scalar_type == C.YSON_SCALAR_FLOAT64:
        return C.yson_reader_get_float64(c_reader)
    elif scalar_type == C.YSON_SCALAR_STRING:
        return _c_yson_reader_get_byte_string(c_reader)


cdef inline bytes _c_yson_reader_get_byte_string(C.yson_reader* c_reader):
    cdef const C.yson_string* ref

    ref = C.yson_reader_get_string(c_reader)
    return PyBytes_FromStringAndSize(ref.ptr, ref.length)


cdef list _reader_read_list(Reader reader):
    cdef C.yson_event_type event_type

    result = []
    while True:
        event_type = _c_yson_reader_get_next_event(reader.c_reader)
        if event_type == C.YSON_EVENT_END_LIST:
            return result
        else:
            result.append(_reader_read_object(reader, event_type))


cdef dict _reader_read_map(
    Reader reader,
    C.yson_event_type end = C.YSON_EVENT_END_MAP,
):
    cdef C.yson_event_type event_type

    result = {}
    while True:
        event_type = _c_yson_reader_get_next_event(reader.c_reader)
        if event_type == end:
            return result
        else:
            key = _c_yson_reader_get_byte_string(reader.c_reader)
            value = _reader_read_object(
                reader,
                _c_yson_reader_get_next_event(reader.c_reader)
            )
            result[key] = value


cdef _reader_read_object(
    Reader reader,
    C.yson_event_type event_type,
):
    if event_type == C.YSON_EVENT_SCALAR:
        return _c_yson_reader_get_scalar(reader.c_reader)

    elif event_type == C.YSON_EVENT_BEGIN_LIST:
        return _reader_read_list(reader)

    elif event_type == C.YSON_EVENT_BEGIN_MAP:
        return _reader_read_map(reader)

    elif event_type == C.YSON_EVENT_BEGIN_ATTRIBUTES:
        attributes = _reader_read_map(reader, C.YSON_EVENT_END_ATTRIBUTES)
        event_type = _c_yson_reader_get_next_event(reader.c_reader)
        return _reader_read_object_with_attributes(reader, event_type, attributes)


cdef _reader_read_object_with_attributes(
    Reader reader,
    C.yson_event_type event_type,
    dict attributes,
):
    cdef ListFragmentIterator l_items
    cdef MapFragmentIterator m_items

    if event_type == C.YSON_EVENT_SCALAR:
        return reader._scalar_handler(
            _c_yson_reader_get_scalar(reader.c_reader),
            attributes
        )

    elif event_type == C.YSON_EVENT_BEGIN_LIST:
        l_items = ListFragmentIterator.__new__(
            ListFragmentIterator,
            reader,
            C.YSON_EVENT_END_LIST,
            False,
            False,
            False
        )
        result = reader._list_handler(l_items, attributes)
        l_items.close()
        return result

    elif event_type == C.YSON_EVENT_BEGIN_MAP:
        m_items = MapFragmentIterator.__new__(
            MapFragmentIterator,
            reader,
            C.YSON_EVENT_END_MAP,
        )
        result = reader._map_handler(m_items, attributes)
        m_items.close()
        return result


# StrictReader

cdef class StrictReader(Reader):

    cdef _read_object(self, C.yson_event_type event_type):
        return _strict_reader_read_object(self, event_type)


cdef object _c_yson_reader_get_scalar_strict(C.yson_reader* c_reader):
    cdef C.yson_scalar_type scalar_type

    scalar_type = C.yson_reader_get_scalar_type(c_reader)
    if scalar_type == C.YSON_SCALAR_ENTITY:
        return YsonEntity()
    elif scalar_type == C.YSON_SCALAR_BOOLEAN:
        return YsonBoolean(C.yson_reader_get_boolean(c_reader))
    elif scalar_type == C.YSON_SCALAR_INT64:
        return YsonInt64(C.yson_reader_get_int64(c_reader))
    elif scalar_type == C.YSON_SCALAR_UINT64:
        return YsonUInt64(C.yson_reader_get_uint64(c_reader))
    elif scalar_type == C.YSON_SCALAR_FLOAT64:
        return YsonFloat64(C.yson_reader_get_float64(c_reader))
    elif scalar_type == C.YSON_SCALAR_STRING:
        return YsonString(_c_yson_reader_get_byte_string(c_reader))


cdef _strict_reader_read_list(Reader reader):
    cdef C.yson_event_type event_type

    result = YsonList([])
    while True:
        event_type = _c_yson_reader_get_next_event(reader.c_reader)
        if event_type == C.YSON_EVENT_END_LIST:
            return result
        else:
            result.append(_strict_reader_read_object(reader, event_type))


cdef _strict_reader_read_map(
    Reader reader,
    C.yson_event_type end = C.YSON_EVENT_END_MAP,
):
    cdef C.yson_event_type event_type

    result = YsonMap({})
    while True:
        event_type = _c_yson_reader_get_next_event(reader.c_reader)
        if event_type == end:
            return result
        else:
            key = _c_yson_reader_get_byte_string(reader.c_reader)
            value = _strict_reader_read_object(
                reader,
                _c_yson_reader_get_next_event(reader.c_reader)
            )
            result[key] = value


cdef _strict_reader_read_object(
    Reader reader,
    C.yson_event_type event_type,
):
    if event_type == C.YSON_EVENT_SCALAR:
        return _c_yson_reader_get_scalar_strict(reader.c_reader)

    elif event_type == C.YSON_EVENT_BEGIN_LIST:
        return _strict_reader_read_list(reader)

    elif event_type == C.YSON_EVENT_BEGIN_MAP:
        return _strict_reader_read_map(reader)

    elif event_type == C.YSON_EVENT_BEGIN_ATTRIBUTES:
        attributes = _strict_reader_read_map(reader, C.YSON_EVENT_END_ATTRIBUTES)
        event_type = _c_yson_reader_get_next_event(reader.c_reader)
        obj = _strict_reader_read_object(reader, event_type)
        obj.attributes = PyDict_Copy(attributes)
        return obj


# UnicodeDecodeReader

cdef class UnicodeReader(Reader):

    cdef _read_object(self, C.yson_event_type event_type):
        return _unicode_reader_read_object(self, event_type)


cdef inline unicode _c_yson_reader_get_unicode_string(C.yson_reader* c_reader):
    cdef const C.yson_string* ref

    ref = C.yson_reader_get_string(c_reader)
    return PyUnicode_DecodeUTF8(ref.ptr, ref.length, NULL)


cdef object _c_yson_unicode_reader_get_scalar(C.yson_reader* c_reader):
    cdef C.yson_scalar_type scalar_type

    scalar_type = C.yson_reader_get_scalar_type(c_reader)
    if scalar_type == C.YSON_SCALAR_ENTITY:
        return None
    elif scalar_type == C.YSON_SCALAR_BOOLEAN:
        return C.yson_reader_get_boolean(c_reader)
    elif scalar_type == C.YSON_SCALAR_INT64:
        return PyInt_FromLong(C.yson_reader_get_int64(c_reader))
    elif scalar_type == C.YSON_SCALAR_UINT64:
        return ConstructPyNumberFromUint(C.yson_reader_get_uint64(c_reader))
    elif scalar_type == C.YSON_SCALAR_FLOAT64:
        return C.yson_reader_get_float64(c_reader)
    elif scalar_type == C.YSON_SCALAR_STRING:
        return _c_yson_reader_get_unicode_string(c_reader)


cdef list _unicode_reader_read_list(Reader reader):
    cdef C.yson_event_type event_type

    result = []
    while True:
        event_type = _c_yson_reader_get_next_event(reader.c_reader)
        if event_type == C.YSON_EVENT_END_LIST:
            return result
        else:
            result.append(_unicode_reader_read_object(reader, event_type))


cdef dict _unicode_reader_read_map(
    Reader reader,
    C.yson_event_type end = C.YSON_EVENT_END_MAP,
):
    cdef C.yson_event_type event_type

    result = {}
    while True:
        event_type = _c_yson_reader_get_next_event(reader.c_reader)
        if event_type == end:
            return result
        else:
            key = _c_yson_reader_get_unicode_string(reader.c_reader)
            value = _unicode_reader_read_object(
                reader,
                _c_yson_reader_get_next_event(reader.c_reader),
            )
            result[key] = value


cdef _unicode_reader_read_object_with_attributes(
    Reader reader,
    C.yson_event_type event_type,
    dict attributes,
):
    cdef ListFragmentIterator l_items
    cdef MapFragmentIterator m_items

    if event_type == C.YSON_EVENT_SCALAR:
        return reader._scalar_handler(
            _c_yson_unicode_reader_get_scalar(reader.c_reader),
            attributes
        )

    elif event_type == C.YSON_EVENT_BEGIN_LIST:
        l_items = ListFragmentIterator.__new__(
            ListFragmentIterator,
            reader,
            C.YSON_EVENT_END_LIST,
            False,
            False,
            False
        )
        result = reader._list_handler(l_items, attributes)
        l_items.close()
        return result

    elif event_type == C.YSON_EVENT_BEGIN_MAP:
        m_items = MapFragmentIterator.__new__(
            MapFragmentIterator,
            reader,
            C.YSON_EVENT_END_MAP,
        )
        result = reader._map_handler(m_items, attributes)
        m_items.close()
        return result


cdef _unicode_reader_read_object(
    Reader reader,
    C.yson_event_type event_type,
):
    if event_type == C.YSON_EVENT_SCALAR:
        return _c_yson_unicode_reader_get_scalar(reader.c_reader)

    elif event_type == C.YSON_EVENT_BEGIN_LIST:
        return _unicode_reader_read_list(reader)

    elif event_type == C.YSON_EVENT_BEGIN_MAP:
        return _unicode_reader_read_map(reader)

    elif event_type == C.YSON_EVENT_BEGIN_ATTRIBUTES:
        attributes = _reader_read_map(reader, C.YSON_EVENT_END_ATTRIBUTES)
        event_type = _c_yson_reader_get_next_event(reader.c_reader)
        return _unicode_reader_read_object_with_attributes(
            reader, event_type, attributes)


# Writer

@cython.freelist(16)
cdef class Writer:
    cdef C.yson_writer* c_writer
    cdef OutputStream stream

    def __cinit__(
        self,
        OutputStream stream not None,
        format=b'text',
        mode=b'node',
        int indent=4
    ):
        cdef C.yson_stream_type stream_type

        cdef bytes bytes_mode = ConvertPyStringToPyBytes(mode)
        cdef bytes bytes_format = ConvertPyStringToPyBytes(format)

        if bytes_mode == b'node':
            stream_type = C.YSON_STREAM_TYPE_NODE
        elif bytes_mode == b'list_fragment':
            stream_type = C.YSON_STREAM_TYPE_LIST_FRAGMENT
        elif bytes_mode == b'map_fragment':
            stream_type = C.YSON_STREAM_TYPE_MAP_FRAGMENT
        else:
            raise ValueError("Invalid writer mode {!r}".format(bytes_mode))

        if bytes_format == b'text':
            self.c_writer = C.yson_writer_new_text(
                stream.ptr(),
                stream_type,
            )
        elif bytes_format == b'pretty':
            self.c_writer = C.yson_writer_new_pretty_text(
                stream.ptr(),
                stream_type,
                indent,
            )
        elif bytes_format == b'binary':
            self.c_writer = C.yson_writer_new_binary(
                stream.ptr(),
                stream_type,
            )
        else:
            raise ValueError("Bad YSON format {!r}".format(bytes_format))

        self.stream = stream

    def __dealloc__(self):
        C.yson_writer_delete(self.c_writer)

    def begin_stream(self):
        _c_writer_begin_stream(self.c_writer)
        return self

    def end_stream(self):
        _c_writer_end_stream(self.c_writer)
        return self

    def begin_list(self):
        _c_writer_begin_list(self.c_writer)
        return self

    def end_list(self):
        _c_writer_end_list(self.c_writer)
        return self

    def begin_map(self):
        _c_writer_begin_map(self.c_writer)
        return self

    def end_map(self):
        _c_writer_end_map(self.c_writer)
        return self

    def begin_attributes(self):
        _c_writer_begin_attributes(self.c_writer)
        return self

    def end_attributes(self):
        _c_writer_end_attributes(self.c_writer)
        return self

    def entity(self):
        _c_writer_entity(self.c_writer)
        return self

    def key(self, value):
        _c_writer_key(self.c_writer, value)
        return self

    def string(self, value):
        if isinstance(value, bytes):
            _c_writer_bytes(self.c_writer, <bytes>value)
        elif isinstance(value, unicode):
            _c_writer_unicode(self.c_writer, <unicode>value)
        else:
            _c_writer_string(self.c_writer, value)

        return self

    def int64(self, int64_t value):
        _c_writer_int64(self.c_writer, value)
        return self

    def uint64(self, uint64_t value):
        _c_writer_uint64(self.c_writer, value)
        return self

    def boolean(self, bint value):
        _c_writer_boolean(self.c_writer, value)
        return self

    def float64(self, double value):
        _c_writer_float64(self.c_writer, value)
        return self

    def attributes(self, attrs):
        _c_writer_begin_attributes(self.c_writer)

        if isinstance(attrs, dict):
            self._dict_common(<dict>attrs)
        else:
            self._mapping_common(attrs)

        _c_writer_end_attributes(self.c_writer)

        return self

    def list(self, obj):
        _c_writer_begin_list(self.c_writer)
        for item in obj:
            self.write(item)
        _c_writer_end_list(self.c_writer)
        return self

    def map(self, obj):
        _c_writer_begin_map(self.c_writer)

        if isinstance(obj, dict):
            self._dict_common(<dict>obj)
        else:
            self._mapping_common(obj)

        _c_writer_end_map(self.c_writer)

        return self

    cpdef write(self, obj):
        _c_writer_write(self.c_writer, obj)
        return self

    def switch_table(self, int index):
        self.write(YsonEntity({'table_index': index}))
        return self

    cdef inline _dict_common(self, dict obj):
        cdef PyObject* key
        cdef PyObject* value
        cdef Py_ssize_t pos = 0

        while PyDict_Next(obj, &pos, &key, &value):
            _c_writer_key(self.c_writer, <object>key)
            self.write(<object>value)

    cdef inline _mapping_common(self, obj):
        for key in obj.keys():
            _c_writer_key(self.c_writer, key)
            self.write(obj[key])


cdef inline int _c_writer_check_exc(C.yson_writer_result result) except? 0:
    if result == C.YSON_WRITER_RESULT_OK:
        return 1
    return 0


cdef inline _c_writer_check(
    C.yson_writer* c_writer,
    C.yson_writer_result result
):
    _c_writer_check_exc(result)
    if result == C.YSON_WRITER_RESULT_BAD_STREAM:
        raise RuntimeError(C.yson_writer_get_error_message(c_writer))
    elif result == C.YSON_WRITER_RESULT_ERROR:
        raise IOError(C.yson_writer_get_error_message(c_writer))


cdef _c_writer_begin_stream(C.yson_writer* c_writer):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_begin_stream(c_writer)
    )


cdef _c_writer_end_stream(C.yson_writer* c_writer):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_end_stream(c_writer)
    )


cdef _c_writer_begin_list(C.yson_writer* c_writer):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_begin_list(c_writer)
    )


cdef _c_writer_end_list(C.yson_writer* c_writer):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_end_list(c_writer)
    )


cdef _c_writer_begin_map(C.yson_writer* c_writer):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_begin_map(c_writer)
    )


cdef _c_writer_end_map(C.yson_writer* c_writer):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_end_map(c_writer)
    )


cdef _c_writer_begin_attributes(C.yson_writer* c_writer):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_begin_attributes(c_writer)
    )


cdef _c_writer_end_attributes(C.yson_writer* c_writer):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_end_attributes(c_writer)
    )


cdef _c_writer_entity(C.yson_writer* c_writer):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_entity(c_writer)
    )


# fallback if key is not bytes or unicode
cdef _c_writer_key(C.yson_writer* c_writer, value):
    cdef const char* data
    cdef size_t size
    cdef object holder = GetCharBufferAndOwner(value, &data, &size)

    _c_writer_check(
        c_writer,
        C.yson_writer_write_key(c_writer, data, size)
    )


# fallback if string-like object is not bytes or unicode
cdef _c_writer_string(C.yson_writer* c_writer, value):
    cdef const char* data
    cdef size_t size
    cdef object holder = GetCharBufferAndOwner(value, &data, &size)

    _c_writer_check(
        c_writer,
        C.yson_writer_write_string(c_writer, data, size)
    )


cdef _c_writer_key_bytes(C.yson_writer* c_writer, bytes value):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_key(
            c_writer,
            PyBytes_AS_STRING(value),
            PyBytes_GET_SIZE(value),
        )
    )


cdef inline _c_writer_key_unicode(C.yson_writer* c_writer, unicode value):
    _c_writer_key_bytes(c_writer, PyUnicode_AsUTF8String(value))


cdef _c_writer_bytes(C.yson_writer* c_writer, bytes value):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_string(
            c_writer,
            PyBytes_AS_STRING(value),
            PyBytes_GET_SIZE(value),
        )
    )


cdef inline _c_writer_unicode(C.yson_writer* c_writer, unicode value):
    _c_writer_bytes(c_writer, PyUnicode_AsUTF8String(value))


cdef _c_writer_int64(C.yson_writer* c_writer, int64_t value):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_int64(c_writer, value)
    )


cdef _c_writer_uint64(C.yson_writer* c_writer, uint64_t value):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_uint64(c_writer, value)
    )


cdef _c_writer_boolean(C.yson_writer* c_writer, bint value):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_boolean(c_writer, value)
    )


cdef _c_writer_float64(C.yson_writer* c_writer, double value):
    _c_writer_check(
        c_writer,
        C.yson_writer_write_float64(c_writer, value)
    )


cdef _c_writer_list(C.yson_writer* c_writer, list value):
    cdef Py_ssize_t index

    _c_writer_begin_list(c_writer)

    for index in range(PyList_GET_SIZE(value)):
        _c_writer_write(c_writer, <object>PyList_GET_ITEM(value, index))

    _c_writer_end_list(c_writer)


cdef _c_writer_tuple(C.yson_writer* c_writer, tuple value):
    cdef Py_ssize_t index

    _c_writer_begin_list(c_writer)

    for index in range(PyTuple_GET_SIZE(value)):
        _c_writer_write(c_writer, <object>PyTuple_GET_ITEM(value, index))

    _c_writer_end_list(c_writer)


cdef inline _c_writer_dict_common(C.yson_writer* c_writer, dict mapping):
    cdef PyObject* key
    cdef PyObject* value
    cdef Py_ssize_t pos = 0

    while PyDict_Next(mapping, &pos, &key, &value):
        if PyBytes_Check(<object>key):
            _c_writer_key_bytes(c_writer, <bytes>key)
        elif PyUnicode_Check(<object>key):
            _c_writer_key_unicode(c_writer, <unicode>key)
        else:
            _c_writer_key(c_writer, <object>key)

        _c_writer_write(c_writer, <object>value)


# fallback if attributes is not dict
cdef inline _c_writer_mapping_common(C.yson_writer* c_writer, mapping):
    cdef object keys_iter = PyObject_GetIter(PyObject_CallMethod(mapping, "keys", NULL))
    cdef PyObject* key = PyIter_Next(keys_iter)

    while key:
        if PyBytes_Check(<object>key):
            _c_writer_key_bytes(c_writer, <bytes>key)
        elif PyUnicode_Check(<object>key):
            _c_writer_key_unicode(c_writer, <unicode>key)
        else:
            _c_writer_key(c_writer, <object>key)

        _c_writer_write(c_writer, PyObject_GetItem(mapping, <object>key))

        # See note in `PyIter_Next` declaration
        Py_DECREF(<object>key)

        key = PyIter_Next(keys_iter)


cdef _c_writer_dict(C.yson_writer* c_writer, dict value):
    _c_writer_begin_map(c_writer)
    _c_writer_dict_common(c_writer, value)
    _c_writer_end_map(c_writer)


cdef _c_writer_attributes(C.yson_writer* c_writer, value):
    _c_writer_begin_attributes(c_writer)

    if PyDict_Check(value):
        _c_writer_dict_common(c_writer, <dict>value)
    else:
        _c_writer_mapping_common(c_writer, value)

    _c_writer_end_attributes(c_writer)


cdef _c_writer_write(C.yson_writer* c_writer, value):
    t = type(value)

    if t is bytes:
        _c_writer_bytes(c_writer, <bytes>value)
    elif t is int:
        _c_writer_int64(c_writer, value)
    elif t is dict:
        _c_writer_dict(c_writer, <dict>value)
    elif t is tuple:
        _c_writer_tuple(c_writer, <tuple>value)
    elif t is list:
        _c_writer_list(c_writer, <list>value)
    elif t is float:
        _c_writer_float64(c_writer, value)
    elif t is type(None):
        _c_writer_entity(c_writer)
    elif PY2 and t is long:
        _c_writer_uint64(c_writer, value)
    elif t is <type>&PyUnsignedLong_Type:
        _c_writer_uint64(c_writer, value)
    elif t is unicode:
        _c_writer_unicode(c_writer, <unicode>value)
    else:
        _c_writer_write_fallback(c_writer, value)


cdef _c_writer_write_fallback(C.yson_writer* c_writer, obj):
    if isinstance(obj, (YsonEntity, YsonString, YsonInt64, YsonUInt64,
                        YsonFloat64, YsonBoolean, YsonList, YsonMap)) and \
            obj.attributes is not None:
        _c_writer_attributes(c_writer, obj.attributes)

    if isinstance(obj, (bytes, unicode, YsonString)):
        _c_writer_string(c_writer, obj)
    elif isinstance(obj, (bool, YsonBoolean)):
        _c_writer_boolean(c_writer, obj)
    elif isinstance(obj, YsonUInt64):
        _c_writer_uint64(c_writer, obj)
    elif isinstance(obj, int):
        _c_writer_int64(c_writer, obj)
    elif PY2 and isinstance(obj, long):
        _c_writer_uint64(c_writer, obj)
    elif isinstance(obj, float):
        _c_writer_float64(c_writer, obj)
    elif isinstance(obj, (type(None), YsonEntity)):
        _c_writer_entity(c_writer)
    elif isinstance(obj, list):
        _c_writer_list(c_writer, <list>obj)
    elif isinstance(obj, tuple):
        _c_writer_tuple(c_writer, <tuple>obj)
    elif isinstance(obj, dict):
        _c_writer_dict(c_writer, <dict>obj)
    else:
        if np_import_failed is None:
            load_numpy_symbols()

        if np is not None and isinstance(obj, npy_generic):
            if isinstance(obj, npy_integers):
                _c_writer_int64(c_writer, obj)
            elif isinstance(obj, npy_uintegers):
                _c_writer_uint64(c_writer, obj)
            elif isinstance(obj, npy_floats):
                _c_writer_float64(c_writer, obj)
            else:
                raise UnableToSerializeError(obj)
        elif GenericCheckBuffer(obj):
            _c_writer_string(c_writer, obj)
        else:
            raise UnableToSerializeError(obj)


cdef void load_numpy_symbols() except *:
    global np
    global np_import_failed

    try:
        import numpy as np
        np_import_failed = False
    except ImportError:
        np_import_failed = True
        return

    global npy_generic
    npy_generic = np.generic

    global npy_integers
    npy_integers = (np.int8, np.int16, np.int32, np.int64)

    global npy_uintegers
    npy_uintegers = (np.uint8, np.uint16, np.uint32, np.uint64)

    global npy_floats
    npy_floats = (np.float16, np.float32, np.float64)


# PyReader

DEFAULT_PYREADER_SCALAR_HANDLERS = {}
DEFAULT_PYREADER_LIST_HANDLERS = {}
DEFAULT_PYREADER_MAP_HANDLERS = {}


cdef class PyReader(Reader):
    cdef public dict scalar_handlers
    cdef public dict list_handlers
    cdef public dict map_handlers

    def __cinit__(
        self,
        InputStream stream not None,
        mode=b'node',
        scalar_handlers=DEFAULT_PYREADER_SCALAR_HANDLERS,
        list_handlers=DEFAULT_PYREADER_LIST_HANDLERS,
        map_handlers=DEFAULT_PYREADER_MAP_HANDLERS,
    ):
        self.scalar_handlers = scalar_handlers
        self.list_handlers = list_handlers
        self.map_handlers = map_handlers


    cdef _scalar_handler(self, value, dict attributes):
        handler = _pyreader_find_handler(attributes, self.scalar_handlers, b'scalar')
        if handler is None:
            return self._generic_scalar_handler(value, attributes)
        else:
            return handler(value)

    cdef _list_handler(self, ListFragmentIterator items, dict attributes):
        handler = _pyreader_find_handler(attributes, self.list_handlers, b'list')
        if handler is None:
            return self._generic_list_handler(items, attributes)
        else:
            return handler(items)

    cdef _map_handler(self, MapFragmentIterator items, dict attributes):
        handler = _pyreader_find_handler(attributes, self.map_handlers, b'map')
        if handler is None:
            return self._generic_map_handler(items, attributes)
        else:
            return handler(items)

    cdef _generic_scalar_handler(self, value, dict attributes):
        return value

    cdef _generic_list_handler(self, ListFragmentIterator items, dict attributes):
        return list(items)

    cdef _generic_map_handler(self, MapFragmentIterator items, dict attributes):
        return dict(items)


def pyreader_scalar_handler(py_type):
    def wrapper(function):
        DEFAULT_PYREADER_SCALAR_HANDLERS[py_type] = function
        return function
    return wrapper


def pyreader_list_handler(py_type):
    def wrapper(function):
        DEFAULT_PYREADER_LIST_HANDLERS[py_type] = function
        return function
    return wrapper


def pyreader_map_handler(py_type):
    def wrapper(function):
        DEFAULT_PYREADER_MAP_HANDLERS[py_type] = function
        return function
    return wrapper


cdef _pyreader_find_handler(dict attributes, dict handlers, bytes type):
    py_type = attributes.get(b'py')
    if py_type is None:
        return None

    handler = handlers.get(py_type)
    if handler is None:
        raise ValueError("No {} handler for {}".format(type, py_type))

    return handler


@pyreader_scalar_handler(b'unicode')
def _pyreader_read_unicode(bytes value not None):
    return PyUnicode_DecodeUTF8(
        PyBytes_AS_STRING(value), PyBytes_GET_SIZE(value), NULL
    )


@pyreader_list_handler(b'dict')
def _pyreader_read_dict(ListFragmentIterator items not None):
    result = {}
    try:
        while True:
            key = next(items)
            value = next(items)
            result[key] = value
    except StopIteration:
        pass
    return result


pyreader_scalar_handler(b'long')(long)

pyreader_list_handler(b'list')(list)
pyreader_list_handler(b'tuple')(tuple)
pyreader_list_handler(b'set')(set)
pyreader_list_handler(b'frozenset')(frozenset)


# PyWriter

DEFAULT_PYWRITER_HANDLERS = {}

cdef class PyWriter(Writer):
    cdef public dict handlers

    def __cinit__(
        self,
        OutputStream stream not None,
        format=b'text',
        mode=b'node',
        int indent=4,
        handlers=DEFAULT_PYWRITER_HANDLERS
    ):
        self.handlers = handlers

    cpdef py_type(self, bytes name):
        _c_pywriter_py_type(self.c_writer, name)
        return self

    cpdef write(self, obj):
        handler = self.handlers.get(type(obj))
        if handler is None:
            raise UnableToSerializeError(obj)

        handler(self, obj)
        return self

def pywriter_handler(type_):
    def wrapper(function):
        DEFAULT_PYWRITER_HANDLERS[type_] = function
        return function

    return wrapper


cdef _c_pywriter_py_type(C.yson_writer* c_writer, bytes name):
    _c_writer_begin_attributes(c_writer)
    _c_writer_key_bytes(c_writer, b'py')
    _c_writer_bytes(c_writer, name)
    _c_writer_end_attributes(c_writer)


@pywriter_handler(type(None))
def _pywriter_write_none(PyWriter writer, _):
    _c_writer_entity(writer.c_writer)


@pywriter_handler(bool)
def _pywriter_write_bool(PyWriter writer, bint obj):
    _c_writer_boolean(writer.c_writer, obj)


if PY2:
    @pywriter_handler(int)
    def _pywriter_write_int(PyWriter writer, int64_t obj):
        _c_writer_int64(writer.c_writer, obj)


@pywriter_handler(long)
def _pywriter_write_long(PyWriter writer, obj not None):
    _c_pywriter_py_type(writer.c_writer, b'long')
    _c_writer_bytes(writer.c_writer, ConvertPyLongToPyBytes(obj))


@pywriter_handler(float)
def _pywriter_write_float(PyWriter writer, double obj):
    _c_writer_float64(writer.c_writer, obj)


@pywriter_handler(bytes)
def _pywriter_write_bytes(PyWriter writer, bytes obj not None):
    _c_writer_bytes(writer.c_writer, obj)


@pywriter_handler(unicode)
def _pywriter_write_unicode(PyWriter writer, unicode obj not None):
    _c_pywriter_py_type(writer.c_writer, b'unicode')
    _c_writer_unicode(writer.c_writer, obj)


cdef inline _pywriter_write_iterable(PyWriter writer, obj):
    _c_writer_begin_list(writer.c_writer)
    for item in obj:
        writer.write(item)
    _c_writer_end_list(writer.c_writer)


@pywriter_handler(list)
def _pywriter_write_list(PyWriter writer, list obj not None):
    _c_pywriter_py_type(writer.c_writer, b'list')
    #_write_iterable(writer, obj)
    # inline manually for type monomorphization
    _c_writer_begin_list(writer.c_writer)
    for item in obj:
        writer.write(item)
    _c_writer_end_list(writer.c_writer)


@pywriter_handler(tuple)
def _pywriter_write_tuple(PyWriter writer, tuple obj not None):
    _c_pywriter_py_type(writer.c_writer, b'tuple')
    # _write_iterable(writer, obj)
    # inline manually for type monomorphization
    _c_writer_begin_list(writer.c_writer)
    for item in obj:
        writer.write(item)
    _c_writer_end_list(writer.c_writer)


@pywriter_handler(set)
def _pywriter_write_set(PyWriter writer, set obj not None):
    _c_pywriter_py_type(writer.c_writer, b'set')
    _pywriter_write_iterable(writer, obj)


@pywriter_handler(frozenset)
def _pywriter_write_frozenset(PyWriter writer, frozenset obj not None):
    _c_pywriter_py_type(writer.c_writer, b'frozenset')
    _pywriter_write_iterable(writer, obj)


@pywriter_handler(dict)
def _pywriter_write_dict(PyWriter writer, dict obj not None):
    cdef bint good_keys
    cdef PyObject* c_key
    cdef PyObject* c_value
    cdef Py_ssize_t c_pos = 0

    # Check whether all keys are strings
    good_keys = True
    while PyDict_Next(obj, &c_pos, &c_key, &c_value):
        if not isinstance(<object>c_key, bytes):
            good_keys = False
            break

    c_pos = 0

    if good_keys:
        # All keys are strings, can use YSON map form
        _c_writer_begin_map(writer.c_writer)
        while PyDict_Next(obj, &c_pos, &c_key, &c_value):
            _c_writer_key_bytes(writer.c_writer, <bytes>c_key)
            writer.write(<object>c_value)
        _c_writer_end_map(writer.c_writer)
    else:
        # Some keys are not strings, need to write dictionary as a list
        _c_pywriter_py_type(writer.c_writer, b'dict')
        _c_writer_begin_list(writer.c_writer)
        while PyDict_Next(obj, &c_pos, &c_key, &c_value):
            writer.write(<object>c_key)
            writer.write(<object>c_value)
        _c_writer_end_list(writer.c_writer)


# Simple API

@cython.returns(bytes)
def dumps(value, format=b'text', Writer not None=Writer):
    r"""Convert an object to YSON node string.

    :param value: Python object to convert.
    :param format: YSON format to use: 'binary', 'text' or 'pretty'.
    :param Writer: YSON Writer class, may be supplied for custom serialization policies.

    >>> from cyson import dumps

    >>> print dumps(1234)
    1234
    >>> print dumps("Hello world! Привет!")
    "Hello world! Привет!"
    >>> print dumps([1, "foo", None, {'aaa': 'bbb'}])
    [1; "foo"; #; {"aaa" = "bbb"}]
    >>> dumps([1, "foo", None, {'aaa': 'bbb'}], format='binary')
    '[\x02\x02;\x01\x06foo;#;{\x01\x06aaa=\x01\x06bbb}]'
    >>> print dumps([1, "foo", None, {'aaa': 'bbb'}], format='pretty')
    [
        1;
        "foo";
        #;
            {
            "aaa" = "bbb"
        }
    ]

    """

    sink = PyByteArray_FromStringAndSize(NULL, 0)
    writer = Writer(
        OutputStream.from_bytearray(sink, 200),
        format
    )
    writer.begin_stream().write(value).end_stream()

    return PyBytes_FromStringAndSize(
        PyByteArray_AS_STRING(sink), PyByteArray_GET_SIZE(sink)
    )


def dumps_into(bytearray dest, value, format=b'text', Writer not None=Writer):
    r"""Convert an object to YSON node string.

    :param dest: Destination bytearray.
    :param value: Python object to convert.
    :param format: YSON format to use: 'binary', 'text' or 'pretty'.
    :param Writer: YSON Writer class, may be supplied for custom serialization policies.

    >>> dest = bytearray()
    >>> dumps_into(dest, [1, "foo", None, {'aaa': 'bbb'}])
    >>> dest
    bytearray(b'[1; "foo"; #; {"aaa" = "bbb"}]')

    """

    writer = Writer(
        OutputStream.from_bytearray(dest, 200),
        format
    )
    writer.begin_stream().write(value).end_stream()


@cython.returns(object)
def loads(value, Reader not None=Reader):
    r"""Convert a YSON node string to Python object.

    :param value: YSON node string.
    :param Reader: YSON Reader class, may be supplied for custom serialization policies.

    >>> from cyson import loads

    >>> loads('1234')
    1234
    >>> loads('3.14')
    3.14
    >>> loads('[1; "foo"; #; {"aaa" = "bbb"}]')
    [1, 'foo', None, {'aaa': 'bbb'}]
    >>> loads('[\x02\x02;\x01\x06foo;#;{\x01\x06aaa=\x01\x06bbb}]')
    [1, 'foo', None, {'aaa': 'bbb'}]

    """

    reader = Reader(
        InputStream.from_string(value)
    )

    return reader.node()


def list_fragments(InputStream stream not None,
                   Reader not None=Reader,
                   bint process_table_index=False,
                   bint process_attributes=False,
                   bint stop_at_key_switch=False,
                   bint keep_control_records=False):

    reader = Reader(stream, b'list_fragment')

    # NOTE: for backward compatibility purpose
    process_attributes = process_attributes or process_table_index

    return reader.list_fragments(
        process_attributes=process_attributes,
        stop_at_key_switch=stop_at_key_switch,
        keep_control_records=keep_control_records,
    )


cdef inline void exhaust_key_switched_iterator(ListFragmentIterator iterator):
    if not (iterator.is_key_switched or iterator.at_end):
        for _ in iterator:
            pass


def _make_first_group_iterator(first_value, ListFragmentIterator iterator):
    yield first_value

    if not (iterator.is_key_switched or iterator.at_end):
        for item in iterator:
            yield item


def key_switched_list_fragments(
    InputStream stream not None, Reader not None=Reader
):
    cdef ListFragmentIterator iterator = list_fragments(
        stream,
        Reader,
        process_attributes=True,
        stop_at_key_switch=True,
        keep_control_records=False,
    )

    cdef PyObject* first_value = PyIter_Next(iterator)

    if not first_value:
        return

    yield _make_first_group_iterator(<object>first_value, iterator)

    Py_DECREF(<object>first_value)

    # manually iterate over unused records in group
    exhaust_key_switched_iterator(iterator)

    while not iterator.at_end:
        yield iterator

        # manually iterate over unused records in group
        exhaust_key_switched_iterator(iterator)


def map_fragments(InputStream stream not None, Reader not None=Reader):
    reader = Reader(stream, b'map_fragment')
    return reader.map_fragments()
