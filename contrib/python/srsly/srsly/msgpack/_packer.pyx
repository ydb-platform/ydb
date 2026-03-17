# coding: utf-8

from cpython cimport *
from cpython.bytearray cimport PyByteArray_Check, PyByteArray_CheckExact
from cpython.datetime cimport (
    PyDateTime_CheckExact, PyDelta_CheckExact,
    datetime_tzinfo, timedelta_days, timedelta_seconds, timedelta_microseconds,
)
from ._epoch import utc, epoch

cdef ExtType
cdef Timestamp

from .ext import ExtType, Timestamp
from .util import ensure_bytes


cdef extern from "Python.h":

    int PyMemoryView_Check(object obj)

cdef extern from "pack.h":
    struct msgpack_packer:
        char* buf
        size_t length
        size_t buf_size
        bint use_bin_type

    int msgpack_pack_nil(msgpack_packer* pk) except -1
    int msgpack_pack_true(msgpack_packer* pk) except -1
    int msgpack_pack_false(msgpack_packer* pk) except -1
    int msgpack_pack_long_long(msgpack_packer* pk, long long d) except -1
    int msgpack_pack_unsigned_long_long(msgpack_packer* pk, unsigned long long d) except -1
    int msgpack_pack_float(msgpack_packer* pk, float d) except -1
    int msgpack_pack_double(msgpack_packer* pk, double d) except -1
    int msgpack_pack_array(msgpack_packer* pk, size_t l) except -1
    int msgpack_pack_map(msgpack_packer* pk, size_t l) except -1
    int msgpack_pack_raw(msgpack_packer* pk, size_t l) except -1
    int msgpack_pack_bin(msgpack_packer* pk, size_t l) except -1
    int msgpack_pack_raw_body(msgpack_packer* pk, char* body, size_t l) except -1
    int msgpack_pack_ext(msgpack_packer* pk, char typecode, size_t l) except -1
    int msgpack_pack_timestamp(msgpack_packer* x, long long seconds, unsigned long nanoseconds) except -1


cdef int DEFAULT_RECURSE_LIMIT=511
cdef long long ITEM_LIMIT = (2**32)-1


cdef inline int PyBytesLike_Check(object o):
    return PyBytes_Check(o) or PyByteArray_Check(o)


cdef inline int PyBytesLike_CheckExact(object o):
    return PyBytes_CheckExact(o) or PyByteArray_CheckExact(o)


cdef class Packer:
    """
    MessagePack Packer

    Usage::

        packer = Packer()
        astream.write(packer.pack(a))
        astream.write(packer.pack(b))

    Packer's constructor has some keyword arguments:

    :param default:
        When specified, it should be callable.
        Convert user type to builtin type that Packer supports.
        See also simplejson's document.

    :param bool use_single_float:
        Use single precision float type for float. (default: False)

    :param bool autoreset:
        Reset buffer after each pack and return its content as `bytes`. (default: True).
        If set this to false, use `bytes()` to get content and `.reset()` to clear buffer.

    :param bool use_bin_type:
        Use bin type introduced in msgpack spec 2.0 for bytes.
        It also enables str8 type for unicode. (default: True)

    :param bool strict_types:
        If set to true, types will be checked to be exact. Derived classes
        from serializeable types will not be serialized and will be
        treated as unsupported type and forwarded to default.
        Additionally tuples will not be serialized as lists.
        This is useful when trying to implement accurate serialization
        for python types.

    :param bool datetime:
        If set to true, datetime with tzinfo is packed into Timestamp type.
        Note that the tzinfo is stripped in the timestamp.
        You can get UTC datetime with `timestamp=3` option of the Unpacker.

    :param str unicode_errors:
        The error handler for encoding unicode. (default: 'strict')
        DO NOT USE THIS!!  This option is kept for very specific usage.

    :param int buf_size:
        The size of the internal buffer. (default: 256*1024)
        Useful if serialisation size can be correctly estimated,
        avoid unnecessary reallocations.
    """
    cdef msgpack_packer pk
    cdef object _default
    cdef size_t exports  # number of exported buffers
    cdef bint strict_types
    cdef bint use_float
    cdef bint autoreset
    cdef bint datetime
    cdef object _bencoding
    cdef object _berrors
    cdef const char *encoding
    cdef const char *unicode_errors


    def __cinit__(self, buf_size=256*1024, **_kwargs):
        self.pk.buf = <char*> PyMem_Malloc(buf_size)
        if self.pk.buf == NULL:
            raise MemoryError("Unable to allocate internal buffer.")
        self.pk.buf_size = buf_size
        self.pk.length = 0
        self.exports = 0

    def __dealloc__(self):
        PyMem_Free(self.pk.buf)
        self.pk.buf = NULL
        assert self.exports == 0

    cdef _check_exports(self):
        if self.exports > 0:
            raise BufferError("Existing exports of data: Packer cannot be changed")

    def __init__(self, *, default=None, encoding=None,
                 bint use_single_float=False, bint autoreset=True, bint use_bin_type=False,
                 bint strict_types=False, bint datetime=False, unicode_errors=None,
                 buf_size=256*1024):
        self.use_float = use_single_float
        self.strict_types = strict_types
        self.autoreset = autoreset
        self.datetime = datetime
        self.pk.use_bin_type = use_bin_type
        if default is not None:
            if not PyCallable_Check(default):
                raise TypeError("default must be a callable.")
        self._default = default

        if encoding is None:
            if PY_MAJOR_VERSION < 3:
                encoding = 'utf-8'
            if encoding is None:
                self._bencoding = None
                self.encoding = NULL
            else:
                self._bencoding = ensure_bytes(encoding)
                self.encoding = self._bencoding
        else:
            self._bencoding = ensure_bytes(encoding)
            self.encoding = self._bencoding
        unicode_errors = ensure_bytes(unicode_errors)
        self._berrors = unicode_errors
        if unicode_errors is None:
            self.unicode_errors = NULL
        else:
            self.unicode_errors = self._berrors

    # returns -2 when default should(o) be called
    cdef int _pack_inner(self, object o, bint will_default, int nest_limit) except -1:
        cdef long long llval
        cdef unsigned long long ullval
        cdef unsigned long ulval
        cdef const char* rawval
        cdef Py_ssize_t L
        cdef Py_buffer view
        cdef bint strict = self.strict_types

        if o is None:
            msgpack_pack_nil(&self.pk)
        elif o is True:
            msgpack_pack_true(&self.pk)
        elif o is False:
            msgpack_pack_false(&self.pk)
        elif PyLong_CheckExact(o) if strict else PyLong_Check(o):
            try:
                if o > 0:
                    ullval = o
                    msgpack_pack_unsigned_long_long(&self.pk, ullval)
                else:
                    llval = o
                    msgpack_pack_long_long(&self.pk, llval)
            except OverflowError as oe:
                if will_default:
                    return -2
                else:
                    raise OverflowError("Integer value out of range")
        elif PyFloat_CheckExact(o) if strict else PyFloat_Check(o):
            if self.use_float:
                msgpack_pack_float(&self.pk, <float>o)
            else:
                msgpack_pack_double(&self.pk, <double>o)
        elif PyBytesLike_CheckExact(o) if strict else PyBytesLike_Check(o):
            L = Py_SIZE(o)
            if L > ITEM_LIMIT:
                PyErr_Format(ValueError, b"%.200s object is too large", Py_TYPE(o).tp_name)
            rawval = o
            msgpack_pack_bin(&self.pk, L)
            msgpack_pack_raw_body(&self.pk, rawval, L)
        elif PyUnicode_CheckExact(o) if strict else PyUnicode_Check(o):
            if self.unicode_errors == NULL and self.encoding == NULL:
                rawval = PyUnicode_AsUTF8AndSize(o, &L)
                if L >ITEM_LIMIT:
                    raise ValueError("unicode string is too large")
            else:
                o = PyUnicode_AsEncodedString(o, self.encoding, self.unicode_errors)
                L = Py_SIZE(o)
                if L > ITEM_LIMIT:
                    raise ValueError("unicode string is too large")
                rawval = o
            msgpack_pack_raw(&self.pk, L)
            msgpack_pack_raw_body(&self.pk, rawval, L)
        elif PyDict_CheckExact(o) if strict else PyDict_Check(o):
            L = len(o)
            if L > ITEM_LIMIT:
                raise ValueError("dict is too large")
            msgpack_pack_map(&self.pk, L)
            for k, v in o.items():
                self._pack(k, nest_limit)
                self._pack(v, nest_limit)
        elif type(o) is ExtType if strict else isinstance(o, ExtType):
            # This should be before Tuple because ExtType is namedtuple.
            rawval = o.data
            L = len(o.data)
            if L > ITEM_LIMIT:
                raise ValueError("EXT data is too large")
            msgpack_pack_ext(&self.pk, <long>o.code, L)
            msgpack_pack_raw_body(&self.pk, rawval, L)
        elif type(o) is Timestamp:
            llval = o.seconds
            ulval = o.nanoseconds
            msgpack_pack_timestamp(&self.pk, llval, ulval)
        elif PyList_CheckExact(o) if strict else (PyTuple_Check(o) or PyList_Check(o)):
            L = Py_SIZE(o)
            if L > ITEM_LIMIT:
                raise ValueError("list is too large")
            msgpack_pack_array(&self.pk, L)
            for v in o:
                self._pack(v, nest_limit)
        elif PyMemoryView_Check(o):
            PyObject_GetBuffer(o, &view, PyBUF_SIMPLE)
            L = view.len
            if L > ITEM_LIMIT:
                PyBuffer_Release(&view);
                raise ValueError("memoryview is too large")
            try:
                msgpack_pack_bin(&self.pk, L)
                msgpack_pack_raw_body(&self.pk, <char*>view.buf, L)
            finally:
                PyBuffer_Release(&view);
        elif self.datetime and PyDateTime_CheckExact(o) and datetime_tzinfo(o) is not None:
            delta = o - epoch
            if not PyDelta_CheckExact(delta):
                raise ValueError("failed to calculate delta")
            llval = timedelta_days(delta) * <long long>(24*60*60) + timedelta_seconds(delta)
            ulval = timedelta_microseconds(delta) * 1000
            msgpack_pack_timestamp(&self.pk, llval, ulval)
        elif will_default:
            return -2
        elif self.datetime and PyDateTime_CheckExact(o):
            # this should be later than will_default
            PyErr_Format(ValueError, b"can not serialize '%.200s' object where tzinfo=None", Py_TYPE(o).tp_name)
        else:
            PyErr_Format(TypeError, b"can not serialize '%.200s' object", Py_TYPE(o).tp_name)

    cdef int _pack(self, object o, int nest_limit=DEFAULT_RECURSE_LIMIT) except -1:
        cdef int ret
        if nest_limit < 0:
            raise ValueError("recursion limit exceeded.")
        nest_limit -= 1
        if self._default is not None:
            ret = self._pack_inner(o, 1, nest_limit)
            if ret == -2:
                o = self._default(o)
            else:
                return ret
        return self._pack_inner(o, 0, nest_limit)

    def pack(self, object obj):
        cdef int ret
        self._check_exports()
        try:
            ret = self._pack(obj, DEFAULT_RECURSE_LIMIT)
        except:
            self.pk.length = 0
            raise
        if ret:  # should not happen.
            raise RuntimeError("internal error")
        if self.autoreset:
            buf = PyBytes_FromStringAndSize(self.pk.buf, self.pk.length)
            self.pk.length = 0
            return buf

    def pack_ext_type(self, typecode, data):
        self._check_exports()
        if len(data) > ITEM_LIMIT:
            raise ValueError("ext data too large")
        msgpack_pack_ext(&self.pk, typecode, len(data))
        msgpack_pack_raw_body(&self.pk, data, len(data))

    def pack_array_header(self, long long size):
        self._check_exports()
        if size > ITEM_LIMIT:
            raise ValueError("array too large")
        msgpack_pack_array(&self.pk, size)
        if self.autoreset:
            buf = PyBytes_FromStringAndSize(self.pk.buf, self.pk.length)
            self.pk.length = 0
            return buf

    def pack_map_header(self, long long size):
        self._check_exports()
        if size > ITEM_LIMIT:
            raise ValueError("map too learge")
        msgpack_pack_map(&self.pk, size)
        if self.autoreset:
            buf = PyBytes_FromStringAndSize(self.pk.buf, self.pk.length)
            self.pk.length = 0
            return buf

    def pack_map_pairs(self, object pairs):
        """
        Pack *pairs* as msgpack map type.

        *pairs* should be a sequence of pairs.
        (`len(pairs)` and `for k, v in pairs:` should be supported.)
        """
        self._check_exports()
        size = len(pairs)
        if size > ITEM_LIMIT:
            raise ValueError("map too large")
        msgpack_pack_map(&self.pk, size)
        for k, v in pairs:
            self._pack(k)
            self._pack(v)
        if self.autoreset:
            buf = PyBytes_FromStringAndSize(self.pk.buf, self.pk.length)
            self.pk.length = 0
            return buf

    def reset(self):
        """Reset internal buffer.

        This method is useful only when autoreset=False.
        """
        self._check_exports()
        self.pk.length = 0

    def bytes(self):
        """Return internal buffer contents as bytes object"""
        return PyBytes_FromStringAndSize(self.pk.buf, self.pk.length)

    def getbuffer(self):
        """Return memoryview of internal buffer.

        Note: Packer now supports buffer protocol. You can use memoryview(packer).
        """
        return memoryview(self)

    def __getbuffer__(self, Py_buffer *buffer, int flags):
        PyBuffer_FillInfo(buffer, self, self.pk.buf, self.pk.length, 1, flags)
        self.exports += 1

    def __releasebuffer__(self, Py_buffer *buffer):
        self.exports -= 1
