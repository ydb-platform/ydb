from cpython cimport PyMem_Malloc, PyMem_Free, PyBytes_AsString, \
    PyBytes_Check, PyBytes_FromStringAndSize, PyBytes_AS_STRING, \
    PyBytes_GET_SIZE
from libc.string cimport memcpy, memset

from . import errors
from .varint import make_varint


cdef class BufferedWriter(object):
    cdef char* buffer
    cdef unsigned long long position, buffer_size

    def __init__(self, unsigned long long bufsize):
        self.buffer = <char *> PyMem_Malloc(bufsize)
        if not self.buffer:
            raise MemoryError()

        self.position = 0
        self.buffer_size = bufsize

        super(BufferedWriter, self).__init__()

    def __dealloc__(self):
        PyMem_Free(self.buffer)

    cpdef write_into_stream(self):
        raise NotImplementedError

    cpdef write(self, data):
        cdef unsigned long long size, written = 0
        cdef unsigned long long data_len = PyBytes_GET_SIZE(data)
        cdef char* c_data = PyBytes_AS_STRING(data)

        while written < data_len:
            size = min(data_len - written, self.buffer_size - self.position)
            memcpy(&self.buffer[self.position], &c_data[written], size)

            if self.position == self.buffer_size:
                self.write_into_stream()

            self.position += size
            written += size

    def flush(self):
        self.write_into_stream()

    def write_strings(self, items, encoding=None):
        cdef int do_encode = encoding is not None

        for value in items:
            if not PyBytes_Check(value):
                if do_encode:
                    value = value.encode(encoding)
                else:
                    raise ValueError('bytes object expected')

            self.write(make_varint(PyBytes_GET_SIZE(value)))
            self.write(value)

    def write_fixed_strings_as_bytes(self, items, Py_ssize_t length):
        cdef Py_ssize_t buf_pos = 0
        cdef Py_ssize_t items_buf_size = length * len(items)

        cdef char* c_value
        cdef char* items_buf = <char *>PyMem_Malloc(items_buf_size)
        if not items_buf:
            raise MemoryError()

        memset(items_buf, 0, items_buf_size)

        for value in items:
            value_len = len(value)
            if length < value_len:
                raise errors.TooLargeStringSize()

            c_value = PyBytes_AsString(value)

            memcpy(&items_buf[buf_pos], c_value, value_len)
            buf_pos += length

        self.write(PyBytes_FromStringAndSize(items_buf, items_buf_size))

        PyMem_Free(items_buf)

    def write_fixed_strings(self, items, Py_ssize_t length, encoding=None):
        if encoding is None:
            self.write_fixed_strings_as_bytes(items, length)
            return

        cdef Py_ssize_t buf_pos = 0
        cdef Py_ssize_t items_buf_size = length * len(items)

        cdef char* c_value
        cdef char* items_buf = <char *>PyMem_Malloc(items_buf_size)
        if not items_buf:
            raise MemoryError()

        memset(items_buf, 0, items_buf_size)

        for value in items:
            if not PyBytes_Check(value):
                value = value.encode(encoding)

            value_len = len(value)
            if length < value_len:
                raise errors.TooLargeStringSize()

            c_value = PyBytes_AsString(value)

            memcpy(&items_buf[buf_pos], c_value, value_len)
            buf_pos += length

        self.write(PyBytes_FromStringAndSize(items_buf, items_buf_size))

        PyMem_Free(items_buf)


cdef class BufferedSocketWriter(BufferedWriter):
    cdef object sock

    def __init__(self, sock, bufsize):
        self.sock = sock
        super(BufferedSocketWriter, self).__init__(bufsize)

    cpdef write_into_stream(self):
        self.sock.sendall(
            PyBytes_FromStringAndSize(self.buffer, self.position)
        )
        self.position = 0


cdef class CompressedBufferedWriter(BufferedWriter):
    cdef object compressor

    def __init__(self, compressor, bufsize):
        self.compressor = compressor
        super(CompressedBufferedWriter, self).__init__(bufsize)

    cpdef write_into_stream(self):
        self.compressor.write(
            PyBytes_FromStringAndSize(self.buffer, self.position)
        )
        self.position = 0

    def flush(self):
        self.write_into_stream()
