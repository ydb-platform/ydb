from cpython cimport Py_INCREF, PyBytes_FromStringAndSize, PyBytes_AsString
from cpython.bytearray cimport PyByteArray_AsString
# Using python's versions of pure c memory management functions for
# proper memory statistics count.
from cpython.mem cimport PyMem_Malloc, PyMem_Realloc, PyMem_Free
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from libc.string cimport memcpy


cdef class BufferedReader(object):
    cdef public unsigned long long position, current_buffer_size
    cdef public bytearray buffer

    def __init__(self, bufsize):
        self.buffer = bytearray(bufsize)

        self.position = 0
        self.current_buffer_size = 0

        super(BufferedReader, self).__init__()

    def read_into_buffer(self):
        raise NotImplementedError

    def read(self, unsigned long long unread):
        # When the buffer is large enough bytes read are almost
        # always hit the buffer.
        cdef unsigned long long next_position = unread + self.position
        if next_position < self.current_buffer_size:
            t = self.position
            self.position = next_position
            return bytes(self.buffer[t:self.position])

        cdef char* buffer_ptr = PyByteArray_AsString(self.buffer)
        cdef unsigned long long read_bytes
        rv = bytes()

        while unread > 0:
            if self.position == self.current_buffer_size:
                self.read_into_buffer()
                buffer_ptr = PyByteArray_AsString(self.buffer)
                self.position = 0

            read_bytes = min(unread, self.current_buffer_size - self.position)
            rv += PyBytes_FromStringAndSize(
                &buffer_ptr[self.position], read_bytes
            )
            self.position += read_bytes
            unread -= read_bytes

        return rv

    def read_one(self):
        if self.position == self.current_buffer_size:
            self.read_into_buffer()
            self.position = 0

        rv = self.buffer[self.position]
        self.position += 1
        return rv

    def read_strings(self, unsigned long long n_items, encoding=None):
        """
        Python has great overhead between function calls.
        We inline strings reading logic here to avoid this overhead.
        """
        items = PyTuple_New(n_items)

        cdef unsigned long long i
        # Buffer vars
        cdef char* buffer_ptr = PyByteArray_AsString(self.buffer)
        cdef unsigned long long right
        # String length vars
        cdef unsigned long long size, shift, bytes_read
        cdef unsigned long long b

        # String for decode vars.
        cdef char *c_string = NULL
        cdef unsigned long long c_string_size = 1024
        cdef char *c_encoding = NULL
        if encoding:
            encoding = encoding.encode('utf-8')
            c_encoding = encoding

        cdef object rv = object()
        # String for decode vars.
        if c_encoding:
            c_string = <char *> PyMem_Realloc(NULL, c_string_size)

        for i in range(n_items):
            shift = size = 0

            # Read string size
            while True:
                if self.position == self.current_buffer_size:
                    self.read_into_buffer()
                    # `read_into_buffer` can override buffer
                    buffer_ptr = PyByteArray_AsString(self.buffer)
                    self.position = 0

                b = buffer_ptr[self.position]
                self.position += 1

                size |= (b & 0x7f) << shift
                if b < 0x80:
                    break

                shift += 7

            right = self.position + size

            if c_encoding:
                if size + 1 > c_string_size:
                    c_string_size = size + 1
                    c_string = <char *> PyMem_Realloc(c_string, c_string_size)
                    if c_string is NULL:
                        raise MemoryError()
                c_string[size] = 0
                bytes_read = 0

            # Decoding pure c strings in Cython is faster than in pure Python.
            # We need to copy it into buffer for adding null symbol at the end.
            # In ClickHouse block there is no null
            if right > self.current_buffer_size:
                if c_encoding:
                    memcpy(&c_string[bytes_read], &buffer_ptr[self.position],
                           self.current_buffer_size - self.position)
                else:
                    rv = PyBytes_FromStringAndSize(
                        &buffer_ptr[self.position],
                        self.current_buffer_size - self.position
                    )

                bytes_read = self.current_buffer_size - self.position
                # Read the rest of the string.
                while bytes_read != size:
                    self.position = size - bytes_read

                    self.read_into_buffer()
                    # `read_into_buffer` can override buffer
                    buffer_ptr = PyByteArray_AsString(self.buffer)
                    # There can be not enough data in buffer.
                    self.position = min(
                        self.position, self.current_buffer_size
                    )
                    if c_encoding:
                        memcpy(
                            &c_string[bytes_read], buffer_ptr, self.position
                        )
                    else:
                        rv += PyBytes_FromStringAndSize(
                            buffer_ptr, self.position
                        )
                    bytes_read += self.position

            else:
                if c_encoding:
                    memcpy(c_string, &buffer_ptr[self.position], size)
                else:
                    rv = PyBytes_FromStringAndSize(
                        &buffer_ptr[self.position], size
                    )
                self.position = right

            if c_encoding:
                try:
                    rv = c_string[:size].decode(c_encoding)
                except UnicodeDecodeError:
                    rv = PyBytes_FromStringAndSize(c_string, size)

            Py_INCREF(rv)
            PyTuple_SET_ITEM(items, i, rv)

        if c_string:
            PyMem_Free(c_string)

        return items

    def read_fixed_strings_as_bytes(self, Py_ssize_t n_items,
                                    Py_ssize_t length):
        cdef Py_ssize_t i
        data = self.read(length * n_items)
        cdef char* data_ptr = PyBytes_AsString(data)

        items = PyTuple_New(n_items)
        for i in range(n_items):
            item = PyBytes_FromStringAndSize(&data_ptr[i * length], length)
            Py_INCREF(item)
            PyTuple_SET_ITEM(items, i, item)
        return items

    def read_fixed_strings(self, Py_ssize_t n_items, Py_ssize_t length,
                           encoding=None):
        if encoding is None:
            return self.read_fixed_strings_as_bytes(n_items, length)

        cdef Py_ssize_t i, j
        encoding = encoding.encode('utf-8')
        cdef char* c_encoding = encoding
        data = self.read(length * n_items)
        cdef char* data_ptr = PyBytes_AsString(data)

        cdef char* c_string = <char *>PyMem_Malloc(length + 1)
        if not c_string:
            raise MemoryError()
        c_string[length] = 0

        items = PyTuple_New(n_items)
        for i in range(n_items):
            memcpy(c_string, &data_ptr[i * length], length)

            # Get last non zero byte of string from the end.
            j = length - 1
            while j >= 0 and not c_string[j]:
                j -= 1

            try:
                item = c_string[:j + 1].decode(c_encoding)
            except UnicodeDecodeError:
                item = PyBytes_FromStringAndSize(c_string, length)
            Py_INCREF(item)
            PyTuple_SET_ITEM(items, i, item)

        PyMem_Free(c_string)

        return items


cdef class BufferedSocketReader(BufferedReader):
    cdef object sock

    def __init__(self, sock, bufsize):
        self.sock = sock
        super(BufferedSocketReader, self).__init__(bufsize)

    def read_into_buffer(self):
        self.current_buffer_size = self.sock.recv_into(self.buffer)

        if self.current_buffer_size == 0:
            raise EOFError('Unexpected EOF while reading bytes')


cdef class CompressedBufferedReader(BufferedReader):
    cdef object read_block

    def __init__(self, read_block, bufsize):
        self.read_block = read_block
        super(CompressedBufferedReader, self).__init__(bufsize)

    def read_into_buffer(self):
        self.buffer = bytearray(self.read_block())
        self.current_buffer_size = len(self.buffer)

        if self.current_buffer_size == 0:
            raise EOFError('Unexpected EOF while reading bytes')
