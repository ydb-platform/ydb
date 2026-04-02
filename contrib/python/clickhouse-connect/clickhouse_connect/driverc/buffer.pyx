import sys
from typing import Iterable, Any, Optional

import cython

from cpython cimport Py_INCREF, array
import array
from cpython.unicode cimport PyUnicode_Decode
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.bytes cimport PyBytes_FromStringAndSize
from cpython.buffer cimport PyObject_GetBuffer, PyBuffer_Release, PyBUF_ANY_CONTIGUOUS, PyBUF_SIMPLE
from cpython.mem cimport PyMem_Free, PyMem_Malloc
from libc.string cimport memcpy

from clickhouse_connect.driver.exceptions import StreamCompleteException

cdef union ull_wrapper:
    char* source
    unsigned long long int_value

cdef char * errors = 'strict'
cdef char * utf8 = 'utf8'
cdef dict array_templates = {}
cdef bint must_swap = sys.byteorder == 'big'
cdef array.array swapper = array.array('Q', [0])

for c in 'bBuhHiIlLqQfd':
    array_templates[c] = array.array(c, [])


cdef class ResponseBuffer:
    def __init__(self, source):
        self.slice_sz = 4096
        self.buf_loc = 0
        self.buf_sz = 0
        self.source = source
        self.gen = source.gen
        self.buffer = NULL
        self.slice = <char*>PyMem_Malloc(self.slice_sz)
        self._exception_tag = getattr(source, "exception_tag", None)
        self.open_marker = None
        self.close_marker = None
        self.carryover = b""
        self.exception_buf = None
        self.last_message_data = None
        self.current_chunk = None
        if self._exception_tag:
            tag_bytes = self._exception_tag.encode()
            self.open_marker = b"__exception__" + tag_bytes
            self.close_marker = tag_bytes + b"__exception__"

    cdef void _check_for_exception(self, object chunk) except *:
        cdef object search_data
        cdef Py_ssize_t marker_pos
        cdef Py_ssize_t carry_size
        if not self._exception_tag:
            return
        if self.exception_buf is not None:
            self.exception_buf += chunk
            if self.close_marker in self.exception_buf:
                self.last_message_data = bytes(self.exception_buf)
                raise StreamCompleteException
            return
        search_data = self.carryover + chunk
        marker_pos = search_data.find(self.open_marker)
        if marker_pos != -1:
            self.exception_buf = bytearray(search_data[marker_pos:])
            if self.close_marker in self.exception_buf:
                self.last_message_data = bytes(self.exception_buf)
                raise StreamCompleteException
            return
        carry_size = len(self.open_marker) - 1
        if len(search_data) >= carry_size:
            self.carryover = search_data[-carry_size:]
        else:
            self.carryover = search_data

    # Note that return char * return from this method is only good until the next call to _read_bytes_c or
    # _read_byte_load, since it points into self.buffer which can be replaced with the next chunk from the stream
    # Accordingly, that memory MUST be copied/processed into another buffer/PyObject immediately
    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef char * read_bytes_c(self, unsigned long long sz) except NULL:
        cdef unsigned long long x, e, tail, cur_len, temp
        cdef char* ptr
        e = self.buf_sz

        if self.buf_loc + sz <= e:
            # We still have "sz" unread bytes available in the buffer, return the currently loc and advance it
            temp = self.buf_loc
            self.buf_loc += sz
            return self.buffer + temp

        # We need more data than is currently in the buffer, copy what's left into the temporary slice,
        # get a new buffer, and append what we need from the new buffer into that slice
        cur_len = e - self.buf_loc
        temp = self.slice_sz #
        while temp < sz * 2:
            temp <<= 1
        if temp > self.slice_sz:
            PyMem_Free(self.slice)
            self.slice = <char*>PyMem_Malloc(temp)
            self.slice_sz = temp
        if cur_len > 0:
            memcpy(self.slice, self.buffer + self.buf_loc, cur_len)
        self.buf_loc = 0
        self.buf_sz = 0

        # Loop until we've read enough chunks to fill the requested size
        while cur_len < sz:
            chunk = next(self.gen, None)
            if not chunk:
                raise StreamCompleteException
            self._check_for_exception(chunk)
            x = len(chunk)
            ptr = <char *> chunk
            if cur_len + x <= sz:
                # We need this whole chunk for the requested size, copy it into the temporary slice and get the next one
                memcpy(self.slice + cur_len, ptr, x)
                cur_len += x
            else:
                # We need just the beginning of this chunk to finish the temporary, copy that and set
                # the pointer into our stored buffer to the first unread data
                tail = sz - cur_len
                memcpy(self.slice + cur_len, ptr, tail)
                PyBuffer_Release(&self.buff_source)
                PyObject_GetBuffer(chunk, &self.buff_source, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
                self.buffer = <char *> self.buff_source.buf
                self.buf_sz = x
                self.buf_loc = tail
                self.current_chunk = chunk
                cur_len += tail
        return self.slice

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef inline unsigned char _read_byte_load(self) except ?255:
        self.buf_loc = 0
        self.buf_sz = 0
        chunk = next(self.gen, None)
        if not chunk:
            raise StreamCompleteException
        self._check_for_exception(chunk)
        x = len(chunk)
        self.current_chunk = chunk
        if x > 1:
            PyBuffer_Release(&self.buff_source)
            PyObject_GetBuffer(chunk, &self.buff_source, PyBUF_SIMPLE | PyBUF_ANY_CONTIGUOUS)
            self.buffer = <char *> self.buff_source.buf
            self.buf_loc = 1
            self.buf_sz = x
        return <unsigned char>chunk[0]

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef inline object _read_str_col(self, unsigned long long num_rows, char * encoding):
        cdef object column = PyTuple_New(num_rows), v
        cdef unsigned long long x = 0, sz, shift
        cdef unsigned char b
        cdef char* buf
        while x < num_rows:
            sz = 0
            shift = 0
            while 1:
                if self.buf_loc < self.buf_sz:
                    b = self.buffer[self.buf_loc]
                    self.buf_loc += 1
                else:
                    b = self._read_byte_load()
                sz += ((b & 0x7f) << shift)
                if (b & 0x80) == 0:
                    break
                shift += 7
            buf = self.read_bytes_c(sz)
            if encoding:
                try:
                    v = PyUnicode_Decode(buf, sz, encoding, errors)
                except UnicodeDecodeError:
                    v = PyBytes_FromStringAndSize(buf, sz).hex()
            else:
                v = PyBytes_FromStringAndSize(buf, sz)
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            x += 1
        return column

    @cython.boundscheck(False)
    @cython.wraparound(False)
    cdef inline object _read_nullable_str_col(self, unsigned long long num_rows, char * encoding, object null_obj):
        cdef object column = PyTuple_New(num_rows), v
        cdef unsigned long long x = 0, sz, shift
        cdef unsigned char b
        cdef char * buf
        cdef char * null_map = <char *> PyMem_Malloc(<size_t> num_rows)
        memcpy(<void *> null_map, <void *> self.read_bytes_c(num_rows), num_rows)
        for x in range(num_rows):
            if self.buf_loc < self.buf_sz:
                b = self.buffer[self.buf_loc]
                self.buf_loc += 1
            else:
                b = self._read_byte_load()
            shift = 0
            sz = b & 0x7f
            while b & 0x80:
                shift += 7
                if self.buf_loc < self.buf_sz:
                    b = self.buffer[self.buf_loc]
                    self.buf_loc += 1
                else:
                    b = self._read_byte_load()
                sz += ((b & 0x7f) << shift)
            buf = self.read_bytes_c(sz)
            if null_map[x]:
                v = null_obj
            elif encoding:
                try:
                    v = PyUnicode_Decode(buf, sz, encoding, errors)
                except UnicodeDecodeError:
                    v = PyBytes_FromStringAndSize(buf, sz).hex()
            else:
                v = PyBytes_FromStringAndSize(buf, sz)
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
        PyMem_Free(<void *> null_map)
        return column

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_byte(self) -> int:
        if self.buf_loc < self.buf_sz:
            b = self.buffer[self.buf_loc]
            self.buf_loc += 1
            return b
        b = self._read_byte_load()
        return b

    def read_leb128_str(self) -> str:
        cdef unsigned long long sz = self.read_leb128()
        cdef char * b = self.read_bytes_c(sz)
        return PyUnicode_Decode(b, sz, utf8, errors)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_leb128(self) -> int:
        cdef:
            unsigned long long sz = 0, shift = 0
            unsigned char b
        while 1:
            if self.buf_loc < self.buf_sz:
                b = self.buffer[self.buf_loc]
                self.buf_loc += 1
            else:
                b = self._read_byte_load()
            sz += ((b & 0x7f) << shift)
            if (b & 0x80) == 0:
                return sz
            shift += 7

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_uint64(self) -> int:
        cdef ull_wrapper* x
        cdef char* b = self.read_bytes_c(8)
        if must_swap:
            memcpy(swapper.data.as_voidptr, b, 8)
            swapper.byteswap()
            return swapper[0]
        x = <ull_wrapper *> b
        return x.int_value

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_bytes(self, unsigned long long sz) -> bytes:
        cdef char* b = self.read_bytes_c(sz)
        return b[:sz]

    def read_str_col(self,
                     unsigned long long num_rows,
                     encoding: Optional[str],
                     nullable: bool = False,
                     null_object: Any = None) -> Iterable[str]:
        cdef char * enc = NULL
        if encoding:
            pyenc = encoding.encode()
            enc = pyenc
        if nullable:
            return self._read_nullable_str_col(num_rows, enc, null_object)
        return self._read_str_col(num_rows, enc)

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_array(self, t: str, unsigned long long num_rows) -> Iterable[Any]:
        cdef array.array template = array_templates[t]
        cdef array.array result = array.clone(template, num_rows, 0)
        cdef unsigned long long sz = result.itemsize * num_rows
        cdef char * b = self.read_bytes_c(sz)
        memcpy(result.data.as_voidptr, b, sz)
        if must_swap:
            result.byteswap()
        return result

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_bytes_col(self, unsigned long long sz, unsigned long long num_rows) -> Iterable[Any]:
        cdef object column = PyTuple_New(num_rows)
        cdef char * b = self.read_bytes_c(sz * num_rows)
        for x in range(num_rows):
            v = PyBytes_FromStringAndSize(b, sz)
            b += sz
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
        return column

    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_fixed_str_col(self, unsigned long long sz, unsigned long long num_rows,
                           encoding:str ='utf8') -> Iterable[str]:
        cdef object column = PyTuple_New(num_rows)
        cdef char * enc
        cdef char * b = self.read_bytes_c(sz * num_rows)
        cdef object v
        pyenc = encoding.encode()
        enc = pyenc
        for x in range(num_rows):
            try:
                v = PyUnicode_Decode(b, sz, enc, errors).rstrip("\x00")
            except UnicodeDecodeError:
                v = PyBytes_FromStringAndSize(b, sz).hex()
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            b += sz
        return column

    def close(self):
        if self.source:
            self.source.close()
            self.source = None

    @property
    def exception_tag(self):
        return self._exception_tag

    @property
    def last_message(self):
        if self.last_message_data is not None:
            return self.last_message_data
        if self.current_chunk is not None:
            return self.current_chunk
        return b""

    def __dealloc__(self):
        self.close()
        PyBuffer_Release(&self.buff_source)
        PyMem_Free(self.slice)
