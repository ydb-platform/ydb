import struct
from typing import Sequence, Optional

import array
from datetime import datetime, date

import cython

from .buffer cimport ResponseBuffer
from cpython cimport Py_INCREF, Py_DECREF
from cpython.buffer cimport PyBUF_READ
from cpython.mem cimport PyMem_Free, PyMem_Malloc
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.bytearray cimport PyByteArray_GET_SIZE, PyByteArray_Resize
from cpython.memoryview cimport PyMemoryView_FromMemory
from cython.view cimport array as cvarray
from ipaddress import IPv4Address
from uuid import UUID, SafeUUID
from libc.string cimport memcpy
from datetime import tzinfo

from clickhouse_connect.driver.errors import NONE_IN_NULLABLE_COLUMN

@cython.boundscheck(False)
@cython.wraparound(False)
def pivot(data: Sequence, unsigned long long start, unsigned long long end):
    cdef unsigned long long row_count = end - start
    cdef unsigned long long col_count = len(data[0])
    cdef object result = PyTuple_New(col_count)
    cdef object col, v
    for x in range(col_count):
        col = PyTuple_New(row_count)
        PyTuple_SET_ITEM(result, x, col)
        Py_INCREF(col)
        for y in range(row_count):
            v = data[y + start][x]
            PyTuple_SET_ITEM(col, y, v)
            Py_INCREF(v)
    return result


@cython.wraparound(False)
@cython.boundscheck(False)
def read_ipv4_col(ResponseBuffer buffer, unsigned long long num_rows):
    cdef unsigned long long x = 0
    cdef char* loc = buffer.read_bytes_c(4 * num_rows)
    cdef object column = PyTuple_New(num_rows), v
    ip_new = IPv4Address.__new__
    while x < num_rows:
        v = ip_new(IPv4Address)
        v._ip = (<unsigned int*>loc)[0]
        PyTuple_SET_ITEM(column, x, v)
        Py_INCREF(v)
        loc += 4
        x += 1
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
def read_datetime_col(ResponseBuffer buffer, unsigned long long num_rows, tzinfo: tzinfo):
    cdef unsigned long long x = 0
    cdef char * loc = buffer.read_bytes_c(4 * num_rows)
    cdef object column = PyTuple_New(num_rows), v
    if tzinfo is None:
        fts = datetime.utcfromtimestamp
        while x < num_rows:
            v = fts((<unsigned int*>loc)[0])
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            loc += 4
            x += 1
    else:
        fts = datetime.fromtimestamp
        while x < num_rows:
            v = fts((<unsigned int*>loc)[0], tzinfo)
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            loc += 4
            x += 1
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
def read_date_col(ResponseBuffer buffer, unsigned long long num_rows):
    cdef unsigned long long x = 0
    cdef char * loc = buffer.read_bytes_c(2 * num_rows)
    cdef object column = PyTuple_New(num_rows), v
    while x < num_rows:
        v = epoch_days_to_date((<unsigned short*>loc)[0])
        PyTuple_SET_ITEM(column, x, v)
        Py_INCREF(v)
        loc += 2
        x += 1
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
def read_date32_col(ResponseBuffer buffer, unsigned long long num_rows):
    cdef unsigned long long x = 0
    cdef char * loc = buffer.read_bytes_c(4 * num_rows)
    cdef object column = PyTuple_New(num_rows), v
    while x < num_rows:
        v = epoch_days_to_date((<int*>loc)[0])
        PyTuple_SET_ITEM(column, x, v)
        Py_INCREF(v)
        loc += 4
        x += 1
    return column


cdef unsigned short* MONTH_DAYS = [0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334, 365]
cdef unsigned short* MONTH_DAYS_LEAP = [0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335, 366]

# Constants used in epoch_days_to_date
# 47482 -- Jan 1, 2100 -- Because all years 1970-2099 divisible by 4 are leap years, some extra division can be avoided
# 134774 -- Number of days between Jan 1 1601 and Jan 1 1970.  Adding this starts all calculations at 1601-01-01
# 1461 -- Number of days in a 4-year cycle (365 * 4) + 1 leap day
# 36524 -- Number of days in a 100-year cycle.  25 4-year cycles - 1 leap day for the year 100
# 146097 -- Number of days in a 400-year cycle.  4 100 year cycles + 1 leap day for the year 400

# Year and offset with in the year are determined by factoring out the largest "known" year blocks in
# descending order (400/100/4/1 years).  Month is then (over) estimated in the "day" arrays (days / 32) and
# adjusted down if too large (logic originally in the Python standard library)

@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef inline object epoch_days_to_date(int days):
    cdef int years, month, year, cycles400, cycles100, cycles, rem
    cdef unsigned short prev
    cdef unsigned short* m_list
    if 0 <= days < 47482:
        cycles = (days + 365) // 1461
        rem = (days + 365) - cycles * 1461
        years = rem // 365
        rem -= years * 365
        year = (cycles << 2) + years + 1969
        if years == 4:
            return date(year - 1, 12, 31)
        if years == 3:
            m_list = MONTH_DAYS_LEAP
        else:
            m_list = MONTH_DAYS
    else:
        cycles400 = (days + 134774) // 146097
        rem = days + 134774 - (cycles400 * 146097)
        cycles100 = rem // 36524
        rem -= cycles100 * 36524
        cycles = rem // 1461
        rem -= cycles * 1461
        years = rem // 365
        rem -= years * 365
        year = (cycles << 2) + cycles400 * 400 + cycles100 * 100  + years + 1601
        if years == 4 or cycles100 == 4:
            return date(year - 1, 12, 31)
        if years == 3 and (year == 2000 or year % 100 != 0):
            m_list = MONTH_DAYS_LEAP
        else:
            m_list = MONTH_DAYS
    month = (rem + 24) >> 5
    prev = m_list[month]
    while rem < prev:
        month -= 1
        prev = m_list[month]
    return date(year, month + 1, rem + 1 - prev)


@cython.boundscheck(False)
@cython.wraparound(False)
def read_uuid_col(ResponseBuffer buffer, unsigned long long num_rows):
    cdef unsigned long long x = 0
    cdef char * loc = buffer.read_bytes_c(16 * num_rows)
    cdef char[16] temp
    cdef object column = PyTuple_New(num_rows), v
    new_uuid = UUID.__new__
    unsafe = SafeUUID.unsafe
    oset = object.__setattr__
    for x in range(num_rows):
        memcpy (<void *>temp, <void *>(loc + 8), 8)
        memcpy (<void *>(temp + 8), <void *>loc, 8)
        v = new_uuid(UUID)
        oset(v, 'int', int.from_bytes(temp[:16], 'little'))
        oset(v, 'is_safe', unsafe)
        PyTuple_SET_ITEM(column, x, v)
        Py_INCREF(v)
        loc += 16
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
def read_nullable_array(ResponseBuffer buffer, array_type: str, unsigned long long num_rows, object null_obj):
    if num_rows == 0:
        return ()
    cdef unsigned long long x = 0
    cdef size_t item_size = struct.calcsize(array_type)
    cdef cvarray cy_array = cvarray((num_rows,), item_size, array_type, mode='c', allocate_buffer=False)

    # We have to make a copy of the incoming null map because the next
    # "read_byes_c" call could invalidate our pointer by replacing the underlying buffer
    cdef char * null_map = <char *>PyMem_Malloc(<size_t>num_rows)
    memcpy(<void *>null_map, <void *>buffer.read_bytes_c(num_rows), num_rows)

    cy_array.data = buffer.read_bytes_c(num_rows * item_size)
    cdef object column = tuple(memoryview(cy_array))
    for x in range(num_rows):
        if null_map[x] != 0:
            Py_DECREF(column[x])
            Py_INCREF(null_obj)
            PyTuple_SET_ITEM(column, x, null_obj)
    PyMem_Free(<void *>null_map)
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
def build_nullable_column(source: Sequence, char * null_map, object null_obj):
    cdef unsigned long long num_rows = len(source), x
    cdef object column = PyTuple_New(num_rows), v
    for x in range(num_rows):
        if null_map[x] == 0:
            v = source[x]
        else:
            v = null_obj
        Py_INCREF(v)
        PyTuple_SET_ITEM(column, x, v)
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
def build_lc_nullable_column(index: Sequence, keys: array.array, object null_obj):
    cdef unsigned long long num_rows = len(keys), x, y
    cdef object column = PyTuple_New(num_rows), v
    for x in range(num_rows):
        y = keys[x]
        if y == 0:
            v = null_obj
        else:
            v = index[y]
        Py_INCREF(v)
        PyTuple_SET_ITEM(column, x, v)
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline extend_byte_array(target: bytearray, int start, object source, Py_ssize_t sz):
    PyByteArray_Resize(target, start + sz)
    target[start:start + sz] = source[0:sz]


@cython.boundscheck(False)
@cython.wraparound(False)
def write_str_col(column: Sequence, nullable: bool, encoding: Optional[str], dest: bytearray) -> int:
    cdef unsigned long long buff_size = len(column) << 5
    cdef unsigned long long buff_loc = 0, sz = 0, dsz = 0
    cdef unsigned long long array_size = PyByteArray_GET_SIZE(dest)
    cdef char * temp_buff = <char *>PyMem_Malloc(<size_t>buff_size)
    cdef object mv = PyMemoryView_FromMemory(temp_buff, buff_size, PyBUF_READ)
    cdef object encoded
    cdef char b
    cdef char * data
    try:
        for x in column:
            if not x:
                if not nullable and x is None:
                    return NONE_IN_NULLABLE_COLUMN
                temp_buff[buff_loc] = 0
                buff_loc += 1
                if buff_loc == buff_size:
                    extend_byte_array(dest, array_size, mv, buff_loc)
                    array_size += buff_loc
                    buff_loc = 0
            else:
                if not encoding:
                    data = x
                    dsz = len(x)
                else:
                    encoded = x.encode(encoding)
                    dsz = len(encoded)
                    data = encoded
                sz = dsz
                while True:
                    b = sz & 0x7f
                    sz >>= 7
                    if sz != 0:
                        b |= 0x80
                    temp_buff[buff_loc] = b
                    buff_loc += 1
                    if buff_loc == buff_size:
                        extend_byte_array(dest, array_size, mv, buff_loc)
                        array_size += buff_loc
                        buff_loc = 0
                    if sz == 0:
                        break
                if dsz + buff_loc >= buff_size:
                    if buff_loc > 0:  # Write what we have so far
                        extend_byte_array(dest, array_size, mv, buff_loc)
                        array_size += buff_loc
                        buff_loc = 0
                    if (dsz << 4) > buff_size:  # resize our buffer for very large strings
                        PyMem_Free(<void *> temp_buff)
                        mv.release()
                        buff_size = dsz << 6
                        temp_buff = <char *> PyMem_Malloc(<size_t> buff_size)
                        mv = PyMemoryView_FromMemory(temp_buff, buff_size, PyBUF_READ)
                memcpy(temp_buff + buff_loc, data, dsz)
                buff_loc += dsz
        if buff_loc > 0:
            extend_byte_array(dest, array_size, mv, buff_loc)
    finally:
        mv.release()
        PyMem_Free(<void *>temp_buff)
    return 0
