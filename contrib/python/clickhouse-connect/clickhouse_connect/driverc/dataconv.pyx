import struct
from typing import Sequence, Optional

import array
from datetime import datetime, date

import cython
import sys

from .buffer cimport ResponseBuffer
from cpython cimport Py_INCREF, Py_DECREF
from cpython.buffer cimport PyBUF_READ, PyObject_GetBuffer, PyBuffer_Release, PyBUF_SIMPLE, Py_buffer
from cpython.mem cimport PyMem_Free, PyMem_Malloc
from cpython.tuple cimport PyTuple_New, PyTuple_SET_ITEM
from cpython.bytearray cimport PyByteArray_GET_SIZE, PyByteArray_Resize, PyByteArray_AS_STRING
from cpython.memoryview cimport PyMemoryView_FromMemory
from cpython.datetime cimport datetime_new, import_datetime
from cython.view cimport array as cvarray
from ipaddress import IPv4Address
from uuid import UUID, SafeUUID
from libc.string cimport memcpy
from datetime import tzinfo

from clickhouse_connect.driver import tzutil, options
from clickhouse_connect.driver.common import must_swap
from clickhouse_connect.driver.errors import NONE_IN_NULLABLE_COLUMN
from clickhouse_connect.driver.exceptions import DataError

# Initialize datetime C API for direct object construction
import_datetime()

@cython.boundscheck(False)
@cython.wraparound(False)
def pivot(data: Sequence, unsigned long long start, unsigned long long end):
    return tuple(zip(*data[start:end]))


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
        # Fast path: naive UTC, construct datetime directly via C API
        while x < num_rows:
            v = _epoch_to_datetime((<unsigned int*>loc)[0], 0, None)
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            loc += 4
            x += 1
    elif tzutil.is_utc_timezone(tzinfo):
        # Fast path: UTC-equivalent timezone, direct C API construction with tzinfo
        while x < num_rows:
            v = _epoch_to_datetime((<unsigned int*>loc)[0], 0, tzinfo)
            PyTuple_SET_ITEM(column, x, v)
            Py_INCREF(v)
            loc += 4
            x += 1
    else:
        # Slow path: non-UTC timezone, requires fromtimestamp for DST-aware conversion
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
        if years == 3 and year % 100 != 0:
            m_list = MONTH_DAYS_LEAP
        else:
            m_list = MONTH_DAYS
    month = (rem + 24) >> 5
    prev = m_list[month]
    while rem < prev:
        month -= 1
        prev = m_list[month]
    return date(year, month + 1, rem + 1 - prev)


@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cdef inline void _epoch_days_to_components_c(int days, int* out_year, int* out_month, int* out_day):
    """Convert days since epoch to (year, month, day) components without allocating objects.

    Low-level helper that computes calendar components directly, reusing the fast
    epoch_days_to_date algorithm but returning components as output parameters.

    Args:
        days: Days since epoch
        out_year, out_month, out_day: Pointers to store results
    """
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
            out_year[0] = year - 1
            out_month[0] = 12
            out_day[0] = 31
            return
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
        year = (cycles << 2) + cycles400 * 400 + cycles100 * 100 + years + 1601
        if years == 4 or cycles100 == 4:
            out_year[0] = year - 1
            out_month[0] = 12
            out_day[0] = 31
            return
        if years == 3 and year % 100 != 0:
            m_list = MONTH_DAYS_LEAP
        else:
            m_list = MONTH_DAYS

    month = (rem + 24) >> 5
    prev = m_list[month]
    while rem < prev:
        month -= 1
        prev = m_list[month]

    out_year[0] = year
    out_month[0] = month + 1
    out_day[0] = rem + 1 - prev


@cython.cdivision(True)
@cython.boundscheck(False)
@cython.wraparound(False)
cpdef inline tuple epoch_seconds_to_components(long long seconds):
    """Convert epoch seconds to (year, month, day, hour, minute, second, microsecond).

    This decomposes a Unix timestamp into datetime components without creating
    intermediate objects. Handles both positive and negative epoch values correctly.

    Args:
        seconds: Unix timestamp (seconds since 1970-01-01 00:00:00 UTC)

    Returns:
        Tuple of (year, month, day, hour, minute, second, microsecond)
    """
    cdef long long days, secs_in_day
    cdef int hour, minute, second
    cdef int year, month, day

    days = seconds // 86400
    secs_in_day = seconds - days * 86400
    if secs_in_day < 0:
        secs_in_day += 86400
        days -= 1

    _epoch_days_to_components_c(days, &year, &month, &day)

    hour = secs_in_day // 3600
    secs_in_day %= 3600
    minute = secs_in_day // 60
    second = secs_in_day % 60

    return (year, month, day, hour, minute, second, 0)


cdef inline object _epoch_to_datetime(long long seconds, int microseconds, object tz):
    """Construct datetime directly from epoch seconds via C API, bypassing tuple + Python constructor.

    Uses cpython.datetime.datetime_new which calls PyDateTimeAPI factory directly,
    avoiding intermediate tuple allocation and Python-level datetime(...) overhead.
    """
    cdef long long days, secs_in_day
    cdef int hour, minute, second
    cdef int year, month, day

    days = seconds // 86400
    secs_in_day = seconds - days * 86400
    if secs_in_day < 0:
        secs_in_day += 86400
        days -= 1

    _epoch_days_to_components_c(days, &year, &month, &day)

    hour = secs_in_day // 3600
    secs_in_day %= 3600
    minute = secs_in_day // 60
    second = secs_in_day % 60

    return datetime_new(year, month, day, hour, minute, second, microseconds, tz, 0)


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
def read_datetime64_naive_col(object column: Sequence, unsigned long long prec, tz: tzinfo = None):
    """Read DateTime64 column using epoch arithmetic, for naive UTC or UTC-equivalent timezones.

    Constructs datetime objects directly from epoch seconds components via the
    CPython datetime C API. When tz is None, the result is naive. When tz is a
    UTC-equivalent timezone (UTC, Etc/UTC, GMT, etc.), the same arithmetic path
    is used and the tz is attached to the constructed datetime.

    Args:
        column: Sequence of integer ticks
        prec: Precision divisor (10**scale)
        tz: Optional UTC-equivalent timezone to attach. Must be None or UTC-equivalent
            (no DST or offset conversion is performed by this function).

    Returns:
        Tuple of datetime objects with microseconds, naive when tz is None
    """
    cdef unsigned long long x = 0
    cdef unsigned long long num_rows = len(column)
    cdef object result = PyTuple_New(num_rows), v
    cdef long long ticks, seconds, fractional_ticks
    cdef unsigned long long microseconds
    cdef long long prec_signed = <long long>prec

    for x in range(num_rows):
        ticks = column[x]
        seconds = ticks // prec_signed
        fractional_ticks = ticks - seconds * prec_signed
        microseconds = (fractional_ticks * 1000000) // prec_signed

        v = _epoch_to_datetime(seconds, microseconds, tz)
        PyTuple_SET_ITEM(result, x, v)
        Py_INCREF(v)

    return result


@cython.boundscheck(False)
@cython.wraparound(False)
def read_datetime64_tz_col(object column: Sequence, unsigned long long prec, tzinfo: tzinfo):
    """Read DateTime64 column with timezone conversion using per-row fromtimestamp.

    This handles non-UTC timezone conversion where DST-aware logic is necessary.
    The loop is in Cython for speed of the per-row datetime construction.

    Args:
        column: Sequence of integer ticks
        prec: Precision divisor (10**scale)
        tzinfo: Target timezone object

    Returns:
        List of datetime objects with specified timezone and microseconds
    """
    cdef unsigned long long x = 0
    cdef unsigned long long num_rows = len(column)
    cdef object result = PyTuple_New(num_rows), v
    cdef long long ticks, seconds, fractional_ticks
    cdef unsigned long long microseconds
    cdef object dt_from = datetime.fromtimestamp
    cdef long long prec_signed = <long long>prec

    for x in range(num_rows):
        ticks = column[x]
        seconds = ticks // prec_signed
        fractional_ticks = ticks - seconds * prec_signed
        microseconds = (fractional_ticks * 1000000) // prec_signed

        v = dt_from(seconds, tzinfo)
        if microseconds != 0:
            v = v.replace(microsecond=microseconds)
        PyTuple_SET_ITEM(result, x, v)
        Py_INCREF(v)

    return result


@cython.boundscheck(False)
@cython.wraparound(False)
def read_nullable_array(ResponseBuffer buffer, array_type: str, unsigned long long num_rows, object null_obj):
    if num_rows == 0:
        return []
    cdef unsigned long long x = 0
    cdef size_t item_size = struct.calcsize(array_type)
    cdef cvarray cy_array = cvarray((num_rows,), item_size, array_type, mode='c', allocate_buffer=False)

    # We have to make a copy of the incoming null map because the next
    # "read_byes_c" call could invalidate our pointer by replacing the underlying buffer
    cdef char * null_map = <char *>PyMem_Malloc(<size_t>num_rows)
    memcpy(<void *>null_map, <void *>buffer.read_bytes_c(num_rows), num_rows)

    cy_array.data = buffer.read_bytes_c(num_rows * item_size)
    cdef object column = list(memoryview(cy_array))
    for x in range(num_rows):
        if null_map[x] != 0:
            column[x] = null_obj
    PyMem_Free(<void *>null_map)
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
def build_nullable_column(source: Sequence, char * null_map, object null_obj):
    cdef unsigned long long num_rows = len(source), x
    cdef object column = [], v
    cdef object app = column.append
    for x in range(num_rows):
        if null_map[x] == 0:
            v = source[x]
        else:
            v = null_obj
        app(v)
    return column


@cython.boundscheck(False)
@cython.wraparound(False)
def build_lc_nullable_column(index: Sequence, keys: array.array, object null_obj):
    cdef unsigned long long num_rows = len(keys), x, y
    cdef object column = [], v
    cdef object app = column.append
    for x in range(num_rows):
        y = keys[x]
        if y == 0:
            v = null_obj
        else:
            v = index[y]
        app(v)
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


# Mapping of struct format codes to expected numpy dtype kind
_code_to_kind = {
    'b': 'i', 'h': 'i', 'i': 'i', 'l': 'i', 'q': 'i',
    'B': 'u', 'H': 'u', 'I': 'u', 'L': 'u', 'Q': 'u',
    'f': 'f', 'd': 'f',
}


@cython.boundscheck(False)
@cython.wraparound(False)
def write_native_col(str code, column, bytearray dest, object col_name=None) -> int:
    """
    Write a column of fixed-width values directly into dest bytearray.
    Fast-paths C-contiguous numpy arrays with matching dtype via memcpy.
    Falls back to struct.pack for Python sequences.
    """
    cdef Py_ssize_t old_size = PyByteArray_GET_SIZE(dest)
    cdef Py_buffer view
    cdef object dtype
    cdef str byteorder
    cdef str expected_kind
    cdef object np
    cdef Py_ssize_t num_rows

    # Numpy fast path: check if array is 1-D, contiguous, matching kind+size, little-endian
    np = options.np
    if np is not None and isinstance(column, np.ndarray):
        dtype = column.dtype
        byteorder = dtype.byteorder
        expected_kind = _code_to_kind.get(code, None)
        expected_size = array.array(code).itemsize

        # Check all safety conditions for memcpy
        if (column.ndim == 1 and
            column.flags['C_CONTIGUOUS'] and
            expected_kind is not None and
            dtype.kind == expected_kind and
            dtype.itemsize == expected_size and
            dtype.kind != 'O' and  # no object arrays
            (byteorder == '<' or
             (byteorder in ('=', '|') and sys.byteorder == 'little'))):

            # All checks passed so do direct memcpy
            PyObject_GetBuffer(column, &view, PyBUF_SIMPLE)
            try:
                PyByteArray_Resize(dest, old_size + view.len)
                memcpy(PyByteArray_AS_STRING(dest) + old_size, view.buf, view.len)
            finally:
                PyBuffer_Release(&view)
            return 0

    # General fallback: struct.pack with C-level argument unpacking, then append to dest.
    num_rows = len(column)
    try:
        dest += struct.Struct(f"<{num_rows}{code}").pack(*column)
    except (TypeError, OverflowError, struct.error) as ex:
        col_msg = f" for column `{str(col_name)}`" if col_name else ""
        error_detail = type(ex).__name__
        if isinstance(ex, OverflowError):
            error_detail = "value out of range"
        elif isinstance(ex, TypeError):
            error_detail = "type mismatch (usually None in non-Nullable column)"
        raise DataError(
            f"Unable to create native array{col_msg}: {error_detail}"
        ) from ex
    return 0


cdef inline unsigned long long _bswap_uint64(unsigned long long v):
    """Byte-swap a 64-bit unsigned integer for big-endian systems."""
    return (((v & 0xFF) << 56) | (((v >> 8) & 0xFF) << 48) |
            (((v >> 16) & 0xFF) << 40) | (((v >> 24) & 0xFF) << 32) |
            (((v >> 32) & 0xFF) << 24) | (((v >> 40) & 0xFF) << 16) |
            (((v >> 48) & 0xFF) << 8) | ((v >> 56) & 0xFF))


@cython.boundscheck(False)
@cython.wraparound(False)
def build_map_columns(column, bytearray dest):
    """
    Flatten a column of dicts into (keys, values) lists and write UInt64 offsets into dest.
    Uses two-pass strategy: first compute offsets, pre-allocate lists, then fill by index.
    """
    cdef unsigned long long num_rows = len(column)
    cdef unsigned long long total = 0, ix = 0, old_size
    cdef Py_ssize_t offset_bytes = num_rows * 8
    cdef char* dest_ptr
    cdef unsigned long long offset_value
    cdef unsigned long long i

    # First pass: compute offsets and total entry count, write into dest via memcpy (safe for alignment)
    old_size = PyByteArray_GET_SIZE(dest)
    PyByteArray_Resize(dest, old_size + offset_bytes)
    dest_ptr = PyByteArray_AS_STRING(dest) + old_size

    for i, v in enumerate(column):
        total += len(v)
        offset_value = total
        if must_swap:
            offset_value = _bswap_uint64(offset_value)
        memcpy(dest_ptr + i * 8, &offset_value, 8)

    # Pre-allocate lists at exact size, second pass fills by index
    keys = [None] * total
    values = [None] * total
    for v in column:
        for k, val in v.items():
            keys[ix] = k
            values[ix] = val
            ix += 1

    return keys, values
