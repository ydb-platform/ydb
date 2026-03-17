#------------------------------------------------------------------------------
# Copyright (c) 2020, 2024, Oracle and/or its affiliates.
#
# This software is dual-licensed to you under the Universal Permissive License
# (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl and Apache License
# 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose
# either license.
#
# If you elect to accept the software under the Apache License, Version 2.0,
# the following applies:
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#------------------------------------------------------------------------------

#------------------------------------------------------------------------------
# buffer.pyx
#
# Cython file defining the low-level read and write methods for packed data.
#------------------------------------------------------------------------------

cdef enum:
    NUMBER_AS_TEXT_CHARS = 172
    NUMBER_MAX_DIGITS = 40

cdef int MACHINE_BYTE_ORDER = BYTE_ORDER_MSB \
        if sys.byteorder == "big" else BYTE_ORDER_LSB

cdef inline uint16_t bswap16(uint16_t value):
    """
    Swap the order of bytes for a 16-bit integer.
    """
    return ((value << 8) & 0xff00) | ((value >> 8) & 0x00ff)


cdef inline uint32_t bswap32(uint32_t value):
    """
    Swap the order of bytes for a 32-bit integer.
    """
    return (
            ((value << 24) & (<uint32_t> 0xff000000)) |
            ((value << 8) & 0x00ff0000) |
            ((value >> 8) & 0x0000ff00) |
            ((value >> 24) & 0x000000ff)
    )


cdef inline uint64_t bswap64(uint64_t value):
    """
    Swap the order of bytes for a 64-bit integer.
    """
    return (
        ((value << 56) & (<uint64_t> 0xff00000000000000ULL)) |
        ((value << 40) & 0x00ff000000000000ULL) |
        ((value << 24) & 0x0000ff0000000000ULL) |
        ((value << 8) & 0x000000ff00000000ULL) |
        ((value >> 8) & 0x00000000ff000000ULL) |
        ((value >> 24) & 0x0000000000ff0000ULL) |
        ((value >> 40) & 0x000000000000ff00ULL) |
        ((value >> 56) & 0x00000000000000ffULL)
    )


cdef inline void pack_uint16(char_type *buf, uint16_t x, int order):
    """
    Pack a 16-bit integer into the buffer using the specified type order.
    """
    if order != MACHINE_BYTE_ORDER:
        x = bswap16(x)
    memcpy(buf, &x, sizeof(x))


cdef inline void pack_uint32(char_type *buf, uint32_t x, int order):
    """
    Pack a 32-bit integer into the buffer using the specified type order.
    """
    if order != MACHINE_BYTE_ORDER:
        x = bswap32(x)
    memcpy(buf, &x, sizeof(x))


cdef inline void pack_uint64(char_type *buf, uint64_t x, int order):
    """
    Pack a 64-bit integer into the buffer using the specified type order.
    """
    if order != MACHINE_BYTE_ORDER:
        x = bswap64(x)
    memcpy(buf, &x, sizeof(x))


cdef inline uint16_t unpack_uint16(const char_type *buf, int order):
    """
    Unpacks a 16-bit integer from the buffer using the specified byte order.
    """
    cdef uint16_t raw_value
    memcpy(&raw_value, buf, sizeof(raw_value))
    return raw_value if order == MACHINE_BYTE_ORDER else bswap16(raw_value)


cdef inline uint32_t unpack_uint32(const char_type *buf, int order):
    """
    Unpacks a 32-bit integer from the buffer using the specified byte order.
    """
    cdef uint32_t raw_value
    memcpy(&raw_value, buf, sizeof(raw_value))
    return raw_value if order == MACHINE_BYTE_ORDER else bswap32(raw_value)


cdef class Buffer:

    cdef int _get_int_length_and_sign(self, uint8_t *length,
                                      bint *is_negative,
                                      uint8_t max_length) except -1:
        """
        Returns the length of an integer stored in the buffer. A check is also
        made to ensure the integer does not exceed the maximum length. If the
        is_negative pointer is NULL, negative integers will result in an
        exception being raised.
        """
        cdef const char_type *ptr = self._get_raw(1)
        if ptr[0] & 0x80:
            if is_negative == NULL:
                errors._raise_err(errors.ERR_UNEXPECTED_NEGATIVE_INTEGER)
            is_negative[0] = True
            length[0] = ptr[0] & 0x7f
        else:
            if is_negative != NULL:
                is_negative[0] = False
            length[0] = ptr[0]
        if length[0] > max_length:
            errors._raise_err(errors.ERR_INTEGER_TOO_LARGE, length=length[0],
                              max_length=max_length)

    cdef const char_type* _get_raw(self, ssize_t num_bytes) except NULL:
        """
        Returns a pointer to a buffer containing the requested number of bytes.
        """
        cdef:
            ssize_t num_bytes_left
            const char_type *ptr
        num_bytes_left = self._size - self._pos
        if num_bytes > num_bytes_left:
            errors._raise_err(errors.ERR_UNEXPECTED_END_OF_DATA,
                              num_bytes_wanted=num_bytes,
                              num_bytes_available=num_bytes_left)
        ptr = &self._data[self._pos]
        self._pos += num_bytes
        return ptr

    cdef int _initialize(self, ssize_t max_size = TNS_CHUNK_SIZE) except -1:
        """
        Initialize the buffer with an empty bytearray of the specified size.
        """
        self._max_size = max_size
        self._data_obj = bytearray(max_size)
        self._data_view = self._data_obj
        self._data = <char_type*> self._data_obj

    cdef int _populate_from_bytes(self, bytes data) except -1:
        """
        Initialize the buffer with the data in the specified byte string.
        """
        self._max_size = self._size = len(data)
        self._data_obj = bytearray(data)
        self._data_view = self._data_obj
        self._data = <char_type*> self._data_obj

    cdef int _read_raw_bytes_and_length(self, const char_type **ptr,
                                        ssize_t *num_bytes) except -1:
        """
        Helper function that processes the length (if needed) and then acquires
        the specified number of bytes from the buffer. The base function simply
        uses the length as given.
        """
        ptr[0] = self._get_raw(num_bytes[0])

    cdef int _resize(self, ssize_t new_max_size) except -1:
        """
        Resizes the buffer to the new maximum size, copying the data already
        stored in the buffer first.
        """
        cdef:
            bytearray data_obj
            char_type* data
        data_obj = bytearray(new_max_size)
        data = <char_type*> data_obj
        memcpy(data, self._data, self._max_size)
        self._max_size = new_max_size
        self._data_obj = data_obj
        self._data_view = data_obj
        self._data = data

    cdef int _skip_int(self, uint8_t max_length, bint *is_negative) except -1:
        """
        Skips reading an integer of the specified maximum length from the
        buffer.
        """
        cdef uint8_t length
        self._get_int_length_and_sign(&length, is_negative, max_length)
        self.skip_raw_bytes(length)

    cdef uint64_t _unpack_int(self, const char_type *ptr, uint8_t length):
        """
        Unpacks an integer received in the buffer into its native format.
        """
        if length == 1:
            return ptr[0]
        elif length == 2:
            return (ptr[0] << 8) | ptr[1]
        elif length == 3:
            return (ptr[0] << 16) | (ptr[1] << 8) | ptr[2]
        elif length == 4:
            return (ptr[0] << 24) | (ptr[1] << 16) | (ptr[2] << 8) | ptr[3]
        elif length == 5:
            return ((<uint64_t> ptr[0]) << 32) | (ptr[1] << 24) | \
                    (ptr[2] << 16) | (ptr[3] << 8) | ptr[4]
        elif length == 6:
            return ((<uint64_t> ptr[0]) << 40) | \
                   ((<uint64_t> ptr[1]) << 32) | (ptr[2] << 24) | \
                   (ptr[3] << 16) | (ptr[4] << 8) | ptr[5]
        elif length == 7:
            return ((<uint64_t> ptr[0]) << 48) | \
                   ((<uint64_t> ptr[1]) << 40) | \
                   ((<uint64_t> ptr[2]) << 32) | \
                   (ptr[3] << 24) | (ptr[4] << 16) | (ptr[5] << 8) | ptr[6]
        elif length == 8:
            return ((<uint64_t> ptr[0]) << 56) | \
                   ((<uint64_t> ptr[1]) << 48) | \
                   ((<uint64_t> ptr[2]) << 40) | \
                   ((<uint64_t> ptr[3]) << 32) | \
                   (ptr[4] << 24) | (ptr[5] << 16) | (ptr[6] << 8) | ptr[7]

    cdef int _write_more_data(self, ssize_t num_bytes_available,
                              ssize_t num_bytes_wanted) except -1:
        """
        Called when the amount of buffer available is less than the amount of
        data requested. By default an error is raised.
        """
        errors._raise_err(errors.ERR_BUFFER_LENGTH_INSUFFICIENT,
                          required_buffer_len=num_bytes_wanted,
                          actual_buffer_len=num_bytes_available)

    cdef int _write_raw_bytes_and_length(self, const char_type *ptr,
                                         ssize_t num_bytes) except -1:
        """
        Helper function that writes the length in the format required before
        writing the bytes.
        """
        cdef ssize_t chunk_len
        if num_bytes <= TNS_MAX_SHORT_LENGTH:
            self.write_uint8(<uint8_t> num_bytes)
            if num_bytes > 0:
                self.write_raw(ptr, num_bytes)
        else:
            self.write_uint8(TNS_LONG_LENGTH_INDICATOR)
            while num_bytes > 0:
                chunk_len = min(num_bytes, TNS_CHUNK_SIZE)
                self.write_ub4(chunk_len)
                num_bytes -= chunk_len
                self.write_raw(ptr, chunk_len)
                ptr += chunk_len
            self.write_ub4(0)

    cdef inline ssize_t bytes_left(self):
        """
        Return the number of bytes remaining in the buffer.
        """
        return self._size - self._pos

    cdef int parse_binary_double(self, const uint8_t* ptr,
                                 double *double_ptr) except -1:
        """
        Read a binary double value from the buffer and return the corresponding
        Python object representing that value.
        """
        cdef:
            uint8_t b0, b1, b2, b3, b4, b5, b6, b7
            uint64_t high_bits, low_bits, all_bits
        b0 = ptr[0]
        b1 = ptr[1]
        b2 = ptr[2]
        b3 = ptr[3]
        b4 = ptr[4]
        b5 = ptr[5]
        b6 = ptr[6]
        b7 = ptr[7]
        if b0 & 0x80:
            b0 = b0 & 0x7f
        else:
            b0 = ~b0
            b1 = ~b1
            b2 = ~b2
            b3 = ~b3
            b4 = ~b4
            b5 = ~b5
            b6 = ~b6
            b7 = ~b7
        high_bits = b0 << 24 | b1 << 16 | b2 << 8 | b3
        low_bits = b4 << 24 | b5 << 16 | b6 << 8 | b7
        all_bits = high_bits << 32 | (low_bits & <uint64_t> 0xffffffff)
        memcpy(double_ptr, &all_bits, 8)

    cdef int parse_binary_float(self, const uint8_t* ptr,
                                float *float_ptr) except -1:
        """
        Parse a binary float value from the buffer and return the corresponding
        Python object representing that value.
        """
        cdef:
            uint8_t b0, b1, b2, b3
            uint64_t all_bits
        b0 = ptr[0]
        b1 = ptr[1]
        b2 = ptr[2]
        b3 = ptr[3]
        if b0 & 0x80:
            b0 = b0 & 0x7f
        else:
            b0 = ~b0
            b1 = ~b1
            b2 = ~b2
            b3 = ~b3
        all_bits = (b0 << 24) | (b1 << 16) | (b2 << 8) | b3
        memcpy(float_ptr, &all_bits, 4)

    cdef object parse_date(self, const uint8_t* ptr, ssize_t num_bytes):
        """
        Read a date from the buffer and return the corresponding Python object
        representing that value.
        """
        cdef:
            int8_t tz_hour = 0, tz_minute = 0
            uint32_t fsecond = 0
            int32_t seconds
            int16_t year
        year = (ptr[0] - 100) * 100 + ptr[1] - 100
        if num_bytes >= 11:
            fsecond = unpack_uint32(&ptr[7], BYTE_ORDER_MSB) // 1000
        value = cydatetime.datetime_new(year, ptr[2], ptr[3], ptr[4] - 1,
                                        ptr[5] - 1, ptr[6] - 1, fsecond, None)
        if num_bytes > 11 and ptr[11] != 0 and ptr[12] != 0:
            if ptr[11] & TNS_HAS_REGION_ID:
                errors._raise_err(errors.ERR_NAMED_TIMEZONE_NOT_SUPPORTED)
            tz_hour = ptr[11] - TZ_HOUR_OFFSET
            tz_minute = ptr[12] - TZ_MINUTE_OFFSET
            if tz_hour != 0 or tz_minute != 0:
                seconds = tz_hour * 3600 + tz_minute * 60
                value += cydatetime.timedelta_new(0, seconds, 0)
        return value

    cdef object parse_interval_ds(self, const uint8_t* ptr):
        """
        Read an interval day to second value from the buffer and return the
        corresponding Python object representing that value.
        """
        cdef:
            int32_t days, hours, minutes, seconds, total_seconds, fseconds
            uint8_t duration_offset = TNS_DURATION_OFFSET
            uint32_t duration_mid = TNS_DURATION_MID
        days = unpack_uint32(ptr, BYTE_ORDER_MSB) - duration_mid
        fseconds = unpack_uint32(&ptr[7], BYTE_ORDER_MSB) - duration_mid
        hours = ptr[4] - duration_offset
        minutes = ptr[5] - duration_offset
        seconds = ptr[6] - duration_offset
        total_seconds = hours * 60 * 60 + minutes * 60 + seconds
        return cydatetime.timedelta_new(days, total_seconds, fseconds // 1000)

    cdef object parse_interval_ym(self, const uint8_t* ptr):
        """
        Read an interval year to month value from the buffer and return the
        corresponding Python object representing that value.
        """
        cdef int32_t years, months
        years = unpack_uint32(ptr, BYTE_ORDER_MSB) - TNS_DURATION_MID
        months = ptr[4] - TNS_DURATION_OFFSET
        return PY_TYPE_INTERVAL_YM(years, months)

    cdef object parse_oracle_number(self, const uint8_t* ptr,
                                    ssize_t num_bytes, int preferred_num_type):
        """
        Parse an Oracle number from the supplied buffer and return the
        corresponding Python object representing that value. The preferred
        numeric type (int, float, decimal.Decimal and str) is used, if
        possible.
        """
        cdef:
            char_type buf[NUMBER_AS_TEXT_CHARS]
            uint8_t digits[NUMBER_MAX_DIGITS]
            uint8_t num_digits, byte, digit
            bint is_positive, is_integer
            int16_t decimal_point_index
            int8_t exponent
            str text

        # the first byte is the exponent; positive numbers have the highest
        # order bit set, whereas negative numbers have the highest order bit
        # cleared and the bits inverted
        exponent = <int8_t> ptr[0]
        is_positive = (exponent & 0x80)
        if not is_positive:
            exponent = ~exponent
        exponent -= 193
        decimal_point_index = exponent * 2 + 2

        # a mantissa length of 0 implies a value of 0 (if positive) or a value
        # of -1e126 (if negative)
        if num_bytes == 1:
            if is_positive:
                if preferred_num_type == NUM_TYPE_INT:
                    return 0
                elif preferred_num_type == NUM_TYPE_DECIMAL:
                    return PY_TYPE_DECIMAL(0)
                elif preferred_num_type == NUM_TYPE_STR:
                    return "0"
                return 0.0
            if preferred_num_type == NUM_TYPE_INT:
                return -10 ** 126
            elif preferred_num_type == NUM_TYPE_DECIMAL:
                return PY_TYPE_DECIMAL("-1e126")
            elif preferred_num_type == NUM_TYPE_STR:
                return "-1e126"
            return -1.0e126

        # check for the trailing 102 byte for negative numbers and, if present,
        # reduce the number of mantissa digits
        if not is_positive and ptr[num_bytes - 1] == 102:
            num_bytes -= 1

        # process the mantissa bytes which are the remaining bytes; each
        # mantissa byte is a base-100 digit
        num_digits = 0
        for i in range(1, num_bytes):

            # positive numbers have 1 added to them; negative numbers are
            # subtracted from the value 101
            byte = ptr[i]
            if is_positive:
                byte -= 1
            else:
                byte = 101 - byte

            # process the first digit; leading zeroes are ignored
            digit = <uint8_t> byte // 10
            if digit == 0 and num_digits == 0:
                decimal_point_index -= 1
            elif digit == 10:
                digits[num_digits] = 1
                digits[num_digits + 1] = 0
                num_digits += 2
                decimal_point_index += 1
            elif digit != 0 or i > 0:
                digits[num_digits] = digit
                num_digits += 1

            # process the second digit; trailing zeroes are ignored
            digit = byte % 10
            if digit != 0 or i < num_bytes - 1:
                digits[num_digits] = digit
                num_digits += 1

        # create string of digits for transformation to Python value
        is_integer = 1
        num_bytes = 0

        # if negative, include the sign
        if not is_positive:
            buf[num_bytes] = 45         # minus sign
            num_bytes += 1

        # if the decimal point index is 0 or less, add the decimal point and
        # any leading zeroes that are needed
        if decimal_point_index <= 0:
            buf[num_bytes] = 48         # zero
            buf[num_bytes + 1] = 46     # decimal point
            num_bytes += 2
            is_integer = 0
            for i in range(decimal_point_index, 0):
                buf[num_bytes] = 48     # zero
                num_bytes += 1

        # add each of the digits
        for i in range(num_digits):
            if i > 0 and i == decimal_point_index:
                buf[num_bytes] = 46     # decimal point
                is_integer = 0
                num_bytes += 1
            buf[num_bytes] = 48 + digits[i]
            num_bytes += 1

        # if the decimal point index exceeds the number of digits, add any
        # trailing zeroes that are needed
        if decimal_point_index > num_digits:
            for i in range(num_digits, decimal_point_index):
                buf[num_bytes] = 48     # zero
                num_bytes += 1

        # convert result to an integer or a decimal number
        if preferred_num_type == NUM_TYPE_INT and is_integer:
            return int(buf[:num_bytes])
        elif preferred_num_type == NUM_TYPE_DECIMAL:
            return PY_TYPE_DECIMAL(buf[:num_bytes].decode())
        elif preferred_num_type == NUM_TYPE_STR:
            return buf[:num_bytes].decode()
        return float(buf[:num_bytes])

    cdef object read_binary_double(self):
        """
        Read a binary double value from the buffer and return the corresponding
        Python object representing that value.
        """
        cdef:
            const uint8_t *ptr
            ssize_t num_bytes
            double value
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if ptr != NULL:
            self.parse_binary_double(ptr, &value)
            return value

    cdef object read_binary_float(self):
        """
        Read a binary float value from the buffer and return the corresponding
        Python object representing that value.
        """
        cdef:
            const uint8_t *ptr
            ssize_t num_bytes
            float value
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if ptr != NULL:
            self.parse_binary_float(ptr, &value)
            return value

    cdef object read_binary_integer(self):
        """
        Read a binary integer from the buffer.
        """
        cdef:
            const char_type *ptr
            ssize_t num_bytes
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if num_bytes > 4:
            errors._raise_err(errors.ERR_INTEGER_TOO_LARGE, length=num_bytes,
                              max_length=4)
        if ptr != NULL:
            return <int32_t> self._unpack_int(ptr, num_bytes)

    cdef object read_bool(self):
        """
        Read a boolean from the buffer and return True, False or None.
        """
        cdef:
            const char_type *ptr
            ssize_t num_bytes
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if ptr != NULL:
            return ptr[num_bytes - 1] == 1

    cdef object read_bytes(self):
        """
        Read bytes from the buffer and return the corresponding Python object
        representing that value.
        """
        cdef:
            const char_type *ptr
            ssize_t num_bytes
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if ptr != NULL:
            return ptr[:num_bytes]

    cdef object read_date(self):
        """
        Read a date from the buffer and return the corresponding Python object
        representing that value.
        """
        cdef:
            const uint8_t *ptr
            ssize_t num_bytes
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if ptr != NULL:
            return self.parse_date(ptr, num_bytes)

    cdef object read_interval_ds(self):
        """
        Read an interval day to second value from the buffer and return the
        corresponding Python object representing that value.
        """
        cdef:
            const uint8_t *ptr
            ssize_t num_bytes
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if ptr != NULL:
            return self.parse_interval_ds(ptr)

    cdef object read_interval_ym(self):
        """
        Read an interval year to month value from the buffer and return the
        corresponding Python object representing that value.
        """
        cdef:
            const uint8_t *ptr
            ssize_t num_bytes
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if ptr != NULL:
            return self.parse_interval_ym(ptr)

    cdef int read_int32(self, int32_t *value,
                        int byte_order=BYTE_ORDER_MSB) except -1:
        """
        Read a signed 32-bit integer from the buffer in the specified byte
        order.
        """
        cdef const char_type *ptr = self._get_raw(4)
        value[0] = <int32_t> unpack_uint32(ptr, byte_order)

    cdef object read_oracle_number(self, int preferred_num_type):
        """
        Read an Oracle number from the buffer and return the corresponding
        Python object representing that value. The preferred numeric type
        (int, float, decimal.Decimal and str) is used, if possible.
        """
        cdef:
            const uint8_t *ptr
            ssize_t num_bytes

        # read the number of bytes in the number; if the value is 0 or the null
        # length indicator, return None
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if ptr != NULL:
            return self.parse_oracle_number(ptr, num_bytes, preferred_num_type)

    cdef const char_type* read_raw_bytes(self, ssize_t num_bytes) except NULL:
        """
        Returns a pointer to a contiguous buffer containing the specified
        number of bytes found in the buffer.
        """
        return self._get_raw(num_bytes)

    cdef int read_raw_bytes_and_length(self, const char_type **ptr,
                                       ssize_t *num_bytes) except -1:
        """
        Reads bytes from the buffer into a contiguous buffer. The first byte
        read is the number of bytes to read.
        """
        cdef uint8_t length
        self.read_ub1(&length)
        if length == 0 or length == TNS_NULL_LENGTH_INDICATOR:
            ptr[0] = NULL
            num_bytes[0] = 0
        else:
            num_bytes[0] = length
            self._read_raw_bytes_and_length(ptr, num_bytes)

    cdef int read_sb1(self, int8_t *value) except -1:
        """
        Reads a signed 8-bit integer from the buffer.
        """
        cdef const char_type *ptr = self._get_raw(1)
        value[0] = <int8_t> ptr[0]

    cdef int read_sb2(self, int16_t *value) except -1:
        """
        Reads a signed 16-bit integer from the buffer.
        """
        cdef:
            const char_type *ptr
            bint is_negative
            uint8_t length
        self._get_int_length_and_sign(&length, &is_negative, 2)
        if length == 0:
            value[0] = 0
        else:
            ptr = self._get_raw(length)
            value[0] = <int16_t> self._unpack_int(ptr, length)
            if is_negative:
                value[0] = -value[0]

    cdef int read_sb4(self, int32_t *value) except -1:
        """
        Reads a signed 32-bit integer from the buffer.
        """
        cdef:
            const char_type *ptr
            bint is_negative
            uint8_t length
        self._get_int_length_and_sign(&length, &is_negative, 4)
        if length == 0:
            value[0] = 0
        else:
            ptr = self._get_raw(length)
            value[0] = <int32_t> self._unpack_int(ptr, length)
            if is_negative:
                value[0] = -value[0]

    cdef int read_sb8(self, int64_t *value) except -1:
        """
        Reads a signed 64-bit integer from the buffer.
        """
        cdef:
            const char_type *ptr
            bint is_negative
            uint8_t length
        self._get_int_length_and_sign(&length, &is_negative, 8)
        if length == 0:
            value[0] = 0
        else:
            ptr = self._get_raw(length)
            value[0] = self._unpack_int(ptr, length)
            if is_negative:
                value[0] = -value[0]

    cdef bytes read_null_terminated_bytes(self):
        """
        Reads null-terminated bytes from the buffer (including the null
        terminator). It is assumed that the buffer contains the full amount. If
        it does not, the remainder of the buffer is returned instead.
        """
        cdef ssize_t start_pos = self._pos, end_pos = self._pos
        while self._data[end_pos] != 0 and end_pos < self._size:
            end_pos += 1
        self._pos = end_pos + 1
        return self._data[start_pos:self._pos]

    cdef object read_str(self, int csfrm, const char* encoding_errors=NULL):
        """
        Reads bytes from the buffer and decodes them into a string following
        the supplied character set form.
        """
        cdef:
            const char_type *ptr
            ssize_t num_bytes
        self.read_raw_bytes_and_length(&ptr, &num_bytes)
        if ptr != NULL:
            if csfrm == CS_FORM_IMPLICIT:
                return ptr[:num_bytes].decode(ENCODING_UTF8, encoding_errors)
            return ptr[:num_bytes].decode(ENCODING_UTF16, encoding_errors)

    cdef int read_ub1(self, uint8_t *value) except -1:
        """
        Reads an unsigned 8-bit integer from the buffer.
        """
        cdef const char_type *ptr = self._get_raw(1)
        value[0] = ptr[0]

    cdef int read_ub2(self, uint16_t *value) except -1:
        """
        Reads an unsigned 16-bit integer from the buffer.
        """
        cdef:
            const char_type *ptr
            uint8_t length
        self._get_int_length_and_sign(&length, NULL, 2)
        if length == 0:
            value[0] = 0
        else:
            ptr = self._get_raw(length)
            value[0] = <uint16_t> self._unpack_int(ptr, length)

    cdef int read_ub4(self, uint32_t *value) except -1:
        """
        Reads an unsigned 32-bit integer from the buffer.
        """
        cdef:
            const char_type *ptr
            uint8_t length
        self._get_int_length_and_sign(&length, NULL, 4)
        if length == 0:
            value[0] = 0
        else:
            ptr = self._get_raw(length)
            value[0] = <uint32_t> self._unpack_int(ptr, length)

    cdef int read_ub8(self, uint64_t *value) except -1:
        """
        Reads an unsigned 64-bit integer from the buffer.
        """
        cdef:
            const char_type *ptr
            uint8_t length
        self._get_int_length_and_sign(&length, NULL, 8)
        if length == 0:
            value[0] = 0
        else:
            ptr = self._get_raw(length)
            value[0] = self._unpack_int(ptr, length)

    cdef int read_uint16(self, uint16_t *value,
                         int byte_order=BYTE_ORDER_MSB) except -1:
        """
        Read a 16-bit integer from the buffer in the specified byte order.
        """
        cdef const char_type *ptr = self._get_raw(2)
        value[0] = unpack_uint16(ptr, byte_order)

    cdef int read_uint32(self, uint32_t *value,
                         int byte_order=BYTE_ORDER_MSB) except -1:
        """
        Read a 32-bit integer from the buffer in the specified byte order.
        """
        cdef const char_type *ptr = self._get_raw(4)
        value[0] = unpack_uint32(ptr, byte_order)

    cdef int skip_raw_bytes(self, ssize_t num_bytes) except -1:
        """
        Skip the specified number of bytes in the buffer. In order to avoid
        copying data, the number of bytes left in the packet is determined and
        only that amount is requested.
        """
        cdef ssize_t num_bytes_this_time
        while num_bytes > 0:
            num_bytes_this_time = min(num_bytes, self.bytes_left())
            self._get_raw(num_bytes_this_time)
            num_bytes -= num_bytes_this_time

    cdef inline int skip_sb4(self) except -1:
        """
        Skips a signed 32-bit integer in the buffer.
        """
        cdef bint is_negative
        return self._skip_int(4, &is_negative)

    cdef inline void skip_to(self, ssize_t pos):
        """
        Skips to the specified location in the buffer.
        """
        self._pos = pos

    cdef inline int skip_ub1(self) except -1:
        """
        Skips an unsigned 8-bit integer in the buffer.
        """
        self._get_raw(1)

    cdef inline int skip_ub2(self) except -1:
        """
        Skips an unsigned 16-bit integer in the buffer.
        """
        return self._skip_int(2, NULL)

    cdef inline int skip_ub4(self) except -1:
        """
        Skips an unsigned 32-bit integer in the buffer.
        """
        return self._skip_int(4, NULL)

    cdef inline int skip_ub8(self) except -1:
        """
        Skips an unsigned 64-bit integer in the buffer.
        """
        return self._skip_int(8, NULL)

    cdef int write_binary_double(self, double value,
                                 bint write_length=True) except -1:
        """
        Writes a double value to the buffer in Oracle canonical double floating
        point format.
        """
        cdef:
            uint8_t b0, b1, b2, b3, b4, b5, b6, b7
            uint64_t all_bits
            char_type buf[8]
            uint64_t *ptr
        ptr = <uint64_t*> &value
        all_bits = ptr[0]
        b7 = all_bits & 0xff
        b6 = (all_bits >> 8) & 0xff
        b5 = (all_bits >> 16) & 0xff
        b4 = (all_bits >> 24) & 0xff
        b3 = (all_bits >> 32) & 0xff
        b2 = (all_bits >> 40) & 0xff
        b1 = (all_bits >> 48) & 0xff
        b0 = (all_bits >> 56) & 0xff
        if b0 & 0x80 == 0:
            b0 = b0 | 0x80
        else:
            b0 = ~b0
            b1 = ~b1
            b2 = ~b2
            b3 = ~b3
            b4 = ~b4
            b5 = ~b5
            b6 = ~b6
            b7 = ~b7
        buf[0] = b0
        buf[1] = b1
        buf[2] = b2
        buf[3] = b3
        buf[4] = b4
        buf[5] = b5
        buf[6] = b6
        buf[7] = b7
        if write_length:
            self.write_uint8(8)
        self.write_raw(buf, 8)

    cdef int write_binary_float(self, float value,
                                bint write_length=True) except -1:
        """
        Writes a float value to the buffer in Oracle canonical floating point
        format.
        """
        cdef:
            uint8_t b0, b1, b2, b3
            uint32_t all_bits
            char_type buf[4]
            uint32_t *ptr
        ptr = <uint32_t*> &value
        all_bits = ptr[0]
        b3 = all_bits & 0xff
        b2 = (all_bits >> 8) & 0xff
        b1 = (all_bits >> 16) & 0xff
        b0 = (all_bits >> 24) & 0xff
        if b0 & 0x80 == 0:
            b0 = b0 | 0x80
        else:
            b0 = ~b0
            b1 = ~b1
            b2 = ~b2
            b3 = ~b3
        buf[0] = b0
        buf[1] = b1
        buf[2] = b2
        buf[3] = b3
        if write_length:
            self.write_uint8(4)
        self.write_raw(buf, 4)

    cdef int write_bool(self, bint value) except -1:
        """
        Writes a boolean value to the buffer.
        """
        if value:
            self.write_uint8(2)
            self.write_uint16(0x0101)
        else:
            self.write_uint16(0x0100)

    cdef int write_bytes(self, bytes value) except -1:
        """
        Writes the bytes to the buffer directly.
        """
        cdef:
            ssize_t value_len
            char_type *ptr
        cpython.PyBytes_AsStringAndSize(value, <char**> &ptr, &value_len)
        self.write_raw(ptr, value_len)

    cdef int write_bytes_with_length(self, bytes value) except -1:
        """
        Writes the bytes to the buffer after first writing the length.
        """
        cdef:
            ssize_t value_len
            char_type *ptr
        cpython.PyBytes_AsStringAndSize(value, <char**> &ptr, &value_len)
        self._write_raw_bytes_and_length(ptr, value_len)

    cdef int write_interval_ds(self, object value,
                               bint write_length=True) except -1:
        """
        Writes an interval to the buffer in Oracle Interval Day To Second
        format.
        """
        cdef:
            int32_t days, seconds, fseconds
            char_type buf[11]
        days = cydatetime.timedelta_days(value)
        pack_uint32(buf, days + TNS_DURATION_MID, BYTE_ORDER_MSB)
        seconds = cydatetime.timedelta_seconds(value)
        buf[4] = (seconds // 3600) + TNS_DURATION_OFFSET
        seconds = seconds % 3600
        buf[5] = (seconds // 60) + TNS_DURATION_OFFSET
        buf[6] = (seconds % 60) + TNS_DURATION_OFFSET
        fseconds = cydatetime.timedelta_microseconds(value) * 1000
        pack_uint32(&buf[7], fseconds + TNS_DURATION_MID, BYTE_ORDER_MSB)
        if write_length:
            self.write_uint8(sizeof(buf))
        self.write_raw(buf, sizeof(buf))

    cdef int write_interval_ym(self, object value,
                               bint write_length=True) except -1:
        """
        Writes an interval to the buffer in Oracle Interval Day To Second
        format.
        """
        cdef:
            int32_t years, months
            char_type buf[5]
        years = (<tuple> value)[0]
        months = (<tuple> value)[1]
        pack_uint32(buf, years + TNS_DURATION_MID, BYTE_ORDER_MSB)
        buf[4] = months + TNS_DURATION_OFFSET
        if write_length:
            self.write_uint8(sizeof(buf))
        self.write_raw(buf, sizeof(buf))

    cdef int write_oracle_date(self, object value, uint8_t length,
                               bint write_length=True) except -1:
        """
        Writes a date to the buffer in Oracle Date format.
        """
        cdef:
            unsigned int year
            char_type buf[13]
            uint32_t fsecond
        year = cydatetime.PyDateTime_GET_YEAR(value)
        buf[0] = <uint8_t> ((year // 100) + 100)
        buf[1] = <uint8_t> ((year % 100) + 100)
        buf[2] = <uint8_t> cydatetime.PyDateTime_GET_MONTH(value)
        buf[3] = <uint8_t> cydatetime.PyDateTime_GET_DAY(value)
        buf[4] = <uint8_t> cydatetime.PyDateTime_DATE_GET_HOUR(value) + 1
        buf[5] = <uint8_t> cydatetime.PyDateTime_DATE_GET_MINUTE(value) + 1
        buf[6] = <uint8_t> cydatetime.PyDateTime_DATE_GET_SECOND(value) + 1
        if length > 7:
            fsecond = <uint32_t> \
                    cydatetime.PyDateTime_DATE_GET_MICROSECOND(value) * 1000
            if fsecond == 0 and length <= 11:
                length = 7
            else:
                pack_uint32(&buf[7], fsecond, BYTE_ORDER_MSB)
        if length > 11:
            buf[11] = TZ_HOUR_OFFSET
            buf[12] = TZ_MINUTE_OFFSET
        if write_length:
            self.write_uint8(length)
        self.write_raw(buf, length)

    cdef int write_oracle_number(self, bytes num_bytes) except -1:
        """
        Writes a number in UTF-8 encoded bytes in Oracle Number format to the
        buffer.
        """
        cdef:
            uint8_t num_digits = 0, digit, num_pairs, pair_num, digits_pos
            bint exponent_is_negative = False, append_sentinel = False
            ssize_t num_bytes_length, exponent_pos, pos = 0
            bint is_negative = False, prepend_zero = False
            uint8_t digits[NUMBER_AS_TEXT_CHARS]
            int16_t decimal_point_index
            int8_t exponent_on_wire
            const char_type *ptr
            int16_t exponent

        # zero length string cannot be converted
        num_bytes_length = len(num_bytes)
        if num_bytes_length == 0:
            errors._raise_err(errors.ERR_NUMBER_STRING_OF_ZERO_LENGTH)
        elif num_bytes_length > NUMBER_AS_TEXT_CHARS:
            errors._raise_err(errors.ERR_NUMBER_STRING_TOO_LONG)

        # check to see if number is negative (first character is '-')
        ptr = num_bytes
        if ptr[0] == b'-':
            is_negative = True
            pos += 1

        # scan for digits until the decimal point or exponent indicator found
        while pos < num_bytes_length:
            if ptr[pos] == b'.' or ptr[pos] == b'e' or ptr[pos] == b'E':
                break
            if ptr[pos] < b'0' or ptr[pos] > b'9':
                errors._raise_err(errors.ERR_INVALID_NUMBER)
            digit = ptr[pos] - <uint8_t> b'0'
            pos += 1
            if digit == 0 and num_digits == 0:
                continue
            digits[num_digits] = digit
            num_digits += 1
        decimal_point_index = num_digits

        # scan for digits following the decimal point, if applicable
        if pos < num_bytes_length and ptr[pos] == b'.':
            pos += 1
            while pos < num_bytes_length:
                if ptr[pos] == b'e' or ptr[pos] == b'E':
                    break
                digit = ptr[pos] - <uint8_t> b'0'
                pos += 1
                if digit == 0 and num_digits == 0:
                    decimal_point_index -= 1
                    continue
                digits[num_digits] = digit
                num_digits += 1

        # handle exponent, if applicable
        if pos < num_bytes_length and (ptr[pos] == b'e' or ptr[pos] == b'E'):
            pos += 1
            if pos < num_bytes_length:
                if ptr[pos] == b'-':
                    exponent_is_negative = True
                    pos += 1
                elif ptr[pos] == b'+':
                    pos += 1
            exponent_pos = pos
            while pos < num_bytes_length:
                if ptr[pos] < b'0' or ptr[pos] > b'9':
                    errors._raise_err(errors.ERR_NUMBER_WITH_INVALID_EXPONENT)
                pos += 1
            if exponent_pos == pos:
                errors._raise_err(errors.ERR_NUMBER_WITH_EMPTY_EXPONENT)
            exponent = <int16_t> int(ptr[exponent_pos:pos])
            if exponent_is_negative:
                exponent = -exponent
            decimal_point_index += exponent

        # if there is anything left in the string, that indicates an invalid
        # number as well
        if pos < num_bytes_length:
            errors._raise_err(errors.ERR_CONTENT_INVALID_AFTER_NUMBER)

        # skip trailing zeros
        while num_digits > 0 and digits[num_digits - 1] == 0:
            num_digits -= 1

        # value must be less than 1e126 and greater than 1e-129; the number of
        # digits also cannot exceed the maximum precision of Oracle numbers
        if num_digits > NUMBER_MAX_DIGITS or decimal_point_index > 126 \
                or decimal_point_index < -129:
            errors._raise_err(errors.ERR_ORACLE_NUMBER_NO_REPR)

        # if the exponent is odd, prepend a zero
        if decimal_point_index % 2 == 1:
            prepend_zero = True
            if num_digits > 0:
                digits[num_digits] = 0
                num_digits += 1
                decimal_point_index += 1

        # determine the number of digit pairs; if the number of digits is odd,
        # append a zero to make the number of digits even
        if num_digits % 2 == 1:
            digits[num_digits] = 0
            num_digits += 1
        num_pairs = num_digits // 2

        # append a sentinel 102 byte for negative numbers if there is room
        if is_negative and num_digits > 0 and num_digits < NUMBER_MAX_DIGITS:
            append_sentinel = True

        # write length of number
        self.write_uint8(num_pairs + 1 + append_sentinel)

        # if the number of digits is zero, the value is itself zero since all
        # leading and trailing zeros are removed from the digits string; this
        # is a special case
        if num_digits == 0:
            self.write_uint8(128)
            return 0

        # write the exponent
        exponent_on_wire = <int8_t> (decimal_point_index / 2) + 192
        if is_negative:
            exponent_on_wire = ~exponent_on_wire
        self.write_uint8(exponent_on_wire)

        # write the mantissa bytes
        digits_pos = 0
        for pair_num in range(num_pairs):
            if pair_num == 0 and prepend_zero:
                digit = digits[digits_pos]
                digits_pos += 1
            else:
                digit = digits[digits_pos] * 10 + digits[digits_pos + 1]
                digits_pos += 2
            if is_negative:
                digit = 101 - digit
            else:
                digit += 1
            self.write_uint8(digit)

        # append 102 byte for negative numbers if the number of digits is less
        # than the maximum allowable
        if append_sentinel:
            self.write_uint8(102)

    cdef int write_raw(self, const char_type *data, ssize_t length) except -1:
        """
        Writes raw bytes of the specified length to the buffer.
        """
        cdef ssize_t bytes_to_write
        while True:
            bytes_to_write = min(self._max_size - self._pos, length)
            if bytes_to_write > 0:
                memcpy(self._data + self._pos, <void*> data, bytes_to_write)
                self._pos += bytes_to_write
            if bytes_to_write == length:
                break
            self._write_more_data(self._max_size - self._pos, length)
            length -= bytes_to_write
            data += bytes_to_write

    cdef int write_str(self, str value) except -1:
        """
        Writes a string to the buffer as UTF-8 encoded bytes.
        """
        self.write_bytes(value.encode())

    cdef int write_uint8(self, uint8_t value) except -1:
        """
        Writes an 8-bit integer to the buffer.
        """
        if self._pos + 1 > self._max_size:
            self._write_more_data(self._max_size - self._pos, 1)
        self._data[self._pos] = value
        self._pos += 1

    cdef int write_uint16(self, uint16_t value,
                          int byte_order=BYTE_ORDER_MSB) except -1:
        """
        Writes a 16-bit integer to the buffer in native format.
        """
        if self._pos + 2 > self._max_size:
            self._write_more_data(self._max_size - self._pos, 2)
        pack_uint16(&self._data[self._pos], value, byte_order)
        self._pos += 2

    cdef int write_uint32(self, uint32_t value,
                          int byte_order=BYTE_ORDER_MSB) except -1:
        """
        Writes a 32-bit integer to the buffer in native format.
        """
        if self._pos + 4 > self._max_size:
            self._write_more_data(self._max_size - self._pos, 4)
        pack_uint32(&self._data[self._pos], value, byte_order)
        self._pos += 4

    cdef int write_uint64(self, uint64_t value,
                          byte_order=BYTE_ORDER_MSB) except -1:
        """
        Writes a 64-bit integer to the buffer in native format.
        """
        if self._pos + 8 > self._max_size:
            self._write_more_data(self._max_size - self._pos, 8)
        pack_uint64(&self._data[self._pos], value, byte_order)
        self._pos += 8

    cdef int write_ub2(self, uint16_t value) except -1:
        """
        Writes a 16-bit integer to the buffer in universal format.
        """
        if value == 0:
            self.write_uint8(0)
        elif value <= UINT8_MAX:
            self.write_uint8(1)
            self.write_uint8(<uint8_t> value)
        else:
            self.write_uint8(2)
            self.write_uint16(value)

    cdef int write_ub4(self, uint32_t value) except -1:
        """
        Writes a 32-bit integer to the buffer in universal format.
        """
        if value == 0:
            self.write_uint8(0)
        elif value <= UINT8_MAX:
            self.write_uint8(1)
            self.write_uint8(<uint8_t> value)
        elif value <= UINT16_MAX:
            self.write_uint8(2)
            self.write_uint16(<uint16_t> value)
        else:
            self.write_uint8(4)
            self.write_uint32(value)

    cdef int write_ub8(self, uint64_t value) except -1:
        """
        Writes a 64-bit integer to the buffer in universal format.
        """
        if value == 0:
            self.write_uint8(0)
        elif value <= UINT8_MAX:
            self.write_uint8(1)
            self.write_uint8(<uint8_t> value)
        elif value <= UINT16_MAX:
            self.write_uint8(2)
            self.write_uint16(<uint16_t> value)
        elif value <= UINT32_MAX:
            self.write_uint8(4)
            self.write_uint32(<uint32_t> value)
        else:
            self.write_uint8(8)
            self.write_uint64(value)


cdef class GrowableBuffer(Buffer):

    cdef int _reserve_space(self, ssize_t num_bytes) except -1:
        """
        Reserves the requested amount of space in the buffer by moving the
        pointer forward, allocating more space if necessary.
        """
        self._pos += num_bytes
        if self._pos > self._size:
            self._write_more_data(self._size - self._pos + num_bytes,
                                  num_bytes)

    cdef int _write_more_data(self, ssize_t num_bytes_available,
                              ssize_t num_bytes_wanted) except -1:
        """
        Called when the amount of buffer available is less than the amount of
        data requested. The buffer is increased in multiples of TNS_CHUNK_SIZE
        in order to accomodate the number of bytes desired.
        """
        cdef:
            ssize_t num_bytes_needed = num_bytes_wanted - num_bytes_available
            ssize_t new_size
        new_size = (self._max_size + num_bytes_needed + TNS_CHUNK_SIZE - 1) & \
                ~(TNS_CHUNK_SIZE - 1)
        self._resize(new_size)
