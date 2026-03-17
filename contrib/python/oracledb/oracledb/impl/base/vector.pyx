#------------------------------------------------------------------------------
# Copyright (c) 2023, 2024, Oracle and/or its affiliates.
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
# vector.pyx
#
# Cython file defining the classes and methods used for encoding and decoding
# VECTOR data (embedded in base_impl.pyx).
#------------------------------------------------------------------------------

cdef array.array float_template = array.array('f')
cdef array.array double_template = array.array('d')
cdef array.array int8_template = array.array('b')
cdef array.array uint8_template = array.array('B')

@cython.final
cdef class VectorDecoder(Buffer):

    cdef object decode(self, bytes data):
        """
        Returns a Python object corresponding to the encoded VECTOR bytes.
        """
        cdef:
            uint8_t magic_byte, version, vector_format, element_size = 0
            uint8_t * uint8_buf = NULL
            double *double_buf = NULL
            int8_t *int8_buf = NULL
            uint32_t num_elements, i
            float *float_buf = NULL
            const char_type* ptr
            array.array result
            uint16_t flags
            object value

        # populate the buffer with the data
        self._populate_from_bytes(data)

        # parse header
        self.read_ub1(&magic_byte)
        if magic_byte != TNS_VECTOR_MAGIC_BYTE:
            errors._raise_err(errors.ERR_UNEXPECTED_DATA,
                              data=bytes([magic_byte]))
        self.read_ub1(&version)
        if version > TNS_VECTOR_VERSION_WITH_BINARY:
            errors._raise_err(errors.ERR_VECTOR_VERSION_NOT_SUPPORTED,
                              version=version)
        self.read_uint16(&flags)
        self.read_ub1(&vector_format)
        self.read_uint32(&num_elements)
        if vector_format == VECTOR_FORMAT_FLOAT32:
            result = array.clone(float_template, num_elements, False)
            float_buf = result.data.as_floats
            element_size = 4
        elif vector_format == VECTOR_FORMAT_FLOAT64:
            result = array.clone(double_template, num_elements, False)
            double_buf = result.data.as_doubles
            element_size = 8
        elif vector_format == VECTOR_FORMAT_INT8:
            result = array.clone(int8_template, num_elements, False)
            int8_buf = result.data.as_schars
            element_size = 1
        elif vector_format == VECTOR_FORMAT_BINARY:
            num_elements = num_elements // 8
            result = array.clone(uint8_template, num_elements, False)
            uint8_buf = result.data.as_uchars
        else:
            errors._raise_err(errors.ERR_VECTOR_FORMAT_NOT_SUPPORTED,
                              vector_format=vector_format)
        if flags & TNS_VECTOR_FLAG_NORM_RESERVED \
                or flags & TNS_VECTOR_FLAG_NORM:
            self.skip_raw_bytes(8)

        # parse data
        for i in range(num_elements):
            if vector_format == VECTOR_FORMAT_FLOAT32:
                ptr = self._get_raw(element_size)
                self.parse_binary_float(ptr, &float_buf[i])
            elif vector_format == VECTOR_FORMAT_FLOAT64:
                ptr = self._get_raw(element_size)
                self.parse_binary_double(ptr, &double_buf[i])
            elif vector_format == VECTOR_FORMAT_INT8:
                self.read_sb1(&int8_buf[i])
            else:
                self.read_ub1(&uint8_buf[i])
        return result


@cython.final
cdef class VectorEncoder(GrowableBuffer):

    cdef int encode(self, array.array value) except -1:
        """
        Encodes the given value to the internal VECTOR format.
        """
        cdef:
            uint16_t flags = TNS_VECTOR_FLAG_NORM_RESERVED
            uint8_t vector_format, vector_version
            double *double_ptr = NULL
            uint8_t *uint8_ptr = NULL
            uint32_t num_elements, i
            float *float_ptr = NULL
            int8_t *int8_ptr = NULL
            object element

        # determine the type of vector to write
        if value.typecode == 'd':
            vector_format = VECTOR_FORMAT_FLOAT64
            double_ptr = value.data.as_doubles
        elif value.typecode == 'f':
            vector_format = VECTOR_FORMAT_FLOAT32
            float_ptr = value.data.as_floats
        elif value.typecode == 'b':
            vector_format = VECTOR_FORMAT_INT8
            int8_ptr = value.data.as_schars
        else:
            vector_format = VECTOR_FORMAT_BINARY
            uint8_ptr = value.data.as_uchars

        # determine vector version and number of elements
        if vector_format == VECTOR_FORMAT_BINARY:
            num_elements = (<uint32_t> len(value)) * 8
            vector_version = TNS_VECTOR_VERSION_WITH_BINARY
        else:
            num_elements = <uint32_t> len(value)
            vector_version = TNS_VECTOR_VERSION_BASE
            flags |= TNS_VECTOR_FLAG_NORM

        # write header
        self.write_uint8(TNS_VECTOR_MAGIC_BYTE)
        self.write_uint8(vector_version)
        self.write_uint16(flags)
        self.write_uint8(vector_format)
        self.write_uint32(num_elements)
        self._reserve_space(8)              # reserve space for norm

        # write elements
        if vector_format == VECTOR_FORMAT_INT8:
            self.write_raw(<char_type*> int8_ptr, num_elements)
        elif vector_format == VECTOR_FORMAT_BINARY:
            self.write_raw(<char_type*> uint8_ptr, num_elements // 8)
        else:
            for i in range(num_elements):
                if vector_format == VECTOR_FORMAT_FLOAT32:
                    self.write_binary_float(float_ptr[i], write_length=False)
                elif vector_format == VECTOR_FORMAT_FLOAT64:
                    self.write_binary_double(double_ptr[i], write_length=False)
