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
# Cython file defining the StringBuffer class, used for transforming Python
# "str" and "bytes" objects into the C buffer representation required by ODPI-C
# (embedded in thick_impl.pyx).
#------------------------------------------------------------------------------

@cython.freelist(20)
cdef class StringBuffer:
    cdef:
        bytes obj
        char *ptr
        uint32_t length
        uint32_t size_in_chars

    cdef int set_value(self, value) except -1:
        if value is None:
            self.obj = None
            self.ptr = NULL
            self.length = self.size_in_chars = 0
            return 0
        elif isinstance(value, str):
            self.obj = (<str> value).encode()
            self.size_in_chars = <uint32_t> len(<str> value)
        elif isinstance(value, bytes):
            self.obj = <bytes> value
            self.size_in_chars = <uint32_t> len(<bytes> value)
        else:
            raise TypeError("expecting string or bytes")
        self.ptr = self.obj
        self.length = <uint32_t> len(self.obj)
