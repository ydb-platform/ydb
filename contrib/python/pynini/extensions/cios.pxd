#cython: language_level=3
# Copyright 2016-2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# See www.openfst.org for extensive documentation on this weighted
# finite-state transducer library.


from libcpp.string cimport string

from libc.stdint cimport int8_t
from libc.stdint cimport int16_t
from libc.stdint cimport int32_t
from libc.stdint cimport int64_t
from libc.stdint cimport uint8_t
from libc.stdint cimport uint16_t
from libc.stdint cimport uint32_t
from libc.stdint cimport uint64_t


cdef extern from "<iostream>" namespace "std" nogil:

  cdef cppclass iostream:

    pass

  cdef cppclass istream(iostream):

    pass

  cdef cppclass ostream(iostream):

    pass


# We are ignoring openmodes for the moment.


cdef extern from "<fstream>" namespace "std" nogil:

  cdef cppclass ifstream(istream):

    ifstream(const string &)

  cdef cppclass ofstream(ostream):

    ofstream(const string &)


cdef extern from "<sstream>" namespace "std" nogil:

  cdef cppclass stringstream(istream, ostream):

    stringstream()

    string str()

    stringstream &operator<<(const string &)

    stringstream &operator<<(bool)

    # We define these in terms of the cstdint types.

    stringstream &operator<<(int8_t)

    stringstream &operator<<(uint8_t)

    stringstream &operator<<(int16_t)

    stringstream &operator<<(uint16_t)

    stringstream &operator<<(int32_t)

    stringstream &operator<<(uint32_t)

    stringstream &operator<<(int64_t)

    stringstream &operator<<(uint64_t)

    stringstream &operator<<(double)

    stringstream &operator<<(long double)

