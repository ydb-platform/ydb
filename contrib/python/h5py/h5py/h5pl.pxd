# cython: language_level=3
# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2019 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

include "config.pxi"
from .defs cimport *

cpdef append(const char* search_path)
cpdef prepend(const char* search_path)
cpdef replace(const char* search_path, unsigned int index)
cpdef insert(const char* search_path, unsigned int index)
cpdef remove(unsigned int index)
cpdef get(unsigned int index)
cpdef size()
