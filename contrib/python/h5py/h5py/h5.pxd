# cython: language_level=3
# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from .defs cimport *

cdef class H5PYConfig:

    cdef readonly object _r_name
    cdef readonly object _i_name
    cdef readonly object _f_name
    cdef readonly object _t_name
    cdef readonly object API_16
    cdef readonly object API_18
    cdef readonly object _bytestrings
    cdef readonly object _track_order
    cdef readonly object _default_file_mode

cpdef H5PYConfig get_config()
