# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

# === Standard C library types and functions ==================================

include 'config.pxi'

IF MPI:
    from mpi4py.libmpi cimport MPI_Comm, MPI_Info

from libc.stdlib cimport malloc, free
from libc.string cimport strlen, strchr, strcpy, strncpy, strcmp,\
                         strdup, strdup, memcpy, memset
ctypedef long size_t
from libc.time cimport time_t

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, int32_t, uint32_t, int64_t, uint64_t

cdef extern from *:
    """
    #if !(defined(_WIN32) || defined(MS_WINDOWS) || defined(_MSC_VER))
      #include <unistd.h>
    #endif
    """
    ctypedef long ssize_t

# Can't use Cython defs because they keep moving them around
cdef extern from "Python.h":
    ctypedef void PyObject
    ctypedef ssize_t Py_ssize_t
    ctypedef size_t Py_uintptr_t

    PyObject* PyErr_Occurred()
    void PyErr_SetString(object type, char *message)
    object PyBytes_FromStringAndSize(char *v, Py_ssize_t len)

# === Compatibility definitions and macros for h5py ===========================

cdef extern from "api_compat.h":

    size_t h5py_size_n64
    size_t h5py_size_n128
    size_t h5py_offset_n64_real
    size_t h5py_offset_n64_imag
    size_t h5py_offset_n128_real
    size_t h5py_offset_n128_imag

    IF COMPLEX256_SUPPORT:
        size_t h5py_size_n256
        size_t h5py_offset_n256_real
        size_t h5py_offset_n256_imag

cdef extern from "lzf_filter.h":

    int H5PY_FILTER_LZF
    int register_lzf() except *
