# cython: language_level=3
# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2019 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Provides access to the low-level HDF5 "H5PL" plugins interface.

    These functions are probably not thread safe.
"""

include "config.pxi"
from .utils cimport emalloc, efree

# === C API ===================================================================

cpdef append(const char* search_path):
    """(STRING search_path)

    Add a directory to the end of the plugin search path.
    """
    H5PLappend(search_path)

cpdef prepend(const char* search_path):
    """(STRING search_path)

    Add a directory to the start of the plugin search path.
    """
    H5PLprepend(search_path)

cpdef replace(const char* search_path, unsigned int index):
    """(STRING search_path, UINT index)

    Replace the directory at the given index in the plugin search path.
    """
    H5PLreplace(search_path, index)

cpdef insert(const char* search_path, unsigned int index):
    """(STRING search_path, UINT index)

    Insert a directory at the given index in the plugin search path.
    """
    H5PLinsert(search_path, index)

cpdef remove(unsigned int index):
    """(UINT index)

    Remove the specified entry from the plugin search path.
    """
    H5PLremove(index)

cpdef get(unsigned int index):
    """(UINT index) => STRING

    Get the directory path at the given index (starting from 0) in the
    plugin search path. Returns a Python bytes object.
    """
    cdef size_t n
    cdef char* buf = NULL

    n = H5PLget(index, NULL, 0)
    buf = <char*>emalloc(sizeof(char)*(n + 1))
    try:
        H5PLget(index, buf, n + 1)
        return PyBytes_FromStringAndSize(buf, n)
    finally:
        efree(buf)

cpdef size():
    """() => UINT

    Get the number of directories currently in the plugin search path.
    """
    cdef unsigned int n = 0
    H5PLsize(&n)
    return n
