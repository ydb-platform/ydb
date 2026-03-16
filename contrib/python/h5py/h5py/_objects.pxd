# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from .defs cimport *

cdef class ObjectID:

    cdef object __weakref__
    cdef readonly hid_t id
    cdef public int locked              # Cannot be closed, explicitly or auto
    cdef object _hash
    cdef size_t _pyid

# Convenience functions
cdef hid_t pdefault(ObjectID pid)
cdef int is_h5py_obj_valid(ObjectID obj)

# Inheritance scheme (for top-level cimport and import statements):
#
# _objects, _proxy, h5fd, h5z
# h5i, h5r, utils
# _conv, h5t, h5s
# h5p
# h5d, h5a, h5f, h5g
# h5l
