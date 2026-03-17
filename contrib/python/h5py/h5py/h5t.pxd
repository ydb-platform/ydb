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

from ._objects cimport ObjectID

cdef class TypeID(ObjectID):

    cdef object py_dtype(self)

# --- Top-level classes ---

cdef class TypeArrayID(TypeID):
    pass

cdef class TypeOpaqueID(TypeID):
    pass

cdef class TypeStringID(TypeID):
    # Both vlen and fixed-len strings
    pass

cdef class TypeVlenID(TypeID):
    # Non-string vlens
    pass

cdef class TypeTimeID(TypeID):
    pass

cdef class TypeBitfieldID(TypeID):
    pass

cdef class TypeReferenceID(TypeID):
    pass

# --- Numeric atomic types ---

cdef class TypeAtomicID(TypeID):
    pass

cdef class TypeIntegerID(TypeAtomicID):
    pass

cdef class TypeFloatID(TypeAtomicID):
    pass

# --- Enums & compound types ---

cdef class TypeCompositeID(TypeID):
    pass

cdef class TypeEnumID(TypeCompositeID):

    cdef int enum_convert(self, long long *buf, int reverse) except -1

cdef class TypeCompoundID(TypeCompositeID):
    pass

# === C API for other modules =================================================

cpdef TypeID typewrap(hid_t id_)
cdef hid_t H5PY_OBJ
cdef char* H5PY_PYTHON_OPAQUE_TAG
cpdef TypeID py_create(object dtype, bint logical=*, bint aligned=*)
