# cython: language_level=3
## This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from .defs cimport *

from ._objects cimport ObjectID

cdef class DatasetID(ObjectID):
    cdef object _dtype
