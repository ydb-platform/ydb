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

# --- Base classes ---

cdef class PropID(ObjectID):
    """ Base class for all property lists """
    pass

cdef class PropClassID(PropID):
    """ Represents an HDF5 property list class.  These can be either (locked)
        library-defined classes or user-created classes.
    """
    pass

cdef class PropInstanceID(PropID):
    """ Represents an instance of a property list class (i.e. an actual list
        which can be passed on to other API functions).
    """
    pass

cdef class PropCreateID(PropInstanceID):
    """ Base class for all object creation lists.

        Also includes string character set methods.
    """
    pass

cdef class PropCopyID(PropInstanceID):
    """ Property list for copying objects (as in h5o.copy) """

# --- Object creation ---

cdef class PropOCID(PropCreateID):
    """ Object creation property list """
    pass

cdef class PropDCID(PropOCID):
    """ Dataset creation property list """
    pass

cdef class PropFCID(PropOCID):
    """ File creation property list """
    pass


# --- Object access ---

cdef class PropFAID(PropInstanceID):
    """ File access property list """
    pass

cdef class PropDXID(PropInstanceID):
    """ Dataset transfer property list """
    pass

cdef class PropDAID(PropInstanceID):
    """ Dataset access property list"""
    cdef char* _virtual_prefix_buf
    cdef char* _efile_prefix_buf

cdef class PropLCID(PropCreateID):
    """ Link creation property list """
    pass

cdef class PropLAID(PropInstanceID):
    """ Link access property list """
    cdef char* _buf

cdef class PropGCID(PropOCID):
    """ Group creation property list """
    pass

cdef hid_t pdefault(PropID pid)
cdef object propwrap(hid_t id_in)
