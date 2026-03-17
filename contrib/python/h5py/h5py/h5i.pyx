# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

include "config.pxi"

"""
    Identifier interface for object inspection.
"""

from ._objects import phil, with_phil


# === Public constants and data structures ====================================

INVALID_HID = H5I_INVALID_HID
BADID       = H5I_BADID
FILE        = H5I_FILE
GROUP       = H5I_GROUP
DATASPACE   = H5I_DATASPACE
DATASET     = H5I_DATASET
ATTR        = H5I_ATTR
IF HDF5_VERSION < VOL_MIN_HDF5_VERSION:
  REFERENCE   = H5I_REFERENCE
GENPROP_CLS = H5I_GENPROP_CLS
GENPROP_LST = H5I_GENPROP_LST
DATATYPE    = H5I_DATATYPE

cpdef ObjectID wrap_identifier(hid_t ident):

    cdef H5I_type_t typecode
    cdef ObjectID obj

    typecode = H5Iget_type(ident)
    if typecode == H5I_FILE:
        from . import h5f
        obj = h5f.FileID(ident)
    elif typecode == H5I_DATASET:
        from . import h5d
        obj = h5d.DatasetID(ident)
    elif typecode == H5I_GROUP:
        from . import h5g
        obj = h5g.GroupID(ident)
    elif typecode == H5I_ATTR:
        from . import h5a
        obj = h5a.AttrID(ident)
    elif typecode == H5I_DATATYPE:
        from . import h5t
        obj = h5t.typewrap(ident)
    elif typecode == H5I_GENPROP_LST:
        from . import h5p
        obj = h5p.propwrap(ident)
    else:
        raise ValueError("Unrecognized type code %d" % typecode)

    return obj


# === Identifier API ==========================================================

@with_phil
def get_type(ObjectID obj not None):
    """ (ObjectID obj) => INT type_code

        Determine the HDF5 typecode of an arbitrary HDF5 object.  The return
        value is always one of the type constants defined in this module; if
        the ID is invalid, BADID is returned.
    """
    return <int>H5Iget_type(obj.id)


@with_phil
def get_name(ObjectID obj not None):
    """ (ObjectID obj) => STRING name, or None

        Determine (a) name of an HDF5 object.  Because an object has as many
        names as there are hard links to it, this may not be unique.

        If the identifier is invalid or is not associated with a name
        (in the case of transient datatypes, dataspaces, etc), returns None.

        For some reason, this does not work on dereferenced objects.
    """
    cdef int namelen
    cdef char* name

    try:
        namelen = <int>H5Iget_name(obj.id, NULL, 0)
    except Exception:
        return None

    if namelen == 0:    # 1.6.5 doesn't raise an exception
        return None

    assert namelen > 0
    name = <char*>malloc(sizeof(char)*(namelen+1))
    try:
        H5Iget_name(obj.id, name, namelen+1)
        pystring = name
        return pystring
    finally:
        free(name)


@with_phil
def get_file_id(ObjectID obj not None):
    """ (ObjectID obj) => FileID

        Obtain an identifier for the file in which this object resides.
    """
    from . import h5f
    cdef hid_t fid
    fid = H5Iget_file_id(obj.id)
    return h5f.FileID(fid)


@with_phil
def inc_ref(ObjectID obj not None):
    """ (ObjectID obj)

        Increment the reference count for the given object.

        This function is provided for debugging only.  Reference counting
        is automatically synchronized with Python, and you can easily break
        ObjectID instances by abusing this function.
    """
    H5Iinc_ref(obj.id)


@with_phil
def get_ref(ObjectID obj not None):
    """ (ObjectID obj) => INT

        Retrieve the reference count for the given object.
    """
    return H5Iget_ref(obj.id)


@with_phil
def dec_ref(ObjectID obj not None):
    """ (ObjectID obj)

        Decrement the reference count for the given object.

        This function is provided for debugging only.  Reference counting
        is automatically synchronized with Python, and you can easily break
        ObjectID instances by abusing this function.
    """
    H5Idec_ref(obj.id)
