# cython: language_level=3
# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    H5R API for object and region references.
"""

# Cython C-level imports
from ._objects cimport ObjectID, pdefault
from .h5p cimport PropID

#Python level imports
from ._objects import phil, with_phil


# === Public constants and data structures ====================================

OBJECT = H5R_OBJECT
DATASET_REGION = H5R_DATASET_REGION


# === Reference API ===========================================================

@with_phil
def create(ObjectID loc not None, char* name, int ref_type, ObjectID space=None):
    """(ObjectID loc, STRING name, INT ref_type, SpaceID space=None)
    => ReferenceObject ref

    Create a new reference. The value of ref_type determines the kind
    of reference created:

    OBJECT
        Reference to an object in an HDF5 file.  Parameters "loc"
        and "name" identify the object; "space" is unused.

    DATASET_REGION
        Reference to a dataset region.  Parameters "loc" and
        "name" identify the dataset; the selection on "space"
        identifies the region.
    """
    cdef hid_t space_id
    cdef Reference ref
    if ref_type == H5R_OBJECT:
        ref = Reference()
    elif ref_type == H5R_DATASET_REGION:
        if space is None:   # work around segfault in HDF5
            raise ValueError("Dataspace required for region reference")
        ref = RegionReference()
    else:
        raise ValueError("Unknown reference typecode")

    if space is None:
        space_id = -1
    else:
        space_id = space.id

    H5Rcreate(&ref.ref, loc.id, name, <H5R_type_t>ref_type, space_id)

    return ref


@with_phil
def dereference(Reference ref not None, ObjectID id not None, PropID oapl=None):
    """(Reference ref, ObjectID id) => ObjectID or None

    Open the object pointed to by the reference and return its
    identifier.  The file identifier (or the identifier for any object
    in the file) must also be provided.  Returns None if the reference
    is zero-filled.

    The reference may be either Reference or RegionReference.
    """
    from . import h5i
    if not ref:
        return None
    return h5i.wrap_identifier(H5Rdereference(
        id.id, pdefault(oapl), <H5R_type_t>ref.typecode, &ref.ref
    ))


@with_phil
def get_region(RegionReference ref not None, ObjectID id not None):
    """(Reference ref, ObjectID id) => SpaceID or None

    Retrieve the dataspace selection pointed to by the reference.
    Returns a copy of the dataset's dataspace, with the appropriate
    elements selected.  The file identifier or the identifier of any
    object in the file (including the dataset itself) must also be
    provided.

    The reference object must be a RegionReference.  If it is zero-filled,
    returns None.
    """
    from . import h5s
    if ref.typecode != H5R_DATASET_REGION or not ref:
        return None
    return h5s.SpaceID(H5Rget_region(id.id, <H5R_type_t>ref.typecode, &ref.ref))


@with_phil
def get_obj_type(Reference ref not None, ObjectID id not None):
    """(Reference ref, ObjectID id) => INT obj_code or None

    Determine what type of object the reference points to.  The
    reference may be a Reference or RegionReference.  The file
    identifier or the identifier of any object in the file must also
    be provided.

    The return value is one of:

    - h5o.TYPE_GROUP
    - h5o.TYPE_DATASET
    - h5o.TYPE_NAMED_DATATYPE

    If the reference is zero-filled, returns None.
    """
    cdef H5O_type_t obj_type
    if not ref:
        return None
    H5Rget_obj_type(id.id, <H5R_type_t>ref.typecode, &ref.ref, &obj_type)
    return <int>obj_type


@with_phil
def get_name(Reference ref not None, ObjectID loc not None):
    """(Reference ref, ObjectID loc) => STRING name

    Determine the name of the object pointed to by this reference.
    """
    cdef ssize_t namesize = 0
    cdef char* namebuf = NULL

    namesize = H5Rget_name(loc.id, <H5R_type_t>ref.typecode, &ref.ref, NULL, 0)
    if namesize > 0:
        namebuf = <char*>malloc(namesize+1)
        try:
            namesize = H5Rget_name(loc.id, <H5R_type_t>ref.typecode, &ref.ref, namebuf, namesize+1)
            return namebuf
        finally:
            free(namebuf)


cdef class Reference:

    """
        Opaque representation of an HDF5 reference.

        Objects of this class are created exclusively by the library and
        cannot be modified.  The read-only attribute "typecode" determines
        whether the reference is to an object in an HDF5 file (OBJECT)
        or a dataset region (DATASET_REGION).

        The object's truth value indicates whether it contains a nonzero
        reference.  This does not guarantee that is valid, but is useful
        for rejecting "background" elements in a dataset.

        Defined attributes:
          cdef ref_u ref
          cdef readonly int typecode
          cdef readonly size_t typesize
    """

    def __cinit__(self, *args, **kwds):
        self.typecode = H5R_OBJECT
        self.typesize = sizeof(hobj_ref_t)

    def __nonzero__(self):
        cdef int i
        for i in range(self.typesize):
            if (<unsigned char*>&self.ref)[i] != 0: return True
        return False

    def __repr__(self):
        return "<HDF5 object reference%s>" % ("" if self else " (null)")

cdef class RegionReference(Reference):

    """
        Opaque representation of an HDF5 region reference.

        This is a subclass of Reference which exists mainly for programming
        convenience.
    """

    def __cinit__(self, *args, **kwds):
        self.typecode = H5R_DATASET_REGION
        self.typesize = sizeof(hdset_reg_ref_t)

    def __repr__(self):
        return "<HDF5 region reference%s>" % ("" if self else " (null")
