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
    Provides access to the low-level HDF5 "H5A" attribute interface.
"""

# C-level imports
from ._objects cimport pdefault
from .h5t cimport TypeID, typewrap, py_create
from .h5s cimport SpaceID
from .h5p cimport PropID
from numpy cimport import_array, ndarray, PyArray_DATA
from .utils cimport check_numpy_read, check_numpy_write, emalloc, efree
from ._proxy cimport attr_rw

cimport cython
#Python level imports
from ._objects import phil, with_phil

# Initialization
import_array()

# === General attribute operations ============================================

# --- create, create_by_name ---

@cython.binding(False)
@with_phil
def create(ObjectID loc not None, char* name, TypeID tid not None,
    SpaceID space not None, *, char* obj_name='.', PropID lapl=None):
    """(ObjectID loc, STRING name, TypeID tid, SpaceID space, **kwds) => AttrID

    Create a new attribute, attached to an existing object.

    STRING obj_name (".")
        Attach attribute to this group member instead

    PropID lapl
        Link access property list for obj_name
    """

    return AttrID(H5Acreate_by_name(loc.id, obj_name, name, tid.id,
            space.id, H5P_DEFAULT, H5P_DEFAULT, pdefault(lapl)))


# --- open, open_by_name, open_by_idx ---
@cython.binding(False)
@with_phil
def open(ObjectID loc not None, char* name=NULL, int index=-1, *,
    char* obj_name='.', int index_type=H5_INDEX_NAME, int order=H5_ITER_INC,
    PropID lapl=None):
    """(ObjectID loc, STRING name=, INT index=, **kwds) => AttrID

    Open an attribute attached to an existing object.  You must specify
    exactly one of either name or idx.  Keywords are:

    STRING obj_name (".")
        Attribute is attached to this group member

    PropID lapl (None)
        Link access property list for obj_name

    INT index_type (h5.INDEX_NAME)

    INT order (h5.ITER_INC)

    """
    if (name == NULL and index < 0) or (name != NULL and index >= 0):
        raise TypeError("Exactly one of name or idx must be specified")

    if name != NULL:
        return AttrID(H5Aopen_by_name(loc.id, obj_name, name,
                        H5P_DEFAULT, pdefault(lapl)))
    else:
        return AttrID(H5Aopen_by_idx(loc.id, obj_name,
            <H5_index_t>index_type, <H5_iter_order_t>order, index,
            H5P_DEFAULT, pdefault(lapl)))


# --- exists, exists_by_name ---

@with_phil
def exists(ObjectID loc not None, char* name, *,
            char* obj_name=".", PropID lapl=None):
    """(ObjectID loc, STRING name, **kwds) => BOOL

    Determine if an attribute is attached to this object.  Keywords:

    STRING obj_name (".")
        Look for attributes attached to this group member

    PropID lapl (None):
        Link access property list for obj_name
    """
    return <bint>H5Aexists_by_name(loc.id, obj_name, name, pdefault(lapl))


# --- rename, rename_by_name ---

@with_phil
def rename(ObjectID loc not None, char* name, char* new_name, *,
    char* obj_name='.', PropID lapl=None):
    """(ObjectID loc, STRING name, STRING new_name, **kwds)

    Rename an attribute.  Keywords:

    STRING obj_name (".")
        Attribute is attached to this group member

    PropID lapl (None)
        Link access property list for obj_name
    """
    H5Arename_by_name(loc.id, obj_name, name, new_name, pdefault(lapl))


@cython.binding(False)
@with_phil
def delete(ObjectID loc not None, char* name=NULL, int index=-1, *,
    char* obj_name='.', int index_type=H5_INDEX_NAME, int order=H5_ITER_INC,
    PropID lapl=None):
    """(ObjectID loc, STRING name=, INT index=, **kwds)

    Remove an attribute from an object.  Specify exactly one of "name"
    or "index". Keyword-only arguments:

    STRING obj_name (".")
        Attribute is attached to this group member

    PropID lapl (None)
        Link access property list for obj_name

    INT index_type (h5.INDEX_NAME)

    INT order (h5.ITER_INC)
    """
    if name != NULL and index < 0:
        H5Adelete_by_name(loc.id, obj_name, name, pdefault(lapl))
    elif name == NULL and index >= 0:
        H5Adelete_by_idx(loc.id, obj_name, <H5_index_t>index_type,
            <H5_iter_order_t>order, index, pdefault(lapl))
    else:
        raise TypeError("Exactly one of index or name must be specified.")


@with_phil
def get_num_attrs(ObjectID loc not None):
    """(ObjectID loc) => INT

    Determine the number of attributes attached to an HDF5 object.
    """
    return H5Aget_num_attrs(loc.id)


cdef class AttrInfo:

    cdef H5A_info_t info

    @property
    def corder_valid(self):
        """Indicates if the creation order is valid"""
        return <bint>self.info.corder_valid

    @property
    def corder(self):
        """Creation order"""
        return <int>self.info.corder

    @property
    def cset(self):
        """Character set of attribute name (integer typecode from h5t)"""
        return <int>self.info.cset

    @property
    def data_size(self):
        """Size of raw data"""
        return self.info.data_size

    def _hash(self):
        return hash((self.corder_valid, self.corder, self.cset, self.data_size))


@cython.binding(False)
@with_phil
def get_info(ObjectID loc not None, char* name=NULL, int index=-1, *,
            char* obj_name='.', PropID lapl=None,
            int index_type=H5_INDEX_NAME, int order=H5_ITER_INC):
    """(ObjectID loc, STRING name=, INT index=, **kwds) => AttrInfo

    Get information about an attribute, in one of two ways:

    1. If you have the attribute identifier, just pass it in
    2. If you have the parent object, supply it and exactly one of
       either name or index.

    STRING obj_name (".")
        Use this group member instead

    PropID lapl (None)
        Link access property list for obj_name

    INT index_type (h5.INDEX_NAME)
        Which index to use

    INT order (h5.ITER_INC)
        What order the index is in
    """
    cdef AttrInfo info = AttrInfo()

    if name == NULL and index < 0:
        H5Aget_info(loc.id, &info.info)
    elif name != NULL and index >= 0:
        raise TypeError("At most one of name and index may be specified")
    elif name != NULL:
        H5Aget_info_by_name(loc.id, obj_name, name, &info.info, pdefault(lapl))
    elif index >= 0:
        H5Aget_info_by_idx(loc.id, obj_name, <H5_index_t>index_type,
            <H5_iter_order_t>order, index, &info.info, pdefault(lapl))

    return info

# === Iteration routines ======================================================

cdef class _AttrVisitor:
    cdef object func
    cdef object retval
    def __init__(self, func):
        self.func = func
        self.retval = None


cdef herr_t cb_attr_iter(hid_t loc_id, const char* attr_name, const H5A_info_t *ainfo, void* vis_in) except 2 with gil:
    cdef _AttrVisitor vis = <_AttrVisitor>vis_in
    cdef AttrInfo info = AttrInfo()
    info.info = ainfo[0]
    vis.retval = vis.func(attr_name, info)
    if vis.retval is not None:
        return 1
    return 0


cdef herr_t cb_attr_simple(hid_t loc_id, const char* attr_name, const H5A_info_t *ainfo, void* vis_in) except 2 with gil:
    cdef _AttrVisitor vis = <_AttrVisitor>vis_in
    vis.retval = vis.func(attr_name)
    if vis.retval is not None:
        return 1
    return 0


@with_phil
def iterate(ObjectID loc not None, object func, int index=0, *,
    int index_type=H5_INDEX_NAME, int order=H5_ITER_INC, bint info=0):
    """(ObjectID loc, CALLABLE func, INT index=0, **kwds) => <Return value from func>

    Iterate a callable (function, method or callable object) over the
    attributes attached to this object.  You callable should have the
    signature::

        func(STRING name) => Result

    or if the keyword argument "info" is True::

        func(STRING name, AttrInfo info) => Result

    Returning None continues iteration; returning anything else aborts
    iteration and returns that value.  Keywords:

    BOOL info (False)
        Callback is func(STRING name, AttrInfo info), not func(STRING name)

    INT index_type (h5.INDEX_NAME)
        Which index to use

    INT order (h5.ITER_INC)
        Index order to use
    """
    if index < 0:
        raise ValueError("Starting index must be a non-negative integer.")

    cdef hsize_t i = index
    cdef _AttrVisitor vis = _AttrVisitor(func)
    cdef H5A_operator2_t cfunc

    if info:
        cfunc = cb_attr_iter
    else:
        cfunc = cb_attr_simple

    H5Aiterate(loc.id, <H5_index_t>index_type, <H5_iter_order_t>order,
        &i, cfunc, <void*>vis)

    return vis.retval



# === Attribute class & methods ===============================================

cdef class AttrID(ObjectID):

    """
        Logical representation of an HDF5 attribute identifier.

        Objects of this class can be used in any HDF5 function call
        which expects an attribute identifier.  Additionally, all ``H5A*``
        functions which always take an attribute instance as the first
        argument are presented as methods of this class.

        * Hashable: No
        * Equality: Identifier comparison
    """

    @property
    def name(self):
        """The attribute's name"""
        with phil:
            return self.get_name()

    @property
    def shape(self):
        """A Numpy-style shape tuple representing the attribute's dataspace"""
        cdef SpaceID space
        with phil:
            space = self.get_space()
            return space.get_simple_extent_dims()

    @property
    def dtype(self):
        """A Numpy-stype dtype object representing the attribute's datatype"""
        cdef TypeID tid
        with phil:
            tid = self.get_type()
            return tid.py_dtype()


    @with_phil
    def read(self, ndarray arr not None, TypeID mtype=None):
        """(NDARRAY arr, TypeID mtype=None)

        Read the attribute data into the given Numpy array.  Note that the
        Numpy array must have the same shape as the HDF5 attribute, and a
        conversion-compatible datatype.

        The Numpy array must be writable and C-contiguous.  If this is not
        the case, the read will fail with an exception.

        If provided, the HDF5 TypeID mtype will override the array's dtype.
        """
        cdef hid_t space_id
        space_id = 0

        try:
            space_id = H5Aget_space(self.id)
            check_numpy_write(arr, space_id)

            if mtype is None:
                mtype = py_create(arr.dtype)

            attr_rw(self.id, mtype.id, PyArray_DATA(arr), 1)

        finally:
            if space_id:
                H5Sclose(space_id)


    @with_phil
    def write(self, ndarray arr not None, TypeID mtype=None):
        """(NDARRAY arr)

        Write the contents of a Numpy array to the attribute.  Note that
        the Numpy array must have the same shape as the HDF5 attribute, and
        a conversion-compatible datatype.

        The Numpy array must be C-contiguous.  If this is not the case,
        the write will fail with an exception.
        """
        cdef hid_t space_id
        space_id = 0

        try:
            space_id = H5Aget_space(self.id)
            check_numpy_read(arr, space_id)

            if mtype is None:
                mtype = py_create(arr.dtype)

            attr_rw(self.id, mtype.id, PyArray_DATA(arr), 0)

        finally:
            if space_id:
                H5Sclose(space_id)


    @with_phil
    def get_name(self):
        """() => STRING name

        Determine the name of this attribute.
        """
        cdef int blen
        cdef char* buf
        buf = NULL

        try:
            blen = H5Aget_name(self.id, 0, NULL)
            assert blen >= 0
            buf = <char*>emalloc(sizeof(char)*blen+1)
            blen = H5Aget_name(self.id, blen+1, buf)
            strout = <bytes>buf
        finally:
            efree(buf)

        return strout


    @with_phil
    def get_space(self):
        """() => SpaceID

        Create and return a copy of the attribute's dataspace.
        """
        return SpaceID(H5Aget_space(self.id))


    @with_phil
    def get_type(self):
        """() => TypeID

        Create and return a copy of the attribute's datatype.
        """
        return typewrap(H5Aget_type(self.id))


    @with_phil
    def get_storage_size(self):
        """() => INT

        Get the amount of storage required for this attribute.
        """
        return H5Aget_storage_size(self.id)
