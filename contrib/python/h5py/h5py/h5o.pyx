# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Module for HDF5 "H5O" functions.
"""

include 'config.pxi'

# Cython C-level imports
from ._objects cimport ObjectID, pdefault
from .h5g cimport GroupID
from .h5i cimport wrap_identifier
from .h5p cimport PropID
from .utils cimport emalloc, efree
cimport cython
# Python level imports:
from ._objects import phil, with_phil


# === Public constants ========================================================

TYPE_GROUP = H5O_TYPE_GROUP
TYPE_DATASET = H5O_TYPE_DATASET
TYPE_NAMED_DATATYPE = H5O_TYPE_NAMED_DATATYPE

COPY_SHALLOW_HIERARCHY_FLAG = H5O_COPY_SHALLOW_HIERARCHY_FLAG
COPY_EXPAND_SOFT_LINK_FLAG  = H5O_COPY_EXPAND_SOFT_LINK_FLAG
COPY_EXPAND_EXT_LINK_FLAG   = H5O_COPY_EXPAND_EXT_LINK_FLAG
COPY_EXPAND_REFERENCE_FLAG  = H5O_COPY_EXPAND_REFERENCE_FLAG
COPY_WITHOUT_ATTR_FLAG      = H5O_COPY_WITHOUT_ATTR_FLAG
COPY_PRESERVE_NULL_FLAG     = H5O_COPY_PRESERVE_NULL_FLAG

# === Giant H5O_info_t structure ==============================================

cdef class _ObjInfoBase:

    cdef H5O_info_t *istr

cdef class _OHdrMesg(_ObjInfoBase):

    @property
    def present(self):
        return self.istr[0].hdr.mesg.present

    @property
    def shared(self):
        return self.istr[0].hdr.mesg.shared

    def _hash(self):
        return hash((self.present, self.shared))

cdef class _OHdrSpace(_ObjInfoBase):

    @property
    def total(self):
        return self.istr[0].hdr.space.total

    @property
    def meta(self):
        return self.istr[0].hdr.space.meta

    @property
    def mesg(self):
        return self.istr[0].hdr.space.mesg

    @property
    def free(self):
        return self.istr[0].hdr.space.free

    def _hash(self):
        return hash((self.total, self.meta, self.mesg, self.free))

cdef class _OHdr(_ObjInfoBase):

    cdef public _OHdrSpace space
    cdef public _OHdrMesg mesg

    @property
    def version(self):
        return self.istr[0].hdr.version

    @property
    def nmesgs(self):
        return self.istr[0].hdr.nmesgs

    @property
    def nchunks(self):
        return self.istr[0].hdr.nchunks

    @property
    def flags(self):
        return self.istr[0].hdr.flags

    def __init__(self):
        self.space = _OHdrSpace()
        self.mesg = _OHdrMesg()

    def _hash(self):
        return hash((self.version, self.nmesgs, self.nchunks, self.flags, self.space, self.mesg))

cdef class _ObjMetaInfo:

    cdef H5_ih_info_t *istr

    @property
    def index_size(self):
        return self.istr[0].index_size

    @property
    def heap_size(self):
        return self.istr[0].heap_size

    def _hash(self):
        return hash((self.index_size, self.heap_size))

cdef class _OMetaSize(_ObjInfoBase):

    cdef public _ObjMetaInfo obj
    cdef public _ObjMetaInfo attr

    def __init__(self):
        self.obj = _ObjMetaInfo()
        self.attr = _ObjMetaInfo()

    def _hash(self):
        return hash((self.obj, self.attr))

cdef class _ObjInfo(_ObjInfoBase):

    @property
    def fileno(self):
        return self.istr[0].fileno

    @property
    def addr(self):
        return self.istr[0].addr

    @property
    def type(self):
        return <int>self.istr[0].type

    @property
    def rc(self):
        return self.istr[0].rc

    @property
    def atime(self):
        return self.istr[0].atime

    @property
    def mtime(self):
        return self.istr[0].mtime

    @property
    def ctime(self):
        return self.istr[0].ctime

    @property
    def btime(self):
        return self.istr[0].btime

    @property
    def num_attrs(self):
        return self.istr[0].num_attrs

    def _hash(self):
        return hash((self.fileno, self.addr, self.type, self.rc, self.atime, self.mtime, self.ctime, self.btime, self.num_attrs))

cdef class ObjInfo(_ObjInfo):

    """
        Represents the H5O_info_t structure
    """

    cdef H5O_info_t infostruct
    cdef public _OHdr hdr
    cdef public _OMetaSize meta_size

    def __init__(self):
        self.hdr = _OHdr()
        self.meta_size = _OMetaSize()

        self.istr = &self.infostruct
        self.hdr.istr = &self.infostruct
        self.hdr.space.istr = &self.infostruct
        self.hdr.mesg.istr = &self.infostruct
        self.meta_size.istr = &self.infostruct
        self.meta_size.obj.istr = &(self.istr[0].meta_size.obj)
        self.meta_size.attr.istr = &(self.istr[0].meta_size.attr)

    def __copy__(self):
        cdef ObjInfo newcopy
        newcopy = ObjInfo()
        newcopy.infostruct = self.infostruct
        return newcopy

@cython.binding(False)
@with_phil
def get_info(ObjectID loc not None, char* name=NULL, int index=-1, *,
        char* obj_name='.', int index_type=H5_INDEX_NAME, int order=H5_ITER_INC,
        PropID lapl=None):
    """(ObjectID loc, STRING name=, INT index=, **kwds) => ObjInfo

    Get information describing an object in an HDF5 file.  Provide the object
    itself, or the containing group and exactly one of "name" or "index".

    STRING obj_name (".")
        When "index" is specified, look in this subgroup instead.
        Otherwise ignored.

    PropID lapl (None)
        Link access property list

    INT index_type (h5.INDEX_NAME)

    INT order (h5.ITER_INC)
    """
    cdef ObjInfo info
    info = ObjInfo()

    if name != NULL and index >= 0:
        raise TypeError("At most one of name or index may be specified")
    elif name != NULL and index < 0:
        H5Oget_info_by_name(loc.id, name, &info.infostruct, pdefault(lapl))
    elif name == NULL and index >= 0:
        H5Oget_info_by_idx(loc.id, obj_name, <H5_index_t>index_type,
            <H5_iter_order_t>order, index, &info.infostruct, pdefault(lapl))
    else:
        H5Oget_info(loc.id, &info.infostruct)

    return info

@with_phil
def exists_by_name(ObjectID loc not None, char *name, PropID lapl=None):
    """ (ObjectID loc, STRING name, PropID lapl=None) => BOOL exists

    Determines whether a link resolves to an actual object.
    """
    return <bint>H5Oexists_by_name(loc.id, name, pdefault(lapl))


# === General object operations ===============================================

@with_phil
def open(ObjectID loc not None, char* name, PropID lapl=None):
    """(ObjectID loc, STRING name, PropID lapl=None) => ObjectID

    Open a group, dataset, or named datatype attached to an existing group.
    """
    return wrap_identifier(H5Oopen(loc.id, name, pdefault(lapl)))


@with_phil
def link(ObjectID obj not None, GroupID loc not None, char* name,
    PropID lcpl=None, PropID lapl=None):
    """(ObjectID obj, GroupID loc, STRING name, PropID lcpl=None,
    PropID lapl=None)

    Create a new hard link to an object.  Useful for objects created with
    h5g.create_anon() or h5d.create_anon().
    """
    H5Olink(obj.id, loc.id, name, pdefault(lcpl), pdefault(lapl))


@with_phil
def copy(ObjectID src_loc not None, char* src_name, GroupID dst_loc not None,
    char* dst_name, PropID copypl=None, PropID lcpl=None):
    """(ObjectID src_loc, STRING src_name, GroupID dst_loc, STRING dst_name,
    PropID copypl=None, PropID lcpl=None)

    Copy a group, dataset or named datatype from one location to another.  The
    source and destination need not be in the same file.

    The default behavior is a recursive copy of the object and all objects
    below it.  This behavior is modified via the "copypl" property list.
    """
    H5Ocopy(src_loc.id, src_name, dst_loc.id, dst_name, pdefault(copypl),
        pdefault(lcpl))


@with_phil
def set_comment(ObjectID loc not None, char* comment, *, char* obj_name=".",
    PropID lapl=None):
    """(ObjectID loc, STRING comment, **kwds)

    Set the comment for any-file resident object.  Keywords:

    STRING obj_name (".")
        Set comment on this group member instead

    PropID lapl (None)
        Link access property list
    """
    H5Oset_comment_by_name(loc.id, obj_name, comment, pdefault(lapl))


@with_phil
def get_comment(ObjectID loc not None, char* comment, *, char* obj_name=".",
    PropID lapl=None):
    """(ObjectID loc, STRING comment, **kwds)

    Get the comment for any-file resident object.  Keywords:

    STRING obj_name (".")
        Set comment on this group member instead

    PropID lapl (None)
        Link access property list
    """
    cdef ssize_t size
    cdef char* buf

    size = H5Oget_comment_by_name(loc.id, obj_name, NULL, 0, pdefault(lapl))
    buf = <char*>emalloc(size+1)
    try:
        H5Oget_comment_by_name(loc.id, obj_name, buf, size+1, pdefault(lapl))
        pstring = buf
    finally:
        efree(buf)

    return pstring


# === Visit routines ==========================================================

cdef class _ObjectVisitor:

    cdef object func
    cdef object retval
    cdef ObjInfo objinfo

    def __init__(self, func):
        self.func = func
        self.retval = None
        self.objinfo = ObjInfo()

cdef herr_t cb_obj_iterate(hid_t obj, const char* name, const H5O_info_t *info, void* data) except -1 with gil:

    cdef _ObjectVisitor visit

    # HDF5 doesn't respect callback return for ".", so skip it
    if strcmp(name, ".") == 0:
        return 0

    visit = <_ObjectVisitor>data
    visit.objinfo.infostruct = info[0]
    visit.retval = visit.func(name, visit.objinfo)

    if visit.retval is not None:
        return 1
    return 0

cdef herr_t cb_obj_simple(hid_t obj, const char* name, const H5O_info_t *info, void* data) except -1 with gil:

    cdef _ObjectVisitor visit

    # Not all versions of HDF5 respect callback value for ".", so skip it
    if strcmp(name, ".") == 0:
        return 0

    visit = <_ObjectVisitor>data
    visit.retval = visit.func(name)

    if visit.retval is not None:
        return 1
    return 0


@with_phil
def visit(ObjectID loc not None, object func, *,
          int idx_type=H5_INDEX_NAME, int order=H5_ITER_INC,
          char* obj_name=".", PropID lapl=None, bint info=0):
    """(ObjectID loc, CALLABLE func, **kwds) => <Return value from func>

    Iterate a function or callable object over all objects below the
    specified one.  Your callable should conform to the signature::

        func(STRING name) => Result

    or if the keyword argument "info" is True::

        func(STRING name, ObjInfo info) => Result

    Returning None continues iteration; returning anything else aborts
    iteration and returns that value.  Keywords:

    BOOL info (False)
        Callback is func(STRING, Objinfo)

    STRING obj_name (".")
        Visit a subgroup of "loc" instead

    PropLAID lapl (None)
        Control how "obj_name" is interpreted

    INT idx_type (h5.INDEX_NAME)
        What indexing strategy to use

    INT order (h5.ITER_INC)
        Order in which iteration occurs

    Compatibility note:  No callback is executed for the starting path ("."),
    as some versions of HDF5 don't correctly handle a return value for this
    case.  This differs from the behavior of the native H5Ovisit, which
    provides a literal "." as the first value.
    """
    cdef _ObjectVisitor visit = _ObjectVisitor(func)
    cdef H5O_iterate_t cfunc

    if info:
        cfunc = cb_obj_iterate
    else:
        cfunc = cb_obj_simple

    H5Ovisit_by_name(loc.id, obj_name, <H5_index_t>idx_type,
        <H5_iter_order_t>order, cfunc, <void*>visit, pdefault(lapl))

    return visit.retval
