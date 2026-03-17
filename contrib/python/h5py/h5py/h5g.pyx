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
    Low-level HDF5 "H5G" group interface.
"""

include "config.pxi"

import sys

# C-level imports
from ._objects cimport pdefault
from .utils cimport emalloc, efree
from .h5p import CRT_ORDER_TRACKED
from .h5p cimport PropID, PropGCID, propwrap
from . cimport _hdf5 # to implement container testing for 1.6
from ._errors cimport set_error_handler, err_cookie

# Python level imports
from ._objects import phil, with_phil


_IS_WINDOWS = sys.platform.startswith("win")

# === Public constants and data structures ====================================

# Enumerated object types for groups "H5G_obj_t"
UNKNOWN  = H5G_UNKNOWN
LINK     = H5G_LINK
GROUP    = H5G_GROUP
DATASET  = H5G_DATASET
TYPE = H5G_TYPE

# Enumerated link types "H5G_link_t"
LINK_ERROR = H5G_LINK_ERROR
LINK_HARD  = H5G_LINK_HARD
LINK_SOFT  = H5G_LINK_SOFT

cdef class GroupStat:
    """Represents the H5G_stat_t structure containing group member info.

    Fields (read-only):

    * fileno:   2-tuple uniquely identifying the current file
    * objno:    2-tuple uniquely identifying this object
    * nlink:    Number of hard links to this object
    * mtime:    Modification time of this object
    * linklen:  Length of the symbolic link name, or 0 if not a link.

    "Uniquely identifying" means unique among currently open files,
    not universally unique.

    * Hashable: Yes
    * Equality: Yes
    """
    cdef H5G_stat_t infostruct

    @property
    def fileno(self):
        return (self.infostruct.fileno[0], self.infostruct.fileno[1])

    @property
    def objno(self):
        return (self.infostruct.objno[0], self.infostruct.objno[1])

    @property
    def nlink(self):
        return self.infostruct.nlink

    @property
    def type(self):
        return self.infostruct.type

    @property
    def mtime(self):
        return self.infostruct.mtime

    @property
    def linklen(self):
        return self.infostruct.linklen

    def _hash(self):
        return hash((self.fileno, self.objno, self.nlink, self.type, self.mtime, self.linklen))


cdef class GroupIter:

    """
        Iterator over the names of group members.  After this iterator is
        exhausted, it releases its reference to the group ID.
    """

    cdef unsigned long idx
    cdef unsigned long nobjs
    cdef GroupID grp
    cdef list names
    cdef bint reversed


    def __init__(self, GroupID grp not None, bint reversed=False):

        self.idx = 0
        self.grp = grp
        self.nobjs = grp.get_num_objs()
        self.names = []
        self.reversed = reversed


    def __iter__(self):

        return self


    def __next__(self):

        if self.idx == self.nobjs:
            self.grp = None
            self.names = None
            raise StopIteration

        if self.idx == 0:
            cpl = self.grp.get_create_plist()
            crt_order = cpl.get_link_creation_order()
            cpl.close()
            if crt_order & CRT_ORDER_TRACKED:
                idx_type = H5_INDEX_CRT_ORDER
            else:
                idx_type = H5_INDEX_NAME

            self.grp.links.iterate(self.names.append,
                                   idx_type=idx_type)
            if self.reversed:
                self.names.reverse()

        retval = self.names[self.idx]
        self.idx += 1

        return retval


# === Basic group management ==================================================

@with_phil
def open(ObjectID loc not None, char* name):
    """(ObjectID loc, STRING name) => GroupID

    Open an existing HDF5 group, attached to some other group.
    """
    return GroupID(H5Gopen(loc.id, name, H5P_DEFAULT))


@with_phil
def create(ObjectID loc not None, object name, PropID lcpl=None,
           PropID gcpl=None):
    """(ObjectID loc, STRING name or None, PropLCID lcpl=None,
        PropGCID gcpl=None)
    => GroupID

    Create a new group, under a given parent group.  If name is None,
    an anonymous group will be created in the file.
    """
    cdef hid_t gid
    cdef char* cname = NULL
    if name is not None:
        cname = name

    if cname != NULL:
        gid = H5Gcreate(loc.id, cname, pdefault(lcpl), pdefault(gcpl), H5P_DEFAULT)
    else:
        gid = H5Gcreate_anon(loc.id, pdefault(gcpl), H5P_DEFAULT)

    return GroupID(gid)


cdef class _GroupVisitor:

    cdef object func
    cdef object retval

    def __init__(self, func):
        self.func = func
        self.retval = None

cdef herr_t cb_group_iter(hid_t gid, char *name, void* vis_in) except 2:

    cdef _GroupVisitor vis = <_GroupVisitor>vis_in

    vis.retval = vis.func(name)

    if vis.retval is not None:
        return 1
    return 0


@with_phil
def iterate(GroupID loc not None, object func, int startidx=0, *,
            char* obj_name='.'):
    """ (GroupID loc, CALLABLE func, UINT startidx=0, **kwds)
    => Return value from func

    Iterate a callable (function, method or callable object) over the
    members of a group.  Your callable should have the signature::

        func(STRING name) => Result

    Returning None continues iteration; returning anything else aborts
    iteration and returns that value. Keywords:

    STRING obj_name (".")
        Iterate over this subgroup instead
    """
    if startidx < 0:
        raise ValueError("Starting index must be non-negative")

    cdef int i = startidx
    cdef _GroupVisitor vis = _GroupVisitor(func)

    H5Giterate(loc.id, obj_name, &i, <H5G_iterate_t>cb_group_iter, <void*>vis)

    return vis.retval


@with_phil
def get_objinfo(ObjectID obj not None, object name=b'.', int follow_link=1):
    """(ObjectID obj, STRING name='.', BOOL follow_link=True) => GroupStat object

    Obtain information about a named object.  If "name" is provided,
    "obj" is taken to be a GroupID object containing the target.
    The return value is a GroupStat object; see that class's docstring
    for a description of its attributes.

    If follow_link is True (default) and the object is a symbolic link,
    the information returned describes its target.  Otherwise the
    information describes the link itself.
    """
    cdef GroupStat statobj
    statobj = GroupStat()
    cdef char* _name
    _name = name

    H5Gget_objinfo(obj.id, _name, follow_link, &statobj.infostruct)

    return statobj


# === Group member management =================================================

cdef class GroupID(ObjectID):

    """
        Represents an HDF5 group identifier

        Python extensions:

        __contains__
            Test for group member ("if name in grpid")

        __iter__
            Get an iterator over member names

        __len__
            Number of members in this group; len(grpid) = N

        The attribute "links" contains a proxy object
        providing access to the H5L family of routines.  See the docs
        for h5py.h5l.LinkProxy for more information.

        * Hashable: Yes, unless anonymous
        * Equality: True HDF5 identity unless anonymous
    """

    def __init__(self, hid_t id_):
        with phil:
            from . import h5l
            self.links = h5l.LinkProxy(id_)


    @with_phil
    def link(self, char* current_name, char* new_name,
             int link_type=H5G_LINK_HARD, GroupID remote=None):
        """(STRING current_name, STRING new_name, INT link_type=LINK_HARD,
        GroupID remote=None)

        Create a new hard or soft link.  current_name identifies
        the link target (object the link will point to).  The new link is
        identified by new_name and (optionally) another group "remote".

        Link types are:

        LINK_HARD
            Hard link to existing object (default)

        LINK_SOFT
            Symbolic link; link target need not exist.
        """
        cdef hid_t remote_id
        if remote is None:
            remote_id = self.id
        else:
            remote_id = remote.id

        H5Glink2(self.id, current_name, <H5G_link_t>link_type, remote_id, new_name)


    @with_phil
    def unlink(self, char* name):
        """(STRING name)

        Remove a link to an object from this group.
        """
        H5Gunlink(self.id, name)


    @with_phil
    def move(self, char* current_name, char* new_name, GroupID remote=None):
        """(STRING current_name, STRING new_name, GroupID remote=None)

        Relink an object.  current_name identifies the object.
        new_name and (optionally) another group "remote" determine
        where it should be moved.
        """
        cdef hid_t remote_id
        if remote is None:
            remote_id = self.id
        else:
            remote_id = remote.id

        H5Gmove2(self.id, current_name, remote_id, new_name)


    @with_phil
    def get_num_objs(self):
        """() => INT number_of_objects

        Get the number of objects directly attached to a given group.
        """
        cdef hsize_t size
        H5Gget_num_objs(self.id, &size)
        return size


    @with_phil
    def get_objname_by_idx(self, hsize_t idx):
        """(INT idx) => STRING

        Get the name of a group member given its zero-based index.
        """
        cdef int size
        cdef char* buf
        buf = NULL

        size = H5Gget_objname_by_idx(self.id, idx, NULL, 0)

        buf = <char*>emalloc(sizeof(char)*(size+1))
        try:
            H5Gget_objname_by_idx(self.id, idx, buf, size+1)
            pystring = buf
            return pystring
        finally:
            efree(buf)


    @with_phil
    def get_objtype_by_idx(self, hsize_t idx):
        """(INT idx) => INT object_type_code

        Get the type of an object attached to a group, given its zero-based
        index.  Possible return values are:

        - LINK
        - GROUP
        - DATASET
        - TYPE
        """
        return <int>H5Gget_objtype_by_idx(self.id, idx)


    @with_phil
    def get_linkval(self, char* name):
        """(STRING name) => STRING link_value

        Retrieve the value (target name) of a symbolic link.
        Limited to 2048 characters on Windows.
        """
        cdef char* value
        cdef H5G_stat_t statbuf
        value = NULL

        H5Gget_objinfo(self.id, name, 0, &statbuf)

        if statbuf.type != H5G_LINK:
            raise ValueError('"%s" is not a symbolic link.' % name)

        if _IS_WINDOWS:
            linklen = 2049  # Windows statbuf.linklen seems broken
        else:
            linklen = statbuf.linklen+1
        value = <char*>emalloc(sizeof(char)*linklen)
        try:
            H5Gget_linkval(self.id, name, linklen, value)
            value[linklen-1] = c'\0'  # in case HDF5 doesn't null terminate on Windows
            pyvalue = value
            return pyvalue
        finally:
            efree(value)


    @with_phil
    def get_create_plist(self):
        """() => PropGCID

        Retrieve a copy of the group creation property list used to
        create this group.
        """
        return propwrap(H5Gget_create_plist(self.id))


    @with_phil
    def set_comment(self, char* name, char* comment):
        """(STRING name, STRING comment)

        Set the comment on a group member.
        """
        H5Gset_comment(self.id, name, comment)


    @with_phil
    def get_comment(self, char* name):
        """(STRING name) => STRING comment

        Retrieve the comment for a group member.
        """
        cdef int cmnt_len
        cdef char* cmnt
        cmnt = NULL

        cmnt_len = H5Gget_comment(self.id, name, 0, NULL)
        assert cmnt_len >= 0

        cmnt = <char*>emalloc(sizeof(char)*(cmnt_len+1))
        try:
            H5Gget_comment(self.id, name, cmnt_len+1, cmnt)
            py_cmnt = cmnt
            return py_cmnt
        finally:
            efree(cmnt)


    # === Special methods =====================================================

    def __contains__(self, name):
        """(STRING name)

        Determine if a group member of the given name is present
        """
        cdef err_cookie old_handler
        cdef err_cookie new_handler
        cdef herr_t retval

        new_handler.func = NULL
        new_handler.data = NULL

        if not self:
            return False

        with phil:
            return _path_valid(self, name)

    def __iter__(self):
        """ Return an iterator over the names of group members. """
        with phil:
            return GroupIter(self)

    def __reversed__(self):
        """ Return an iterator over group member names in reverse order. """
        with phil:
            return GroupIter(self, reversed=True)

    def __len__(self):
        """ Number of group members """
        cdef hsize_t size
        with phil:
            H5Gget_num_objs(self.id, &size)
            return size


@with_phil
def _path_valid(GroupID grp not None, object path not None, PropID lapl=None):
    """ Determine if *path* points to an object in the file.

    If *path* represents an external or soft link, the link's validity is not
    checked.
    """
    from . import h5o

    if isinstance(path, bytes):
        path = path.decode('utf-8')
    else:
        path = unicode(path)

    # Empty names are not allowed by HDF5
    if len(path) == 0:
        return False

    # Note: we cannot use pp.normpath as it resolves ".." components,
    # which don't exist in HDF5

    path_parts = path.split('/')

    # Absolute path (started with slash)
    if path_parts[0] == '':
        current_loc = h5o.open(grp, b'/', lapl=lapl)
    else:
        current_loc = grp

    # HDF5 ignores duplicate or trailing slashes
    path_parts = [x for x in path_parts if x != '']

    # Special case: path was entirely composed of slashes!
    if len(path_parts) == 0:
        path_parts = ['.']  # i.e. the root group

    path_parts = [x.encode('utf-8') for x in path_parts]
    nparts = len(path_parts)

    for idx, p in enumerate(path_parts):

        # Special case; '.' always refers to the present group
        if p == b'.':
            continue

        # Is there any kind of link by that name in this group?
        if not current_loc.links.exists(p, lapl=lapl):
            return False

        # If we're at the last link in the chain, we're done.
        # We don't check to see if the last part points to a valid object;
        # it's enough that it exists.
        if idx == nparts - 1:
            return True

        # Otherwise, does the link point to a real object?
        if not h5o.exists_by_name(current_loc, p, lapl=lapl):
            return False

        # Is that object a group?
        next_loc = h5o.open(current_loc, p, lapl=lapl)
        info = h5o.get_info(next_loc)
        if info.type != H5O_TYPE_GROUP:
            return False

        # Go into that group
        current_loc = next_loc

    return True
