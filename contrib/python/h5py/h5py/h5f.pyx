# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

"""
    Low-level operations on HDF5 file objects.
"""

include "config.pxi"

# C level imports
from cpython.buffer cimport PyObject_CheckBuffer, \
                            PyObject_GetBuffer, PyBuffer_Release, \
                            PyBUF_SIMPLE
from ._objects cimport pdefault
from .h5p cimport propwrap, PropFAID, PropFCID
from .h5i cimport wrap_identifier
from .h5ac cimport CacheConfig
from .utils cimport emalloc, efree

# Python level imports
from collections import namedtuple
import gc
from . import _objects, h5o
from ._objects import phil, with_phil

from cpython.bytes cimport PyBytes_FromStringAndSize, PyBytes_AsString

# Initialization

# === Public constants and data structures ====================================

ACC_TRUNC   = H5F_ACC_TRUNC
ACC_EXCL    = H5F_ACC_EXCL
ACC_RDWR    = H5F_ACC_RDWR
ACC_RDONLY  = H5F_ACC_RDONLY
ACC_SWMR_WRITE = H5F_ACC_SWMR_WRITE
ACC_SWMR_READ  = H5F_ACC_SWMR_READ


SCOPE_LOCAL     = H5F_SCOPE_LOCAL
SCOPE_GLOBAL    = H5F_SCOPE_GLOBAL

CLOSE_DEFAULT = H5F_CLOSE_DEFAULT
CLOSE_WEAK  = H5F_CLOSE_WEAK
CLOSE_SEMI  = H5F_CLOSE_SEMI
CLOSE_STRONG = H5F_CLOSE_STRONG

OBJ_FILE    = H5F_OBJ_FILE
OBJ_DATASET = H5F_OBJ_DATASET
OBJ_GROUP   = H5F_OBJ_GROUP
OBJ_DATATYPE = H5F_OBJ_DATATYPE
OBJ_ATTR    = H5F_OBJ_ATTR
OBJ_ALL     = H5F_OBJ_ALL
OBJ_LOCAL   = H5F_OBJ_LOCAL
UNLIMITED   = H5F_UNLIMITED

LIBVER_EARLIEST = H5F_LIBVER_EARLIEST
LIBVER_LATEST = H5F_LIBVER_LATEST
LIBVER_V18 = H5F_LIBVER_V18
LIBVER_V110 = H5F_LIBVER_V110

IF HDF5_VERSION >= VOL_MIN_HDF5_VERSION:
    LIBVER_V112 = H5F_LIBVER_V112

IF HDF5_VERSION >= (1, 13, 0):
    LIBVER_V114 = H5F_LIBVER_V114

FILE_IMAGE_OPEN_RW = H5LT_FILE_IMAGE_OPEN_RW

FSPACE_STRATEGY_FSM_AGGR = H5F_FSPACE_STRATEGY_FSM_AGGR
FSPACE_STRATEGY_PAGE = H5F_FSPACE_STRATEGY_PAGE
FSPACE_STRATEGY_AGGR = H5F_FSPACE_STRATEGY_AGGR
FSPACE_STRATEGY_NONE = H5F_FSPACE_STRATEGY_NONE

# Used in FileID.get_page_buffering_stats()
PageBufStats = namedtuple('PageBufferStats', ['meta', 'raw'])
PageStats = namedtuple('PageStats', ['accesses', 'hits', 'misses', 'evictions', 'bypasses'])


# === File operations =========================================================

@with_phil
def open(char* name, unsigned int flags=H5F_ACC_RDWR, PropFAID fapl=None):
    """(STRING name, UINT flags=ACC_RDWR, PropFAID fapl=None) => FileID

    Open an existing HDF5 file.  Keyword "flags" may be:

    ACC_RDWR
        Open in read-write mode

    ACC_RDONLY
        Open in readonly mode

    Keyword fapl may be a file access property list.
    """
    return FileID(H5Fopen(name, flags, pdefault(fapl)))


@with_phil
def create(char* name, int flags=H5F_ACC_TRUNC, PropFCID fcpl=None,
                                                PropFAID fapl=None):
    """(STRING name, INT flags=ACC_TRUNC, PropFCID fcpl=None,
    PropFAID fapl=None) => FileID

    Create a new HDF5 file.  Keyword "flags" may be:

    ACC_TRUNC
        Truncate an existing file, discarding its data

    ACC_EXCL
        Fail if a conflicting file exists

    To keep the behavior in line with that of Python's built-in functions,
    the default is ACC_TRUNC.  Be careful!
    """
    return FileID(H5Fcreate(name, flags, pdefault(fcpl), pdefault(fapl)))

@with_phil
def open_file_image(image, flags=0):
    """(STRING image, INT flags=0) => FileID

    Load a new HDF5 file into memory.  Keyword "flags" may be:

    FILE_IMAGE_OPEN_RW
        Specifies opening the file image in read/write mode.
    """
    cdef Py_buffer buf

    if not PyObject_CheckBuffer(image):
        raise TypeError("image must support the buffer protocol")

    PyObject_GetBuffer(image, &buf, PyBUF_SIMPLE)
    try:
        return FileID(H5LTopen_file_image(buf.buf, buf.len, flags))
    finally:
        PyBuffer_Release(&buf)


@with_phil
def flush(ObjectID obj not None, int scope=H5F_SCOPE_LOCAL):
    """(ObjectID obj, INT scope=SCOPE_LOCAL)

    Tell the HDF5 library to flush file buffers to disk.  "obj" may
    be the file identifier, or the identifier of any object residing in
    the file.  Keyword "scope" may be:

    SCOPE_LOCAL
        Flush only the given file

    SCOPE_GLOBAL
        Flush the entire virtual file
    """
    H5Fflush(obj.id, <H5F_scope_t>scope)


@with_phil
def is_hdf5(char* name):
    """(STRING name) => BOOL

    Determine if a given file is an HDF5 file.  Note this raises an
    exception if the file doesn't exist.
    """
    return <bint>(H5Fis_hdf5(name))


@with_phil
def mount(ObjectID loc not None, char* name, FileID fid not None):
    """(ObjectID loc, STRING name, FileID fid)

    Mount an open file on the group "name" under group loc_id.  Note that
    "name" must already exist.
    """
    H5Fmount(loc.id, name, fid.id, H5P_DEFAULT)


@with_phil
def unmount(ObjectID loc not None, char* name):
    """(ObjectID loc, STRING name)

    Unmount a file, mounted at "name" under group loc_id.
    """
    H5Funmount(loc.id, name)


@with_phil
def get_name(ObjectID obj not None):
    """(ObjectID obj) => STRING

    Determine the name of the file in which the specified object resides.
    """
    cdef ssize_t size
    cdef char* name
    name = NULL

    size = H5Fget_name(obj.id, NULL, 0)
    assert size >= 0
    name = <char*>emalloc(sizeof(char)*(size+1))
    try:
        H5Fget_name(obj.id, name, size+1)
        pname = name
        return pname
    finally:
        efree(name)


@with_phil
def get_obj_count(object where=OBJ_ALL, int types=H5F_OBJ_ALL):
    """(OBJECT where=OBJ_ALL, types=OBJ_ALL) => INT

    Get the number of open objects.

    where
        Either a FileID instance representing an HDF5 file, or the
        special constant OBJ_ALL, to count objects in all files.

    type
        Specify what kinds of object to include.  May be one of OBJ*,
        or any bitwise combination (e.g. ``OBJ_FILE | OBJ_ATTR``).

        The special value OBJ_ALL matches all object types, and
        OBJ_LOCAL will only match objects opened through a specific
        identifier.
    """
    cdef hid_t where_id
    if isinstance(where, FileID):
        where_id = where.id
    elif isinstance(where, int):
        where_id = where
    else:
        raise TypeError("Location must be a FileID or OBJ_ALL.")

    return H5Fget_obj_count(where_id, types)


@with_phil
def get_obj_ids(object where=OBJ_ALL, int types=H5F_OBJ_ALL):
    """(OBJECT where=OBJ_ALL, types=OBJ_ALL) => LIST

    Get a list of identifier instances for open objects.

    where
        Either a FileID instance representing an HDF5 file, or the
        special constant OBJ_ALL, to list objects in all files.

    type
        Specify what kinds of object to include.  May be one of OBJ*,
        or any bitwise combination (e.g. ``OBJ_FILE | OBJ_ATTR``).

        The special value OBJ_ALL matches all object types, and
        OBJ_LOCAL will only match objects opened through a specific
        identifier.
    """
    cdef int count
    cdef int i
    cdef hid_t where_id
    cdef hid_t *obj_list = NULL
    cdef list py_obj_list = []

    if isinstance(where, FileID):
        where_id = where.id
    else:
        try:
            where_id = int(where)
        except TypeError:
            raise TypeError("Location must be a FileID or OBJ_ALL.")

    try:
        count = H5Fget_obj_count(where_id, types)
        obj_list = <hid_t*>emalloc(sizeof(hid_t)*count)

        if count > 0: # HDF5 complains that obj_list is NULL, even if count==0
            # Garbage collection might dealloc a Python object & call H5Idec_ref
            # between getting an HDF5 ID and calling H5Iinc_ref, breaking it.
            # Disable GC until we have inc_ref'd the IDs to keep them alive.
            gc_was_enabled = gc.isenabled()
            gc.disable()
            try:
                H5Fget_obj_ids(where_id, types, count, obj_list)
                for i in range(count):
                    py_obj_list.append(wrap_identifier(obj_list[i]))
                    # The HDF5 function returns a borrowed reference for each hid_t.
                    H5Iinc_ref(obj_list[i])
            finally:
                if gc_was_enabled:
                    gc.enable()

        return py_obj_list

    finally:
        efree(obj_list)


# === FileID implementation ===================================================

cdef class FileID(GroupID):

    """
        Represents an HDF5 file identifier.

        These objects wrap a small portion of the H5F interface; all the
        H5F functions which can take arbitrary objects in addition to
        file identifiers are provided as functions in the h5f module.

        Properties:

        * name:   File name on disk

        Behavior:

        * Hashable: Yes, unique to the file (but not the access mode)
        * Equality: Hash comparison
    """

    @property
    def name(self):
        """ File name on disk (according to h5f.get_name()) """
        with phil:
            return get_name(self)


    @with_phil
    def close(self):
        """()

        Terminate access through this identifier.  Note that depending on
        what property list settings were used to open the file, the
        physical file might not be closed until all remaining open
        identifiers are freed.
        """
        self._close()
        _objects.nonlocal_close()

    @with_phil
    def _close_open_objects(self, int types):
        # Used by File.close(). This avoids the get_obj_ids wrapper, which
        # creates Python objects and increments HDF5 ref counts while we're
        # trying to clean up. E.g. that can be problematic at Python shutdown.
        cdef int count, i
        cdef hid_t *obj_list = NULL

        count = H5Fget_obj_count(self.id, types)
        if count == 0:
            return
        obj_list = <hid_t*> emalloc(sizeof(hid_t) * count)
        try:
            H5Fget_obj_ids(self.id, types, count, obj_list)
            for i in range(count):
                while H5Iis_valid(obj_list[i]):
                    H5Idec_ref(obj_list[i])
        finally:
            efree(obj_list)

    @with_phil
    def reopen(self):
        """() => FileID

        Retrieve another identifier for a file (which must still be open).
        The new identifier is guaranteed to neither be mounted nor contain
        a mounted file.
        """
        return FileID(H5Freopen(self.id))


    @with_phil
    def get_filesize(self):
        """() => LONG size

        Determine the total size (in bytes) of the HDF5 file,
        including any user block.
        """
        cdef hsize_t size
        H5Fget_filesize(self.id, &size)
        return size


    @with_phil
    def get_create_plist(self):
        """() => PropFCID

        Retrieve a copy of the file creation property list used to
        create this file.
        """
        return propwrap(H5Fget_create_plist(self.id))


    @with_phil
    def get_access_plist(self):
        """() => PropFAID

        Retrieve a copy of the file access property list which manages access
        to this file.
        """
        return propwrap(H5Fget_access_plist(self.id))


    @with_phil
    def get_freespace(self):
        """() => LONG freespace

        Determine the amount of free space in this file.  Note that this
        only tracks free space until the file is closed.
        """
        return H5Fget_freespace(self.id)


    @with_phil
    def get_intent(self):
        """ () => INT

        Determine the file's write intent, either of:
        - H5F_ACC_RDONLY
        - H5F_ACC_RDWR
        """
        cdef unsigned int mode
        H5Fget_intent(self.id, &mode)
        return mode


    @with_phil
    def get_vfd_handle(self, fapl=None):
        """ (PropFAID) => INT

        Retrieve the file handle used by the virtual file driver.

        This may not be supported for all file drivers, and the meaning of the
        return value may depend on the file driver.

        The 'family' and 'multi' drivers access multiple files, and a file
        access property list (fapl) can be used to indicate which to access,
        with H5Pset_family_offset or H5Pset_multi_type.
        """
        cdef int *handle
        H5Fget_vfd_handle(self.id, pdefault(fapl), <void**>&handle)
        return handle[0]

    @with_phil
    def get_file_image(self):
        """ () => BYTES

        Retrieves a copy of the image of an existing, open file.
        """

        cdef ssize_t size

        size = H5Fget_file_image(self.id, NULL, 0)
        image = PyBytes_FromStringAndSize(NULL, size)

        H5Fget_file_image(self.id, PyBytes_AsString(image), size)

        return image

    IF MPI:

        @with_phil
        def set_mpi_atomicity(self, bint atomicity):
            """ (BOOL atomicity)

            For MPI-IO driver, set to atomic (True), which guarantees sequential
            I/O semantics, or non-atomic (False), which improves  performance.

            Default is False.

            Feature requires: Parallel HDF5
            """
            H5Fset_mpi_atomicity(self.id, <hbool_t>atomicity)


        @with_phil
        def get_mpi_atomicity(self):
            """ () => BOOL

            Return atomicity setting for MPI-IO driver.

            Feature requires: Parallel HDF5
            """
            cdef hbool_t atom

            H5Fget_mpi_atomicity(self.id, &atom)
            return <bint>atom


    @with_phil
    def get_mdc_hit_rate(self):
        """() => DOUBLE

        Retrieve the cache hit rate

        """
        cdef double hit_rate
        H5Fget_mdc_hit_rate(self.id, &hit_rate)
        return hit_rate


    @with_phil
    def get_mdc_size(self):
        """() => (max_size, min_clean_size, cur_size, cur_num_entries) [SIZE_T, SIZE_T, SIZE_T, INT]

        Obtain current metadata cache size data for specified file.

        """
        cdef size_t max_size
        cdef size_t min_clean_size
        cdef size_t cur_size
        cdef int cur_num_entries


        H5Fget_mdc_size(self.id, &max_size, &min_clean_size, &cur_size, &cur_num_entries)

        return (max_size, min_clean_size, cur_size, cur_num_entries)


    @with_phil
    def reset_mdc_hit_rate_stats(self):
        """no return

        rests the hit-rate statistics

        """
        H5Freset_mdc_hit_rate_stats(self.id)


    @with_phil
    def get_mdc_config(self):
        """() => CacheConfig
        Returns an object that stores all the information about the meta-data cache
        configuration. This config is created for every file in-memory with the default
        cache config values, it is not saved to the hdf5 file.
        """

        cdef CacheConfig config = CacheConfig()

        H5Fget_mdc_config(self.id, &config.cache_config)

        return config

    @with_phil
    def set_mdc_config(self, CacheConfig config not None):
        """(CacheConfig) => None
        Sets the meta-data cache configuration for a file. This config is created for every file
        in-memory with the default config values, it is not saved to the hdf5 file. Any change to
        the configuration lives until the hdf5 file is closed.
        """
        # I feel this should have some sanity checking to make sure that
        H5Fset_mdc_config(self.id, &config.cache_config)

    @with_phil
    def start_swmr_write(self):
        """ no return

        Enables SWMR writing mode for a file.

        This function will activate SWMR writing mode for a file associated
        with file_id. This routine will prepare and ensure the file is safe
        for SWMR writing as follows:

            * Check that the file is opened with write access (H5F_ACC_RDWR).
            * Check that the file is opened with the latest library format
              to ensure data structures with check-summed metadata are used.
            * Check that the file is not already marked in SWMR writing mode.
            * Enable reading retries for check-summed metadata to remedy
              possible checksum failures from reading inconsistent metadata
              on a system that is not atomic.
            * Turn off usage of the library’s accumulator to avoid possible
              ordering problem on a system that is not atomic.
            * Perform a flush of the file’s data buffers and metadata to set
              a consistent state for starting SWMR write operations.

        Library objects are groups, datasets, and committed datatypes. For
        the current implementation, groups and datasets can remain open when
        activating SWMR writing mode, but not committed datatypes. Attributes
        attached to objects cannot remain open.

        Feature requires: 1.9.178 HDF5
        """
        H5Fstart_swmr_write(self.id)

    @with_phil
    def reset_page_buffering_stats(self):
        """ ()

        Reset page buffer statistics for the file.
        """
        H5Freset_page_buffering_stats(self.id)

    @with_phil
    def get_page_buffering_stats(self):
        """ () -> NAMEDTUPLE PageBufStats(NAMEDTUPLE meta=PageStats, NAMEDTUPLE raw=PageStats)

        Retrieve page buffering statistics for the file as the number of
        metadata and raw data accesses, hits, misses, evictions, and
        accesses that bypass the page buffer (bypasses).
        """
        cdef:
            unsigned int accesses[2]
            unsigned int hits[2]
            unsigned int misses[2]
            unsigned int evictions[2]
            unsigned int bypasses[2]

        H5Fget_page_buffering_stats(self.id, &accesses[0], &hits[0],
                                    &misses[0], &evictions[0], &bypasses[0])
        meta = PageStats(int(accesses[0]), int(hits[0]), int(misses[0]),
                         int(evictions[0]), int(bypasses[0]))
        raw = PageStats(int(accesses[1]), int(hits[1]), int(misses[1]),
                        int(evictions[1]), int(bypasses[1]))

        return PageBufStats(meta, raw)

    # === Special methods =====================================================

    # Redefine these methods to use the root group explicitly, because otherwise
    # the setting for link order tracking can be missed.
    @with_phil
    def __iter__(self):
        """ Return an iterator over the names of group members. """
        return iter(h5o.open(self, b'/'))

    @with_phil
    def __reversed__(self):
        """ Return an iterator over group member names in reverse order. """
        return reversed(h5o.open(self, b'/'))
