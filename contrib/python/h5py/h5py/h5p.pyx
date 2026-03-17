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
    HDF5 property list interface.
"""

include "config.pxi"

# C-level imports
from cpython.buffer cimport PyObject_CheckBuffer, \
                            PyObject_GetBuffer, PyBuffer_Release, \
                            PyBUF_SIMPLE
from cpython.long cimport PyLong_AsVoidPtr

from .utils cimport  require_tuple, convert_dims, convert_tuple, \
                    emalloc, efree, \
                    check_numpy_write, check_numpy_read
from numpy cimport ndarray, import_array
from .h5t cimport TypeID, py_create
from .h5s cimport SpaceID
from .h5ac cimport CacheConfig

# Python level imports
from ._objects import phil, with_phil

if MPI:
    from mpi4py.libmpi cimport (
        MPI_Comm, MPI_Info, MPI_Comm_dup, MPI_Info_dup,
        MPI_Comm_free, MPI_Info_free)


# Initialization
import_array()

# === C API ===================================================================

cdef hid_t pdefault(PropID pid):

    if pid is None:
        return <hid_t>H5P_DEFAULT
    return pid.id

cdef object propwrap(hid_t id_in):

    clsid = H5Pget_class(id_in)
    try:
        if H5Pequal(clsid, H5P_FILE_CREATE):
            pcls = PropFCID
        elif H5Pequal(clsid, H5P_FILE_ACCESS):
            pcls = PropFAID
        elif H5Pequal(clsid, H5P_DATASET_CREATE):
            pcls = PropDCID
        elif H5Pequal(clsid, H5P_DATASET_XFER):
            pcls = PropDXID
        elif H5Pequal(clsid, H5P_OBJECT_COPY):
            pcls = PropCopyID
        elif H5Pequal(clsid, H5P_LINK_CREATE):
            pcls = PropLCID
        elif H5Pequal(clsid, H5P_LINK_ACCESS):
            pcls = PropLAID
        elif H5Pequal(clsid, H5P_GROUP_CREATE):
            pcls = PropGCID
        elif H5Pequal(clsid, H5P_DATATYPE_CREATE):
            pcls = PropTCID
        elif H5Pequal(clsid, H5P_DATASET_ACCESS):
            pcls = PropDAID
        elif H5Pequal(clsid, H5P_OBJECT_CREATE):
            pcls = PropOCID

        else:
            raise ValueError("No class found for ID %d" % id_in)

        return pcls(id_in)
    finally:
        H5Pclose_class(clsid)

cdef object lockcls(hid_t id_in):
    cdef PropClassID pid
    pid = PropClassID(id_in)
    pid.locked = 1
    return pid


# === Public constants and data structures ====================================

# Property list classes
# These need to be locked, as the library won't let you close them.


NO_CLASS       = lockcls(H5P_NO_CLASS)
FILE_CREATE    = lockcls(H5P_FILE_CREATE)
FILE_ACCESS    = lockcls(H5P_FILE_ACCESS)
DATASET_CREATE = lockcls(H5P_DATASET_CREATE)
DATASET_XFER   = lockcls(H5P_DATASET_XFER)
DATASET_ACCESS = lockcls(H5P_DATASET_ACCESS)

OBJECT_COPY = lockcls(H5P_OBJECT_COPY)

LINK_CREATE = lockcls(H5P_LINK_CREATE)
LINK_ACCESS = lockcls(H5P_LINK_ACCESS)
GROUP_CREATE = lockcls(H5P_GROUP_CREATE)
OBJECT_CREATE = lockcls(H5P_OBJECT_CREATE)

CRT_ORDER_TRACKED = H5P_CRT_ORDER_TRACKED
CRT_ORDER_INDEXED = H5P_CRT_ORDER_INDEXED

DEFAULT = None   # In the HDF5 header files this is actually 0, which is an
                 # invalid identifier.  The new strategy for default options
                 # is to make them all None, to better match the Python style
                 # for keyword arguments.


# === Property list functional API ============================================

@with_phil
def create(PropClassID cls not None):
    """(PropClassID cls) => PropID

    Create a new property list as an instance of a class; classes are:

    - FILE_CREATE
    - FILE_ACCESS
    - DATASET_CREATE
    - DATASET_XFER
    - DATASET_ACCESS
    - LINK_CREATE
    - LINK_ACCESS
    - GROUP_CREATE
    - OBJECT_COPY
    - OBJECT_CREATE
    """
    cdef hid_t newid
    newid = H5Pcreate(cls.id)
    return propwrap(newid)


# === Class API ===============================================================

cdef class PropID(ObjectID):

    """
        Base class for all property lists and classes
    """


    @with_phil
    def equal(self, PropID plist not None):
        """(PropID plist) => BOOL

        Compare this property list (or class) to another for equality.
        """
        return <bint>(H5Pequal(self.id, plist.id))

    def __richcmp__(self, object other, int how):
        cdef bint truthval = 0

        with phil:
            if how != 2 and how != 3:
                return NotImplemented
            if type(self) == type(other):
                truthval = self.equal(other)

            if how == 2:
                return truthval
            return not truthval

    def __hash__(self):
        raise TypeError("Property lists are unhashable")

cdef class PropClassID(PropID):

    """
        An HDF5 property list class.

        * Hashable: Yes, by identifier
        * Equality: Logical H5P comparison
    """

    def __richcmp__(self, object other, int how):
        return PropID.__richcmp__(self, other, how)

    def __hash__(self):
        """ Since classes are library-created and immutable, they are uniquely
            identified by their HDF5 identifiers.
        """
        return hash(self.id)

cdef class PropInstanceID(PropID):

    """
        Base class for property list instance objects.  Provides methods which
        are common across all HDF5 property list classes.

        * Hashable: No
        * Equality: Logical H5P comparison
    """


    @with_phil
    def copy(self):
        """() => PropList newid

         Create a new copy of an existing property list object.
        """
        return type(self)(H5Pcopy(self.id))


    def get_class(self):
        """() => PropClassID

        Determine the class of a property list object.
        """
        return PropClassID(H5Pget_class(self.id))


cdef class PropCreateID(PropInstanceID):

    """
        Generic object creation property list.
    """
    pass


cdef class PropCopyID(PropInstanceID):

    """
        Generic object copy property list
    """


    @with_phil
    def set_copy_object(self, unsigned int flags):
        """(UINT flags)

        Set flags for object copying process.  Legal flags are
        from the h5o.COPY* family:

        h5o.COPY_SHALLOW_HIERARCHY_FLAG
            Copy only immediate members of a group.

        h5o.COPY_EXPAND_SOFT_LINK_FLAG
            Expand soft links into new objects.

        h5o.COPY_EXPAND_EXT_LINK_FLAG
            Expand external link into new objects.

        h5o.COPY_EXPAND_REFERENCE_FLAG
            Copy objects that are pointed to by references.

        h5o.COPY_WITHOUT_ATTR_FLAG
            Copy object without copying attributes.
        """
        H5Pset_copy_object(self.id, flags)


    @with_phil
    def get_copy_object(self):
        """() => UINT flags

        Get copy process flags. Legal flags are h5o.COPY*.
        """
        cdef unsigned int flags
        H5Pget_copy_object(self.id, &flags)
        return flags


# === Concrete list implementations ===========================================

# File creation

cdef class PropFCID(PropOCID):

    """
        File creation property list.
    """


    @with_phil
    def get_version(self):
        """() => TUPLE version_info

        Determine version information of various file attributes.
        Elements are:

        0.  UINT Super block version number
        1.  UINT Freelist version number
        2.  UINT Symbol table version number
        3.  UINT Shared object header version number
        """
        cdef herr_t retval
        cdef unsigned int super_
        cdef unsigned int freelist
        cdef unsigned int stab
        cdef unsigned int shhdr

        H5Pget_version(self.id, &super_, &freelist, &stab, &shhdr)

        return (super_, freelist, stab, shhdr)


    @with_phil
    def set_userblock(self, hsize_t size):
        """(INT/LONG size)

        Set the file user block size, in bytes.
        Must be a power of 2, and at least 512.
        """
        H5Pset_userblock(self.id, size)


    @with_phil
    def get_userblock(self):
        """() => LONG size

        Determine the user block size, in bytes.
        """
        cdef hsize_t size
        H5Pget_userblock(self.id, &size)
        return size


    @with_phil
    def set_sizes(self, size_t addr, size_t size):
        """(UINT addr, UINT size)

        Set the addressing offsets and lengths for objects
        in an HDF5 file, in bytes.
        """
        H5Pset_sizes(self.id, addr, size)


    @with_phil
    def get_sizes(self):
        """() => TUPLE sizes

        Determine addressing offsets and lengths for objects in an
        HDF5 file, in bytes.  Return value is a 2-tuple with values:

        0.  UINT Address offsets
        1.  UINT Lengths
        """
        cdef size_t addr
        cdef size_t size
        H5Pget_sizes(self.id, &addr, &size)
        return (addr, size)


    @with_phil
    def set_link_creation_order(self, unsigned int flags):
        """ (UINT flags)

        Set tracking and indexing of creation order for links added to this group

        flags -- h5p.CRT_ORDER_TRACKED, h5p.CRT_ORDER_INDEXED
        """
        H5Pset_link_creation_order(self.id, flags)


    @with_phil
    def get_link_creation_order(self):
        """ () -> UINT flags

        Get tracking and indexing of creation order for links added to this group
        """
        cdef unsigned int flags
        H5Pget_link_creation_order(self.id, &flags)
        return flags

    @with_phil
    def set_file_space_strategy(self, unsigned int strategy, bint persist,
            unsigned long long threshold):
        """ (UINT strategy, BOOL persist, ULONGLONG threshold)

        Set the file space handling strategy and persisting free-space values.
        """
        H5Pset_file_space_strategy(self.id, <H5F_fspace_strategy_t>strategy,
                <hbool_t>persist, <hsize_t>threshold)

    @with_phil
    def get_file_space_strategy(self):
        """ () => TUPLE(UINT strategy, BOOL persist, ULONGLONG threshold)

        Retrieve the file space handling strategy, persisting free-space
        condition and threshold value for a file creation property list.
        """
        cdef H5F_fspace_strategy_t strategy
        cdef hbool_t persist
        cdef hsize_t threshold

        H5Pget_file_space_strategy(self.id, &strategy, &persist, &threshold)
        return (strategy, persist, threshold)

    @with_phil
    def set_file_space_page_size(self, hsize_t fsp_size):
        """ (LONG fsp_size)

        Set the file space page size used in paged aggregation and paged
        buffering. Minimum page size is 512 bytes. A value less than 512 will raise
        an error. The size set may not be changed for the life of the file.
        """
        H5Pset_file_space_page_size(self.id, <hsize_t>fsp_size)

    @with_phil
    def get_file_space_page_size(self):
        """ () -> LONG fsp_size

        Retrieve the file space page size.
        """
        cdef hsize_t fsp_size
        H5Pget_file_space_page_size(self.id, &fsp_size)
        return fsp_size

# Dataset creation
cdef class PropDCID(PropOCID):

    """
        Dataset creation property list.
    """

    @with_phil
    def set_layout(self, int layout_code):
        """(INT layout_code)

        Set dataset storage strategy; legal values are:

        - h5d.COMPACT
        - h5d.CONTIGUOUS
        - h5d.CHUNKED
        - h5d.VIRTUAL (If using HDF5 library version 1.10 or later)
        """
        H5Pset_layout(self.id, <H5D_layout_t>layout_code)


    @with_phil
    def get_layout(self):
        """() => INT layout_code

        Determine the storage strategy of a dataset; legal values are:

        - h5d.COMPACT
        - h5d.CONTIGUOUS
        - h5d.CHUNKED
        - h5d.VIRTUAL (If using HDF5 library version 1.10 or later)
        """
        return <int>H5Pget_layout(self.id)

    @with_phil
    def set_chunk(self, object chunksize):
        """(TUPLE chunksize)

        Set the dataset chunk size.  It's up to you to provide
        values which are compatible with your dataset.
        """
        cdef int rank
        cdef hsize_t* dims
        dims = NULL

        require_tuple(chunksize, 0, -1, b"chunksize")
        rank = len(chunksize)

        dims = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
        try:
            convert_tuple(chunksize, dims, rank)
            H5Pset_chunk(self.id, rank, dims)
        finally:
            efree(dims)


    @with_phil
    def get_chunk(self):
        """() => TUPLE chunk_dimensions

        Obtain the dataset chunk size, as a tuple.
        """
        cdef int rank
        cdef hsize_t *dims

        rank = H5Pget_chunk(self.id, 0, NULL)
        assert rank >= 0
        dims = <hsize_t*>emalloc(sizeof(hsize_t)*rank)

        try:
            H5Pget_chunk(self.id, rank, dims)
            tpl = convert_dims(dims, rank)
            return tpl
        finally:
            efree(dims)


    @with_phil
    def set_fill_value(self, ndarray value not None):
        """(NDARRAY value)

        Set the dataset fill value.  The object provided should be an
        0-dimensional NumPy array; otherwise, the value will be read from
        the first element.
        """
        from .h5t import check_string_dtype
        cdef TypeID tid
        cdef char * c_ptr

        check_numpy_read(value, -1)

        # check for strings
        # create correct typeID and pointer to c_str
        string_info = check_string_dtype(value.dtype)
        if string_info is not None:
            # if needed encode fill_value
            fill_value = value.item()
            if not isinstance(fill_value, bytes):
                fill_value = fill_value.encode(string_info.encoding)
            c_ptr = fill_value
            tid = py_create(value.dtype, logical=1)
            H5Pset_fill_value(self.id, tid.id, &c_ptr)
            return

        tid = py_create(value.dtype)
        H5Pset_fill_value(self.id, tid.id, value.data)


    @with_phil
    def get_fill_value(self, ndarray value not None):
        """(NDARRAY value)

        Read the dataset fill value into a NumPy array.  It will be
        converted to match the array dtype.  If the array has nonzero
        rank, only the first element will contain the value.
        """
        from .h5t import check_string_dtype
        cdef TypeID tid
        cdef char * c_ptr = NULL

        check_numpy_write(value, -1)

        # check for vlen strings
        # create correct typeID and convert from c_str pointer to string
        string_info = check_string_dtype(value.dtype)
        if string_info is not None and string_info.length is None:
            tid = py_create(value.dtype, logical=1)
            ret = H5Pget_fill_value(self.id, tid.id, &c_ptr)
            if c_ptr == NULL:
                # If the pointer is NULL (either the value did not get changed,
                # or maybe the 0 length string, it's unclear currently), if
                # PyBytes_FromString is called on the pointer, we get a
                # segfault. If we set the value to empty bytes, then we
                # shouldn't segfault.
                value[0] = b""
                return
            fill_value = c_ptr
            value[0] = fill_value
            return

        tid = py_create(value.dtype)
        H5Pget_fill_value(self.id, tid.id, value.data)

    @with_phil
    def fill_value_defined(self):
        """() => INT fill_status

        Determine the status of the dataset fill value.  Return values are:

        - h5d.FILL_VALUE_UNDEFINED
        - h5d.FILL_VALUE_DEFAULT
        - h5d.FILL_VALUE_USER_DEFINED
        """
        cdef H5D_fill_value_t val
        H5Pfill_value_defined(self.id, &val)
        return <int>val


    @with_phil
    def set_fill_time(self, int fill_time):
        """(INT fill_time)

        Define when fill values are written to the dataset.  Legal
        values (defined in module h5d) are:

        - h5d.FILL_TIME_ALLOC
        - h5d.FILL_TIME_NEVER
        - h5d.FILL_TIME_IFSET
        """
        H5Pset_fill_time(self.id, <H5D_fill_time_t>fill_time)


    @with_phil
    def get_fill_time(self):
        """ () => INT

        Determine when fill values are written to the dataset.  Legal
        values (defined in module h5d) are:

        - h5d.FILL_TIME_ALLOC
        - h5d.FILL_TIME_NEVER
        - h5d.FILL_TIME_IFSET
        """
        cdef H5D_fill_time_t fill_time
        H5Pget_fill_time(self.id, &fill_time)
        return <int>fill_time


    @with_phil
    def set_alloc_time(self, int alloc_time):
        """(INT alloc_time)

        Set the storage space allocation time.  One of h5d.ALLOC_TIME*.
        """
        H5Pset_alloc_time(self.id, <H5D_alloc_time_t>alloc_time)


    @with_phil
    def get_alloc_time(self):
        """() => INT alloc_time

        Get the storage space allocation time.  One of h5d.ALLOC_TIME*.
        """
        cdef H5D_alloc_time_t alloc_time
        H5Pget_alloc_time(self.id, &alloc_time)
        return <int>alloc_time


    # === Filter functions ====================================================

    @with_phil
    def set_filter(self, int filter_code, unsigned int flags=0, object values=None):
        """(INT filter_code, UINT flags=0, TUPLE values=None)

        Set a filter in the pipeline.  Params are:

        filter_code
            One of the following:

            - h5z.FILTER_DEFLATE
            - h5z.FILTER_SHUFFLE
            - h5z.FILTER_FLETCHER32
            - h5z.FILTER_SZIP

        flags
            Bit flags (h5z.FLAG*) setting filter properties

        values
            TUPLE of UINTs giving auxiliary data for the filter
        """
        cdef size_t nelements
        cdef unsigned int *cd_values
        cdef int i
        cd_values = NULL

        require_tuple(values, 1, -1, b"values")

        try:
            if values is None or len(values) == 0:
                nelements = 0
                cd_values = NULL
            else:
                nelements = len(values)
                cd_values = <unsigned int*>emalloc(sizeof(unsigned int)*nelements)

                for i in range(nelements):
                    cd_values[i] = int(values[i])

            H5Pset_filter(self.id, <H5Z_filter_t>filter_code, flags, nelements, cd_values)
        finally:
            efree(cd_values)


    @with_phil
    def all_filters_avail(self):
        """() => BOOL

        Determine if all the filters in the pipelist are available to
        the library.
        """
        return <bint>(H5Pall_filters_avail(self.id))


    @with_phil
    def get_nfilters(self):
        """() => INT

        Determine the number of filters in the pipeline.
        """
        return H5Pget_nfilters(self.id)


    @with_phil
    def get_filter(self, int filter_idx):
        """(UINT filter_idx) => TUPLE filter_info

        Get information about a filter, identified by its index.  Tuple
        elements are:

        0. INT filter code (h5z.FILTER*)
        1. UINT flags (h5z.FLAG*)
        2. TUPLE of UINT values; filter aux data (16 values max)
        3. STRING name of filter (256 chars max)
        """
        cdef list vlist
        cdef int filter_code
        cdef unsigned int flags
        cdef size_t nelements
        cdef unsigned int cd_values[16]
        cdef char name[257]
        cdef int i
        nelements = 16 # HDF5 library actually complains if this is too big.

        if filter_idx < 0:
            raise ValueError("Filter index must be a non-negative integer")

        filter_code = <int>H5Pget_filter(self.id, filter_idx, &flags,
                                         &nelements, cd_values, 256, name, NULL)
        name[256] = c'\0'  # in case it's > 256 chars

        vlist = []
        for i in range(nelements):
            vlist.append(cd_values[i])

        return (filter_code, flags, tuple(vlist), name)


    @with_phil
    def _has_filter(self, int filter_code):
        """(INT filter_code)

        Slow & stupid method to determine if a filter is used in this
        property list.  Used because the HDF5 function H5Pget_filter_by_id
        is broken.
        """
        cdef int i, nfilters
        nfilters = self.get_nfilters()
        for i in range(nfilters):
            if self.get_filter(i)[0] == filter_code:
                return True
        return False


    @with_phil
    def get_filter_by_id(self, int filter_code):
        """(INT filter_code) => TUPLE filter_info or None

        Get information about a filter, identified by its code (one
        of h5z.FILTER*).  If the filter doesn't exist, returns None.
        Tuple elements are:

        0. UINT flags (h5z.FLAG*)
        1. TUPLE of UINT values; filter aux data (16 values max)
        2. STRING name of filter (256 chars max)
        """
        cdef list vlist
        cdef unsigned int flags
        cdef size_t nelements
        cdef unsigned int cd_values[16]
        cdef char name[257]
        cdef herr_t retval
        cdef int i
        nelements = 16 # HDF5 library actually complains if this is too big.

        if not self._has_filter(filter_code):
            # Avoid library segfault
            return None

        retval = H5Pget_filter_by_id(self.id, <H5Z_filter_t>filter_code,
                                     &flags, &nelements, cd_values, 256, name, NULL)
        assert nelements <= 16

        name[256] = c'\0'  # In case HDF5 doesn't terminate it properly

        vlist = []
        for i in range(nelements):
            vlist.append(cd_values[i])

        return (flags, tuple(vlist), name)


    @with_phil
    def remove_filter(self, int filter_class):
        """(INT filter_class)

        Remove a filter from the pipeline.  The class code is one of
        h5z.FILTER*.
        """
        H5Premove_filter(self.id, <H5Z_filter_t>filter_class)


    @with_phil
    def set_deflate(self, unsigned int level=5):
        """(UINT level=5)

        Enable deflate (gzip) compression, at the given level.
        Valid levels are 0-9, default is 5.
        """
        H5Pset_deflate(self.id, level)


    @with_phil
    def set_fletcher32(self):
        """()

        Enable Fletcher32 error correction on this list.
        """
        H5Pset_fletcher32(self.id)


    @with_phil
    def set_shuffle(self):
        """()

        Enable to use of the shuffle filter.  Use this immediately before
        the deflate filter to increase the compression ratio.
        """
        H5Pset_shuffle(self.id)


    @with_phil
    def set_szip(self, unsigned int options, unsigned int pixels_per_block):
        """(UINT options, UINT pixels_per_block)

        Enable SZIP compression.  See the HDF5 docs for argument meanings,
        and general restrictions on use of the SZIP format.
        """
        H5Pset_szip(self.id, options, pixels_per_block)


    @with_phil
    def set_scaleoffset(self, H5Z_SO_scale_type_t scale_type, int scale_factor):
        '''(H5Z_SO_scale_type_t scale_type, INT scale_factor)

        Enable scale/offset (usually lossy) compression; lossless (e.g. gzip)
        compression and other filters may be applied on top of this.

        Note that error detection (i.e. fletcher32) cannot precede this in
        the filter chain, or else all reads on lossily-compressed data will
        fail.'''
        H5Pset_scaleoffset(self.id, scale_type, scale_factor)


    # === External dataset functions ===========================================

    @with_phil
    def set_external(self, name, offset, size):
        '''(STR name, UINT offset, UINT size)

        Adds an external file to the list of external files for the dataset.

        The first call sets the external storage property in the property list,
        thus designating that the dataset will be stored in one or more non-HDF5
        file(s) external to the HDF5 file.'''
        H5Pset_external(self.id, name, offset, size)

    @with_phil
    def get_external_count(self):
        """() => INT

        Returns the number of external files for the dataset.
        """
        return <int>(H5Pget_external_count(self.id))

    @with_phil
    def get_external(self, idx=0):
        """(UINT idx=0) => TUPLE external_file_info

        Returns information about the indexed external file.
        Tuple elements are:

        0. STRING name of file (256 chars max)
        1. UINT offset
        2. UINT size
        """
        cdef char name[257]
        cdef off_t offset
        cdef hsize_t size
        cdef herr_t retval

        retval = H5Pget_external(self.id, idx, 256, name, &offset, &size)
        name[256] = c'\0'  # In case HDF5 doesn't terminate name properly

        result = None
        if retval==0:
            result = (name, offset, size)
        return result

    # === Virtual dataset functions ===========================================
    @with_phil
    def set_virtual(self, SpaceID vspace not None, char* src_file_name,
                    char* src_dset_name, SpaceID src_space not None):
        """(SpaceID vspace, STR src_file_name, STR src_dset_name, SpaceID src_space)

        Set the mapping between virtual and source datasets.

        The virtual dataset is described by its virtual dataspace (vspace)
        to the elements. The source dataset is described by the name of the
        file where it is located (src_file_name), the name of the dataset
        (src_dset_name) and its dataspace (src_space).
        """
        H5Pset_virtual(self.id, vspace.id, src_file_name, src_dset_name, src_space.id)

    @with_phil
    def get_virtual_count(self):
        """() => UINT

        Get the number of mappings for the virtual dataset.
        """
        cdef size_t count
        H5Pget_virtual_count(self.id, &count)
        return count

    @with_phil
    def get_virtual_dsetname(self, size_t index=0):
        """(UINT index=0) => STR

        Get the name of a source dataset used in the mapping of the virtual
        dataset at the position index.
        """
        cdef char* name = NULL
        cdef ssize_t size

        size = H5Pget_virtual_dsetname(self.id, index, NULL, 0)
        name = <char*>emalloc(size+1)
        try:
            # TODO check return size
            H5Pget_virtual_dsetname(self.id, index, name, <size_t>size+1)
            src_dset_name = bytes(name).decode('utf-8')
        finally:
            efree(name)

        return src_dset_name

    @with_phil
    def get_virtual_filename(self, size_t index=0):
        """(UINT index=0) => STR

        Get the file name of a source dataset used in the mapping of the
        virtual dataset at the position index.
        """
        cdef char* name = NULL
        cdef ssize_t size

        size = H5Pget_virtual_filename(self.id, index, NULL, 0)
        name = <char*>emalloc(size+1)
        try:
            # TODO check return size
            H5Pget_virtual_filename(self.id, index, name, <size_t>size+1)
            src_fname = bytes(name).decode('utf-8')
        finally:
            efree(name)

        return src_fname

    @with_phil
    def get_virtual_vspace(self, size_t index=0):
        """(UINT index=0) => SpaceID

        Get a dataspace for the selection within the virtual dataset used
        in the mapping.
        """
        return SpaceID(H5Pget_virtual_vspace(self.id, index))

    @with_phil
    def get_virtual_srcspace(self, size_t index=0):
        """(UINT index=0) => SpaceID

        Get a dataspace for the selection within the source dataset used
        in the mapping.
        """
        return SpaceID(H5Pget_virtual_srcspace(self.id, index))

# File access
cdef class PropFAID(PropInstanceID):

    """
        File access property list
    """


    @with_phil
    def set_fclose_degree(self, int close_degree):
        """(INT close_degree)

        Set the file-close degree, which determines library behavior when
        a file is closed when objects are still open.  Legal values:

        * h5f.CLOSE_DEFAULT
        * h5f.CLOSE_WEAK
        * h5f.CLOSE_SEMI
        * h5f.CLOSE_STRONG
        """
        H5Pset_fclose_degree(self.id, <H5F_close_degree_t>close_degree)


    @with_phil
    def get_fclose_degree(self):
        """() => INT close_degree
        - h5fd.
        Get the file-close degree, which determines library behavior when
        a file is closed when objects are still open.  Legal values:

        * h5f.CLOSE_DEFAULT
        * h5f.CLOSE_WEAK
        * h5f.CLOSE_SEMI
        * h5f.CLOSE_STRONG
        """
        cdef H5F_close_degree_t deg
        H5Pget_fclose_degree(self.id, &deg)
        return deg


    @with_phil
    def set_fapl_core(self, size_t block_size=64*1024, hbool_t backing_store=1):
        """(UINT increment=64k, BOOL backing_store=True)

        Use the h5fd.CORE (memory-resident) file driver.

        block_size
            Chunk size for new memory requests (default 64 KiB)

        backing_store
            If True (default), memory contents are associated with an
            on-disk file, which is updated when the file is closed.
            Set to False for a purely in-memory file.
        """
        H5Pset_fapl_core(self.id, block_size, backing_store)


    @with_phil
    def get_fapl_core(self):
        """() => TUPLE core_settings

        Determine settings for the h5fd.CORE (memory-resident) file driver.
        Tuple elements are:

        0. UINT "increment": Chunk size for new memory requests
        1. BOOL "backing_store": If True, write the memory contents to
           disk when the file is closed.
        """
        cdef size_t increment
        cdef hbool_t backing_store
        H5Pget_fapl_core(self.id, &increment, &backing_store)
        return (increment, <bint>(backing_store))


    @with_phil
    def set_fapl_family(self, hsize_t memb_size=2147483647, PropID memb_fapl=None):
        """(UINT memb_size=2**31-1, PropFAID memb_fapl=None)

        Set up the family driver.

        memb_size
            Member file size

        memb_fapl
            File access property list for each member access
        """
        cdef hid_t plist_id
        plist_id = pdefault(memb_fapl)
        H5Pset_fapl_family(self.id, memb_size, plist_id)


    @with_phil
    def get_fapl_family(self):
        """() => TUPLE info

        Determine family driver settings. Tuple values are:

        0. UINT memb_size
        1. PropFAID memb_fapl or None
        """
        cdef hid_t mfapl_id
        cdef hsize_t msize
        cdef PropFAID plist
        plist = None

        H5Pget_fapl_family(self.id, &msize, &mfapl_id)

        if mfapl_id > 0:
            plist = PropFAID(mfapl_id)

        return (msize, plist)


    if ROS3:
        @with_phil
        def set_fapl_ros3(self, char* aws_region="", char* secret_id="",
                          char* secret_key=""):
            """(STRING aws_region, STRING secret_id, STRING secret_key)

            Set up the ros3 driver.
            """
            cdef H5FD_ros3_fapl_t config
            config.version = H5FD_CURR_ROS3_FAPL_T_VERSION
            if len(aws_region) and len(secret_id) and len(secret_key):
                config.authenticate = <hbool_t>1
            else:
                config.authenticate = <hbool_t>0
            config.aws_region = aws_region
            config.secret_id = secret_id
            config.secret_key = secret_key
            H5Pset_fapl_ros3(self.id, &config)


        @with_phil
        def get_fapl_ros3(self):
            """ () => STRUCT config

            Retrieve the ROS3 config
            """
            cdef H5FD_ros3_fapl_t config

            H5Pget_fapl_ros3(self.id, &config)
            return config

        IF HDF5_VERSION >= (1, 14, 2):
            @with_phil
            def get_fapl_ros3_token(self):
                """ () => BYTES token

                Get session token from the file access property list.
                """
                cdef size_t size = 0
                cdef char *token = NULL

                size = H5FD_ROS3_MAX_SECRET_TOK_LEN + 1
                try:
                    token = <char*>emalloc(size)
                    token[0] = 0
                    H5Pget_fapl_ros3_token(self.id, size, token)
                    pytoken = <bytes>token
                finally:
                    efree(token)

                return pytoken


            @with_phil
            def set_fapl_ros3_token(self, char *token=""):
                """ (BYTES token="")

                Set session token in the file access property list.
                """
                H5Pset_fapl_ros3_token(self.id, token)


    @with_phil
    def set_fapl_log(self, char* logfile, unsigned int flags, size_t buf_size):
        """(STRING logfile, UINT flags, UINT buf_size)

        Enable the use of the logging driver.  See the HDF5 documentation
        for details.  Flag constants are stored in module h5fd.
        """
        H5Pset_fapl_log(self.id, logfile, flags, buf_size)


    @with_phil
    def set_fapl_sec2(self):
        """()

        Select the "section-2" driver (h5fd.SEC2).
        """
        H5Pset_fapl_sec2(self.id)

    if DIRECT_VFD:
        @with_phil
        def set_fapl_direct(self, size_t alignment=0, size_t block_size=0, size_t cbuf_size=0):
            """(size_t alignment, size_t block_size, size_t cbuf_size)

            Select the "direct" driver (h5fd.DIRECT).

            Parameters:
                hid_t fapl_id       IN: File access property list identifier
                size_t alignment    IN: Required memory alignment boundary
                size_t block_size   IN: File system block size
                size_t cbuf_size    IN: Copy buffer size

            Properties with value of 0 indicate that the HDF5 library should
            choose the value.
            """
            H5Pset_fapl_direct(self.id, alignment, block_size, cbuf_size)

        @with_phil
        def get_fapl_direct(self):
            """ () => (alignment, block_size, cbuf_size)

            Retrieve the DIRECT VFD config
            """
            cdef size_t alignment
            cdef size_t block_size
            cdef size_t cbuf_size

            H5Pget_fapl_direct(self.id, &alignment, &block_size, &cbuf_size)
            return alignment, block_size, cbuf_size


    @with_phil
    def set_fapl_stdio(self):
        """()

        Select the "stdio" driver (h5fd.STDIO)
        """
        H5Pset_fapl_stdio(self.id)

    @with_phil
    def set_fapl_split(self, const char* meta_ext="-m.h5", PropID meta_plist_id=None, const char* raw_ext="-r.h5", PropID raw_plist_id=None):
        """()

        Select the "split" driver (h5fd.SPLIT)
        """
        H5Pset_fapl_split(self.id, meta_ext, pdefault(meta_plist_id), raw_ext, pdefault(raw_plist_id))


    @with_phil
    def set_driver(self, hid_t driver_id):
        """(INT driver_id)

        Sets the file driver identifier for this file access or data
        transfer property list.
        """
        return H5Pset_driver(self.id, driver_id, NULL)


    @with_phil
    def set_fileobj_driver(self, hid_t driver_id, object fileobj):
        """(INT driver_id, OBJECT fileobj)

        Select the "fileobj" file driver (h5py-specific).
        """
        return H5Pset_driver(self.id, driver_id, <PyObject *>fileobj)


    @with_phil
    def get_driver(self):
        """() => INT driver code

        Return an integer identifier for the driver used by this list.
        Although HDF5 implements these as full-fledged objects, they are
        treated as integers by Python.  Built-in drivers identifiers are
        listed in module h5fd; they are:

        - h5fd.CORE
        - h5fd.FAMILY
        - h5fd.LOG
        - h5fd.MPIO
        - h5fd.MULTI
        - h5fd.SEC2
        - h5fd.DIRECT  (if available)
        - h5fd.STDIO
        - h5fd.ROS3    (if available)
        """
        return H5Pget_driver(self.id)


    @with_phil
    def set_cache(self, int mdc, int rdcc, size_t rdcc_nbytes, double rdcc_w0):
        """(INT mdc, INT rdcc, UINT rdcc_nbytes, DOUBLE rdcc_w0)

        Set the metadata (mdc) and raw data chunk (rdcc) cache properties.
        See the HDF5 docs for a full explanation.
        """
        H5Pset_cache(self.id, mdc, rdcc, rdcc_nbytes, rdcc_w0)


    @with_phil
    def get_cache(self):
        """() => TUPLE cache info

        Get the metadata and raw data chunk cache settings.  See the HDF5
        docs for element definitions.  Return is a 4-tuple with entries:

        1. INT mdc:              Number of metadata objects
        2. INT rdcc:             Number of raw data chunks
        3. UINT rdcc_nbytes:     Size of raw data cache
        4. DOUBLE rdcc_w0:       Preemption policy for data cache.
        """
        cdef int mdc
        cdef size_t rdcc, rdcc_nbytes
        cdef double w0

        H5Pget_cache(self.id, &mdc, &rdcc, &rdcc_nbytes, &w0)
        return (mdc, rdcc, rdcc_nbytes, w0)


    @with_phil
    def set_sieve_buf_size(self, size_t size):
        """ (UINT size)

        Set the maximum size of the data sieve buffer (in bytes).  This
        buffer can improve I/O performance for hyperslab I/O, by combining
        reads and writes into blocks of the given size.  The default is 64k.
        """
        H5Pset_sieve_buf_size(self.id, size)


    @with_phil
    def get_sieve_buf_size(self):
        """ () => UINT size

        Get the current maximum size of the data sieve buffer (in bytes).
        """
        cdef size_t size
        H5Pget_sieve_buf_size(self.id, &size)
        return size


    @with_phil
    def set_libver_bounds(self, int low, int high):
        """ (INT low, INT high)

        Set the compatibility level for file format. Legal values are:

        - h5f.LIBVER_EARLIEST
        - h5f.LIBVER_V18
        - h5f.LIBVER_V110
        - h5f.LIBVER_V112 (HDF5 1.11.4 or later)
        - h5f.LIBVER_V114 (HDF5 1.13.0 or later)
        - h5f.LIBVER_LATEST
        """
        H5Pset_libver_bounds(self.id, <H5F_libver_t>low, <H5F_libver_t>high)

    @with_phil
    def set_meta_block_size(self, size_t size):
        """ (UINT size)

        Set the current minimum size, in bytes, of new metadata block allocations.
        """
        H5Pset_meta_block_size(self.id, size)

    @with_phil
    def get_meta_block_size(self):
        """ () => UINT size

        Get the current minimum size, in bytes, of new metadata block allocations.
        """
        cdef hsize_t size
        H5Pget_meta_block_size(self.id, &size)
        return size

    @with_phil
    def get_libver_bounds(self):
        """ () => (INT low, INT high)

        Get the compatibility level for file format. Returned values are from:

        - h5f.LIBVER_EARLIEST
        - h5f.LIBVER_V18
        - h5f.LIBVER_V110
        - h5f.LIBVER_V112 (HDF5 1.11.4 or later)
        - h5f.LIBVER_V114 (HDF5 1.13.0 or later)
        - h5f.LIBVER_LATEST
        """
        cdef H5F_libver_t low
        cdef H5F_libver_t high
        H5Pget_libver_bounds(self.id, &low, &high)

        return (<int>low, <int>high)

    IF MPI:
        @with_phil
        def set_fapl_mpio(self, comm, info):
            """ (Comm comm, Info info)

            Set MPI-I/O Parallel HDF5 driver.

            Comm: An mpi4py.MPI.Comm instance
            Info: An mpi4py.MPI.Info instance
            """
            from mpi4py.MPI import Comm, Info, _handleof
            assert isinstance(comm, Comm)
            assert isinstance(info, Info)
            cdef Py_uintptr_t _comm = _handleof(comm)
            cdef Py_uintptr_t _info = _handleof(info)
            H5Pset_fapl_mpio(self.id, <MPI_Comm>_comm, <MPI_Info>_info)

        @with_phil
        def get_fapl_mpio(self):
            """ () => (mpi4py.MPI.Comm, mpi4py.MPI.Info)

            Determine mpio driver MPI information.

            0. The mpi4py.MPI.Comm Communicator
            1. The mpi4py.MPI.Comm Info
            """
            cdef MPI_Comm comm
            cdef MPI_Info info
            from mpi4py.MPI import Comm, Info, _addressof

            H5Pget_fapl_mpio(self.id, &comm, &info)

            # TODO: Do we actually need these dup steps? Could we pass the
            # addresses directly to H5Pget_fapl_mpio?
            pycomm = Comm()
            MPI_Comm_dup(comm, <MPI_Comm *>PyLong_AsVoidPtr(_addressof(pycomm)))
            MPI_Comm_free(&comm)

            pyinfo = Info()
            MPI_Info_dup(info, <MPI_Info *>PyLong_AsVoidPtr(_addressof(pyinfo)))
            MPI_Info_free(&info)

            return (pycomm, pyinfo)


        @with_phil
        def set_fapl_mpiposix(self, comm, bint use_gpfs_hints=0):
            """ Obsolete.
            """
            raise RuntimeError("MPI-POSIX driver is broken; removed in h5py 2.3.1")


    @with_phil
    def get_mdc_config(self):
        """() => CacheConfig
        Returns an object that stores all the information about the meta-data cache
        configuration
        """

        cdef CacheConfig config = CacheConfig()

        H5Pget_mdc_config(self.id, &config.cache_config)

        return config


    @with_phil
    def set_mdc_config(self, CacheConfig config not None):
        """(CacheConfig) => None
        Returns an object that stores all the information about the meta-data cache
        configuration
        """
        H5Pset_mdc_config(self.id, &config.cache_config)

    def get_alignment(self):
        """
        Retrieves the current settings for alignment properties from a file access property list.
        """
        cdef hsize_t threshold, alignment
        H5Pget_alignment(self.id, &threshold, &alignment)

        return threshold, alignment

    def set_alignment(self, threshold, alignment):
        """
        Sets alignment properties of a file access property list.
        """
        H5Pset_alignment(self.id, threshold, alignment)

    @with_phil
    def set_file_image(self, image):
        """
        Copy a file image into the property list. Passing None releases
        any image currently loaded. The parameter image must either be
        None or support the buffer protocol.
        """

        cdef Py_buffer buf

        if image is None:
            H5Pset_file_image(self.id, NULL, 0)
            return

        if not PyObject_CheckBuffer(image):
            raise TypeError("image must support the buffer protocol")

        PyObject_GetBuffer(image, &buf, PyBUF_SIMPLE)

        try:
            H5Pset_file_image(self.id, buf.buf, buf.len)
        finally:
            PyBuffer_Release(&buf)

    @with_phil
    def set_page_buffer_size(self, size_t buf_size, unsigned int min_meta_per=0,
                             unsigned int min_raw_per=0):
        """ (LONG buf_size, UINT min_meta_per, UINT min_raw_per)

        Set the maximum size in bytes of the page buffer. The default value is
        zero, meaning that page buffering is disabled. When a non-zero page
        buffer size is set, HDF5 library will enable page buffering if that size
        is larger or equal than a single page size if a paged file space
        strategy was set at file creation.

        The function also allows setting the criteria for metadata and raw data
        page eviction from the buffer. The default values for both are zero.
        """
        H5Pset_page_buffer_size(self.id, buf_size, min_meta_per, min_raw_per)

    @with_phil
    def get_page_buffer_size(self):
        """ () -> (LONG buf_size, UINT min_meta_per, UINT min_raw_per)

        Retrieves the maximum size for the page buffer and the minimum
        percentage for metadata and raw data pages evicition criteria.
        """
        cdef size_t buf_size
        cdef unsigned int min_meta_per, min_raw_per
        H5Pget_page_buffer_size(self.id, &buf_size, &min_meta_per, &min_raw_per)
        return (buf_size, min_meta_per, min_raw_per)

    IF HDF5_VERSION >= (1, 12, 1) or (HDF5_VERSION[:2] == (1, 10) and HDF5_VERSION[2] >= 7):

        @with_phil
        def get_file_locking(self):
            """ () => (BOOL, BOOL)

            Return file locking information as a 2-tuple of boolean:
            (use_file_locking, ignore_when_disabled)
            """
            cdef hbool_t use_file_locking = 0
            cdef hbool_t ignore_when_disabled = 0

            H5Pget_file_locking(self.id, &use_file_locking, &ignore_when_disabled)
            return use_file_locking, ignore_when_disabled

        @with_phil
        def set_file_locking(self, bint use_file_locking, bint ignore_when_disabled):
            """ (BOOL use_file_locking, BOOL ignore_when_disabled)

            Set HDF5 file locking behavior.
            Warning: This setting is overridden by the HDF5_USE_FILE_LOCKING environment variable.
            """
            H5Pset_file_locking(
                self.id, <hbool_t>use_file_locking, <hbool_t>ignore_when_disabled)


# Link creation
cdef class PropLCID(PropCreateID):

    """ Link creation property list """

    @with_phil
    def set_char_encoding(self, int encoding):
        """ (INT encoding)

        Set the character encoding for link names.  Legal values are:

        - h5t.CSET_ASCII
        - h5t.CSET_UTF8
        """
        H5Pset_char_encoding(self.id, <H5T_cset_t>encoding)


    @with_phil
    def get_char_encoding(self):
        """ () => INT encoding

        Get the character encoding for link names.  Legal values are:

        - h5t.CSET_ASCII
        - h5t.CSET_UTF8
        """
        cdef H5T_cset_t encoding
        H5Pget_char_encoding(self.id, &encoding)
        return <int>encoding


    @with_phil
    def set_create_intermediate_group(self, bint create):
        """(BOOL create)

        Set whether missing intermediate groups are automatically created.
        """
        H5Pset_create_intermediate_group(self.id, create)


    @with_phil
    def get_create_intermediate_group(self):
        """() => BOOL

        Determine if missing intermediate groups are automatically created.
        """
        cdef unsigned int create
        H5Pget_create_intermediate_group(self.id, &create)
        return <bint>create

# Link access
cdef class PropLAID(PropInstanceID):

    """ Link access property list """

    def __cinit__(self, *args):
        self._buf = NULL

    def __dealloc__(self):
        efree(self._buf)


    @with_phil
    def set_nlinks(self, size_t nlinks):
        """(UINT nlinks)

        Set the maximum traversal depth for soft links
        """
        H5Pset_nlinks(self.id, nlinks)


    @with_phil
    def get_nlinks(self):
        """() => UINT

        Get the maximum traversal depth for soft links
        """
        cdef size_t nlinks
        H5Pget_nlinks(self.id, &nlinks)
        return nlinks


    @with_phil
    def set_elink_prefix(self, char* prefix):
        """(STRING prefix)

        Set the external link prefix.
        """
        cdef size_t size

        # HDF5 requires that we hang on to this buffer
        efree(self._buf)
        size = strlen(prefix)
        self._buf = <char*>emalloc(size+1)
        strcpy(self._buf, prefix)

        H5Pset_elink_prefix(self.id, self._buf)


    @with_phil
    def get_elink_prefix(self):
        """() => STRING prefix

        Get the external link prefix
        """
        cdef char* buf = NULL
        cdef ssize_t size

        size = H5Pget_elink_prefix(self.id, NULL, 0)
        buf = <char*>emalloc(size+1)
        buf[0] = 0
        try:
            H5Pget_elink_prefix(self.id, buf, size+1)
            pstr = buf
        finally:
            efree(buf)

        return pstr


    @with_phil
    def set_elink_fapl(self, PropID fapl not None):
        """ (PropFAID fapl)

        Set the file access property list used when opening external files.
        """
        H5Pset_elink_fapl(self.id, fapl.id)


    @with_phil
    def get_elink_fapl(self):
        """ () => PropFAID fapl

        Get the file access property list used when opening external files.
        """
        cdef hid_t fid
        fid = H5Pget_elink_fapl(self.id)
        if H5Iget_ref(fid) > 1:
            H5Idec_ref(fid)
        return propwrap(fid)


    @with_phil
    def set_elink_acc_flags(self, unsigned int flags):
        """ (UNIT flags)

        Sets the external link traversal file access flag in a link access property list.
        """
        H5Pset_elink_acc_flags(self.id, flags)


    @with_phil
    def get_elink_acc_flags(self):
        """() => UINT

        Retrieves the external link traversal file access flag from the specified link access property list.
        """
        cdef unsigned int flags
        H5Pget_elink_acc_flags(self.id, &flags)
        return flags


# Datatype creation
cdef class PropTCID(PropOCID):
    """ Datatype creation property list

    No methods yet.
    """

    pass

# Group creation
cdef class PropGCID(PropOCID):
    """ Group creation property list """

    @with_phil
    def set_link_creation_order(self, unsigned int flags):
        """ (UINT flags)

        Set tracking and indexing of creation order for links added to this group

        flags -- h5p.CRT_ORDER_TRACKED, h5p.CRT_ORDER_INDEXED
        """
        H5Pset_link_creation_order(self.id, flags)


    @with_phil
    def get_link_creation_order(self):
        """ () -> UINT flags

        Get tracking and indexing of creation order for links added to this group
        """
        cdef unsigned int flags
        H5Pget_link_creation_order(self.id, &flags)
        return flags


# Object creation property list
cdef class PropOCID(PropCreateID):
    """ Object creation property list

    This seems to be a super class for dataset creation property list
    and group creation property list.

    The documentation is somewhat hazy
    """

    @with_phil
    def set_attr_creation_order(self, unsigned int flags):
        """ (UINT flags)

        Set tracking and indexing of creation order for object attributes

        flags -- h5p.CRT_ORDER_TRACKED, h5p.CRT_ORDER_INDEXED
        """
        H5Pset_attr_creation_order(self.id, flags)


    @with_phil
    def get_attr_creation_order(self):
        """ () -> UINT flags

        Get tracking and indexing of creation order for object attributes
        """
        cdef unsigned int flags
        H5Pget_attr_creation_order(self.id, &flags)
        return flags

    @with_phil
    def set_attr_phase_change(self, max_compact=8, min_dense=6):
        """ (UINT max_compact, UINT min_dense)

        Set threshold value for attribute storage on an object

        max_compact -- maximum number of attributes to be stored in compact storage(default:8)
        must be greater than or equal to min_dense
        min_dense  -- minimum number of attributes to be stored in dense storage(default:6)

        """
        H5Pset_attr_phase_change(self.id, max_compact, min_dense)

    @with_phil
    def get_attr_phase_change(self):
        """ () -> (max_compact, min_dense)

        Retrieves threshold values for attribute storage on an object.

        """
        cdef unsigned int max_compact
        cdef unsigned int min_dense
        H5Pget_attr_phase_change(self.id, &max_compact, &min_dense)
        return (max_compact, min_dense)

    @with_phil
    def set_obj_track_times(self,track_times):
        """Sets the recording of times associated with an object."""
        H5Pset_obj_track_times(self.id,track_times)


    @with_phil
    def get_obj_track_times(self):
        """
        Determines whether times associated with an object are being recorded.
        """

        cdef hbool_t track_times

        H5Pget_obj_track_times(self.id,&track_times)

        return track_times


# Dataset access
cdef class PropDAID(PropInstanceID):

    """ Dataset access property list """

    def __cinit__(self, *args):
        self._efile_prefix_buf = NULL
        self._virtual_prefix_buf = NULL

    def __dealloc__(self):
        efree(self._efile_prefix_buf)
        efree(self._virtual_prefix_buf)

    @with_phil
    def set_chunk_cache(self, size_t rdcc_nslots,size_t rdcc_nbytes, double rdcc_w0):
        """(size_t rdcc_nslots,size_t rdcc_nbytes, double rdcc_w0)

        Sets the raw data chunk cache parameters.
        """
        H5Pset_chunk_cache(self.id,rdcc_nslots,rdcc_nbytes,rdcc_w0)


    @with_phil
    def get_chunk_cache(self):
        """() => TUPLE chunk cache info

        Get the metadata and raw data chunk cache settings.  See the HDF5
        docs for element definitions.  Return is a 3-tuple with entries:

        0. size_t rdcc_nslots: Number of chunk slots in the raw data chunk cache hash table.
        1. size_t rdcc_nbytes: Total size of the raw data chunk cache, in bytes.
        2. DOUBLE rdcc_w0:     Preemption policy.
        """
        cdef size_t rdcc_nslots
        cdef size_t rdcc_nbytes
        cdef double rdcc_w0

        H5Pget_chunk_cache(self.id, &rdcc_nslots, &rdcc_nbytes, &rdcc_w0 )
        return (rdcc_nslots,rdcc_nbytes,rdcc_w0)

    @with_phil
    def get_efile_prefix(self):
        """() => STR

        Get the filesystem path prefix configured for accessing external
        datasets.
        """
        cdef char* cprefix = NULL
        cdef ssize_t size

        size = H5Pget_efile_prefix(self.id, NULL, 0)
        cprefix = <char*>emalloc(size+1)
        cprefix[0] = 0
        try:
            # TODO check return size
            H5Pget_efile_prefix(self.id, cprefix, <size_t>size+1)
            prefix = bytes(cprefix)
        finally:
            efree(cprefix)

        return prefix

    @with_phil
    def set_efile_prefix(self, char* prefix):
        """(STR prefix)

        Set a filesystem path prefix for looking up external datasets.
        This is prepended to all filenames specified in the external dataset.
        """
        cdef size_t size

        # HDF5 requires that we hang on to this buffer
        efree(self._efile_prefix_buf)
        size = strlen(prefix)
        self._efile_prefix_buf = <char*>emalloc(size+1)
        strcpy(self._efile_prefix_buf, prefix)

        H5Pset_efile_prefix(self.id, self._efile_prefix_buf)

    # === Virtual dataset functions ===========================================
    @with_phil
    def set_virtual_view(self, unsigned int view):
        """(UINT view)

        Set the view of the virtual dataset (VDS) to include or exclude
        missing mapped elements.

        If view is set to h5d.VDS_FIRST_MISSING, the view includes all data
        before the first missing mapped data. This setting provides a view
        containing only the continuous data starting with the dataset’s
        first data element. Any break in continuity terminates the view.

        If view is set to h5d.VDS_LAST_AVAILABLE, the view includes all
        available mapped data.

        Missing mapped data is filled with the fill value set in the
        virtual dataset's creation property list.
        """
        H5Pset_virtual_view(self.id, <H5D_vds_view_t>view)

    @with_phil
    def get_virtual_view(self):
        """() => UINT view

        Retrieve the view of the virtual dataset.

        Valid values are:

        - h5d.VDS_FIRST_MISSING
        - h5d.VDS_LAST_AVAILABLE
        """
        cdef H5D_vds_view_t view
        H5Pget_virtual_view(self.id, &view)
        return <unsigned int>view

    @with_phil
    def set_virtual_printf_gap(self, hsize_t gap_size=0):
        """(LONG gap_size=0)

        Set the maximum number of missing source files and/or datasets
        with the printf-style names when getting the extent of an unlimited
        virtual dataset.

        Instruct the library to stop looking for the mapped data stored in
        the files and/or datasets with the printf-style names after not
        finding gap_size files and/or datasets. The found source files and
        datasets will determine the extent of the unlimited virtual dataset
        with the printf-style mappings. Default value: 0.
        """
        H5Pset_virtual_printf_gap(self.id, gap_size)

    @with_phil
    def get_virtual_printf_gap(self):
        """() => LONG gap_size

        Return the maximum number of missing source files and/or datasets
        with the printf-style names when getting the extent for an
        unlimited virtual dataset.
        """
        cdef hsize_t gap_size
        H5Pget_virtual_printf_gap(self.id, &gap_size)
        return gap_size

    @with_phil
    def get_virtual_prefix(self):
        """() => STR

        Get the filesystem path prefix configured for accessing virtual
        datasets.
        """
        cdef char* cprefix = NULL
        cdef ssize_t size

        size = H5Pget_virtual_prefix(self.id, NULL, 0)
        cprefix = <char*>emalloc(size+1)
        cprefix[0] = 0
        try:
            # TODO check return size
            H5Pget_virtual_prefix(self.id, cprefix, <size_t>size+1)
            prefix = bytes(cprefix)
        finally:
            efree(cprefix)

        return prefix

    @with_phil
    def set_virtual_prefix(self, char* prefix):
        """(STR prefix)

        Set a filesystem path prefix for looking up virtual datasets.
        This is prepended to all filenames specified in the virtual dataset.
        """
        cdef size_t size

        # HDF5 requires that we hang on to this buffer
        efree(self._virtual_prefix_buf)
        size = strlen(prefix)
        self._virtual_prefix_buf = <char*>emalloc(size+1)
        strcpy(self._virtual_prefix_buf, prefix)

        H5Pset_virtual_prefix(self.id, self._virtual_prefix_buf)

cdef class PropDXID(PropInstanceID):

    """ Data transfer property list """

    IF MPI:
        def set_dxpl_mpio(self, int xfer_mode):
            """ Set the transfer mode for MPI I/O.
            Must be one of:
            - h5fd.MPIO_INDEPENDENT (default)
            - h5fd.MPIO_COLLECTIVE
            """
            H5Pset_dxpl_mpio(self.id, <H5FD_mpio_xfer_t>xfer_mode)

        def get_dxpl_mpio(self):
            """ Get the current transfer mode for MPI I/O.
            Will be one of:
            - h5fd.MPIO_INDEPENDENT (default)
            - h5fd.MPIO_COLLECTIVE
            """
            cdef H5FD_mpio_xfer_t mode
            H5Pget_dxpl_mpio(self.id, &mode)
            return <int>mode
