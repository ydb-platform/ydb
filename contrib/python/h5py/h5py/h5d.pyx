# cython: language_level=3
## This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2013 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.
"""
    Provides access to the low-level HDF5 "H5D" dataset interface.
"""

include "config.pxi"

# Compile-time imports

from collections import namedtuple
cimport cython
from ._objects cimport pdefault
from numpy cimport ndarray, import_array, PyArray_DATA
from .utils cimport  check_numpy_read, check_numpy_write, \
                     convert_tuple, convert_dims, emalloc, efree
from .h5t cimport TypeID, typewrap, py_create
from .h5s cimport SpaceID
from .h5p cimport PropID, propwrap
from ._proxy cimport dset_rw

from ._objects import phil, with_phil
from cpython cimport PyBUF_ANY_CONTIGUOUS, \
                     PyBuffer_Release, \
                     PyBytes_AsString, \
                     PyBytes_FromStringAndSize, \
                     PyObject_GetBuffer


# Initialization
import_array()

# === Public constants and data structures ====================================

COMPACT     = H5D_COMPACT
CONTIGUOUS  = H5D_CONTIGUOUS
CHUNKED     = H5D_CHUNKED

ALLOC_TIME_DEFAULT  = H5D_ALLOC_TIME_DEFAULT
ALLOC_TIME_LATE     = H5D_ALLOC_TIME_LATE
ALLOC_TIME_EARLY    = H5D_ALLOC_TIME_EARLY
ALLOC_TIME_INCR     = H5D_ALLOC_TIME_INCR

SPACE_STATUS_NOT_ALLOCATED  = H5D_SPACE_STATUS_NOT_ALLOCATED
SPACE_STATUS_PART_ALLOCATED = H5D_SPACE_STATUS_PART_ALLOCATED
SPACE_STATUS_ALLOCATED      = H5D_SPACE_STATUS_ALLOCATED

FILL_TIME_ALLOC = H5D_FILL_TIME_ALLOC
FILL_TIME_NEVER = H5D_FILL_TIME_NEVER
FILL_TIME_IFSET = H5D_FILL_TIME_IFSET

FILL_VALUE_UNDEFINED    = H5D_FILL_VALUE_UNDEFINED
FILL_VALUE_DEFAULT      = H5D_FILL_VALUE_DEFAULT
FILL_VALUE_USER_DEFINED = H5D_FILL_VALUE_USER_DEFINED

VIRTUAL = H5D_VIRTUAL
VDS_FIRST_MISSING   = H5D_VDS_FIRST_MISSING
VDS_LAST_AVAILABLE  = H5D_VDS_LAST_AVAILABLE

StoreInfo = namedtuple('StoreInfo',
                       'chunk_offset, filter_mask, byte_offset, size')

# === Dataset chunk iterator ==================================================

IF HDF5_VERSION >= (1, 12, 3) or (HDF5_VERSION >= (1, 10, 10) and HDF5_VERSION < (1, 10, 99)):

    cdef class _ChunkVisitor:
        cdef int rank
        cdef object func
        cdef object retval

        def __init__(self, rank, func):
            self.rank = rank
            self.func = func
            self.retval = None


    cdef int _cb_chunk_info(const hsize_t *offset, unsigned filter_mask, haddr_t addr, hsize_t size, void *op_data) except -1 with gil:
        """Callback function for chunk iteration. (Not to be used directly.)

        This function is called for every chunk with the following information:
            * offset - Logical position of the chunk’s first element in units of dataset elements
            * filter_mask - Bitmask indicating the filters used when the chunk was written
            * addr - Chunk file address
            * size - Chunk size in bytes, 0 if the chunk does not exist

        Chunk information is converted to a StoreInfo namedtuple and passed as input
        to the user-supplied callback object in the "op_data".

        Feature requires: HDF5 1.10.10 or any later 1.10
                          HDF5 1.12.3 or later

        .. versionadded:: 3.8
        """
        cdef _ChunkVisitor visit
        cdef object chunk_info
        cdef tuple cot

        visit = <_ChunkVisitor>op_data
        if addr != HADDR_UNDEF:
            cot = convert_dims(offset, <hsize_t>visit.rank)
            chunk_info = StoreInfo(cot, filter_mask, addr, size)
        else:
            chunk_info = StoreInfo(None, filter_mask, None, size)

        visit.retval = visit.func(chunk_info)
        if visit.retval is not None:
            return 1
        return 0

# === Dataset operations ======================================================

@with_phil
def create(ObjectID loc not None, object name, TypeID tid not None,
           SpaceID space not None, PropID dcpl=None, PropID lcpl=None,
           PropID dapl = None):
    """ (objectID loc, STRING name or None, TypeID tid, SpaceID space,
         PropDCID dcpl=None, PropID lcpl=None) => DatasetID

    Create a new dataset.  If "name" is None, the dataset will be
    anonymous.
    """
    cdef hid_t dsid
    cdef char* cname = NULL
    if name is not None:
        cname = name

    if cname != NULL:
        dsid = H5Dcreate(loc.id, cname, tid.id, space.id,
                 pdefault(lcpl), pdefault(dcpl), pdefault(dapl))
    else:
        dsid = H5Dcreate_anon(loc.id, tid.id, space.id,
                 pdefault(dcpl), pdefault(dapl))
    return DatasetID(dsid)

@with_phil
def open(ObjectID loc not None, char* name, PropID dapl=None):
    """ (ObjectID loc, STRING name, PropID dapl=None) => DatasetID

    Open an existing dataset attached to a group or file object, by name.

    If specified, dapl may be a dataset access property list.
    """
    return DatasetID(H5Dopen(loc.id, name, pdefault(dapl)))

# --- Proxy functions for safe(r) threading -----------------------------------


cdef class DatasetID(ObjectID):

    """
        Represents an HDF5 dataset identifier.

        Objects of this class may be used in any HDF5 function which expects
        a dataset identifier.  Also, all H5D* functions which take a dataset
        instance as their first argument are presented as methods of this
        class.

        Properties:
        dtype:  Numpy dtype representing the dataset type
        shape:  Numpy-style shape tuple representing the dataspace
        rank:   Integer giving dataset rank

        * Hashable: Yes, unless anonymous
        * Equality: True HDF5 identity if unless anonymous
    """

    @property
    def dtype(self):
        """ Numpy dtype object representing the dataset type """
        # Dataset type can't change
        cdef TypeID tid
        with phil:
            if self._dtype is None:
                tid = self.get_type()
                self._dtype = tid.dtype
            return self._dtype

    @property
    def shape(self):
        """ Numpy-style shape tuple representing the dataspace """
        # Shape can change (DatasetID.extend), so don't cache it
        cdef SpaceID sid
        with phil:
            sid = self.get_space()
            return sid.get_simple_extent_dims()

    @property
    def rank(self):
        """ Integer giving the dataset rank (0 = scalar) """
        cdef SpaceID sid
        with phil:
            sid = self.get_space()
            return sid.get_simple_extent_ndims()


    @with_phil
    def read(self, SpaceID mspace not None, SpaceID fspace not None,
             ndarray arr_obj not None, TypeID mtype=None,
             PropID dxpl=None):
        """ (SpaceID mspace, SpaceID fspace, NDARRAY arr_obj,
             TypeID mtype=None, PropDXID dxpl=None)

            Read data from an HDF5 dataset into a Numpy array.

            It is your responsibility to ensure that the memory dataspace
            provided is compatible with the shape of the Numpy array.  Since a
            wide variety of dataspace configurations are possible, this is not
            checked.  You can easily crash Python by reading in data from too
            large a dataspace.

            If a memory datatype is not specified, one will be auto-created
            based on the array's dtype.

            The provided Numpy array must be writable and C-contiguous.  If
            this is not the case, ValueError will be raised and the read will
            fail.  Keyword dxpl may be a dataset transfer property list.
        """
        cdef hid_t self_id, mtype_id, mspace_id, fspace_id, plist_id
        cdef void* data
        cdef int oldflags

        if mtype is None:
            mtype = py_create(arr_obj.dtype)
        check_numpy_write(arr_obj, -1)

        self_id = self.id
        mtype_id = mtype.id
        mspace_id = mspace.id
        fspace_id = fspace.id
        plist_id = pdefault(dxpl)
        data = PyArray_DATA(arr_obj)

        dset_rw(self_id, mtype_id, mspace_id, fspace_id, plist_id, data, 1)


    @with_phil
    def write(self, SpaceID mspace not None, SpaceID fspace not None,
              ndarray arr_obj not None, TypeID mtype=None,
              PropID dxpl=None):
        """ (SpaceID mspace, SpaceID fspace, NDARRAY arr_obj,
             TypeID mtype=None, PropDXID dxpl=None)

            Write data from a Numpy array to an HDF5 dataset. Keyword dxpl may
            be a dataset transfer property list.

            It is your responsibility to ensure that the memory dataspace
            provided is compatible with the shape of the Numpy array.  Since a
            wide variety of dataspace configurations are possible, this is not
            checked.  You can easily crash Python by writing data from too
            large a dataspace.

            If a memory datatype is not specified, one will be auto-created
            based on the array's dtype.

            The provided Numpy array must be C-contiguous.  If this is not the
            case, ValueError will be raised and the read will fail.
        """
        cdef hid_t self_id, mtype_id, mspace_id, fspace_id, plist_id
        cdef void* data
        cdef int oldflags

        if mtype is None:
            mtype = py_create(arr_obj.dtype)
        check_numpy_read(arr_obj, -1)

        self_id = self.id
        mtype_id = mtype.id
        mspace_id = mspace.id
        fspace_id = fspace.id
        plist_id = pdefault(dxpl)
        data = PyArray_DATA(arr_obj)

        dset_rw(self_id, mtype_id, mspace_id, fspace_id, plist_id, data, 0)


    @with_phil
    def extend(self, tuple shape):
        """ (TUPLE shape)

            Extend the given dataset so it's at least as big as "shape".  Note
            that a dataset may only be extended up to the maximum dimensions of
            its dataspace, which are fixed when the dataset is created.
        """
        cdef int rank
        cdef hid_t space_id = 0
        cdef hsize_t* dims = NULL

        try:
            space_id = H5Dget_space(self.id)
            rank = H5Sget_simple_extent_ndims(space_id)

            if len(shape) != rank:
                raise TypeError("New shape length (%d) must match dataset rank (%d)" % (len(shape), rank))

            dims = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            convert_tuple(shape, dims, rank)
            H5Dextend(self.id, dims)

        finally:
            efree(dims)
            if space_id:
                H5Sclose(space_id)


    @with_phil
    def set_extent(self, tuple shape):
        """ (TUPLE shape)

            Set the size of the dataspace to match the given shape.  If the new
            size is larger in any dimension, it must be compatible with the
            maximum dataspace size.
        """
        cdef int rank
        cdef hid_t space_id = 0
        cdef hsize_t* dims = NULL

        try:
            space_id = H5Dget_space(self.id)
            rank = H5Sget_simple_extent_ndims(space_id)

            if len(shape) != rank:
                raise TypeError("New shape length (%d) must match dataset rank (%d)" % (len(shape), rank))

            dims = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            convert_tuple(shape, dims, rank)
            H5Dset_extent(self.id, dims)

        finally:
            efree(dims)
            if space_id:
                H5Sclose(space_id)


    @with_phil
    def get_space(self):
        """ () => SpaceID

            Create and return a new copy of the dataspace for this dataset.
        """
        return SpaceID(H5Dget_space(self.id))


    @with_phil
    def get_space_status(self):
        """ () => INT space_status_code

            Determine if space has been allocated for a dataset.
            Return value is one of:

            * SPACE_STATUS_NOT_ALLOCATED
            * SPACE_STATUS_PART_ALLOCATED
            * SPACE_STATUS_ALLOCATED
        """
        cdef H5D_space_status_t status
        H5Dget_space_status(self.id, &status)
        return <int>status


    @with_phil
    def get_type(self):
        """ () => TypeID

            Create and return a new copy of the datatype for this dataset.
        """
        return typewrap(H5Dget_type(self.id))


    @with_phil
    def get_create_plist(self):
        """ () => PropDCID

            Create an return a new copy of the dataset creation property list
            used when this dataset was created.
        """
        return propwrap(H5Dget_create_plist(self.id))


    @with_phil
    def get_access_plist(self):
        """ () => PropDAID

            Create an return a new copy of the dataset access property list.
        """
        return propwrap(H5Dget_access_plist(self.id))


    @with_phil
    def get_offset(self):
        """ () => LONG offset or None

            Get the offset of this dataset in the file, in bytes, or None if
            it doesn't have one.  This is always the case for datasets which
            use chunked storage, compact datasets, and datasets for which space
            has not yet been allocated in the file.
        """
        cdef haddr_t offset
        offset = H5Dget_offset(self.id)
        if offset == HADDR_UNDEF:
            return None
        return offset


    @with_phil
    def get_storage_size(self):
        """ () => LONG storage_size

            Report the size of storage, in bytes, that is allocated in the
            file for the dataset's raw data. The reported amount is the storage
            allocated in the written file, which will typically differ from the
            space required to hold a dataset in working memory (any associated
            HDF5 metadata is excluded).

            For contiguous datasets, the returned size equals the current
            allocated size of the raw data. For unfiltered chunked datasets, the
            returned size is the number of allocated chunks times the chunk
            size. For filtered chunked datasets, the returned size is the space
            required to store the filtered data.
        """
        return H5Dget_storage_size(self.id)


    @with_phil
    def flush(self):
        """ no return

        Flushes all buffers associated with a dataset to disk.

        This function causes all buffers associated with a dataset to be
        immediately flushed to disk without removing the data from the cache.

        Use this in SWMR write mode to allow readers to be updated with the
        dataset changes.

        Feature requires: 1.9.178 HDF5
        """
        H5Dflush(self.id)

    @with_phil
    def refresh(self):
        """ no return

        Refreshes all buffers associated with a dataset.

        This function causes all buffers associated with a dataset to be
        cleared and immediately re-loaded with updated contents from disk.

        This function essentially closes the dataset, evicts all metadata
        associated with it from the cache, and then re-opens the dataset.
        The reopened dataset is automatically re-registered with the same ID.

        Use this in SWMR read mode to poll for dataset changes.

        Feature requires: 1.9.178 HDF5
        """
        H5Drefresh(self.id)

    def write_direct_chunk(self, offsets, data, filter_mask=0x00000000, PropID dxpl=None):
        """ (offsets, data, uint32_t filter_mask=0x00000000, PropID dxpl=None)

        This function bypasses any filters HDF5 would normally apply to
        written data. However, calling code may apply filters (e.g. gzip
        compression) itself before writing the data.

        `data` is a Python object that implements the Py_buffer interface.
        In case of a ndarray the shape and dtype are ignored. It's the
        user's responsibility to make sure they are compatible with the
        dataset.

        `filter_mask` is a bit field of up to 32 values. It records which
        filters have been applied to this chunk, of the filter pipeline
        defined for that dataset. Each bit set to `1` means that the filter
        in the corresponding position in the pipeline was not applied.
        So the default value of `0` means that all defined filters have
        been applied to the data before calling this function.
        """

        cdef hid_t dset_id
        cdef hid_t dxpl_id
        cdef hid_t space_id = 0
        cdef hsize_t *offset = NULL
        cdef size_t data_size
        cdef int rank
        cdef Py_buffer view

        dset_id = self.id
        dxpl_id = pdefault(dxpl)
        space_id = H5Dget_space(self.id)
        rank = H5Sget_simple_extent_ndims(space_id)

        if len(offsets) != rank:
            raise TypeError("offset length (%d) must match dataset rank (%d)" % (len(offsets), rank))

        try:
            offset = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            convert_tuple(offsets, offset, rank)
            PyObject_GetBuffer(data, &view, PyBUF_ANY_CONTIGUOUS)
            H5DOwrite_chunk(dset_id, dxpl_id, filter_mask, offset, view.len, view.buf)
        finally:
            efree(offset)
            PyBuffer_Release(&view)
            if space_id:
                H5Sclose(space_id)


    @cython.boundscheck(False)
    @cython.wraparound(False)
    def read_direct_chunk(self, offsets, PropID dxpl=None, unsigned char[::1] out=None):
        """ (offsets, PropID dxpl=None, out=None)

        Reads data to a bytes array directly from a chunk at position
        specified by the `offsets` argument and bypasses any filters HDF5
        would normally apply to the written data. However, the written data
        may be compressed or not.

        Returns a tuple containing the `filter_mask` and the raw data
        storing this chunk as bytes if `out` is None, else as a memoryview.

        `filter_mask` is a bit field of up to 32 values. It records which
        filters have been applied to this chunk, of the filter pipeline
        defined for that dataset. Each bit set to `1` means that the filter
        in the corresponding position in the pipeline was not applied to
        compute the raw data. So the default value of `0` means that all
        defined filters have been applied to the raw data.

        If the `out` argument is not None, it must be a writeable
        contiguous 1D array-like of bytes (e.g., `bytearray` or
        `numpy.ndarray`) and large enough to contain the whole chunk.
        """
        cdef hid_t dset_id
        cdef hid_t dxpl_id
        cdef hid_t space_id
        cdef hsize_t *offset = NULL
        cdef int rank
        cdef uint32_t filters
        cdef hsize_t chunk_bytes, out_bytes
        cdef int nb_offsets = len(offsets)
        cdef void * chunk_buffer

        dset_id = self.id
        dxpl_id = pdefault(dxpl)
        space_id = H5Dget_space(dset_id)
        rank = H5Sget_simple_extent_ndims(space_id)
        H5Sclose(space_id)

        if nb_offsets != rank:
            raise TypeError(
                f"offsets length ({nb_offsets}) must match dataset rank ({rank})"
            )

        offset = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
        try:
            convert_tuple(offsets, offset, rank)
            H5Dget_chunk_storage_size(dset_id, offset, &chunk_bytes)

            if out is None:
                retval = PyBytes_FromStringAndSize(NULL, chunk_bytes)
                chunk_buffer = PyBytes_AsString(retval)
            else:
                out_bytes = out.shape[0]  # Fast way to get out length
                if out_bytes < chunk_bytes:
                    raise ValueError(
                        f"out buffer is only {out_bytes} bytes, {chunk_bytes} bytes required"
                    )
                retval = memoryview(out[:chunk_bytes])
                chunk_buffer = &out[0]

            H5Dread_chunk(dset_id, dxpl_id, offset, &filters, chunk_buffer)
        finally:
            efree(offset)

        return filters, retval

    @with_phil
    def get_num_chunks(self, SpaceID space=None):
        """ (SpaceID space=None) => INT num_chunks

        Retrieve the number of chunks that have nonempty intersection with a
        specified dataspace. Currently, this function only gets the number
        of all written chunks, regardless of the dataspace.

        .. versionadded:: 3.0
        """
        cdef hsize_t num_chunks

        if space is None:
            space = self.get_space()
        H5Dget_num_chunks(self.id, space.id, &num_chunks)
        return num_chunks

    @with_phil
    def get_chunk_info(self, hsize_t index, SpaceID space=None):
        """ (hsize_t index, SpaceID space=None) => StoreInfo

        Retrieve storage information about a chunk specified by its index.

        .. versionadded:: 3.0
        """
        cdef haddr_t byte_offset
        cdef hsize_t size
        cdef hsize_t *chunk_offset
        cdef unsigned filter_mask
        cdef hid_t space_id = 0
        cdef int rank

        if space is None:
            space_id = H5Dget_space(self.id)
        else:
            space_id = space.id

        rank = H5Sget_simple_extent_ndims(space_id)
        chunk_offset = <hsize_t*>emalloc(sizeof(hsize_t) * rank)
        H5Dget_chunk_info(self.id, space_id, index, chunk_offset,
                          &filter_mask, &byte_offset, &size)
        cdef tuple cot = convert_dims(chunk_offset, <hsize_t>rank)
        efree(chunk_offset)

        if space is None:
            H5Sclose(space_id)

        return StoreInfo(cot if byte_offset != HADDR_UNDEF else None,
                         filter_mask,
                         byte_offset if byte_offset != HADDR_UNDEF else None,
                         size)


    @with_phil
    def get_chunk_info_by_coord(self, tuple chunk_offset not None):
        """ (TUPLE chunk_offset) => StoreInfo

        Retrieve information about a chunk specified by the array
        address of the chunk’s first element in each dimension.

        .. versionadded:: 3.0
        """
        cdef haddr_t byte_offset
        cdef hsize_t size
        cdef unsigned filter_mask
        cdef hid_t space_id = 0
        cdef int rank
        cdef hsize_t *co = NULL

        space_id = H5Dget_space(self.id)
        rank = H5Sget_simple_extent_ndims(space_id)
        H5Sclose(space_id)
        co = <hsize_t*>emalloc(sizeof(hsize_t) * rank)
        convert_tuple(chunk_offset, co, rank)
        H5Dget_chunk_info_by_coord(self.id, co,
                                   &filter_mask, &byte_offset,
                                   &size)
        efree(co)

        return StoreInfo(chunk_offset if byte_offset != HADDR_UNDEF else None,
                         filter_mask,
                         byte_offset if byte_offset != HADDR_UNDEF else None,
                         size)

    IF HDF5_VERSION >= (1, 12, 3) or (HDF5_VERSION >= (1, 10, 10) and HDF5_VERSION < (1, 10, 99)):

        @with_phil
        def chunk_iter(self, object func, PropID dxpl=None):
            """(CALLABLE func, PropDXID dxpl=None) => <Return value from func>

            Iterate over each chunk and invoke user-supplied "func" callable object.
            The "func" receives chunk information: logical offset, filter mask,
            file location, and size. Any not-None return value from "func" ends iteration.

            Feature requires: HDF5 1.10.10 or any later 1.10
                            HDF5 1.12.3 or later

            .. versionadded:: 3.8
            """
            cdef int rank
            cdef hid_t space_id
            cdef _ChunkVisitor visit

            space_id = H5Dget_space(self.id)
            rank = H5Sget_simple_extent_ndims(space_id)
            H5Sclose(space_id)
            visit = _ChunkVisitor(rank, func)
            H5Dchunk_iter(self.id, pdefault(dxpl), <H5D_chunk_iter_op_t>_cb_chunk_info, <void*>visit)

            return visit.retval
