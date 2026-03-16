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
    Low-level interface to the "H5S" family of data-space functions.
"""

include "config.pxi"

# C-level imports
from .utils cimport  require_tuple, convert_dims, convert_tuple, \
                    emalloc, efree, create_numpy_hsize, create_hsize_array
cimport numpy as cnp

#Python level imports
from ._objects import phil, with_phil

cdef object lockid(hid_t id_):
    cdef SpaceID space
    space = SpaceID(id_)
    space.locked = 1
    return space

# === Public constants and data structures ====================================

#enum H5S_seloper_t:
SELECT_NOOP     = H5S_SELECT_NOOP
SELECT_SET      = H5S_SELECT_SET
SELECT_OR       = H5S_SELECT_OR
SELECT_AND      = H5S_SELECT_AND
SELECT_XOR      = H5S_SELECT_XOR
SELECT_NOTB     = H5S_SELECT_NOTB
SELECT_NOTA     = H5S_SELECT_NOTA
SELECT_APPEND   = H5S_SELECT_APPEND
SELECT_PREPEND  = H5S_SELECT_PREPEND
SELECT_INVALID  = H5S_SELECT_INVALID

ALL = lockid(H5S_ALL)   # This is accepted in lieu of an actual identifier
                        # in functions like H5Dread, so wrap it.
UNLIMITED = H5S_UNLIMITED

#enum H5S_class_t
NO_CLASS = H5S_NO_CLASS
SCALAR   = H5S_SCALAR
SIMPLE   = H5S_SIMPLE
globals()["NULL"] = H5S_NULL  # "NULL" is reserved in Cython

#enum H5S_sel_type
SEL_ERROR       = H5S_SEL_ERROR
SEL_NONE        = H5S_SEL_NONE
SEL_POINTS      = H5S_SEL_POINTS
SEL_HYPERSLABS  = H5S_SEL_HYPERSLABS
SEL_ALL         = H5S_SEL_ALL


# === Basic dataspace operations ==============================================

@with_phil
def create(int class_code):
    """(INT class_code) => SpaceID

    Create a new HDF5 dataspace object, of the given class.
    Legal values are SCALAR and SIMPLE.
    """
    return SpaceID(H5Screate(<H5S_class_t>class_code))


@with_phil
def create_simple(object dims_tpl, object max_dims_tpl=None):
    """(TUPLE dims_tpl, TUPLE max_dims_tpl) => SpaceID

    Create a simple (slab) dataspace from a tuple of dimensions.
    Every element of dims_tpl must be a positive integer.

    You can optionally specify the maximum dataspace size. The
    special value UNLIMITED, as an element of max_dims, indicates
    an unlimited dimension.
    """
    cdef int rank
    cdef hsize_t* dims = NULL
    cdef hsize_t* max_dims = NULL

    require_tuple(dims_tpl, 0, -1, b"dims_tpl")
    rank = len(dims_tpl)
    require_tuple(max_dims_tpl, 1, rank, b"max_dims_tpl")

    try:
        dims = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
        convert_tuple(dims_tpl, dims, rank)

        if max_dims_tpl is not None:
            max_dims = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            convert_tuple(max_dims_tpl, max_dims, rank)

        return SpaceID(H5Screate_simple(rank, dims, max_dims))

    finally:
        efree(dims)
        efree(max_dims)


@with_phil
def decode(buf):
    """(STRING buf) => SpaceID

    Unserialize a dataspace.  Bear in mind you can also use the native
    Python pickling machinery to do this.
    """
    cdef char* buf_ = buf
    return SpaceID(H5Sdecode(buf_))


# === H5S class API ===========================================================

cdef class SpaceID(ObjectID):

    """
        Represents a dataspace identifier.

        Properties:

        shape
            Numpy-style shape tuple with dimensions.

        * Hashable: No
        * Equality: Unimplemented
    """

    @property
    def shape(self):
        """ Numpy-style shape tuple representing dimensions.  () == scalar.
        """
        with phil:
            return self.get_simple_extent_dims()


    @with_phil
    def copy(self):
        """() => SpaceID

        Create a new copy of this dataspace.
        """
        return SpaceID(H5Scopy(self.id))


    @with_phil
    def encode(self):
        """() => STRING

        Serialize a dataspace, including its selection.  Bear in mind you
        can also use the native Python pickling machinery to do this.
        """
        cdef void* buf = NULL
        cdef size_t nalloc = 0

        H5Sencode(self.id, NULL, &nalloc)
        buf = emalloc(nalloc)
        try:
            H5Sencode(self.id, buf, &nalloc)
            pystr = PyBytes_FromStringAndSize(<char*>buf, nalloc)
        finally:
            efree(buf)

        return pystr

    def __reduce__(self):
        with phil:
            return (type(self), (-1,), self.encode())


    def __setstate__(self, state):
        cdef char* buf = state
        with phil:
            self.id = H5Sdecode(buf)


    # === Simple dataspaces ===================================================

    @with_phil
    def is_simple(self):
        """() => BOOL is_simple

        Determine if an existing dataspace is "simple" (including scalar
        dataspaces). Currently all HDF5 dataspaces are simple.
        """
        return <bint>(H5Sis_simple(self.id))


    @with_phil
    def offset_simple(self, object offset=None):
        """(TUPLE offset=None)

        Set the offset of a dataspace.  The length of the given tuple must
        match the rank of the dataspace. If None is provided (default),
        the offsets on all axes will be set to 0.
        """
        cdef int rank
        cdef int i
        cdef hssize_t *dims = NULL

        try:
            if not H5Sis_simple(self.id):
                raise ValueError("%d is not a simple dataspace" % self.id)

            rank = H5Sget_simple_extent_ndims(self.id)

            require_tuple(offset, 1, rank, b"offset")
            dims = <hssize_t*>emalloc(sizeof(hssize_t)*rank)
            if(offset is not None):
                convert_tuple(offset, <hsize_t*>dims, rank)
            else:
                # The HDF5 docs say passing in NULL resets the offset to 0.
                # Instead it raises an exception.  Imagine my surprise. We'll
                # do this manually.
                for i in range(rank):
                    dims[i] = 0

            H5Soffset_simple(self.id, dims)

        finally:
            efree(dims)


    @with_phil
    def get_simple_extent_ndims(self):
        """() => INT rank

        Determine the rank of a "simple" (slab) dataspace.
        """
        return H5Sget_simple_extent_ndims(self.id)


    @with_phil
    def get_simple_extent_dims(self, int maxdims=0):
        """(BOOL maxdims=False) => TUPLE shape

        Determine the shape of a "simple" (slab) dataspace.  If "maxdims"
        is True, retrieve the maximum dataspace size instead.
        """
        cdef int rank
        cdef hsize_t* dims = NULL

        if self.get_simple_extent_type() == H5S_NULL:
            return None

        rank = H5Sget_simple_extent_dims(self.id, NULL, NULL)

        dims = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
        try:
            if maxdims:
                H5Sget_simple_extent_dims(self.id, NULL, dims)
            else:
                H5Sget_simple_extent_dims(self.id, dims, NULL)

            return convert_dims(dims, rank)

        finally:
            efree(dims)


    @with_phil
    def get_simple_extent_npoints(self):
        """() => LONG npoints

        Determine the total number of elements in a dataspace.
        """
        return H5Sget_simple_extent_npoints(self.id)


    @with_phil
    def get_simple_extent_type(self):
        """() => INT class_code

        Class code is either SCALAR or SIMPLE.
        """
        return <int>H5Sget_simple_extent_type(self.id)


    # === Extents =============================================================

    @with_phil
    def extent_copy(self, SpaceID source not None):
        """(SpaceID source)

        Replace this dataspace's extent with another's, changing its
        typecode if necessary.
        """
        H5Sextent_copy(self.id, source.id)


    @with_phil
    def set_extent_simple(self, object dims_tpl, object max_dims_tpl=None):
        """(TUPLE dims_tpl, TUPLE max_dims_tpl=None)

        Reset the dataspace extent via a tuple of dimensions.
        Every element of dims_tpl must be a positive integer.

        You can optionally specify the maximum dataspace size. The
        special value UNLIMITED, as an element of max_dims, indicates
        an unlimited dimension.
        """
        cdef int rank
        cdef hsize_t* dims = NULL
        cdef hsize_t* max_dims = NULL

        require_tuple(dims_tpl, 0, -1, b"dims_tpl")
        rank = len(dims_tpl)
        require_tuple(max_dims_tpl, 1, rank, b"max_dims_tpl")

        try:
            dims = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            convert_tuple(dims_tpl, dims, rank)

            if max_dims_tpl is not None:
                max_dims = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
                convert_tuple(max_dims_tpl, max_dims, rank)

            H5Sset_extent_simple(self.id, rank, dims, max_dims)

        finally:
            efree(dims)
            efree(max_dims)


    @with_phil
    def set_extent_none(self):
        """()

        Remove the dataspace extent; typecode changes to NO_CLASS.
        """
        H5Sset_extent_none(self.id)


    # === General selection operations ========================================

    @with_phil
    def get_select_type(self):
        """ () => INT select_code

            Determine selection type.  Return values are:

            - SEL_NONE
            - SEL_ALL
            - SEL_POINTS
            - SEL_HYPERSLABS
        """
        return <int>H5Sget_select_type(self.id)


    @with_phil
    def get_select_npoints(self):
        """() => LONG npoints

        Determine the total number of points currently selected.
        Works for all selection techniques.
        """
        return H5Sget_select_npoints(self.id)


    @with_phil
    def get_select_bounds(self):
        """() => (TUPLE start, TUPLE end)

        Determine the bounding box which exactly contains
        the current selection.
        """
        cdef int rank
        cdef hsize_t *start = NULL
        cdef hsize_t *end = NULL

        rank = H5Sget_simple_extent_ndims(self.id)

        if H5Sget_select_npoints(self.id) == 0:
            return None

        start = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
        end = <hsize_t*>emalloc(sizeof(hsize_t)*rank)

        try:
            H5Sget_select_bounds(self.id, start, end)

            start_tpl = convert_dims(start, rank)
            end_tpl = convert_dims(end, rank)
            return (start_tpl, end_tpl)

        finally:
            efree(start)
            efree(end)

    IF HDF5_VERSION >= (1, 10, 7):
        @with_phil
        def select_shape_same(self, SpaceID space2):
            """(SpaceID space2) => BOOL

            Check if two selections are the same shape. HDF5 may read data
            faster if the source & destination selections are the same shape.
            """
            return <bint>H5Sselect_shape_same(self.id, space2.id)

    @with_phil
    def select_all(self):
        """()

        Select all points in the dataspace.
        """
        H5Sselect_all(self.id)


    @with_phil
    def select_none(self):
        """()

        Deselect entire dataspace.
        """
        H5Sselect_none(self.id)


    @with_phil
    def select_valid(self):
        """() => BOOL

        Determine if the current selection falls within
        the dataspace extent.
        """
        return <bint>(H5Sselect_valid(self.id))


    # === Point selection functions ===========================================

    @with_phil
    def get_select_elem_npoints(self):
        """() => LONG npoints

        Determine the number of elements selected in point-selection mode.
        """
        return H5Sget_select_elem_npoints(self.id)


    @with_phil
    def get_select_elem_pointlist(self):
        """() => NDARRAY

        Get a list of all selected elements.  Return is a Numpy array of
        unsigned ints, with shape ``(<npoints>, <space rank)``.
        """
        cdef hsize_t dims[2]
        cdef cnp.ndarray buf

        dims[0] = H5Sget_select_elem_npoints(self.id)
        dims[1] = H5Sget_simple_extent_ndims(self.id)

        buf = create_numpy_hsize(2, dims)

        H5Sget_select_elem_pointlist(self.id, 0, dims[0], <hsize_t*>buf.data)

        return buf


    @with_phil
    def select_elements(self, object coords, int op=H5S_SELECT_SET):
        """(SEQUENCE coords, INT op=SELECT_SET)

        Select elements by specifying coordinates points.  The argument
        "coords" may be an ndarray or any nested sequence which can be
        converted to an array of uints with the shape::

            (<npoints>, <space rank>)

        Examples::

            >>> obj.shape
            (10, 10)
            >>> obj.select_elements([(1,2), (3,4), (5,9)])

        A zero-length selection (i.e. shape ``(0, <rank>)``) is not allowed
        by the HDF5 library.
        """
        cdef cnp.ndarray hcoords
        cdef size_t nelements

        # The docs say the selection list should be an hsize_t**, but it seems
        # that HDF5 expects the coordinates to be a static, contiguous
        # array.  We simulate that by creating a contiguous NumPy array of
        # a compatible type and initializing it to the input.

        hcoords = create_hsize_array(coords)

        if hcoords.ndim != 2 or hcoords.shape[1] != H5Sget_simple_extent_ndims(self.id):
            raise ValueError("Coordinate array must have shape (<npoints>, %d)" % self.get_simple_extent_ndims())

        nelements = hcoords.shape[0]

        H5Sselect_elements(self.id, <H5S_seloper_t>op, nelements, <hsize_t*>hcoords.data)


    # === Hyperslab selection functions =======================================

    @with_phil
    def get_select_hyper_nblocks(self):
        """() => LONG nblocks

        Get the number of hyperslab blocks currently selected.
        """
        return H5Sget_select_hyper_nblocks(self.id)


    @with_phil
    def get_select_hyper_blocklist(self):
        """() => NDARRAY

        Get the current hyperslab selection.  The returned array has shape::

            (<npoints>, 2, <rank>)

        and can be interpreted as a nested sequence::

            [ (corner_coordinate_1, opposite_coordinate_1), ... ]

        with length equal to the total number of blocks.
        """
        cdef hsize_t dims[3]  # 0=nblocks 1=(#2), 2=rank
        cdef cnp.ndarray buf

        dims[0] = H5Sget_select_hyper_nblocks(self.id)
        dims[1] = 2
        dims[2] = H5Sget_simple_extent_ndims(self.id)

        buf = create_numpy_hsize(3, dims)

        H5Sget_select_hyper_blocklist(self.id, 0, dims[0], <hsize_t*>buf.data)

        return buf


    @with_phil
    def select_hyperslab(self, object start, object count, object stride=None,
                         object block=None, int op=H5S_SELECT_SET):
        """(TUPLE start, TUPLE count, TUPLE stride=None, TUPLE block=None,
             INT op=SELECT_SET)

        Select a block region from an existing dataspace.  See the HDF5
        documentation for the meaning of the "block" and "op" keywords.
        """
        cdef int rank
        cdef hsize_t* start_array = NULL
        cdef hsize_t* count_array = NULL
        cdef hsize_t* stride_array = NULL
        cdef hsize_t* block_array = NULL

        # Dataspace rank.  All provided tuples must match this.
        rank = H5Sget_simple_extent_ndims(self.id)

        require_tuple(start, 0, rank, b"start")
        require_tuple(count, 0, rank, b"count")
        require_tuple(stride, 1, rank, b"stride")
        require_tuple(block, 1, rank, b"block")

        try:
            start_array = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            count_array = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            convert_tuple(start, start_array, rank)
            convert_tuple(count, count_array, rank)

            if stride is not None:
                stride_array = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
                convert_tuple(stride, stride_array, rank)
            if block is not None:
                block_array = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
                convert_tuple(block, block_array, rank)

            H5Sselect_hyperslab(self.id, <H5S_seloper_t>op, start_array,
                                         stride_array, count_array, block_array)

        finally:
            efree(start_array)
            efree(count_array)
            efree(stride_array)
            efree(block_array)

    @with_phil
    def is_regular_hyperslab(self):
        """() => BOOL

        Determine whether a hyperslab selection is regular.
        """
        return <bint>H5Sis_regular_hyperslab(self.id)

    @with_phil
    def get_regular_hyperslab(self):
        """() => (TUPLE start, TUPLE stride, TUPLE count, TUPLE block)

        Retrieve a regular hyperslab selection.
        """
        cdef int rank
        cdef hsize_t* start_array = NULL
        cdef hsize_t* count_array = NULL
        cdef hsize_t* stride_array = NULL
        cdef hsize_t* block_array = NULL
        cdef list start = []
        cdef list stride = []
        cdef list count = []
        cdef list block = []
        cdef int i

        rank = H5Sget_simple_extent_ndims(self.id)
        try:
            start_array = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            stride_array = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            count_array = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            block_array = <hsize_t*>emalloc(sizeof(hsize_t)*rank)
            H5Sget_regular_hyperslab(self.id, start_array, stride_array,
                                     count_array, block_array)

            for i in range(rank):
                start.append(start_array[i])
                stride.append(stride_array[i])
                count.append(count_array[i])
                block.append(block_array[i])

            return (tuple(start), tuple(stride), tuple(count), tuple(block))

        finally:
            efree(start_array)
            efree(stride_array)
            efree(count_array)
            efree(block_array)
