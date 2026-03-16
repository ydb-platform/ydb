# cython: language_level=3
"""Class to efficiently select and read data from an HDF5 dataset

This is written in Cython to reduce overhead when reading small amounts of
data. The core of it is translating between numpy-style slicing & indexing and
HDF5's H5Sselect_hyperslab calls.

Python & numpy distinguish indexing a[3] from slicing a single element a[3:4],
but there is no equivalent to this when selecting data in HDF5. So we store a
separate boolean ('scalar') for each dimension to distinguish these cases.
"""
from numpy cimport (
    ndarray, npy_intp, PyArray_ZEROS, PyArray_DATA, import_array,
    PyArray_IsNativeByteOrder,
)
from cpython cimport PyNumber_Index

import numpy as np
from .defs cimport *
from .h5d cimport DatasetID
from .h5s cimport SpaceID
from .h5t cimport TypeID, typewrap, py_create
from .utils cimport emalloc, efree, convert_dims

import_array()


cdef object convert_bools(bint* data, hsize_t rank):
    # Convert a bint array to a Python tuple of bools.
    cdef list bools_l
    cdef int i
    bools_l = []

    for i in range(rank):
        bools_l.append(bool(data[i]))

    return tuple(bools_l)


cdef class Selector:
    cdef SpaceID spaceobj
    cdef hid_t space
    cdef int rank
    cdef bint is_fancy
    cdef hsize_t* dims
    cdef hsize_t* start
    cdef hsize_t* stride
    cdef hsize_t* count
    cdef hsize_t* block
    cdef bint* scalar

    def __cinit__(self, SpaceID space):
        self.spaceobj = space
        self.space = space.id
        self.rank = H5Sget_simple_extent_ndims(self.space)
        self.is_fancy = False

        self.dims = <hsize_t*>emalloc(sizeof(hsize_t) * self.rank)
        self.start = <hsize_t*>emalloc(sizeof(hsize_t) * self.rank)
        self.stride = <hsize_t*>emalloc(sizeof(hsize_t) * self.rank)
        self.count = <hsize_t*>emalloc(sizeof(hsize_t) * self.rank)
        self.block = <hsize_t*>emalloc(sizeof(hsize_t) * self.rank)
        self.scalar = <bint*>emalloc(sizeof(bint) * self.rank)

        H5Sget_simple_extent_dims(self.space, self.dims, NULL)

    def __dealloc__(self):
        efree(self.dims)
        efree(self.start)
        efree(self.stride)
        efree(self.count)
        efree(self.block)
        efree(self.scalar)

    cdef bint apply_args(self, tuple args) except 0:
        """Apply indexing arguments to this Selector object"""
        cdef:
            int nargs, ellipsis_ix, array_ix = -1
            bint seen_ellipsis = False
            int dim_ix = -1
            hsize_t l
            ndarray array_arg

        # If no explicit ellipsis, implicit ellipsis is after args
        nargs = ellipsis_ix = len(args)

        for a in args:
            dim_ix += 1

            if a is Ellipsis:
                # [...] - Ellipsis (fill any unspecified dimensions here)
                if seen_ellipsis:
                    raise ValueError("Only one ellipsis may be used.")
                seen_ellipsis = True

                ellipsis_ix = dim_ix
                nargs -= 1  # Don't count the ... itself
                if nargs > self.rank:
                    raise ValueError(f"{nargs} indexing arguments for {self.rank} dimensions")

                # Skip ahead to the remaining dimensions
                # -1 because the next iteration will increment dim_ix
                dim_ix += self.rank - nargs - 1
                continue

            if dim_ix >= self.rank:
                raise ValueError(f"{nargs} indexing arguments for {self.rank} dimensions")

            # Length of the relevant dimension
            l = self.dims[dim_ix]

            # [0:10] - slicing
            if isinstance(a, slice):
                start, stop, step = a.indices(l)
                # Now if step > 0, then start and stop are in [0, length];
                # if step < 0, they are in [-1, length - 1] (Python 2.6b2 and later;
                # Python issue 3004).

                if step < 1:
                    raise ValueError("Step must be >= 1 (got %d)" % step)
                if stop < start:
                    # list/tuple and numpy consider stop < start to be an empty selection
                    start, count, step = 0, 0, 1
                else:
                    count = 1 + (stop - start - 1) // step

                self.start[dim_ix] = start
                self.stride[dim_ix] = step
                self.count[dim_ix] = count
                self.block[dim_ix] = 1
                self.scalar[dim_ix] = False

                continue

            # [0] - simple integer indices
            try:
                # PyIndex_Check only checks the type - e.g. all numpy arrays
                # pass PyIndex_Check, but only scalar arrays are valid.
                a = PyNumber_Index(a)
            except TypeError:
                pass  # Fall through to check for list/array
            else:
                if a < 0:
                    a += l

                if not 0 <= a < l:
                    if l == 0:
                        msg = f"Index ({a}) out of range for empty dimension"
                    else:
                        msg = f"Index ({a}) out of range for (0-{l-1})"
                    raise IndexError(msg)

                self.start[dim_ix] = a
                self.stride[dim_ix] = 1
                self.count[dim_ix] = 1
                self.block[dim_ix] = 1
                self.scalar[dim_ix] = True

                continue

            # MultiBlockSlice exposes h5py's separate count & block parameters
            # to allow more complex repeating selections.
            if isinstance(a, MultiBlockSlice):
                (
                    self.start[dim_ix],
                    self.stride[dim_ix],
                    self.count[dim_ix],
                    self.block[dim_ix],
                ) = a.indices(l)
                self.scalar[dim_ix] = False

                continue

            # [[0, 2, 10]] - list/array of indices ('fancy indexing')
            if isinstance(a, np.ndarray):
                if a.ndim != 1:
                    raise TypeError("Only 1D arrays allowed for fancy indexing")
            else:
                arr = np.asarray(a)
                if arr.ndim != 1:
                    raise TypeError("Selection can't process %r" % a)
                a = arr
                if a.size == 0:
                    # asarray([]) gives float dtype by default
                    a = a.astype(np.intp)

            if a.dtype.kind == 'b':
                if self.rank == 1:
                    # The dataset machinery should fall back to a faster
                    # alternative (using PointSelection) in this case.
                    # https://github.com/h5py/h5py/issues/2189
                    raise TypeError("Use other code for boolean selection on 1D dataset")
                if a.size != l:
                    raise TypeError("boolean index did not match indexed array")
                a = a.nonzero()[0]
            if not np.issubdtype(a.dtype, np.integer):
                raise TypeError("Indexing arrays must have integer dtypes")
            if array_ix != -1:
                raise TypeError("Only one indexing vector or array is currently allowed for fancy indexing")

            # Convert negative indices to positive
            if np.any(a < 0):
                a = a.copy()
                a[a < 0] += l

            # Bounds check
            if np.any((a < 0) | (a > l)):
                if l == 0:
                    msg = "Fancy indexing out of range for empty dimension"
                else:
                    msg = f"Fancy indexing out of range for (0-{l-1})"
                raise IndexError(msg)

            if np.any(np.diff(a) <= 0):
                raise TypeError("Indexing elements must be in increasing order")

            array_ix = dim_ix
            array_arg = a
            self.start[dim_ix] = 0
            self.stride[dim_ix] = 1
            self.count[dim_ix] = a.shape[0]
            self.block[dim_ix] = 1
            self.scalar[dim_ix] = False

        if nargs < self.rank:
            # Fill in ellipsis or trailing dimensions
            ellipsis_end = ellipsis_ix + (self.rank - nargs)
            for dim_ix in range(ellipsis_ix, ellipsis_end):
                self.start[dim_ix] = 0
                self.stride[dim_ix] = 1
                self.count[dim_ix] = self.dims[dim_ix]
                self.block[dim_ix] = 1
                self.scalar[dim_ix] = False

        if nargs == 0:
            H5Sselect_all(self.space)
            self.is_fancy = False
        elif array_ix != -1:
            self.select_fancy(array_ix, array_arg)
            self.is_fancy = True
        else:
            H5Sselect_hyperslab(self.space, H5S_SELECT_SET, self.start, self.stride, self.count, self.block)
            self.is_fancy = False
        return True

    cdef select_fancy(self, int array_ix, ndarray array_arg):
        """Apply a 'fancy' selection (array of indices) to the dataspace"""
        cdef hsize_t* tmp_start
        cdef hsize_t* tmp_count
        cdef uint64_t i

        H5Sselect_none(self.space)

        tmp_start = <hsize_t*>emalloc(sizeof(hsize_t) * self.rank)
        tmp_count = <hsize_t*>emalloc(sizeof(hsize_t) * self.rank)
        try:
            memcpy(tmp_start, self.start, sizeof(hsize_t) * self.rank)
            memcpy(tmp_count, self.count, sizeof(hsize_t) * self.rank)
            tmp_count[array_ix] = 1

            # Iterate over the array of indices, add each hyperslab to the selection
            for i in array_arg:
                tmp_start[array_ix] = i
                H5Sselect_hyperslab(self.space, H5S_SELECT_OR, tmp_start, self.stride, tmp_count, self.block)
        finally:
            efree(tmp_start)
            efree(tmp_count)


    def make_selection(self, tuple args):
        """Apply indexing/slicing args and create a high-level selection object

        Returns an instance of SimpleSelection or FancySelection, with a copy
        of the selector's dataspace.
        """
        cdef:
            SpaceID space
            tuple shape, start, count, step, scalar, arr_shape
            int arr_rank, i
            npy_intp* arr_shape_p

        self.apply_args(args)
        space = SpaceID(H5Scopy(self.space))

        shape = convert_dims(self.dims, self.rank)
        count = convert_dims(self.count, self.rank)
        block = convert_dims(self.block, self.rank)
        mshape = tuple(c * b for c, b in zip(count, block))

        from ._hl.selections import SimpleSelection, FancySelection

        if self.is_fancy:
            arr_shape = tuple(
                mshape[i] for i in range(self.rank) if not self.scalar[i]
            )
            return FancySelection(shape, space, count, arr_shape)
        else:
            start = convert_dims(self.start, self.rank)
            step = convert_dims(self.stride, self.rank)
            scalar = convert_bools(self.scalar, self.rank)
            return SimpleSelection(shape, space, (start, mshape, step, scalar))


cdef class Reader:
    cdef hid_t dataset
    cdef Selector selector
    cdef TypeID h5_memory_datatype
    cdef int np_typenum
    cdef bint native_byteorder

    def __cinit__(self, DatasetID dsid):
        self.dataset = dsid.id
        self.selector = Selector(dsid.get_space())

        # HDF5 can use e.g. custom float datatypes which don't have an exact
        # match in numpy. Translating it to a numpy dtype chooses the smallest
        # dtype which won't lose any data, then we translate that back to a
        # HDF5 datatype (h5_memory_datatype).
        h5_stored_datatype = typewrap(H5Dget_type(self.dataset))
        np_dtype = h5_stored_datatype.py_dtype()
        self.np_typenum = np_dtype.num
        self.native_byteorder = PyArray_IsNativeByteOrder(ord(np_dtype.byteorder))
        self.h5_memory_datatype = py_create(np_dtype)

    cdef ndarray make_array(self, hsize_t* mshape):
        """Create an array to read the selected data into.

        .apply_args() should be called first, to set self.count and self.scalar.
        Only works for simple numeric dtypes which can be defined with typenum.
        """
        cdef int i, arr_rank = 0
        cdef npy_intp* arr_shape

        arr_shape = <npy_intp*>emalloc(sizeof(npy_intp) * self.selector.rank)
        try:
            # Copy any non-scalar selection dimensions for the array shape
            for i in range(self.selector.rank):
                if not self.selector.scalar[i]:
                    arr_shape[arr_rank] = mshape[i]
                    arr_rank += 1

            arr = PyArray_ZEROS(arr_rank, arr_shape, self.np_typenum, 0)
            if not self.native_byteorder:
                arr = arr.view(arr.dtype.newbyteorder())
        finally:
            efree(arr_shape)

        return arr

    def read(self, tuple args):
        """Index the dataset using args and read into a new numpy array

        Only works for simple numeric dtypes.
        """
        cdef void* buf
        cdef ndarray arr
        cdef hsize_t* mshape
        cdef hid_t mspace
        cdef int i

        self.selector.apply_args(args)

        # The selected length of each dimension is count * block
        mshape = <hsize_t*>emalloc(sizeof(hsize_t) * self.selector.rank)
        try:
            for i in range(self.selector.rank):
                mshape[i] = self.selector.count[i] * self.selector.block[i]
            arr = self.make_array(mshape)
            buf = PyArray_DATA(arr)

            mspace = H5Screate_simple(self.selector.rank, mshape, NULL)
        finally:
            efree(mshape)

        try:
            H5Dread(self.dataset, self.h5_memory_datatype.id, mspace,
                    self.selector.space, H5P_DEFAULT, buf)
        finally:
            H5Sclose(mspace)

        if arr.ndim == 0:
            return arr[()]
        else:
            return arr


class MultiBlockSlice:
    """
        A conceptual extension of the built-in slice object to allow selections
        using start, stride, count and block.

        If given, these parameters will be passed directly to
        H5Sselect_hyperslab. The defaults are start=0, stride=1, block=1,
        count=length, which will select the full extent.

        __init__(start, stride, count, block) => Create a new MultiBlockSlice, storing
            any given selection parameters and using defaults for the others
        start => The offset of the starting element of the specified hyperslab
        stride => The number of elements between the start of one block and the next
        count => The number of blocks to select
        block => The number of elements in each block

    """

    def __init__(self, start=0, stride=1, count=None, block=1):
        if start < 0:
            raise ValueError("Start can't be negative")
        if stride < 1 or (count is not None and count < 1) or block < 1:
            raise ValueError("Stride, count and block can't be 0 or negative")
        if block > stride:
            raise ValueError("Blocks will overlap if block > stride")

        self.start = start
        self.stride = stride
        self.count = count
        self.block = block

    def indices(self, length):
        """Calculate and validate start, stride, count and block for the given length"""
        if self.count is None:
            # Select as many full blocks as possible without exceeding extent
            count = (length - self.start - self.block) // self.stride + 1
            if count < 1:
                raise ValueError(
                    "No full blocks can be selected using {} "
                    "on dimension of length {}".format(self._repr(), length)
                )
        else:
            count = self.count

        end_index = self.start + self.block + (count - 1) * self.stride - 1
        if end_index >= length:
            raise ValueError(
                "{} range ({} - {}) extends beyond maximum index ({})".format(
                    self._repr(count), self.start, end_index, length - 1
                ))

        return self.start, self.stride, count, self.block

    def _repr(self, count=None):
        if count is None:
            count = self.count
        return "{}(start={}, stride={}, count={}, block={})".format(
            self.__class__.__name__, self.start, self.stride, count, self.block
        )

    def __repr__(self):
        return self._repr(count=None)
