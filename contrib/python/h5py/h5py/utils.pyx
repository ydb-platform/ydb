# cython: language_level=3
# cython: profile=False

# This file is part of h5py, a Python interface to the HDF5 library.
#
# http://www.h5py.org
#
# Copyright 2008-2019 Andrew Collette and contributors
#
# License:  Standard 3-clause BSD; see "license.txt" for full license terms
#           and contributor agreement.

from numpy cimport ndarray, import_array,\
                   NPY_UINT16, NPY_UINT32, NPY_UINT64,  npy_intp,\
                   PyArray_SimpleNew, PyArray_FROM_OTF,\
                   NPY_ARRAY_C_CONTIGUOUS, NPY_ARRAY_NOTSWAPPED, NPY_ARRAY_FORCECAST

# Initialization
import_array()

# === Exception-aware memory allocation =======================================

cdef inline void* emalloc(size_t size) except? NULL:
    """Wrapper for malloc(size) with the following behavior:

    1. Always returns NULL for emalloc(0)
    2. Raises MemoryError if allocation fails and returns NULL

    This function expects to be called with the GIL held

    :param size: Size of the memory (in bytes) to allocate
    :return: memory address with the given memory
    """
    cdef void *retval = NULL

    if size == 0:
        return NULL

    retval = malloc(size)
    if retval == NULL:
        errmsg = b"Can't malloc %d bytes" % size
        PyErr_SetString(MemoryError, errmsg)
    return retval


cdef inline void efree(void* what):
    free(what)


def _test_emalloc(size_t size):
    """Stub to simplify unit tests"""
    cdef void* mem
    mem = emalloc(size)
    if size == 0:
        assert mem == NULL
    efree(mem)


# === Testing of NumPy arrays =================================================

cdef int check_numpy(ndarray arr, hid_t space_id, int write):
    # -1 if exception, NOT AUTOMATICALLY CHECKED

    cdef int required_flags
    cdef hsize_t arr_rank
    cdef hsize_t space_rank
    cdef int i

    if arr is None:
        PyErr_SetString(TypeError, b"Array is None")
        return -1

    # Validate array flags

    if write:
        if not (arr.flags["C_CONTIGUOUS"] and arr.flags["WRITEABLE"]):
            PyErr_SetString(TypeError, b"Array must be C-contiguous and writable")
            return -1
    else:
        if not arr.flags["C_CONTIGUOUS"]:
            PyErr_SetString(TypeError, b"Array must be C-contiguous")
            return -1

    return 1

cpdef int check_numpy_write(ndarray arr, hid_t space_id=-1) except -1:
    return check_numpy(arr, space_id, 1)

cpdef int check_numpy_read(ndarray arr, hid_t space_id=-1) except -1:
    return check_numpy(arr, space_id, 0)

# === Conversion between HDF5 buffers and tuples ==============================

cdef int convert_tuple(object tpl, hsize_t *dims, hsize_t rank) except -1:
    # Convert a Python tuple to an hsize_t array.  You must allocate
    # the array yourself and pass both it and the size to this function.
    # Returns 0 on success, -1 on failure and raises an exception.
    cdef int i

    if len(tpl) != rank:
        raise ValueError("Tuple length incompatible with array")

    try:
        for i in range(rank):
            dims[i] = tpl[i]
    except TypeError:
        raise TypeError("Can't convert element %d (%s) to hsize_t" % (i, tpl[i]))

    return 0

cdef object convert_dims(hsize_t* dims, hsize_t rank):
    # Convert an hsize_t array to a Python tuple of ints.

    cdef list dims_list
    cdef int i
    dims_list = []

    for i in range(rank):
        dims_list.append(int(dims[i]))

    return tuple(dims_list)


cdef object create_numpy_hsize(int rank, hsize_t* dims):
    # Create an empty Numpy array which can hold HDF5 hsize_t entries

    cdef int typecode
    cdef npy_intp* dims_npy
    cdef ndarray arr
    cdef int i

    if sizeof(hsize_t) == 2:
        typecode = NPY_UINT16
    elif sizeof(hsize_t) == 4:
        typecode = NPY_UINT32
    elif sizeof(hsize_t) == 8:
        typecode = NPY_UINT64
    else:
        raise RuntimeError("Can't map hsize_t %d to Numpy typecode" % sizeof(hsize_t))

    dims_npy = <npy_intp*>emalloc(sizeof(npy_intp)*rank)

    try:
        for i in range(rank):
            dims_npy[i] = dims[i]
        arr = PyArray_SimpleNew(rank, dims_npy, typecode)
    finally:
        efree(dims_npy)

    return arr

cdef object create_hsize_array(object arr):
    # Create a NumPy array of hsize_t uints initialized to an existing array

    cdef int typecode
    cdef ndarray outarr

    if sizeof(hsize_t) == 2:
        typecode = NPY_UINT16
    elif sizeof(hsize_t) == 4:
        typecode = NPY_UINT32
    elif sizeof(hsize_t) == 8:
        typecode = NPY_UINT64
    else:
        raise RuntimeError("Can't map hsize_t %d to Numpy typecode" % sizeof(hsize_t))

    return PyArray_FROM_OTF(arr, typecode,
        NPY_ARRAY_C_CONTIGUOUS
        | NPY_ARRAY_NOTSWAPPED
        | NPY_ARRAY_FORCECAST)


# === Argument testing ========================================================

cdef int require_tuple(object tpl, int none_allowed, int size, char* name) except -1:
    # Ensure that tpl is in fact a tuple, or None if none_allowed is nonzero.
    # If size >= 0, also ensure that the length matches.
    # Otherwise raises ValueError

    if (tpl is None and none_allowed) or \
      (isinstance(tpl, tuple) and (size < 0 or len(tpl) == size)):
        return 1

    nmsg = b"" if size < 0 else b" of size %d" % size
    smsg = b"" if not none_allowed else b" or None"

    msg = b"%s must be a tuple%s%s." % (name, smsg, nmsg)
    PyErr_SetString(ValueError, msg)
    return -1
