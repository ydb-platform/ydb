from libcpp.vector cimport vector

# define a Cython array wrapper class to hold a C++ vector of ints, adhering to numpy's buffer protocol:
cdef class ArrayWrapper_int:
    cdef int view_count
    cdef vector[int] vec
    cdef Py_ssize_t shape[2]
    cdef Py_ssize_t strides[2]


# define a Cython array wrapper class to hold a C++ vector of doubles, adhering to numpy's buffer protocol:
cdef class ArrayWrapper_double:
    cdef int view_count
    cdef vector[double] vec
    cdef Py_ssize_t shape[2]
    cdef Py_ssize_t strides[2]

# define a Cython array wrapper class to hold a C++ vector of floats, adhering to numpy's buffer protocol:
cdef class ArrayWrapper_float:
    cdef int view_count
    cdef vector[float] vec
    cdef Py_ssize_t shape[2]
    cdef Py_ssize_t strides[2]
