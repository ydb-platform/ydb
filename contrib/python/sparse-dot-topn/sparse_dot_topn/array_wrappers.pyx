from cpython cimport Py_buffer
from libcpp.vector cimport vector

# define a Cython array wrapper class to hold a C++ vector of ints, adhering to numpy's buffer protocol:
cdef class ArrayWrapper_int:
    # constructor and destructor are fairly unimportant now since
    # vec will be destroyed automatically.

    def __cinit__(self, vector[int]& data):
        self.vec.swap(data)
        self.view_count = 0

    # now implement the buffer protocol for the class
    # which makes it generally useful to anything that expects an array
    def __getbuffer__(self, Py_buffer *buffer, int flags):
        # relevant documentation http://cython.readthedocs.io/en/latest/src/userguide/buffer.html#a-matrix-class
        cdef Py_ssize_t itemsize = sizeof(self.vec[0])

        self.shape[1] = self.vec.size()
        self.shape[0] = 1
        self.strides[1] = <Py_ssize_t>(  <char *>&(self.vec[1]) - <char *>&(self.vec[0]))
        self.strides[0] = self.vec.size() * self.strides[1]
        buffer.buf = <char *>&(self.vec[0])
        buffer.format = 'i'
        buffer.internal = NULL
        buffer.itemsize = itemsize
        buffer.len = self.vec.size() * itemsize   # product(shape) * itemsize
        buffer.ndim = 2
        buffer.obj = self
        buffer.readonly = 0
        buffer.shape = self.shape
        buffer.strides = self.strides
        buffer.suboffsets = NULL
        self.view_count += 1

    def __releasebuffer__(self, Py_buffer *buffer):
        self.view_count -= 1


# define a Cython array wrapper class to hold a C++ vector of doubles, adhering to numpy's buffer protocol:
cdef class ArrayWrapper_double:
    # constructor and destructor are fairly unimportant now since
    # vec will be destroyed automatically.

    def __cinit__(self, vector[double]& data):
        self.vec.swap(data)
        self.view_count = 0

    # now implement the buffer protocol for the class
    # which makes it generally useful to anything that expects an array
    def __getbuffer__(self, Py_buffer *buffer, int flags):
        # relevant documentation http://cython.readthedocs.io/en/latest/src/userguide/buffer.html#a-matrix-class
        cdef Py_ssize_t itemsize = sizeof(self.vec[0])

        self.shape[1] = self.vec.size()
        self.shape[0] = 1
        self.strides[1] = <Py_ssize_t>(  <char *>&(self.vec[1]) - <char *>&(self.vec[0]))
        self.strides[0] = self.vec.size() * self.strides[1]
        buffer.buf = <char *>&(self.vec[0])
        buffer.format = 'd'
        buffer.internal = NULL
        buffer.itemsize = itemsize
        buffer.len = self.vec.size() * itemsize   # product(shape) * itemsize
        buffer.ndim = 2
        buffer.obj = self
        buffer.readonly = 0
        buffer.shape = self.shape
        buffer.strides = self.strides
        buffer.suboffsets = NULL
        self.view_count += 1

    def __releasebuffer__(self, Py_buffer *buffer):
        self.view_count -= 1


# define a Cython array wrapper class to hold a C++ vector of floats, adhering to numpy's buffer protocol:
cdef class ArrayWrapper_float:
    # constructor and destructor are fairly unimportant now since
    # vec will be destroyed automatically.

    def __cinit__(self, vector[float]& data):
        self.vec.swap(data)
        self.view_count = 0

    # now implement the buffer protocol for the class
    # which makes it generally useful to anything that expects an array
    def __getbuffer__(self, Py_buffer *buffer, int flags):
        # relevant documentation http://cython.readthedocs.io/en/latest/src/userguide/buffer.html#a-matrix-class
        cdef Py_ssize_t itemsize = sizeof(self.vec[0])

        self.shape[1] = self.vec.size()
        self.shape[0] = 1
        self.strides[1] = <Py_ssize_t>(  <char *>&(self.vec[1]) - <char *>&(self.vec[0]))
        self.strides[0] = self.vec.size() * self.strides[1]
        buffer.buf = <char *>&(self.vec[0])
        buffer.format = 'f'
        buffer.internal = NULL
        buffer.itemsize = itemsize
        buffer.len = self.vec.size() * itemsize   # product(shape) * itemsize
        buffer.ndim = 2
        buffer.obj = self
        buffer.readonly = 0
        buffer.shape = self.shape
        buffer.strides = self.strides
        buffer.suboffsets = NULL
        self.view_count += 1

    def __releasebuffer__(self, Py_buffer *buffer):
        self.view_count -= 1
