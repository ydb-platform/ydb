from cpython.ref cimport PyObject

from math import degrees, radians


cdef double _DG2RAD = radians(1.)
cdef double _RAD2DG = degrees(1.)
cdef int _DOUBLESIZE = sizeof(double)


cdef extern from "math.h" nogil:
    ctypedef enum:
        HUGE_VAL


cdef extern from "Python.h":
    ctypedef enum:
        PyBUF_WRITABLE
    int PyObject_GetBuffer(PyObject *exporter, Py_buffer *view, int flags)
    void PyBuffer_Release(Py_buffer *view)


cdef class PyBuffWriteManager:
    cdef Py_buffer buffer
    cdef double* data
    cdef public Py_ssize_t len

    def __cinit__(self):
        self.data = NULL

    def __init__(self, object data):
        if PyObject_GetBuffer(<PyObject *>data, &self.buffer, PyBUF_WRITABLE) <> 0:
            raise BufferError("pyproj had a problem getting the buffer from data.")
        self.data = <double *>self.buffer.buf
        self.len = self.buffer.len // self.buffer.itemsize

    def __dealloc__(self):
        PyBuffer_Release(&self.buffer)
        self.data = NULL
