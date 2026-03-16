from cpp_common cimport RfEditops, RfOpcodes
from libcpp cimport bool
from libcpp.vector cimport vector


cdef class Editops:
    cdef RfEditops editops

cdef class Opcodes:
    cdef RfOpcodes opcodes

cdef class ScoreAlignment:
    cdef public object score
    cdef public Py_ssize_t src_start
    cdef public Py_ssize_t src_end
    cdef public Py_ssize_t dest_start
    cdef public Py_ssize_t dest_end
