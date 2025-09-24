from libcpp cimport bool

cdef extern from "util/system/sanitizers.h" namespace "NSan" nogil:
    bool ASanIsOn()
    bool TSanIsOn()
    bool MSanIsOn()
