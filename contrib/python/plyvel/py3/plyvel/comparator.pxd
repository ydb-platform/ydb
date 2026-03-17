# distutils: language = c++

from libc.string cimport const_char

from leveldb cimport Comparator

cdef extern from "comparator.h":

    Comparator* NewPlyvelCallbackComparator(const_char* name, object comparator) nogil
