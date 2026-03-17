from libcpp.string cimport string

cdef extern from "<iostream>" namespace "std":
    cdef cppclass ostream:
        pass
    cdef cppclass istream:
        pass
    cdef cppclass ostringstream(ostream):
        ostringstream()
        string str()
    cdef cppclass ifstream(istream):
        ifstream(char* filename)
