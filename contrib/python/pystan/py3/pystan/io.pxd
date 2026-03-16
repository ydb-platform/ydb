# distutils: language = c++

from libcpp cimport bool
from libcpp.map cimport map
from libcpp.pair cimport pair
from libcpp.string cimport string
from libcpp.vector cimport vector


cdef extern from "stan/io/var_context.hpp" namespace "stan::io":
    cdef cppclass var_context:
        bool contains_r(const string&)
        bool contains_i(const string&)
        vector[double] vals_r(const string&)
        vector[size_t] dims_r(const string&)
        vector[int] vals_i(const string&)
        vector[size_t] dims_i(const string&)
        void names_r(vector[string]&)
        void names_i(vector[string]&)
        bool remove(const string&)


cdef extern from "py_var_context.hpp" namespace "pystan::io":
    cdef cppclass py_var_context(var_context):
        py_var_context(map[string, pair[vector[double], vector[size_t]]]&,
                       map[string, pair[vector[int], vector[size_t]]]&)
    cdef get_var_context(const string&)
