# distutils: language = c++
#-----------------------------------------------------------------------------
# Copyright (c) 2013-2015, PyStan developers
#
# This file is licensed under Version 3.0 of the GNU General Public
# License. See LICENSE for a text of the license.
#-----------------------------------------------------------------------------

from libcpp cimport bool
from libcpp.string cimport string
from libcpp.vector cimport vector

cdef extern from "stanc.hpp":
    struct PyStancResult:
        int status
        string msg
        string model_cppname
        string cppcode
        vector[string] include_paths
    int stanc(string& model_stancode, string& model_name,
              bool& allow_undefined, string& filename,
              vector[string]& include_paths, PyStancResult&)
