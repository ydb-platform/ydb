# distutils: language = c++
# cython: language_level=2
#-----------------------------------------------------------------------------
# Copyright (c) 2013, Allen B. Riddell
#
# This file is licensed under Version 3.0 of the GNU General Public
# License. See LICENSE for a text of the license.
#-----------------------------------------------------------------------------
from libcpp cimport bool
from pystan.stanc cimport PyStancResult, stanc as c_stanc
from libcpp cimport bool
from libcpp.string cimport string
from libcpp.vector cimport vector

def stanc(bytes model_stancode, bytes model_name,
          bool allow_undefined, bytes filename,
          vector[string] include_paths):
    cdef PyStancResult result
    c_stanc(model_stancode, model_name, allow_undefined,
            filename, include_paths, result)
    result_include_paths = []
    for include_path in result.include_paths:
        result_include_paths.append(include_path.decode('utf-8'))
    return {'status': result.status,
            'msg': result.msg.decode('utf-8'),
            'model_cppname': result.model_cppname.decode('ascii'),
            'cppcode': result.cppcode.decode('ascii'),
            'include_paths' : result_include_paths,
            }
