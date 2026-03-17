#cython: freethreading_compatible = True

from cpp_common cimport (
    CreateProcessorContext,
    SetProcessorAttrs,
    conv_sequence,
)
from libcpp cimport bool

from rapidfuzz cimport RF_Preprocessor, RF_String

from array import array

from . import utils_py


cdef extern from "utils_cpp.hpp":
    object default_process_impl(object) except + nogil
    void validate_string(object py_str, const char* err) except +
    RF_String default_process_func(RF_String sentence) except +

def default_process(sentence):
    validate_string(sentence, "sentence must be a String")
    return default_process_impl(sentence)


cdef bool default_process_capi(sentence, RF_String* str_) except False:
    validate_string(sentence, "sentence must be a String")
    proc_str = conv_sequence(sentence)
    try:
        proc_str = default_process_func(proc_str)
    except:
        if proc_str.dtor:
            proc_str.dtor(&proc_str)
        raise

    str_[0] = proc_str
    return True

cdef RF_Preprocessor DefaultProcessContext = CreateProcessorContext(default_process_capi)
SetProcessorAttrs(default_process, utils_py.default_process, &DefaultProcessContext)
