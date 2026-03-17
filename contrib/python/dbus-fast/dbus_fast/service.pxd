"""cdefs for service.py"""

import cython

from .message cimport Message
from .signature cimport SignatureTree


cdef class _Method:

    cdef public str name
    cdef public object fn
    cdef public bint disabled
    cdef public object introspection
    cdef public str in_signature
    cdef public str out_signature
    cdef public SignatureTree in_signature_tree
    cdef public SignatureTree out_signature_tree



cdef tuple _real_fn_result_to_body(
    object result,
    SignatureTree signature_tree,
    bint replace_fds
)

cdef class ServiceInterface:

    cdef public str name
    cdef list __methods
    cdef list __properties
    cdef list __signals
    cdef set __buses
    cdef dict __handlers
    cdef dict __handlers_by_name_signature

    @cython.locals(handlers=dict,in_signature=str,method=_Method)
    @staticmethod
    cdef object _get_enabled_handler_by_name_signature(ServiceInterface interface, object bus, object name, object signature)

    @staticmethod
    cdef list _c_msg_body_to_args(Message msg)

    @staticmethod
    cdef tuple _c_fn_result_to_body(
        object result,
        SignatureTree signature_tree,
        bint replace_fds,
    )
