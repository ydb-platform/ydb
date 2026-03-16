"""cdefs for message.py"""

import cython

from ._private.marshaller cimport Marshaller
from .signature cimport Variant, SignatureTree, SignatureType

cdef object ErrorType
cdef object MessageType

cdef object HEADER_PATH
cdef object HEADER_INTERFACE
cdef object HEADER_MEMBER
cdef object HEADER_ERROR_NAME
cdef object HEADER_REPLY_SERIAL
cdef object HEADER_DESTINATION
cdef object HEADER_SENDER
cdef object HEADER_SIGNATURE
cdef object HEADER_UNIX_FDS


cdef object LITTLE_ENDIAN
cdef object PROTOCOL_VERSION

cdef object MESSAGE_FLAG
cdef object MESSAGE_FLAG_NONE
cdef object MESSAGE_TYPE_METHOD_CALL

cdef SignatureTree SIGNATURE_TREE_G
cdef SignatureTree SIGNATURE_TREE_O
cdef SignatureTree SIGNATURE_TREE_S
cdef SignatureTree SIGNATURE_TREE_U

cdef get_signature_tree

cdef class Message:

    cdef public object destination
    cdef public object path
    cdef public object interface
    cdef public object member
    cdef public object message_type
    cdef public object flags
    cdef public object error_name
    cdef public unsigned int reply_serial
    cdef public object sender
    cdef public list unix_fds
    cdef public str signature
    cdef public SignatureTree signature_tree
    cdef public object body
    cdef public unsigned int serial

    @cython.locals(
        body_buffer=bytearray,
        header_buffer=bytearray,
        var=Variant
    )
    cpdef _marshall(self, bint negotiate_unix_fd)

    cdef _fast_init(
        self,
        object destination,
        object path,
        object interface,
        object member,
        object message_type,
        object flags,
        object error_name,
        unsigned int reply_serial,
        object sender,
        list unix_fds,
        SignatureTree signature_tree,
        object body,
        unsigned int serial,
        bint validate
    )
