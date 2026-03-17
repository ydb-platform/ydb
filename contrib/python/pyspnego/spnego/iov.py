# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import enum
import typing


class BufferType(enum.IntEnum):
    """Buffer types to use for an IOVBuffer type.

    These are the IOVBuffer type flags that can be set for an IOVBuffer. The keys are a generified name for the
    respective SSPI and GSSAPI flags.
    """

    empty = 0  # SECBUFFER_EMPTY | GSS_IOV_BUFFER_TYPE_EMPTY
    data = 1  # SECBUFFER_DATA | GSS_IOV_BUFFER_TYPE_DATA
    header = 2  # SECBUFFER_TOKEN | GSS_IOV_BUFFER_TYPE_HEADER
    pkg_params = 3  # SECBUFFER_PKG_PARAMS | GSS_IOV_BUFFER_TYPE_MECH_PARAMS
    trailer = 7  # SECBUFFER_STREAM_HEADER | GSS_IOV_BUFFER_TYPE_TRAILER
    padding = 9  # SECBUFFER_PADDING | GSS_IOV_BUFFER_TYPE_PADDING
    stream = 10  # SECBUFFER_STREAM | GSS_IOV_BUFFER_TYPE_STREAM
    sign_only = 11  # (SECBUFFER_DATA | SECBUFFER_READONLY_WITH_CHECKSUM) | GSS_IOV_BUFFER_TYPE_SIGN_ONLY
    mic_token = 12  # SECBUFFER_MECHLIST_SIGNATURE | GSS_IOV_BUFFER_TYPE_MIC_TOKEN

    # This doesn't have an equivalent in GSSAPI but is mapped internally to
    # the SSPI data + readonly buffer. Luckily GSSAPI seems to treat an empty
    # buffer as the same as SSPI so that will be used there.
    data_readonly = 4096  # (SECBUFFER_DATA | SECBUFFER_READONLY) | GSS_IOV_BUFFER_TYPE_EMPTY


class IOVBuffer(typing.NamedTuple):
    """A buffer to pass as a list to :meth:`wrap_iov()`.

    Defines the buffer inside a list that is passed to :meth:`wrap_iov()`. A list of these buffers are also returned in
    the `IOVUnwrapResult` under the `buffers` attribute.

    On SSPI only a buffer of the type `header`, `trailer`, or `padding` can be auto allocated. On GSSAPI all buffers
    can be auto allocated when `data=True` but the behaviour behind this is dependent on the mech it is run for.

    On the output from the `*_iov` functions the data is the bytes buffer or `None` if the buffer wasn't set. When used
    as an input to the `*_iov` functions the data can be the buffer bytes, the length of buffer to allocate or a bool
    to state whether the buffer is auto allocated or not.
    """

    type: BufferType  #: The type of IOV buffer
    data: typing.Optional[typing.Union[bytes, int, bool]]  #: The IOV buffer type.


class IOVResBuffer(typing.NamedTuple):
    """Results of an IOV operation.

    Unlike :class:`IOVBuffer` this limits the value of `data` to just an
    optionally set bytes. It is used as the return value of an IOV operation to
    better match what the expected values would be.
    """

    type: BufferType
    data: typing.Optional[bytes]
