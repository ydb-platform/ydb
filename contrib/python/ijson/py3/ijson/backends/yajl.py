'''
Wrapper for YAJL C library version 1.x.
'''

import ctypes
from ctypes import Structure, c_uint, byref

from ijson import common, utils
from ijson.backends import _yajl2_ctypes_common


yajl = _yajl2_ctypes_common.get_yajl(1)

class Config(Structure):
    _fields_ = [
        ("allowComments", c_uint),
        ("checkUTF8", c_uint)
    ]


@utils.coroutine
def basic_parse_basecoro(target, allow_comments=False, multiple_values=False,
                         use_float=False):
    '''
    Iterator yielding unprefixed events.

    Parameters:

    - f: a readable file-like object with JSON input
    - allow_comments: tells parser to allow comments in JSON input
    - check_utf8: if True, parser will cause an error if input is invalid utf-8
    - buf_size: a size of an input buffer
    '''
    if multiple_values:
        raise ValueError("yajl backend doesn't support multiple_values")
    callbacks, _keepalive = _yajl2_ctypes_common.make_callbaks(target.send, use_float, 1)
    config = Config(allow_comments, True)
    handle = yajl.yajl_alloc(byref(callbacks), byref(config), None, None)
    try:
        while True:
            try:
                buffer = (yield)
            except GeneratorExit:
                buffer = b''
            if buffer:
                result = yajl.yajl_parse(handle, buffer, len(buffer))
            else:
                result = yajl.yajl_parse_complete(handle)
            if result == _yajl2_ctypes_common.YAJL_ERROR:
                error = _yajl2_ctypes_common.yajl_get_error(yajl, handle, buffer)
                raise common.JSONError(error)
            elif not buffer:
                if result == _yajl2_ctypes_common.YAJL_INSUFFICIENT_DATA:
                    raise common.IncompleteJSONError('Incomplete JSON data')
                break
    finally:
        yajl.yajl_free(handle)


common.enrich_backend(
    globals(),
    multiple_values=False,
    invalid_leading_zeros_detection=False,
    incomplete_json_tokens_detection=False,
    int64=ctypes.sizeof(ctypes.c_long) == 0,
)
