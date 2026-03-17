'''
Wrapper for YAJL C library version 2.x.
'''

from ctypes import byref

from ijson import common, utils
from ijson.backends import _yajl2_ctypes_common


yajl = _yajl2_ctypes_common.get_yajl(2)

# constants defined in yajl_parse.h
YAJL_ALLOW_COMMENTS = 1
YAJL_MULTIPLE_VALUES = 8


@utils.coroutine
def basic_parse_basecoro(target, allow_comments=False, multiple_values=False,
                         use_float=False):
    '''
    Iterator yielding unprefixed events.

    Parameters:

    - f: a readable file-like object with JSON input
    - allow_comments: tells parser to allow comments in JSON input
    - buf_size: a size of an input buffer
    - multiple_values: allows the parser to parse multiple JSON objects
    '''
    callbacks, _keepalive = _yajl2_ctypes_common.make_callbaks(target.send, use_float, 2)
    handle = yajl.yajl_alloc(byref(callbacks), None, None)
    if allow_comments:
        yajl.yajl_config(handle, YAJL_ALLOW_COMMENTS, 1)
    if multiple_values:
        yajl.yajl_config(handle, YAJL_MULTIPLE_VALUES, 1)
    try:
        while True:
            try:
                buffer = (yield)
            except GeneratorExit:
                buffer = b''
            if buffer:
                result = yajl.yajl_parse(handle, buffer, len(buffer))
            else:
                result = yajl.yajl_complete_parse(handle)
            if result != _yajl2_ctypes_common.YAJL_OK:
                error = _yajl2_ctypes_common.yajl_get_error(yajl, handle, buffer)
                exception = common.IncompleteJSONError if result == _yajl2_ctypes_common.YAJL_INSUFFICIENT_DATA else common.JSONError
                raise exception(error)
            if not buffer:
                break
    finally:
        yajl.yajl_free(handle)


common.enrich_backend(globals())
