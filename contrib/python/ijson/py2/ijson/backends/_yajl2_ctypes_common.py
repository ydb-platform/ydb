'''
Common ctypes routines for yajl library handling
'''

from ctypes import Structure, c_uint, c_char, c_ubyte, c_int, c_long, c_longlong, c_double,\
                   c_void_p, c_char_p, CFUNCTYPE, POINTER, string_at, cast

from ijson import common, backends
from ijson.compat import b2s


C_EMPTY = CFUNCTYPE(c_int, c_void_p)
C_INT = CFUNCTYPE(c_int, c_void_p, c_int)
C_LONG = CFUNCTYPE(c_int, c_void_p, c_long)
C_LONGLONG = CFUNCTYPE(c_int, c_void_p, c_longlong)
C_DOUBLE = CFUNCTYPE(c_int, c_void_p, c_double)
C_STR = CFUNCTYPE(c_int, c_void_p, POINTER(c_ubyte), c_uint)


def _get_callback_data(yajl_version):
    return  [
        # Mapping of JSON parser events to callback C types and value converters.
        # Used to define the Callbacks structure and actual callback functions
        # inside the parse function.
        ('null', 'null', C_EMPTY, lambda: None),
        ('boolean', 'boolean', C_INT, lambda v: bool(v)),
        ('integer', 'number', C_LONG if yajl_version == 1 else C_LONGLONG, lambda v: int(v)),
        ('double', 'number', C_DOUBLE, lambda v: v),
        ('number', 'number', C_STR, lambda v, l: common.integer_or_decimal(b2s(string_at(v, l)))),
        ('string', 'string', C_STR, lambda v, l: string_at(v, l).decode('utf-8')),
        ('start_map', 'start_map', C_EMPTY, lambda: None),
        ('map_key', 'map_key', C_STR, lambda v, l: string_at(v, l).decode('utf-8')),
        ('end_map', 'end_map', C_EMPTY, lambda: None),
        ('start_array', 'start_array', C_EMPTY, lambda: None),
        ('end_array', 'end_array', C_EMPTY, lambda: None),
    ]


YAJL_OK = 0
YAJL_CANCELLED = 1
YAJL_INSUFFICIENT_DATA = 2
YAJL_ERROR = 3


def get_yajl(version):
    yajl = backends.find_yajl_ctypes(version)
    yajl.yajl_alloc.restype = POINTER(c_char)
    yajl.yajl_get_error.restype = POINTER(c_char)
    return yajl

def _callback(send, use_float, field, event, func_type, func):
    if use_float and field == 'number':
        return func_type()
    def c_callback(_context, *args):
        send((event, func(*args)))
        return 1
    return func_type(c_callback)

def make_callbaks(send, use_float, yajl_version):
    callback_data = _get_callback_data(yajl_version)
    class Callbacks(Structure):
        _fields_ = [(name, type) for name, _, type, _ in callback_data]
    return Callbacks(*[_callback(send, use_float, *data) for data in callback_data])

def yajl_get_error(yajl, handle, buffer):
    perror = yajl.yajl_get_error(handle, 1, buffer, len(buffer))
    error = cast(perror, c_char_p).value
    try:
        error = error.decode('utf-8')
    except UnicodeDecodeError:
        pass
    yajl.yajl_free_error(handle, perror)
    return error
