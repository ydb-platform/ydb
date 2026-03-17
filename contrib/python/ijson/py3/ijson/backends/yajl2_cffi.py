'''
CFFI-Wrapper for YAJL C library version 2.x.
'''

from cffi import FFI
import functools

from ijson import common, backends, utils


ffi = FFI()
ffi.cdef("""
typedef void * (*yajl_malloc_func)(void *ctx, size_t sz);
typedef void (*yajl_free_func)(void *ctx, void * ptr);
typedef void * (*yajl_realloc_func)(void *ctx, void * ptr, size_t sz);
typedef struct
{
    yajl_malloc_func malloc;
    yajl_realloc_func realloc;
    yajl_free_func free;
    void * ctx;
} yajl_alloc_funcs;
typedef struct yajl_handle_t * yajl_handle;
typedef enum {
    yajl_status_ok,
    yajl_status_client_canceled,
    yajl_status_error
} yajl_status;
typedef enum {
    yajl_allow_comments = 0x01,
    yajl_dont_validate_strings     = 0x02,
    yajl_allow_trailing_garbage = 0x04,
    yajl_allow_multiple_values = 0x08,
    yajl_allow_partial_values = 0x10
} yajl_option;
typedef struct {
    int (* yajl_null)(void * ctx);
    int (* yajl_boolean)(void * ctx, int boolVal);
    int (* yajl_integer)(void * ctx, long long integerVal);
    int (* yajl_double)(void * ctx, double doubleVal);
    int (* yajl_number)(void * ctx, const char * numberVal,
                        size_t numberLen);
    int (* yajl_string)(void * ctx, const unsigned char * stringVal,
                        size_t stringLen);
    int (* yajl_start_map)(void * ctx);
    int (* yajl_map_key)(void * ctx, const unsigned char * key,
                         size_t stringLen);
    int (* yajl_end_map)(void * ctx);
    int (* yajl_start_array)(void * ctx);
    int (* yajl_end_array)(void * ctx);
} yajl_callbacks;
int yajl_version(void);
yajl_handle yajl_alloc(const yajl_callbacks *callbacks, yajl_alloc_funcs *afs, void *ctx);
int yajl_config(yajl_handle h, yajl_option opt, ...);
yajl_status yajl_parse(yajl_handle hand, const unsigned char *jsonText, size_t jsonTextLength);
yajl_status yajl_complete_parse(yajl_handle hand);
unsigned char* yajl_get_error(yajl_handle hand, int verbose, const unsigned char *jsonText, size_t jsonTextLength);
void yajl_free_error(yajl_handle hand, unsigned char * str);
void yajl_free(yajl_handle handle);
""")


yajl = backends.find_yajl_cffi(ffi, 2)

YAJL_OK = 0
YAJL_CANCELLED = 1
YAJL_INSUFFICIENT_DATA = 2
YAJL_ERROR = 3

# constants defined in yajl_parse.h
YAJL_ALLOW_COMMENTS = 1
YAJL_MULTIPLE_VALUES = 8


def append_event_to_ctx(event):
    def wrapper(func):
        @functools.wraps(func)
        def wrapped(ctx, *args, **kwargs):
            value = func(*args, **kwargs)
            send = ffi.from_handle(ctx)
            send((event, value))
            return 1
        return wrapped
    return wrapper


@ffi.callback('int(void *ctx)')
@append_event_to_ctx('null')
def null():
    return None


@ffi.callback('int(void *ctx, int val)')
@append_event_to_ctx('boolean')
def boolean(val):
    return bool(val)


@ffi.callback('int(void *ctx, long long integerVal)')
@append_event_to_ctx('number')
def integer(val):
    return int(val)


@ffi.callback('int(void * ctx, double doubleVal)')
@append_event_to_ctx('number')
def double(val):
    return val


@ffi.callback('int(void *ctx, const char *numberVal, size_t numberLen)')
@append_event_to_ctx('number')
def number(val, length):
    return common.integer_or_decimal(ffi.string(val, maxlen=length).decode("utf-8"))


@ffi.callback('int(void *ctx, const unsigned char *stringVal, size_t stringLen)')
@append_event_to_ctx('string')
def string(val, length):
    return ffi.string(val, maxlen=length).decode('utf-8')


@ffi.callback('int(void *ctx)')
@append_event_to_ctx('start_map')
def start_map():
    return None


@ffi.callback('int(void *ctx, const unsigned char *key, size_t stringLen)')
@append_event_to_ctx('map_key')
def map_key(key, length):
    return ffi.string(key, maxlen=length).decode('utf-8')


@ffi.callback('int(void *ctx)')
@append_event_to_ctx('end_map')
def end_map():
    return None


@ffi.callback('int(void *ctx)')
@append_event_to_ctx('start_array')
def start_array():
    return None


@ffi.callback('int(void *ctx)')
@append_event_to_ctx('end_array')
def end_array():
    return None


_decimal_callback_data = (
    null, boolean, ffi.NULL, ffi.NULL, number, string,
    start_map, map_key, end_map, start_array, end_array
)

_float_callback_data = (
    null, boolean, integer, double, ffi.NULL, string,
    start_map, map_key, end_map, start_array, end_array
)


def yajl_init(scope, send, allow_comments=False, multiple_values=False, use_float=False):
    scope.ctx = ffi.new_handle(send)
    if use_float:
        scope.callbacks = ffi.new('yajl_callbacks*', _float_callback_data)
    else:
        scope.callbacks = ffi.new('yajl_callbacks*', _decimal_callback_data)
    handle = yajl.yajl_alloc(scope.callbacks, ffi.NULL, scope.ctx)

    if allow_comments:
        yajl.yajl_config(handle, YAJL_ALLOW_COMMENTS, ffi.cast('int', 1))
    if multiple_values:
        yajl.yajl_config(handle, YAJL_MULTIPLE_VALUES, ffi.cast('int', 1))

    return handle


def yajl_parse(handle, buffer):
    if buffer:
        result = yajl.yajl_parse(handle, buffer, len(buffer))
    else:
        result = yajl.yajl_complete_parse(handle)

    if result != YAJL_OK:
        perror = yajl.yajl_get_error(handle, 1, buffer, len(buffer))
        error = ffi.string(perror)
        try:
            error = error.decode('utf8')
        except UnicodeDecodeError:
            pass
        yajl.yajl_free_error(handle, perror)
        exception = common.IncompleteJSONError if result == YAJL_INSUFFICIENT_DATA else common.JSONError
        raise exception(error)


class Container:
    pass


@utils.coroutine
def basic_parse_basecoro(target, **config):
    '''
    Coroutine dispatching unprefixed events.

    Parameters:

    - allow_comments: tells parser to allow comments in JSON input
    - multiple_values: allows the parser to parse multiple JSON objects
    '''

    # the scope objects makes sure the C objects allocated in _yajl.init
    # are kept alive until this function is done
    scope = Container()

    handle = yajl_init(scope, target.send, **config)
    try:
        while True:
            try:
                buffer = (yield)
            except GeneratorExit:
                buffer = b''
            yajl_parse(handle, buffer)
            if not buffer:
                break
    finally:
        yajl.yajl_free(handle)


common.enrich_backend(globals())
