from .errors import ServerException
from .reader import read_binary_str, read_binary_uint8, read_binary_int32


def read_exception(buf, additional_message=None):
    code = read_binary_int32(buf)
    name = read_binary_str(buf)
    message = read_binary_str(buf)
    stack_trace = read_binary_str(buf)
    has_nested = bool(read_binary_uint8(buf))

    new_message = ''

    if additional_message:
        new_message += additional_message + '. '

    if name != 'DB::Exception':
        new_message += name + ". "

    new_message += message + ". Stack trace:\n\n" + stack_trace

    nested = None
    if has_nested:
        nested = read_exception(buf)

    return ServerException(new_message, code, nested=nested)
