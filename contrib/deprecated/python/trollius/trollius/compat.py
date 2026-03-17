"""Compatibility helpers for the different Python versions."""

import six
import sys

# Python 2.6 or older?
PY26 = (sys.version_info < (2, 7))

# Python 3.3 or newer?
PY33 = (sys.version_info >= (3, 3))

# Python 3.4 or newer?
PY34 = sys.version_info >= (3, 4)

# Python 3.5 or newer?
PY35 = sys.version_info >= (3, 5)

if six.PY3:
    integer_types = (int,)
    bytes_type = bytes
    text_type = str
    string_types = (bytes, str)
    BYTES_TYPES = (bytes, bytearray, memoryview)
else:
    integer_types = (int, long,)
    bytes_type = str
    text_type = unicode
    string_types = basestring
    if PY26:
        BYTES_TYPES = (str, bytearray, buffer)
    else: # Python 2.7
        BYTES_TYPES = (str, bytearray, memoryview, buffer)


if six.PY3:
    def reraise(tp, value, tb=None):
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value
else:
    exec("""def reraise(tp, value, tb=None):  raise tp, value, tb""")


def flatten_bytes(data):
    """
    Convert bytes-like objects (bytes, bytearray, memoryview, buffer) to
    a bytes string.
    """
    if not isinstance(data, BYTES_TYPES):
        raise TypeError('data argument must be byte-ish (%r)',
                        type(data))
    if PY34:
        # In Python 3.4, socket.send() and bytes.join() accept memoryview
        # and bytearray
        return data
    if not data:
        return b''
    if six.PY2 and isinstance(data, (buffer, bytearray)):
        return str(data)
    elif not PY26 and isinstance(data, memoryview):
        return data.tobytes()
    else:
        return data


def flatten_list_bytes(data):
    """Concatenate a sequence of bytes-like objects."""
    data = map(flatten_bytes, data)
    return b''.join(data)
