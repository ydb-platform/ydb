#-------------------------------------------------------------------------------
# elftools: common/py3compat.py
#
# Python 2/3 compatibility code
#
# Eli Bendersky (eliben@gmail.com)
# This code is in the public domain
#-------------------------------------------------------------------------------
import sys
PY3 = sys.version_info[0] == 3


if PY3:
    import io
    from pathlib import Path

    StringIO = io.StringIO
    BytesIO = io.BytesIO

    # Functions for acting on bytestrings and strings. In Python 2 and 3,
    # strings and bytes are the same and chr/ord can be used to convert between
    # numeric byte values and their string representations. In Python 3, bytes
    # and strings are different types and bytes hold numeric values when
    # iterated over.

    def bytes2hex(b, sep=''):
        if not sep:
            return b.hex()
        return sep.join(map('{:02x}'.format, b))

    def bytes2str(b): return b.decode('latin-1')
    def str2bytes(s): return s.encode('latin-1')
    def int2byte(i): return bytes((i,))
    def byte2int(b): return b

    def iterbytes(b):
        """Return an iterator over the elements of a bytes object.

        For example, for b'abc' yields b'a', b'b' and then b'c'.
        """
        for i in range(len(b)):
            yield b[i:i+1]

    ifilter = filter

    maxint = sys.maxsize

    def path_to_posix(s):
        return Path(s).as_posix()

else:
    import cStringIO
    import os
    import posixpath

    StringIO = BytesIO = cStringIO.StringIO

    def bytes2hex(b, sep=''):
        res = b.encode('hex')
        if not sep:
            return res
        return sep.join(res[i:i+2] for i in range(0, len(res), 2))

    def bytes2str(b): return b
    def str2bytes(s): return s
    int2byte = chr
    byte2int = ord
    def iterbytes(b):
        return iter(b)

    from itertools import ifilter

    maxint = sys.maxint

    def path_to_posix(s):
        return posixpath.join(*os.path.split(s))


def iterkeys(d):
    """Return an iterator over the keys of a dictionary."""
    return getattr(d, 'keys' if PY3 else 'iterkeys')()

def itervalues(d):
    """Return an iterator over the values of a dictionary."""
    return getattr(d, 'values' if PY3 else 'itervalues')()

def iteritems(d):
    """Return an iterator over the items of a dictionary."""
    return getattr(d, 'items' if PY3 else 'iteritems')()

try:
    from collections.abc import Mapping  # python >= 3.3
except ImportError:
    from collections import Mapping  # python < 3.3
