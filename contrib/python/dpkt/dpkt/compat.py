from __future__ import absolute_import

from struct import pack, unpack
import sys

if sys.version_info < (3,):
    compat_ord = ord
else:
    def compat_ord(char):
        return char

try:
    from itertools import izip
    compat_izip = izip
except ImportError:
    compat_izip = zip

try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

try:
    from BytesIO import BytesIO
except ImportError:
    from io import BytesIO

if sys.version_info < (3,):
    def iteritems(d, **kw):
        return d.iteritems(**kw)

    def intround(num):
        return int(round(num))

else:
    def iteritems(d, **kw):
        return iter(d.items(**kw))

    # python3 will return an int if you round to 0 decimal places
    intround = round


def ntole(v):
    """convert a 2-byte word from the network byte order (big endian) to little endian;
    replaces socket.ntohs() to work on both little and big endian architectures
    """
    return unpack('<H', pack('!H', v))[0]


def ntole64(v):
    """
    Convert an 8-byte word from network byte order (big endian) to little endian.
    """
    return unpack('<Q', pack('!Q', v))[0]


def isstr(s):
    """True if 's' is an instance of basestring in py2, or of str in py3"""
    bs = getattr(__builtins__, 'basestring', str)
    return isinstance(s, bs)
