import os
import sys
import types
import platform
import warnings

try:
    import urlparse
except ImportError:  # pragma: no cover
    from urllib import parse as urlparse

try:
    import fcntl
except ImportError:  # pragma: no cover
    fcntl = None  # windows

# True if we are running on Python 3.
PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3

# True if we are running on Windows
WIN = platform.system() == "Windows"

if PY3:  # pragma: no cover
    string_types = (str,)
    integer_types = (int,)
    class_types = (type,)
    text_type = str
    binary_type = bytes
    long = int
else:
    string_types = (basestring,)
    integer_types = (int, long)
    class_types = (type, types.ClassType)
    text_type = unicode
    binary_type = str
    long = long

if PY3:  # pragma: no cover
    from urllib.parse import unquote_to_bytes

    def unquote_bytes_to_wsgi(bytestring):
        return unquote_to_bytes(bytestring).decode("latin-1")


else:
    from urlparse import unquote as unquote_to_bytes

    def unquote_bytes_to_wsgi(bytestring):
        return unquote_to_bytes(bytestring)


def text_(s, encoding="latin-1", errors="strict"):
    """ If ``s`` is an instance of ``binary_type``, return
    ``s.decode(encoding, errors)``, otherwise return ``s``"""
    if isinstance(s, binary_type):
        return s.decode(encoding, errors)
    return s  # pragma: no cover


if PY3:  # pragma: no cover

    def tostr(s):
        if isinstance(s, text_type):
            s = s.encode("latin-1")
        return str(s, "latin-1", "strict")

    def tobytes(s):
        return bytes(s, "latin-1")


else:
    tostr = str

    def tobytes(s):
        return s


if PY3:  # pragma: no cover
    import builtins

    exec_ = getattr(builtins, "exec")

    def reraise(tp, value, tb=None):
        if value is None:
            value = tp
        if value.__traceback__ is not tb:
            raise value.with_traceback(tb)
        raise value

    del builtins

else:  # pragma: no cover

    def exec_(code, globs=None, locs=None):
        """Execute code in a namespace."""
        if globs is None:
            frame = sys._getframe(1)
            globs = frame.f_globals
            if locs is None:
                locs = frame.f_locals
            del frame
        elif locs is None:
            locs = globs
        exec("""exec code in globs, locs""")

    exec_(
        """def reraise(tp, value, tb=None):
    raise tp, value, tb
"""
    )

try:
    from StringIO import StringIO as NativeIO
except ImportError:  # pragma: no cover
    from io import StringIO as NativeIO

try:
    import httplib
except ImportError:  # pragma: no cover
    from http import client as httplib

try:
    MAXINT = sys.maxint
except AttributeError:  # pragma: no cover
    MAXINT = sys.maxsize


# Fix for issue reported in https://github.com/Pylons/waitress/issues/138,
# Python on Windows may not define IPPROTO_IPV6 in socket.
import socket

HAS_IPV6 = socket.has_ipv6

if hasattr(socket, "IPPROTO_IPV6") and hasattr(socket, "IPV6_V6ONLY"):
    IPPROTO_IPV6 = socket.IPPROTO_IPV6
    IPV6_V6ONLY = socket.IPV6_V6ONLY
else:  # pragma: no cover
    if WIN:
        IPPROTO_IPV6 = 41
        IPV6_V6ONLY = 27
    else:
        warnings.warn(
            "OS does not support required IPv6 socket flags. This is requirement "
            "for Waitress. Please open an issue at https://github.com/Pylons/waitress. "
            "IPv6 support has been disabled.",
            RuntimeWarning,
        )
        HAS_IPV6 = False


def set_nonblocking(fd):  # pragma: no cover
    if PY3 and sys.version_info[1] >= 5:
        os.set_blocking(fd, False)
    elif fcntl is None:
        raise RuntimeError("no fcntl module present")
    else:
        flags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
        flags = flags | os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, flags)


if PY3:
    ResourceWarning = ResourceWarning
else:
    ResourceWarning = UserWarning


def qualname(cls):
    if PY3:
        return cls.__qualname__
    return cls.__name__


try:
    import thread
except ImportError:
    # py3
    import _thread as thread
