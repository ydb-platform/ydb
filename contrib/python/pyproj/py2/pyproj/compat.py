# -*- coding: utf-8 -*-
import sys

# Python 2/3 compatibility
if sys.version_info[0] == 2:  # Python 2
    string_types = (basestring,)  # noqa: F821
else:  # Python 3
    string_types = (str,)


def cstrencode(pystr):
    """
    encode a string into bytes.  If already bytes, do nothing.
    """
    try:
        return pystr.encode("utf-8")
    except UnicodeDecodeError:
        return pystr.decode("utf-8").encode("utf-8")
    except AttributeError:
        return pystr  # already bytes


def pystrdecode(cstr):
    """
    Decode a string to a python string.
    """
    if sys.version_info[0] > 2:
        try:
            return cstr.decode("utf-8")
        except AttributeError:
            pass
    return cstr
