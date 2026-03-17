'''
Python2/Python3 compatibility utilities.
'''

import sys
import warnings


IS_PY2 = sys.version_info[0] < 3
IS_PY35 = sys.version_info[0:2] >= (3, 5)


if IS_PY2:
    b2s = lambda s: s
    bytetype = str
    texttype = unicode
    from StringIO import StringIO
    BytesIO = StringIO
else:
    b2s = lambda b: b.decode('utf-8')
    bytetype = bytes
    texttype = str
    from io import BytesIO, StringIO

class utf8reader(object):
    """Takes a utf8-encoded string reader and reads bytes out of it"""

    def __init__(self, str_reader):
        self.str_reader = str_reader

    def read(self, n):
        return self.str_reader.read(n).encode('utf-8')

_str_vs_bytes_warning = '''
ijson works by reading bytes, but a string reader has been given instead. This
probably, but not necessarily, means a file-like object has been opened in text
mode ('t') rather than binary mode ('b').

An automatic conversion is being performed on the fly to continue, but on the
other hand this creates unnecessary encoding/decoding operations that decrease
the efficiency of the system. In the future this automatic conversion will be
removed, and users will receive errors instead of this warning. To avoid this
problem make sure file-like objects are opened in binary mode instead of text
mode.
'''

def _warn_and_return(o):
    warnings.warn(_str_vs_bytes_warning, DeprecationWarning)
    return o

def bytes_reader(f):
    """Returns a file-like object that reads bytes"""
    if type(f.read(0)) == bytetype:
        return f
    return _warn_and_return(utf8reader(f))