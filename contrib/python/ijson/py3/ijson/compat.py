'''
Python2/Python3 compatibility utilities.
'''

import sys
import warnings


class utf8reader:
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
    if type(f.read(0)) == bytes:
        return f
    return _warn_and_return(utf8reader(f))