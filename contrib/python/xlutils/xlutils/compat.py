import sys

PY3 = sys.version_info[0] >= 3

if PY3:
    unicode = str
    basestring = str
    xrange = range
    from io import StringIO
    from io import BytesIO
else:
    unicode = unicode
    basestring = basestring
    xrange = xrange
    from StringIO import StringIO
    from StringIO import StringIO as BytesIO
