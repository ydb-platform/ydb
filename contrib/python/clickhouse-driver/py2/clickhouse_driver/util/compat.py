import types
import sys


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


if PY3:
    from urllib.parse import parse_qs, urlparse, unquote  # noqa: F401

    string_types = str,
    integer_types = int,
    class_types = type,
    text_type = str
    binary_type = bytes
    range = range
    StandardError = Exception

else:
    from urlparse import parse_qs, urlparse, unquote  # noqa: F401

    string_types = basestring,    # noqa: F821
    integer_types = (int, long)  # noqa: F821
    class_types = (type, types.ClassType)
    text_type = unicode  # noqa: F821
    binary_type = str
    range = xrange  # noqa: F821
    StandardError = StandardError


# from paste.deploy.converters
def asbool(obj):
    if isinstance(obj, string_types):
        obj = obj.strip().lower()
        if obj in ['true', 'yes', 'on', 'y', 't', '1']:
            return True
        elif obj in ['false', 'no', 'off', 'n', 'f', '0']:
            return False
        else:
            raise ValueError('String is not true/false: %r' % obj)
    return bool(obj)
