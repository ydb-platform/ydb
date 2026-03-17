import types
import sys


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


if PY3:
    string_types = str,
    integer_types = int,
    class_types = type,
    text_type = str
    binary_type = bytes

else:
    string_types = basestring,    # noqa: F821
    integer_types = (int, long)  # noqa: F821
    class_types = (type, types.ClassType)
    text_type = unicode  # noqa: F821
    binary_type = str
