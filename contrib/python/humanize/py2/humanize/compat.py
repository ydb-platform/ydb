import sys

if sys.version_info < (3,):
    string_types = (basestring,)  # noqa: F821
else:
    string_types = (str,)
