import sys


PY2 = sys.version_info[0] == 2
PY3 = sys.version_info[0] == 3


if PY2:
    iteritems = lambda x: x.iteritems()
    iterkeys = lambda x: x.iterkeys()
    builtin_module_name = "__builtin__"
else:
    iteritems = lambda x: x.items()
    iterkeys = lambda x: x.keys()
    builtin_module_name = "builtins"
