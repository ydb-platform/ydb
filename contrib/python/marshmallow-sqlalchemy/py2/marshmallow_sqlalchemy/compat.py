# -*- coding: utf-8 -*-
# flake8: noqa
import sys

PY2 = int(sys.version_info[0]) == 2

if PY2:
    iteritems = lambda d: d.iteritems()
else:
    iteritems = lambda d: d.items()


# From six
def with_metaclass(meta, *bases):
    """Create a base class with a metaclass."""
    # This requires a bit of explanation: the basic idea is to make a dummy
    # metaclass for one level of class instantiation that replaces itself with
    # the actual metaclass.
    class metaclass(meta):  # noqa
        def __new__(cls, name, this_bases, d):
            return meta(name, bases, d)

    return type.__new__(metaclass, "temporary_class", (), {})
