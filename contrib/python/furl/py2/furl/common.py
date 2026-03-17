# -*- coding: utf-8 -*-

#
# furl - URL manipulation made simple.
#
# Ansgar Grunseid
# grunseid.com
# grunseid@gmail.com
#
# License: Build Amazing Things (Unlicense)
#

from .compat import string_types


absent = object()


def callable_attr(obj, attr):
    return hasattr(obj, attr) and callable(getattr(obj, attr))


def is_iterable_but_not_string(v):
    return callable_attr(v, '__iter__') and not isinstance(v, string_types)
