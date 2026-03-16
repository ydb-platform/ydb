from collections.abc import Mapping, Set
from operator import itemgetter

from .strings import re_tester, re_finder, _re_type


__all__ = ('make_func', 'make_pred')


def make_func(f, test=False):
    if callable(f):
        return f
    elif f is None:
        # pass None to builtin as predicate or mapping function for speed
        return bool if test else lambda x: x
    elif isinstance(f, (bytes, str, _re_type)):
        return re_tester(f) if test else re_finder(f)
    elif isinstance(f, (int, slice)):
        return itemgetter(f)
    elif isinstance(f, Mapping):
        return f.__getitem__
    elif isinstance(f, Set):
        return f.__contains__
    else:
        raise TypeError("Can't make a func from %s" % f.__class__.__name__)

def make_pred(pred):
    return make_func(pred, test=True)
