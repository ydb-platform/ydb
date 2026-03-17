import re
from operator import methodcaller

from .primitives import EMPTY


__all__ = ['re_iter', 're_all', 're_find', 're_finder', 're_test', 're_tester',
           'str_join',
           'cut_prefix', 'cut_suffix']


def _make_getter(regex):
    if regex.groups == 0:
        return methodcaller('group')
    elif regex.groups == 1 and regex.groupindex == {}:
        return methodcaller('group', 1)
    elif regex.groupindex == {}:
        return methodcaller('groups')
    elif regex.groups == len(regex.groupindex):
        return methodcaller('groupdict')
    else:
        return lambda m: m

_re_type = type(re.compile(r''))  # re.Pattern was added in Python 3.7

def _prepare(regex, flags):
    if not isinstance(regex, _re_type):
        regex = re.compile(regex, flags)
    return regex, _make_getter(regex)


def re_iter(regex, s, flags=0):
    """Iterates over matches of regex in s, presents them in simplest possible form"""
    regex, getter = _prepare(regex, flags)
    return map(getter, regex.finditer(s))

def re_all(regex, s, flags=0):
    """Lists all matches of regex in s, presents them in simplest possible form"""
    return list(re_iter(regex, s, flags))

def re_find(regex, s, flags=0):
    """Matches regex against the given string,
       returns the match in the simplest possible form."""
    return re_finder(regex, flags)(s)

def re_test(regex, s, flags=0):
    """Tests whether regex matches against s."""
    return re_tester(regex, flags)(s)


def re_finder(regex, flags=0):
    """Creates a function finding regex in passed string."""
    regex, _getter = _prepare(regex, flags)
    getter = lambda m: _getter(m) if m else None
    return lambda s: getter(regex.search(s))

def re_tester(regex, flags=0):
    """Creates a predicate testing passed string with regex."""
    if not isinstance(regex, _re_type):
        regex = re.compile(regex, flags)
    return lambda s: bool(regex.search(s))


def str_join(sep, seq=EMPTY):
    """Joins the given sequence with sep.
       Forces stringification of seq items."""
    if seq is EMPTY:
        return str_join('', sep)
    else:
        return sep.join(map(sep.__class__, seq))

def cut_prefix(s, prefix):
    """Cuts prefix from given string if it's present."""
    return s[len(prefix):] if s.startswith(prefix) else s

def cut_suffix(s, suffix):
    """Cuts suffix from given string if it's present."""
    return s[:-len(suffix)] if s.endswith(suffix) else s
