#
# Forked from m9dicts.{api,dicts}.
#
# Copyright (C) 2011 - 2015 Red Hat, Inc.
# Copyright (C) 2011 - 2017 Satoru SATOH <ssato redhat.com>
# License: MIT
#
r"""Utility functions to operate on mapping objects such as get, set and merge.

.. versionadded: 0.8.3
   define _update_* and merge functions based on classes in
   :mod:`m9dicts.dicts`

"""
from __future__ import absolute_import
import functools
import operator
import re
import anyconfig.utils


# Merge strategies:
MS_REPLACE = "replace"
MS_NO_REPLACE = "noreplace"
MS_DICTS = "merge_dicts"
MS_DICTS_AND_LISTS = "merge_dicts_and_lists"
MERGE_STRATEGIES = (MS_REPLACE, MS_NO_REPLACE, MS_DICTS, MS_DICTS_AND_LISTS)

PATH_SEPS = ('/', '.')

_JSNP_GET_ARRAY_IDX_REG = re.compile(r"(?:0|[1-9][0-9]*)")
_JSNP_SET_ARRAY_IDX = re.compile(r"(?:0|[1-9][0-9]*|-)")


def _jsnp_unescape(jsn_s):
    """
    Parse and decode given encoded JSON Pointer expression, convert ~1 to
    / and ~0 to ~.

    .. note:: JSON Pointer: http://tools.ietf.org/html/rfc6901

    >>> _jsnp_unescape("/a~1b")
    '/a/b'
    >>> _jsnp_unescape("~1aaa~1~0bbb")
    '/aaa/~bbb'
    """
    return jsn_s.replace('~1', '/').replace('~0', '~')


def _split_path(path, seps=PATH_SEPS):
    """
    Parse path expression and return list of path items.

    :param path: Path expression may contain separator chars.
    :param seps: Separator char candidates.
    :return: A list of keys to fetch object[s] later.

    >>> assert _split_path('') == []
    >>> assert _split_path('/') == ['']  # JSON Pointer spec expects this.
    >>> for p in ('/a', '.a', 'a', 'a.'):
    ...     assert _split_path(p) == ['a'], p
    >>> assert _split_path('/a/b/c') == _split_path('a.b.c') == ['a', 'b', 'c']
    >>> assert _split_path('abc') == ['abc']
    """
    if not path:
        return []

    for sep in seps:
        if sep in path:
            if path == sep:  # Special case, '/' or '.' only.
                return ['']
            return [x for x in path.split(sep) if x]

    return [path]


def mk_nested_dic(path, val, seps=PATH_SEPS):
    """
    Make a nested dict iteratively.

    :param path: Path expression to make a nested dict
    :param val: Value to set
    :param seps: Separator char candidates

    >>> mk_nested_dic("a.b.c", 1)
    {'a': {'b': {'c': 1}}}
    >>> mk_nested_dic("/a/b/c", 1)
    {'a': {'b': {'c': 1}}}
    """
    ret = None
    for key in reversed(_split_path(path, seps)):
        ret = {key: val if ret is None else ret.copy()}

    return ret


def get(dic, path, seps=PATH_SEPS, idx_reg=_JSNP_GET_ARRAY_IDX_REG):
    """getter for nested dicts.

    :param dic: a dict[-like] object
    :param path: Path expression to point object wanted
    :param seps: Separator char candidates
    :return: A tuple of (result_object, error_message)

    >>> d = {'a': {'b': {'c': 0, 'd': [1, 2]}}, '': 3}
    >>> assert get(d, '/') == (3, '')  # key becomes '' (empty string).
    >>> assert get(d, "/a/b/c") == (0, '')
    >>> sorted(get(d, "a.b")[0].items())
    [('c', 0), ('d', [1, 2])]
    >>> (get(d, "a.b.d"), get(d, "/a/b/d/1"))
    (([1, 2], ''), (2, ''))
    >>> get(d, "a.b.key_not_exist")  # doctest: +ELLIPSIS
    (None, "'...'")
    >>> get(d, "/a/b/d/2")
    (None, 'list index out of range')
    >>> get(d, "/a/b/d/-")  # doctest: +ELLIPSIS
    (None, 'list indices must be integers...')
    """
    items = [_jsnp_unescape(p) for p in _split_path(path, seps)]
    if not items:
        return (dic, '')
    try:
        if len(items) == 1:
            return (dic[items[0]], '')

        prnt = functools.reduce(operator.getitem, items[:-1], dic)
        arr = anyconfig.utils.is_list_like(prnt) and idx_reg.match(items[-1])
        return (prnt[int(items[-1])], '') if arr else (prnt[items[-1]], '')

    except (TypeError, KeyError, IndexError) as exc:
        return (None, str(exc))


def set_(dic, path, val, seps=PATH_SEPS):
    """setter for nested dicts.

    :param dic: a dict[-like] object support recursive merge operations
    :param path: Path expression to point object wanted
    :param seps: Separator char candidates

    >>> d = dict(a=1, b=dict(c=2, ))
    >>> set_(d, 'a.b.d', 3)
    >>> d['a']['b']['d']
    3
    """
    merge(dic, mk_nested_dic(path, val, seps), ac_merge=MS_DICTS)


def _are_list_like(*objs):
    """
    >>> _are_list_like([], (), [x for x in range(10)], (x for x in range(4)))
    True
    >>> _are_list_like([], {})
    False
    >>> _are_list_like([], "aaa")
    False
    """
    return all(anyconfig.utils.is_list_like(obj) for obj in objs)


def _update_with_replace(self, other, key, val=None, **options):
    """
    Replace value of a mapping object 'self' with 'other' has if both have same
    keys on update. Otherwise, just keep the value of 'self'.

    :param self: mapping object to update with 'other'
    :param other: mapping object to update 'self'
    :param key: key of mapping object to update
    :param val: value to update self alternatively

    :return: None but 'self' will be updated
    """
    self[key] = other[key] if val is None else val


def _update_wo_replace(self, other, key, val=None, **options):
    """
    Never update (replace) the value of 'self' with 'other''s, that is, only
    the values 'self' does not have its key will be added on update.

    :param self: mapping object to update with 'other'
    :param other: mapping object to update 'self'
    :param key: key of mapping object to update
    :param val: value to update self alternatively

    :return: None but 'self' will be updated
    """
    if key not in self:
        _update_with_replace(self, other, key, val=val)


def _merge_list(self, key, lst):
    """
    :param key: self[key] will be updated
    :param lst: Other list to merge
    """
    self[key] += [x for x in lst if x not in self[key]]


def _merge_other(self, key, val):
    """
    :param key: self[key] will be updated
    :param val: Other val to merge (update/replace)
    """
    self[key] = val  # Just overwrite it by default implementation.


def _update_with_merge(self, other, key, val=None, merge_lists=False,
                       **options):
    """
    Merge the value of self with other's recursively. Behavior of merge will be
    vary depends on types of original and new values.

    - mapping vs. mapping -> merge recursively
    - list vs. list -> vary depends on 'merge_lists'. see its description.

    :param other: a dict[-like] object or a list of (key, value) tuples
    :param key: key of mapping object to update
    :param val: value to update self[key]
    :param merge_lists:
        Merge not only dicts but also lists. For example,

        [1, 2, 3], [3, 4] ==> [1, 2, 3, 4]
        [1, 2, 2], [2, 4] ==> [1, 2, 2, 4]

    :return: None but 'self' will be updated
    """
    if val is None:
        val = other[key]

    if key in self:
        val0 = self[key]  # Original value
        if anyconfig.utils.is_dict_like(val0):  # It needs recursive updates.
            merge(self[key], val, merge_lists=merge_lists, **options)
        elif merge_lists and _are_list_like(val, val0):
            _merge_list(self, key, val)
        else:
            _merge_other(self, key, val)
    else:
        self[key] = val


def _update_with_merge_lists(self, other, key, val=None, **options):
    """
    Similar to _update_with_merge but merge lists always.

    :param self: mapping object to update with 'other'
    :param other: mapping object to update 'self'
    :param key: key of mapping object to update
    :param val: value to update self alternatively

    :return: None but 'self' will be updated
    """
    _update_with_merge(self, other, key, val=val, merge_lists=True, **options)


_MERGE_FNS = {MS_REPLACE: _update_with_replace,
              MS_NO_REPLACE: _update_wo_replace,
              MS_DICTS: _update_with_merge,
              MS_DICTS_AND_LISTS: _update_with_merge_lists}


def _get_update_fn(strategy):
    """
    Select dict-like class based on merge strategy and orderness of keys.

    :param merge: Specify strategy from MERGE_STRATEGIES of how to merge dicts.
    :return: Callable to update objects
    """
    if strategy is None:
        strategy = MS_DICTS
    try:
        return _MERGE_FNS[strategy]
    except KeyError:
        if callable(strategy):
            return strategy

        raise ValueError("Wrong merge strategy: %r" % strategy)


def merge(self, other, ac_merge=MS_DICTS, **options):
    """
    Update (merge) a mapping object 'self' with other mapping object or an
    iterable yields (key, value) tuples based on merge strategy 'ac_merge'.

    :param others: a list of dict[-like] objects or (key, value) tuples
    :param another: optional keyword arguments to update self more
    :param ac_merge: Merge strategy to choose
    """
    _update_fn = _get_update_fn(ac_merge)

    if hasattr(other, "keys"):
        for key in other:
            _update_fn(self, other, key, **options)
    else:
        try:
            for key, val in other:
                _update_fn(self, other, key, val=val, **options)
        except (ValueError, TypeError) as exc:  # Re-raise w/ info.
            raise type(exc)("%s other=%r" % (str(exc), other))


def _make_recur(obj, make_fn, ac_ordered=False, ac_dict=None, **options):
    """
    :param obj: A mapping objects or other primitive object
    :param make_fn: Function to make/convert to
    :param ac_ordered: Use OrderedDict instead of dict to keep order of items
    :param ac_dict: Callable to convert 'obj' to mapping object
    :param options: Optional keyword arguments.

    :return: Mapping object
    """
    if ac_dict is None:
        ac_dict = anyconfig.compat.OrderedDict if ac_ordered else dict

    return ac_dict((k, None if v is None else make_fn(v, **options))
                   for k, v in obj.items())


def _make_iter(obj, make_fn, **options):
    """
    :param obj: A mapping objects or other primitive object
    :param make_fn: Function to make/convert to
    :param options: Optional keyword arguments.

    :return: Mapping object
    """
    return type(obj)(make_fn(v, **options) for v in obj)


def convert_to(obj, ac_ordered=False, ac_dict=None, **options):
    """
    Convert a mapping objects to a dict or object of 'to_type' recursively.
    Borrowed basic idea and implementation from bunch.unbunchify. (bunch is
    distributed under MIT license same as this.)

    :param obj: A mapping objects or other primitive object
    :param ac_ordered: Use OrderedDict instead of dict to keep order of items
    :param ac_dict: Callable to convert 'obj' to mapping object
    :param options: Optional keyword arguments.

    :return: A dict or OrderedDict or object of 'cls'

    >>> OD = anyconfig.compat.OrderedDict
    >>> convert_to(OD((('a', 1) ,)), cls=dict)
    {'a': 1}
    >>> convert_to(OD((('a', OD((('b', OD((('c', 1), ))), ))), )), cls=dict)
    {'a': {'b': {'c': 1}}}
    """
    options.update(ac_ordered=ac_ordered, ac_dict=ac_dict)
    if anyconfig.utils.is_dict_like(obj):
        return _make_recur(obj, convert_to, **options)
    if anyconfig.utils.is_list_like(obj):
        return _make_iter(obj, convert_to, **options)

    return obj

# vim:sw=4:ts=4:et:
