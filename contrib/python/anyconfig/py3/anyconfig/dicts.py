#
# Forked from m9dicts.{api,dicts}.
#
# Copyright (C) 2011 - 2021 Red Hat, Inc.
# Copyright (C) 2018 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
r"""Utility functions to operate on mapping objects such as get, set and merge.

.. versionadded: 0.8.3
   define _update_* and merge functions based on classes in
   :mod:`m9dicts.dicts`

"""
from __future__ import annotations

import collections
import functools
import operator
import re
import typing

from . import utils


# Merge strategies:
MS_REPLACE: str = "replace"
MS_NO_REPLACE: str = "noreplace"
MS_DICTS: str = "merge_dicts"
MS_DICTS_AND_LISTS: str = "merge_dicts_and_lists"
MERGE_STRATEGIES: tuple[str, ...] = (
    MS_REPLACE, MS_NO_REPLACE, MS_DICTS, MS_DICTS_AND_LISTS,
)

PATH_SEPS: tuple[str, ...] = ("/", ".")

_JSNP_GET_ARRAY_IDX_REG: re.Pattern = re.compile(r"(?:0|[1-9][0-9]*)")


DictT = dict[str, typing.Any]


def _jsnp_unescape(jsn_s: str) -> str:
    """Parse and decode given encoded JSON Pointer expression.

    It will convert ~1 to / and ~0 to ~ by following RFC 6901.

    .. seealso:: JSON Pointer: http://tools.ietf.org/html/rfc6901
    """
    return jsn_s.replace("~1", "/").replace("~0", "~")


def _split_path(
    path: str, seps: tuple[str, ...] = PATH_SEPS,
) -> list[str]:
    """Parse a path expression and return a list of path items.

    :param path: Path expression may contain separator chars.
    :param seps: Separator char candidates.
    :return: A list of keys to fetch object[s] later.
    """
    if not path:
        return []

    for sep in seps:
        if sep in path:
            if path == sep:  # Special case, "/" or "." only.
                return [""]
            return [x for x in path.split(sep) if x]

    return [path]


def mk_nested_dic(
    path: str, val: typing.Any, seps: tuple[str, ...] = PATH_SEPS,
) -> DictT:
    """Make a nested dict iteratively.

    :param path: Path expression to make a nested dict
    :param val: Value to set
    :param seps: Separator char candidates
    """
    ret: DictT = {}
    for key in reversed(_split_path(path, seps)):
        ret = {key: val if not ret else ret.copy()}

    return ret


def get(
    dic: DictT, path: str, seps: tuple[str, ...] = PATH_SEPS,
    idx_reg: re.Pattern = _JSNP_GET_ARRAY_IDX_REG,
) -> tuple[typing.Any, str]:
    """Getter for nested dicts.

    :param dic: a dict[-like] object
    :param path: Path expression to point object wanted
    :param seps: Separator char candidates
    :return: A tuple of (result_object, error_message)
    """
    items = [_jsnp_unescape(s) for s in _split_path(path, seps)]  # : [str]
    if not items:
        return (dic, "")
    try:
        if len(items) == 1:
            return (dic[items[0]], "")

        prnt: typing.Any = functools.reduce(operator.getitem, items[:-1], dic)
        arr = (idx_reg.match(items[-1])
               if utils.is_list_like(prnt) else False)

        return (prnt[int(items[-1])], "") if arr else (prnt[items[-1]], "")

    except (TypeError, KeyError, IndexError) as exc:
        return (None, str(exc))


def set_(
    dic: DictT, path: str, val: typing.Any,
    seps: tuple[str, ...] = PATH_SEPS,
) -> None:
    """Setter for nested dicts.

    :param dic: a dict[-like] object support recursive merge operations
    :param path: Path expression to point object wanted
    :param seps: Separator char candidates
    """
    merge(dic, mk_nested_dic(path, val, seps), ac_merge=MS_DICTS)


def _are_list_like(*objs: typing.Any) -> bool:
    """Test if given objects are list like ones or not."""
    return all(utils.is_list_like(obj) for obj in objs)


def _update_with_replace(
    self: DictT, other: DictT, key: str,
    default: typing.Any = None, **_options: typing.Any,
) -> None:
    """Update ``self`` by replacements using ``other``.

    Replace value of a mapping object 'self' with 'other' has if both have same
    keys on update. Otherwise, just keep the value of 'self'.

    :param self: mapping object to update with 'other'
    :param other: mapping object to update 'self'
    :param key: key of mapping object to update
    :param val: value to update self alternatively

    :return: None but 'self' will be updated
    """
    oval = other.get(key, None)
    if oval is not None:
        self[key] = oval
    elif default is not None:
        self[key] = default


def _update_wo_replace(
    self: DictT, other: DictT, key: str,
    val: typing.Any = None, **_options: typing.Any,
) -> None:
    """Update ``self`` without any replacements using ``other``.

    Never update (replace) the value of 'self' with 'other''s, that is, only
    the values 'self' does not have its key will be added on update.

    :param self: mapping object to update with 'other'
    :param other: mapping object to update 'self'
    :param key: key of mapping object to update
    :param val: value to update self alternatively

    :return: None but 'self' will be updated
    """
    if key not in self:
        self[key] = other.get(key, val)


def _merge_list(
    self: DictT, key: str, lst: collections.abc.Iterable[typing.Any],
) -> None:
    """Update a dict ``self`` using an iterable ``lst``.

    :param key: self[key] will be updated
    :param lst: Other list to merge
    """
    self[key] += [x for x in lst if x not in self[key]]


def _merge_other(self: DictT, key: str, val: typing.Any) -> None:
    """Update an item in a dict ``self`` using a pair of key and value.

    :param key: self[key] will be updated
    :param val: Other val to merge (update/replace)
    """
    self[key] = val  # Just overwrite it by default implementation.


def _update_with_merge(
    self: DictT, other: DictT, key: str, *,
    val: typing.Any = None,
    merge_lists: bool = False, **options: typing.Any,
) -> None:
    """Update a dict ``self`` using ``other`` and optional arguments.

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
        if utils.is_dict_like(val0):  # It needs recursive updates.
            merge(self[key], val, merge_lists=merge_lists, **options)
        elif merge_lists and _are_list_like(val, val0):
            _merge_list(self, key, val)
        else:
            _merge_other(self, key, val)
    else:
        self[key] = val


def _update_with_merge_lists(
    self: DictT, other: DictT, key: str,
    val: typing.Any = None, **options: typing.Any,
) -> None:
    """Similar to _update_with_merge but merge lists always.

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


def _get_update_fn(strategy: str) -> collections.abc.Callable[..., None]:
    """Select dict-like class based on merge strategy and orderness of keys.

    :param merge: Specify strategy from MERGE_STRATEGIES of how to merge dicts.
    :return: Callable to update objects
    """
    if strategy is None:
        strategy = MS_DICTS
    try:
        return typing.cast(
            "collections.abc.Callable[..., None]",
            _MERGE_FNS[strategy],
        )
    except KeyError as exc:
        if callable(strategy):
            return strategy

        msg = f"Wrong merge strategy: {strategy!r}"
        raise ValueError(msg) from exc


def merge(
    self: DictT,
    other: collections.abc.Iterable[tuple[str, typing.Any]] | DictT,
    ac_merge: str = MS_DICTS,
    **options: typing.Any,
) -> None:
    """Update (merge) a mapping object ``self`` with ``other``.

    ``other`` may be a mapping object or an iterable yields (key, value) tuples
    based on merge strategy 'ac_merge'.

    :param others: a list of dict[-like] objects or (key, value) tuples
    :param another: optional keyword arguments to update self more
    :param ac_merge: Merge strategy to choose
    """
    _update_fn = _get_update_fn(ac_merge)

    if isinstance(other, dict):
        for key in other:
            _update_fn(self, other, key, **options)
    else:
        try:
            iother = typing.cast(
                "collections.abc.Iterable[tuple[str, typing.Any]]",
                other,
            )
            for key, val in iother:
                _update_fn(self, dict(other), key, val=val, **options)
        except (ValueError, TypeError) as exc:  # Re-raise w/ info.
            msg = f"{exc!s} other={other!r}"
            raise type(exc)(msg) from exc


def _make_recur(
    obj: typing.Any, make_fn: collections.abc.Callable, *,
    ac_ordered: bool = False,
    ac_dict: collections.abc.Callable | None = None,
    **options: typing.Any,
) -> DictT:
    """Apply ``make_fn`` to ``obj`` recursively.

    :param obj: A mapping objects or other primitive object
    :param make_fn: Function to make/convert to
    :param ac_ordered: Use OrderedDict instead of dict to keep order of items
    :param ac_dict: Callable to convert 'obj' to mapping object
    :param options: Optional keyword arguments.

    :return: Mapping object
    """
    if ac_dict is None:
        ac_dict = collections.OrderedDict if ac_ordered else dict

    return ac_dict((k, None if v is None else make_fn(v, **options))
                   for k, v in obj.items())


def _make_iter(
    obj: typing.Any, make_fn: collections.abc.Callable,
    **options: typing.Any,
) -> DictT:
    """Apply ``make_fn`` to ``obj`` iteratively.

    :param obj: A mapping objects or other primitive object
    :param make_fn: Function to make/convert to
    :param options: Optional keyword arguments.

    :return: Mapping object
    """
    return type(obj)(make_fn(v, **options) for v in obj)


def convert_to(
    obj: typing.Any, *, ac_ordered: bool = False,
    ac_dict: collections.abc.Callable | None = None,
    **options: typing.Any,
) -> DictT:
    """Convert a mapping objects to a dict or object of 'to_type' recursively.

    Borrowed basic idea and implementation from bunch.unbunchify. (bunch is
    distributed under MIT license same as this.)

    :param obj: A mapping objects or other primitive object
    :param ac_ordered: Use OrderedDict instead of dict to keep order of items
    :param ac_dict: Callable to convert 'obj' to mapping object
    :param options: Optional keyword arguments.

    :return: A dict or OrderedDict or object of 'cls'
    """
    options.update(ac_ordered=ac_ordered, ac_dict=ac_dict)
    if utils.is_dict_like(obj):
        return _make_recur(obj, convert_to, **options)
    if utils.is_list_like(obj):
        return _make_iter(obj, convert_to, **options)

    return obj
