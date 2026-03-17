# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from copy import deepcopy

from mo_future import first
from mo_imports import expect, delay_import, export

from mo_dots import utils
from mo_dots.datas import is_missing, hash_value
from mo_dots.nones import Null, NullType
from mo_dots.utils import CLASS, SLOT, is_null, is_many, is_list, is_sequence, register_list

Log = delay_import("mo_logs.Log")
object_to_data, coalesce, to_data, from_data, get_attr = expect(
    "object_to_data", "coalesce", "to_data", "from_data", "get_attr"
)

_null_hash = hash(None)
_get = object.__getattribute__
_set = object.__setattr__
_new = object.__new__


class FlatList:
    """
    ENCAPSULATES HANDING OF Nulls BY wrapING ALL MEMBERS AS NEEDED
    ENCAPSULATES FLAT SLICES ([::]) FOR USE IN WINDOW FUNCTIONS
    https://github.com/klahnakoski/mo-dots/tree/dev/docs#flatlist-is-flat
    """

    __slots__ = [SLOT]

    def __init__(self, vals=None):
        """ USE THE vals, NOT A COPY """
        # list.__init__(self)
        if is_null(vals):
            _set(self, SLOT, [])
        elif _get(vals, CLASS) is FlatList:
            _set(self, SLOT, vals.list)
        else:
            _set(self, SLOT, vals)

    def __getitem__(self, index):
        if index == ".":
            return self

        if _get(index, CLASS) is slice:
            # IMPLEMENT FLAT SLICES (for i not in range(0, len(self)): assert self[i]==None)
            if index.step is not None:
                Log.error("slice step must be None, do not know how to deal with values")
            length = len(_get(self, SLOT))

            i = index.start
            if i is None:
                i = 0
            else:
                i = min(max(i, 0), length)
            j = index.stop
            if j is None:
                j = length
            else:
                j = max(min(j, length), 0)
            return FlatList(_get(self, SLOT)[i:j])

        if not isinstance(index, int) or index < 0 or len(_get(self, SLOT)) <= index:
            return Null
        return to_data(_get(self, SLOT)[index])

    def __setitem__(self, key, value):
        _list = _get(self, SLOT)
        if isinstance(key, int):
            if key >= len(_list):
                _list.extend([None] * (key - len(_list) + 1))
            _list[key] = from_data(value)
            return

        for v in _list:
            to_data(v)[key] = value
        return

    def __setattr__(self, key, value):
        _list = _get(self, SLOT)
        for v in _list:
            to_data(v)[key] = value
        return

    def __getattr__(self, key):
        if key in ["__json__", "__call__"]:
            raise AttributeError()
        return FlatList.get(self, key)

    def get(self, key):
        """
        simple `select`
        """
        if key == ".":
            output = []
            for v in _get(self, SLOT):
                if is_many(v):
                    element = from_data(object_to_data(v).get(key))
                    output.extend(element)
                else:
                    output.append(from_data(v))

            return list_to_data(output)
        output = []
        for v in _get(self, SLOT):
            element = from_data(get_attr(to_data(v), key))
            if is_missing(element):
                continue
            elif is_many(element):
                output.extend(element)
            else:
                output.append(element)
        return list_to_data(output)

    def select(self, key):
        Log.error("Not supported.  Use `get()`")

    def filter(self, _filter):
        return list_to_data([from_data(u) for u in _get(self, SLOT) if _filter(to_data(u))])

    def map(self, oper, includeNone=True):
        if includeNone:
            return FlatList([oper(v) for v in _get(self, SLOT)])
        else:
            return FlatList([oper(v) for v in _get(self, SLOT) if v != None])

    def to_list(self):
        return _get(self, SLOT)

    def __delitem__(self, i):
        del _get(self, SLOT)[i]

    def clear(self):
        _set(self, SLOT, [])

    def __iter__(self):
        temp = [to_data(v) for v in _get(self, SLOT)]
        return iter(temp)

    def __contains__(self, item):
        return list.__contains__(_get(self, SLOT), item)

    def append(self, val):
        _get(self, SLOT).append(from_data(val))
        return self

    def __str__(self):
        return str(_get(self, SLOT))

    def __repr__(self):
        return f"to_data({repr(_get(self, SLOT))})"

    def __len__(self):
        return _get(self, SLOT).__len__()

    def copy(self):
        return FlatList(list(_get(self, SLOT)))

    def __copy__(self):
        return FlatList(list(_get(self, SLOT)))

    def __deepcopy__(self, memo):
        d = _get(self, SLOT)
        return to_data(deepcopy(d, memo))

    def remove(self, x):
        _get(self, SLOT).remove(x)
        return self

    def extend(self, values):
        lst = _get(self, SLOT)
        for v in values:
            lst.append(from_data(v))
        return self

    def pop(self, index=None):
        if index is None:
            return to_data(_get(self, SLOT).pop())
        else:
            return to_data(_get(self, SLOT).pop(index))

    def __hash__(self):
        lst = _get(self, SLOT)
        if not lst:
            return _null_hash
        return hash_value(lst[0])

    def __eq__(self, other):
        lst = _get(self, SLOT)
        if other is None:
            return False

        try:
            if len(lst) != len(other):
                return False
            return all([s == o for s, o in zip(lst, other)])
        except Exception:
            return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def __add__(self, other):
        output = list(_get(self, SLOT))
        if is_null(other):
            return self
        elif is_many(other):
            output.extend(from_data(other))
        else:
            output.append(other)
        return FlatList(vals=output)

    __or__ = __add__

    def __radd__(self, other):
        output = list(_get(self, SLOT))
        if is_null(other):
            return self
        elif is_many(other):
            output = list(from_data(other)) + output
        else:
            output = [other] + output
        return FlatList(vals=output)

    def __iadd__(self, other):
        if is_null(other):
            return self
        elif is_many(other):
            self.extend(from_data(other))
        else:
            self.append(other)
        return self

    def right(self, num):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE RIGHT [-num:]
        """
        if is_null(num):
            return self
        if num <= 0:
            return Null

        return FlatList(_get(self, SLOT)[-num:])

    def limit(self, num):
        """
        NOT REQUIRED, BUT EXISTS AS OPPOSITE OF right()
        """
        if is_null(num):
            return self
        if num <= 0:
            return Null

        return FlatList(_get(self, SLOT)[:num])

    left = limit

    def not_right(self, num):
        """
        WITH SLICES BEING FLAT, WE NEED A SIMPLE WAY TO SLICE FROM THE LEFT [:-num:]
        """
        if not num:
            return self
        if num < 0:
            return self

        return FlatList(_get(self, SLOT)[:-num:])

    def not_left(self, num):
        """
        NOT REQUIRED, EXISTS AS OPPOSITE OF not_right()
        """
        if not num:
            return self
        if num < 0:
            return self

        return FlatList(_get(self, SLOT)[num::])

    def last(self):
        """
        RETURN LAST ELEMENT IN FlatList [-1]
        """
        lst = _get(self, SLOT)
        if lst:
            return to_data(lst[-1])
        return Null


register_list(FlatList)


def last(values):
    if is_many(values):
        if not values:
            return Null
        if isinstance(values, FlatList):
            return values.last()
        elif is_list(values):
            if not values:
                return Null
            return values[-1]
        elif is_sequence(values):
            l = Null
            for i in values:
                l = i
            return l
        else:
            return first(values)

    return values


def list_to_data(v):
    """
    to_data, BUT WITHOUT CHECKS
    """
    output = _new(FlatList)
    _set(output, SLOT, v)
    return output


export("mo_dots.datas", list_to_data)
export("mo_dots.datas", FlatList)
