# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from collections import OrderedDict
from copy import deepcopy

from mo_future import generator_types, get_function_arguments, get_function_defaults, Mapping
from mo_imports import export, expect

from mo_dots.datas import Data, _iadd, dict_to_data
from mo_dots.lists import FlatList, list_to_data
from mo_dots.nones import NullType, Null
from mo_dots.utils import (
    CLASS,
    SLOT,
    register_data,
    get_logger,
    is_primitive,
    is_known_data_type,
    is_null,
    register_type,
)

get_attr, set_attr, to_data, from_data, set_default = expect(
    "get_attr", "set_attr", "to_data", "from_data", "set_default"
)

_new = object.__new__
_get = object.__getattribute__
_set = object.__setattr__


_known_fields = {}  #  map from type to field names
ignored_attributes = set(dir(object)) | set(dir(dict.values.__class__))


class DataObject(Mapping):
    """
    TREAT AN OBJECT LIKE DATA
    """

    __slots__ = [SLOT]

    def __init__(self, obj):
        _set(self, SLOT, obj)

    def __getattr__(self, item):
        obj = _get(self, SLOT)
        output = get_attr(obj, item)
        return object_to_data(output)

    def __setattr__(self, key, value):
        obj = _get(self, SLOT)
        set_attr(obj, key, value)

    def __getitem__(self, item):
        obj = _get(self, SLOT)
        output = get_attr(obj, item)
        return object_to_data(output)

    def __or__(self, other):
        return set_default({}, self, other)

    def __ror__(self, other):
        return to_data(other) | self

    def __add__(self, other):
        return to_data(_iadd(_iadd({}, self), other))

    def __radd__(self, other):
        return to_data(_iadd(_iadd({}, other), self))

    def get(self, item):
        obj = _get(self, SLOT)
        output = get_attr(obj, item)
        return object_to_data(output)

    def keys(self):
        return get_keys(self)

    def items(self):
        keys = self.keys()
        try:
            for k in keys:
                yield k, self[k]
        except Exception as cause:
            get_logger().error("problem with items", cause=cause)

    def __deepcopy__(self, memodict={}):
        output = {}
        for k, v in self.items():
            output[k] = from_data(deepcopy(v))
        return dict_to_data(output)

    def __data__(self):
        return self

    def __iter__(self):
        return (k for k in self.keys())

    def __str__(self):
        obj = _get(self, SLOT)
        return str(obj)

    def __len__(self):
        obj = _get(self, SLOT)
        return len(obj)

    def __call__(self, *args, **kwargs):
        obj = _get(self, SLOT)
        return obj(*args, **kwargs)


register_data(DataObject)


def get_keys(obj):
    """
    RETURN keys OF obj, AS IF DATA
    """
    while isinstance(obj, (DataObject, Data)):
        obj = _get(obj, SLOT)

    if isinstance(obj, dict):
        return obj.keys()

    try:
        return obj.__dict__.keys()
    except Exception:
        pass

    _type = _get(obj, CLASS)
    keys = _known_fields.get(_type)
    if keys is not None:
        return keys

    try:
        keys = _known_fields[_type] = _type.__slots__
        return keys
    except Exception:
        pass

    keys = _known_fields[_type] = tuple(
        k
        for k in dir(_type)
        if k not in ignored_attributes
        and getattr(_type, k).__class__.__name__ in ["member_descriptor", "getset_descriptor"]
    )
    return keys


def object_to_data(v):
    try:
        if is_null(v):
            return Null
    except Exception:
        pass

    if is_primitive(v):
        return v

    _class = _get(v, CLASS)
    if _class in (dict, OrderedDict):
        m = _new(Data)
        _set(m, SLOT, v)
        return m
    elif _class in (tuple, list):
        return list_to_data(v)
    elif _class in (Data, DataObject, FlatList, NullType):
        return v
    elif _class in generator_types:
        return (to_data(vv) for vv in v)
    elif is_known_data_type(_class):
        return DataObject(v)
    else:
        return v


class DataClass:
    """
    ALLOW INSTANCES OF class_ TO ACT LIKE dicts
    ALLOW CONSTRUCTOR TO ACCEPT @override
    """

    def __init__(self, _class):
        register_type(_class)
        self.class_ = _class
        self.constructor = _class.__init__

    def __call__(self, *args, **kwargs):
        settings = to_data(kwargs).settings

        params = get_function_arguments(self.constructor)[1:]
        func_defaults = get_function_defaults(self.constructor)
        if not func_defaults:
            defaults = {}
        else:
            defaults = {k: v for k, v in zip(reversed(params), reversed(func_defaults))}

        ordered_params = dict(zip(params, args))

        output = self.class_(**params_pack(params, ordered_params, kwargs, settings, defaults))
        return DataObject(output)


def params_pack(params, *args):
    settings = {}
    for a in args:
        for k, v in a.items():
            k = str(k)
            if k in settings:
                continue
            settings[k] = v

    output = {str(k): from_data(settings[k]) for k in params if k in settings}
    return output


export("mo_dots.lists", object_to_data)
export("mo_dots.datas", object_to_data)
export("mo_dots.datas", DataObject)
export("mo_dots.datas", get_keys)
