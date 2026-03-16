# encoding: utf-8
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at https://www.mozilla.org/en-US/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
import importlib
import types
from collections import OrderedDict
from dataclasses import is_dataclass
from datetime import datetime, date, timedelta, time
from decimal import Decimal

from mo_future import none_type, generator_types

from mo_dots.logging import get_logger

KEY = "_key"
SLOT = "_internal_value"
CLASS = "__class__"
_get = object.__getattribute__


def get_module(name):
    try:
        return importlib.import_module(name)
    except Exception as e:
        get_logger().error(
            "`pip install " + name.split(".")[0].replace("_", "-") + "` to enable this feature", cause=e,
        )


_null_types = (none_type,)


def register_null_type(_type):
    global _null_types
    _null_types = tuple(set(_null_types + (_type,)))


def is_null(value):
    # RETURN True IF EFFECTIVELY NOTHING
    _class = _get(value, CLASS)
    if _class in _null_types:
        return True
    return False


def is_not_null(value):
    _class = _get(value, CLASS)
    if _class in _null_types:
        return False
    return True


def is_missing(t) -> bool:
    # RETURN True IF EFFECTIVELY NOTHING
    return isinstance(t, (str, *_null_types, *_many_types)) and not t


def exists(value) -> bool:
    return not is_missing(value)


_primitive_types = (
    str,
    bytes,
    int,
    float,
    bool,
    Decimal,
    datetime,
    date,
    time,
    timedelta,
    dict.values.__class__,
    object.__class__,
    types.FunctionType,
    types.MethodType,
)


def is_primitive(value):
    return isinstance(value, _primitive_types)


def register_primitive(_type):
    global _primitive_types
    _primitive_types = tuple(set(_primitive_types + (_type,)))


_data_types = data_types = (dict, OrderedDict)  # TYPES TO HOLD DATA


def register_data(type_):
    """
    :param type_:  ADD OTHER TYPE THAT HOLDS DATA
    :return:
    """
    global _data_types
    _data_types = tuple(set(_data_types + (type_,)))


def is_data(d):
    """
    :param d:
    :return: True IF d IS A TYPE THAT HOLDS DATA
    """
    return _get(d, CLASS) in _data_types


_known_data_types = tuple()


def register_type(*_classes):
    global _known_data_types
    _known_data_types = tuple(set(_known_data_types + _classes))


def is_namedtuple(obj):
    return isinstance(obj, tuple) and hasattr(obj, "_fields")


def is_data_object(obj):
    return isinstance(obj, _known_data_types) or is_namedtuple(obj) or is_dataclass(obj)


def is_known_data_type(_class):
    return _class in _known_data_types


list_types = (list,)
container_types = (list, set)
finite_types = (list, set, tuple)
sequence_types = (list, tuple) + generator_types
_many_types = tuple(set(list_types + container_types + sequence_types))


def register_list(_type):
    # lists belong to all categories
    global list_types, container_types, finite_types, sequence_types, _many_types
    list_types = tuple(set(list_types + (_type,)))
    container_types = tuple(set(container_types + (_type,)))
    finite_types = tuple(set(finite_types + (_type,)))
    sequence_types = tuple(set(sequence_types + (_type,)))
    _many_types = tuple(set(_many_types + (_type,)))


# ITERATORS THAT ARE CONSIDERED PRIMITIVE
not_many_names = ("str", "unicode", "binary", "NullType", "NoneType", "dict", "Data")


def is_list(l):
    # ORDERED, AND CAN CHANGE CONTENTS
    return isinstance(l, list_types)


def is_container(l):
    # CAN ADD AND REMOVE ELEMENTS
    return isinstance(l, container_types)


def is_sequence(l):
    # HAS AN ORDER, INCLUDES GENERATORS
    return isinstance(l, sequence_types)


def is_finite(l):
    # CAN PERFORM len(l); NOT A GENERATOR
    return isinstance(l, finite_types)


def is_many(value):
    # REPRESENTS MULTIPLE VALUES
    # TODO: CLEAN UP THIS LOGIC
    # THIS IS COMPLICATED BECAUSE I AM UNSURE ABOUT ALL THE "PRIMITIVE TYPES"
    # I WOULD LIKE TO POSITIVELY CATCH many_types, BUT MAYBE IT IS EASIER TO DETECT: Iterable, BUT NOT PRIMITIVE
    # UNTIL WE HAVE A COMPLETE SLOT, WE KEEP ALL THIS warning() CODE
    global _many_types
    if isinstance(value, _many_types):
        return True

    type_ = _get(value, CLASS)
    if issubclass(type_, types.GeneratorType):
        _many_types = _many_types + (type_,)
        get_logger.warning("is_many() can not detect generator {type}", type=type_.__name__)
        return True
    return False


def register_many(_type):
    global _many_types
    _many_types = _many_types + (_type,)


def cache(func):
    """
    DECORATOR TO CACHE THE RESULT OF A FUNCTION
    """
    cache = {}

    def wrapper(*args):
        if args in cache:
            return cache[args]
        else:
            result = func(*args)
            cache[args] = result
            return result

    return wrapper
