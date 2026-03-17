#  Copyright (c) Kuba Szczodrzyński 2024-10-12.

import struct
import typing
from types import UnionType
from typing import Any, Tuple, Union

ARRAYS = (list, tuple)
EXCEPTIONS = (ValueError, TypeError, AttributeError, struct.error)
BYTES = (bytes, bytearray)

FieldTypes = Union[type, Tuple["FieldTypes", ...]]


def decode_type(cls: type) -> FieldTypes:
    if isinstance(cls, tuple):
        return cls
    if cls is None:
        # literal NoneType (e.g. optional fields, cond())
        return type(None)
    if cls is Ellipsis:
        # special fields only (seek, padding, etc)
        return type(Ellipsis)
    if cls is Any:
        # represent Any as an empty tuple
        return ()
    if typing.get_origin(cls):
        origin: type = typing.get_origin(cls)
        args = typing.get_args(cls)
        if origin in [Union, UnionType]:
            assert len(args) > 1
            union = tuple(decode_type(cls) for cls in args)
            if () in union:
                # found Any, skip all other args
                return ()
            return union
        # 'True' indicates that it's a generic type, not a union
        return origin, True, *(decode_type(cls) for cls in args)
    if cls in ARRAYS:
        # represent non-parameterized lists in the same format
        return cls, True, ()
    return cls


def check_class_type(cls: type, types: FieldTypes) -> bool:
    if types == type(Ellipsis):
        return True
    if isinstance(types, type):
        return issubclass(cls, types)
    if types == ():
        return True
    if True in types:
        return issubclass(cls, types[0])
    return any(issubclass(cls, typ) for typ in types)


def check_value_type(value: object, types: FieldTypes) -> bool:
    return check_class_type(type(value), types)


def check_types_match(subtypes: FieldTypes, supertypes: FieldTypes) -> bool:
    if type(Ellipsis) in [subtypes, supertypes]:
        # special case - Ellipsis fields are exempt
        return True
    if isinstance(subtypes, type):
        # simple type
        return check_class_type(subtypes, supertypes)
    if subtypes == ():
        # 'Any' subtype will only match the 'Any' supertype
        return supertypes == ()
    if supertypes == ():
        # 'Any' supertype will match any subtype
        return True
    if True in subtypes:
        # generic subtype
        return check_class_type(subtypes[0], supertypes)
    # union type - make sure all subtypes match
    return all(check_class_type(typ, supertypes) for typ in subtypes)
