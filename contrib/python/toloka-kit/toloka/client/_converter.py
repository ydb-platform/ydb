__all__: list = []
import datetime
from decimal import Decimal
import typing
import re
import uuid
import pkg_resources
from typing import List, Union

import cattr
from ..util._extendable_enum import ExtendableStrEnum

_CATTRS_VERSION = tuple(map(int, pkg_resources.get_distribution('cattrs').version.split('.')))

if _CATTRS_VERSION < (22, 2, 0):
    converter = cattr.Converter()
else:
    converter = cattr.converters.BaseConverter()

converter.register_structure_hook_func(
    lambda type_: hasattr(type_, 'structure'),
    lambda data, type_: getattr(type_, 'structure').__func__(type_, data)  # type: ignore
)
converter.register_unstructure_hook_func(  # type: ignore
    lambda obj: hasattr(obj, 'unstructure'),
    lambda obj: obj.unstructure()  # type: ignore
)

converter.register_structure_hook(uuid.UUID, lambda data, type_: type_(data))  # type: ignore
converter.register_unstructure_hook(uuid.UUID, str)

converter.register_structure_hook(
    Union[str, List[str]],
    lambda data, type_: converter.structure(data, List[str] if isinstance(data, list) else str)  # type: ignore
)

MS_IN_ISO_REGEX = re.compile(r'(?<=.{19})\.\d*')


def str_to_datetime(str_dt: str) -> datetime.datetime:
    # Some dirty fix for not matching miliseconds
    # The problem is that sometimes we get here 5 digits in ms. That leads to raising an exception.
    # drop after TOLOKA-16759
    def fill_miliseconds(miliseconds) -> str:
        return miliseconds.group().ljust(7 if len(miliseconds.group()) > 4 else 4, '0')

    str_dt = MS_IN_ISO_REGEX.sub(fill_miliseconds, str_dt)
    return ensure_timezone(datetime.datetime.fromisoformat(str_dt))


def ensure_timezone(dt: datetime.datetime):
    return dt.replace(tzinfo=datetime.timezone.utc) if dt.tzinfo is None else dt


# Dates are represented as ISO 8601: YYYY-MM-DDThh:mm:ss[.sss]
# and structured to datetime.datetime
converter.register_structure_hook(
    datetime.datetime,
    lambda data, type_: ensure_timezone(data) if isinstance(data, datetime.datetime) else str_to_datetime(data)  # type: ignore
)


def datetime_to_naive_utc(dt: datetime.datetime):
    # consider naive time as utc time
    if dt.tzinfo is not None:
        dt = dt.astimezone(datetime.timezone.utc)
    return dt.replace(tzinfo=None)


converter.register_unstructure_hook(datetime.datetime, lambda data: datetime_to_naive_utc(data).isoformat())  # type: ignore


converter.register_structure_hook(
    Decimal,
    lambda data, type_: Decimal(data)  # type: ignore
)

# We need to redefine structure/unstructure hook for ExtendableStrEnum because hasattr(type_, 'structure') works
# incorrect in that case
converter.register_unstructure_hook(ExtendableStrEnum, converter._unstructure_enum)


def _try_to_structure_to_extendable_enum(data, type_):
    possible_types = set(type(item.value) for item in type_)
    if not isinstance(data, (*possible_types, type_)):
        raise ValueError
    return converter._structure_call(data, type_)


converter.register_structure_hook(  # type: ignore
    ExtendableStrEnum,
    _try_to_structure_to_extendable_enum
)


def _is_union_of_any(type_):
    """Checks if type_ is Union[from_type, Any] or Union[from_type, Any, None]
    """

    if type_ == typing.Optional[typing.Any] or\
            getattr(type_, '__origin__', None) is not typing.Union or\
            typing.Any not in type_.__args__:
        return False
    return len(type_.__args__) == 2 or len(type_.__args__) == 3 and type(None) in type_.__args__


def _try_to_structure_union_of_any(data: typing.Any, type_: typing.Type) -> typing.Any:
    args = type_.__args__
    is_optional = type(None) in args
    args = [arg for arg in args if arg is not type(None) and arg is not typing.Any]  # noqa: E721
    nested_type = args[0]
    try:
        return converter.structure(data, nested_type if not is_optional else typing.Optional[nested_type])
    except (ValueError, TypeError):
        # no conversion found, consider data being type Any
        return data


# Supports Union[from_type, Any] -> Union[to_type, Any] conversion (from_type is supposed to be structured to to_type).
# This conversion is primarily used during autocasts (see toloka.client.primitives.base).
converter.register_structure_hook_func(
    _is_union_of_any,
    _try_to_structure_union_of_any
)


structure = converter.structure
unstructure = converter.unstructure
