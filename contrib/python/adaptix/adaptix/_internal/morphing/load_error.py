import dataclasses
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial
from typing import Any, Optional, Union

from ..common import TypeHint, VarTuple
from ..compat import CompatExceptionGroup
from ..utils import fix_dataclass_from_builtin, with_module


def _str_by_fields(cls):
    template = ", ".join("%s={self.%s!r}" % (fld.name, fld.name) for fld in dataclasses.fields(cls))  # noqa: UP031
    body = f'def __str__(self):\n    return f"{template}"'
    ns = {}
    exec(body, ns, ns)  # noqa: S102
    cls.__str__ = ns["__str__"]
    return cls


def custom_exception(cls=None, /, *, str_by_fields: bool = True, public_module: bool = True):
    if cls is None:
        return partial(custom_exception, str_by_fields=str_by_fields, public_module=public_module)

    if str_by_fields:
        cls = _str_by_fields(cls)
    if public_module:
        cls = with_module("adaptix.load_error")(cls)
    return cls


# __init__ of these classes do not call super().__init__, but it's ok!
# BaseException.__init__ does nothing useful


@custom_exception(str_by_fields=False)
@dataclass(eq=False, init=False)
@fix_dataclass_from_builtin
class LoadError(Exception):
    """The base class for the exceptions that are raised
    when the loader gets invalid input data
    """


@custom_exception(str_by_fields=False)
@dataclass(eq=False, init=False)
class LoadExceptionGroup(CompatExceptionGroup[LoadError], LoadError):
    """The base class integrating ``ExceptionGroup`` into the ``LoadError`` hierarchy"""

    message: str
    exceptions: VarTuple[LoadError]

    # stub `__init__` is required for right introspection
    def __init__(self, message: str, exceptions: VarTuple[LoadError]):
        pass


@custom_exception(str_by_fields=False)
@dataclass(eq=False, init=False)
class AggregateLoadError(LoadExceptionGroup):
    """The class collecting distinct load errors"""


@custom_exception(str_by_fields=False)
@dataclass(eq=False, init=False)
class UnionLoadError(LoadExceptionGroup):
    pass


@custom_exception
@dataclass(eq=False)
class MsgLoadError(LoadError):
    msg: Optional[str]
    input_value: Any


@custom_exception
@dataclass(eq=False)
class ExtraFieldsLoadError(LoadError):
    fields: Iterable[str]
    input_value: Any


@custom_exception
@dataclass(eq=False)
class ExtraItemsLoadError(LoadError):
    expected_len: int
    input_value: Any


@custom_exception
@dataclass(eq=False)
class NoRequiredFieldsLoadError(LoadError):
    fields: Iterable[str]
    input_value: Any


@custom_exception
@dataclass(eq=False)
class NoRequiredItemsLoadError(LoadError):
    expected_len: int
    input_value: Any


@custom_exception
@dataclass(eq=False)
class TypeLoadError(LoadError):
    expected_type: TypeHint
    input_value: Any


@custom_exception
@dataclass(eq=False)
class ExcludedTypeLoadError(TypeLoadError):
    expected_type: TypeHint
    excluded_type: TypeHint
    input_value: Any


@custom_exception(str_by_fields=False)
@dataclass(eq=False)
class ValueLoadError(MsgLoadError):
    pass


@custom_exception(str_by_fields=False)
@dataclass(eq=False)
class ValidationLoadError(MsgLoadError):
    pass


@custom_exception
@dataclass(eq=False)
class BadVariantLoadError(LoadError):
    allowed_values: Iterable[Any]
    input_value: Any


@custom_exception
@dataclass(eq=False)
class MultipleBadVariantLoadError(LoadError):
    allowed_values: Iterable[Any]
    invalid_values: Iterable[Any]
    input_value: Any


@custom_exception
@dataclass(eq=False)
class FormatMismatchLoadError(LoadError):
    format: str
    input_value: Any


@custom_exception
@dataclass(eq=False)
class DuplicatedValuesLoadError(LoadError):
    input_value: Any


@custom_exception
@dataclass(eq=False)
class OutOfRangeLoadError(LoadError):
    min_value: Optional[Union[int, float]]
    max_value: Optional[Union[int, float]]
    input_value: Any
