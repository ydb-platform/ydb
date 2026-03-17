"""Custom types and type aliases.

.. warning::

    This module is private. Types may be modified, added, and removed between minor releases.
"""

from __future__ import annotations

import enum
import typing

try:
    from typing import Unpack
except ImportError:  # Remove when dropping Python 3.10
    from typing_extensions import Unpack

import marshmallow as ma

if typing.TYPE_CHECKING:
    from datetime import timedelta

    from marshmallow.fields import Field

T = typing.TypeVar("T")
SubcastT = typing.TypeVar("SubcastT")
EnumT = typing.TypeVar("EnumT", bound=enum.Enum)

ErrorMapping: typing.TypeAlias = typing.Mapping[str, list[str]]
FieldFactory: typing.TypeAlias = typing.Callable[..., ma.fields.Field]
Subcast: typing.TypeAlias = type[T] | typing.Callable[[typing.Any], T] | ma.fields.Field
ParserMethod: typing.TypeAlias = typing.Callable[..., typing.Any]


class BaseMethodKwargs(typing.TypedDict, total=False):
    # kwargs shared by all parser methods
    validate: (
        typing.Callable[[typing.Any], typing.Any]
        | typing.Iterable[typing.Callable[[typing.Any], typing.Any]]
        | None
    )


class FieldMethod(typing.Generic[T]):
    @typing.overload
    def __call__(
        self,
        name: str,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> T: ...

    @typing.overload
    def __call__(
        self,
        name: str,
        default: None = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> T | None: ...

    @typing.overload
    def __call__(
        self,
        name: str,
        default: T = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> T: ...

    def __call__(
        self,
        name: str,
        default: typing.Any = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> T | None: ...


class ListFieldMethod:
    @typing.overload
    def __call__(
        self,
        name: str,
        default: list[T] = ...,
        subcast: Subcast[T] = ...,
        *,
        delimiter: str | None = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> list[T]: ...

    @typing.overload
    def __call__(
        self,
        name: str,
        default: T = ...,
        subcast: None = ...,
        *,
        delimiter: str | None = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> list[typing.Any] | T: ...

    @typing.overload
    def __call__(
        self,
        name: str,
        default: T = ...,
        subcast: Subcast[SubcastT] = ...,
        *,
        delimiter: str | None = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> list[SubcastT] | T: ...

    def __call__(
        self,
        name: str,
        default: T = ...,
        subcast: Subcast[SubcastT] | None = ...,
        *,
        delimiter: str | None = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> list[SubcastT] | list[typing.Any] | T | None: ...


KeysT = typing.TypeVar("KeysT")
ValuesT = typing.TypeVar("ValuesT")


class DictFieldMethod:
    @typing.overload
    def __call__(
        self,
        name: str,
        default: dict[KeysT, ValuesT] = ...,
        *,
        subcast_keys: Subcast[KeysT] | None = None,
        subcast_values: Subcast[ValuesT] | None = None,
        delimiter: str | None = None,
        key_value_delimiter: str | None = None,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> dict[KeysT, ValuesT]: ...

    @typing.overload
    def __call__(
        self,
        name: str,
        default: None = ...,
        *,
        subcast_keys: Subcast[KeysT] | None = None,
        subcast_values: Subcast[ValuesT] | None = None,
        delimiter: str | None = None,
        key_value_delimiter: str | None = None,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> dict[KeysT, ValuesT] | None: ...

    def __call__(
        self,
        name: str,
        default: typing.Any = ...,
        *,
        subcast_keys: Subcast[KeysT] | None = None,
        subcast_values: Subcast[ValuesT] | None = None,
        delimiter: str | None = None,
        key_value_delimiter: str | None = None,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> dict[KeysT, ValuesT] | None: ...


class EnumFieldMethod(typing.Generic[EnumT]):
    @typing.overload
    def __call__(
        self,
        name: str,
        *,
        enum: type[EnumT],
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> EnumT: ...

    @typing.overload
    def __call__(
        self,
        name: str,
        default: None = ...,
        *,
        enum: type[EnumT],
        by_value: bool | Field | type[Field] = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> EnumT | None: ...

    @typing.overload
    def __call__(
        self,
        name: str,
        default: EnumT = ...,
        *,
        enum: type[EnumT],
        by_value: bool | Field | type[Field] = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> EnumT: ...

    def __call__(
        self,
        name: str,
        default: EnumT | None = ...,
        *,
        enum: type[EnumT],
        by_value: bool | Field | type[Field] = False,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> EnumT | None: ...


class TimeDeltaFieldMethod:
    @typing.overload
    def __call__(
        self,
        name: str,
        *,
        format: typing.Literal["gep2257", "iso8601"] | None = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> timedelta: ...

    @typing.overload
    def __call__(
        self,
        name: str,
        default: None = ...,
        *,
        format: typing.Literal["gep2257", "iso8601"] | None = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> timedelta | None: ...

    @typing.overload
    def __call__(
        self,
        name: str,
        default: timedelta = ...,
        *,
        format: typing.Literal["gep2257", "iso8601"] | None = ...,
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> timedelta: ...

    def __call__(
        self,
        name: str,
        default: typing.Any = ...,
        *,
        format: typing.Literal["gep2257", "iso8601"] | None = ...,  # noqa: A002
        **kwargs: Unpack[BaseMethodKwargs],
    ) -> timedelta | None: ...
