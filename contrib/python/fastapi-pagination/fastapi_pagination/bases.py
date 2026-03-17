from __future__ import annotations

__all__ = [
    "AbstractPage",
    "AbstractParams",
    "BaseAbstractPage",
    "BasePage",
    "BaseRawParams",
    "CursorRawParams",
    "RawParams",
    "is_cursor",
    "is_limit_offset",
]

from abc import ABC, abstractmethod
from collections.abc import Sequence
from contextlib import suppress
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
)

from typing_extensions import Self, TypeIs, TypeVar

from .pydantic import IS_PYDANTIC_V2
from .pydantic.types import LatestConfiguredBaseModel, LatestGenericModel
from .pydantic.v2 import PydanticUndefinedAnnotationV2
from .types import Cursor, GreaterEqualZero, ParamsType

TAny = TypeVar("TAny", default=Any)


class BaseRawParams:
    type: ClassVar[ParamsType]
    include_total: bool

    def as_limit_offset(self) -> RawParams:
        if is_limit_offset(self):
            return self

        raise ValueError("Not a 'limit-offset' params")

    def as_cursor(self) -> CursorRawParams:
        if is_cursor(self):
            return self

        raise ValueError("Not a 'cursor' params")


def is_limit_offset(params: BaseRawParams) -> TypeIs[RawParams]:
    return params.type == "limit-offset"


def is_cursor(params: BaseRawParams) -> TypeIs[CursorRawParams]:
    return params.type == "cursor"


@dataclass
class RawParams(BaseRawParams):
    limit: int | None = None
    offset: int | None = None
    include_total: bool = True

    type: ClassVar[ParamsType] = "limit-offset"

    def as_slice(self) -> slice:
        return slice(
            self.offset,
            (self.offset or 0) + self.limit if self.limit is not None else None,
        )


@dataclass
class CursorRawParams(BaseRawParams):
    cursor: Cursor | None
    size: int
    include_total: bool = True

    type: ClassVar[ParamsType] = "cursor"


def connect_page_and_params(page_cls: type[AbstractPage[Any]], params_cls: type[AbstractParams]) -> None:
    page_cls.__params_type__ = params_cls
    params_cls.__page_type__ = page_cls


class AbstractParams(ABC):
    __page_type__: ClassVar[type[AbstractPage[Any]] | None] = None

    @abstractmethod
    def to_raw_params(self) -> BaseRawParams:
        pass

    @classmethod
    def set_page(cls, page_cls: type[AbstractPage[Any]]) -> None:
        connect_page_and_params(page_cls, cls)


class BaseAbstractPage(ABC, Generic[TAny]):
    """
    Marker class for page classes.

    Used to mark classes that can be used as pages but don't directly inherit from AbstractPage.
    """

    __params_type__: ClassVar[type[AbstractParams]]

    @classmethod
    @abstractmethod
    def create(
        cls,
        items: Sequence[TAny],
        params: AbstractParams,
        **kwargs: Any,
    ) -> Self:
        pass


class AbstractPage(BaseAbstractPage[TAny], LatestConfiguredBaseModel, LatestGenericModel, ABC, Generic[TAny]):
    # used by pydantic v2
    __model_aliases__: ClassVar[dict[str, str]] = {}
    __model_exclude__: ClassVar[set[str]] = set()

    if TYPE_CHECKING:  # only for pydantic v1
        __concrete__: ClassVar[bool]
        __parameters__: ClassVar[tuple[Any, ...]]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

        with suppress(AttributeError):
            # call set_page only if params not yet connected to another page
            if cls.__params_type__ and cls.__params_type__.__page_type__ is None:
                cls.__params_type__.set_page(cls)

    @classmethod
    def set_params(cls, params_cls: type[AbstractParams], /) -> None:
        connect_page_and_params(cls, params_cls)

    if IS_PYDANTIC_V2:

        @classmethod
        def __pydantic_init_subclass__(cls, **kwargs: Any) -> None:
            super().__pydantic_init_subclass__(**kwargs)

            for exclude in cls.__model_exclude__:
                cls.model_fields[exclude].exclude = True
            for name, alias in cls.__model_aliases__.items():
                cls.model_fields[name].serialization_alias = alias

            # rebuild model only in case if customizations is present
            if cls.__model_exclude__ or cls.__model_aliases__:
                with suppress(PydanticUndefinedAnnotationV2):
                    cls.model_rebuild(force=True)


class BasePage(AbstractPage[TAny], ABC, Generic[TAny]):
    items: Sequence[TAny]
    total: GreaterEqualZero
