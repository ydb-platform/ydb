__all__ = [
    "OptionalLimitOffsetPage",
    "OptionalLimitOffsetParams",
    "OptionalPage",
    "OptionalParams",
]

from typing import TYPE_CHECKING, TypeVar

from fastapi_pagination.customization import CustomizedPage, UseOptionalFields
from fastapi_pagination.default import Page
from fastapi_pagination.limit_offset import LimitOffsetPage

T = TypeVar("T")

OptionalPage = CustomizedPage[
    Page[T],
    UseOptionalFields(),
]
OptionalLimitOffsetPage = CustomizedPage[
    LimitOffsetPage[T],
    UseOptionalFields(),
]

if TYPE_CHECKING:
    from fastapi_pagination.default import Params as OptionalParams
    from fastapi_pagination.limit_offset import LimitOffsetParams as OptionalLimitOffsetParams
else:
    OptionalParams = OptionalPage.__params_type__
    OptionalLimitOffsetParams = OptionalLimitOffsetPage.__params_type__
