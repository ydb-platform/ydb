__all__ = [
    "AnyBaseModel",
    "AnyField",
    "LatestConfiguredBaseModel",
    "LatestGenericModel",
]

from typing import TYPE_CHECKING, Any, TypeAlias

from .consts import IS_PYDANTIC_V2

if TYPE_CHECKING:
    from .v1 import BaseModelV1, FieldV1
    from .v2 import BaseModelV2, FieldV2

    AnyBaseModel: TypeAlias = BaseModelV1 | BaseModelV2
    AnyField: TypeAlias = FieldV1 | FieldV2
else:
    AnyBaseModel = Any
    AnyField = Any


if IS_PYDANTIC_V2:
    from .v2 import ConfiguredBaseModelV2 as LatestConfiguredBaseModel
    from .v2 import GenericModelV2 as LatestGenericModel
else:
    from .v1 import ConfiguredBaseModelV1 as LatestConfiguredBaseModel
    from .v1 import GenericModelV1 as LatestGenericModel
