__all__ = [
    "BaseModelV1",
    "ConfiguredBaseModelV1",
    "FieldInfoV1",
    "FieldV1",
    "GenericModelV1",
    "UndefinedV1",
    "create_model_v1",
    "is_pydantic_v1_model",
]

from typing import Any

from fastapi.dependencies.utils import lenient_issubclass
from typing_extensions import TypeIs

try:
    from pydantic.v1 import BaseConfig as BaseConfigV1
    from pydantic.v1 import BaseModel as BaseModelV1
    from pydantic.v1 import create_model as create_model_v1
    from pydantic.v1.fields import FieldInfo as FieldInfoV1
    from pydantic.v1.fields import ModelField as FieldV1
    from pydantic.v1.fields import Undefined as UndefinedV1
    from pydantic.v1.generics import GenericModel as GenericModelV1
except ImportError:  # pragma: no cover
    from pydantic import BaseConfig as BaseConfigV1  # type: ignore[deprecated]
    from pydantic import BaseModel as BaseModelV1
    from pydantic import create_model as create_model_v1
    from pydantic.fields import FieldInfo as FieldInfoV1
    from pydantic.fields import ModelField as FieldV1  # type: ignore[unresolved-import]
    from pydantic.fields import Undefined as UndefinedV1  # type: ignore[unresolved-import]
    from pydantic.generics import GenericModel as GenericModelV1


def is_pydantic_v1_model(model_cls: type[Any]) -> TypeIs[type[BaseModelV1]]:
    return lenient_issubclass(model_cls, BaseModelV1)


class ConfiguredBaseModelV1(BaseModelV1):  # type: ignore[unsupported-base]
    class Config(BaseConfigV1):  # type: ignore[unsupported-base]
        orm_mode = True
        arbitrary_types_allowed = True
        allow_population_by_field_name = True
