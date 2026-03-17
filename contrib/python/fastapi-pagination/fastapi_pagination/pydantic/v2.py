__all__ = [
    "BaseModelV2",
    "ConfiguredBaseModelV2",
    "FieldV2",
    "GenericModelV2",
    "PydanticUndefinedAnnotationV2",
    "UndefinedV2",
    "is_pydantic_v2_model",
]

from typing import Any

from fastapi.dependencies.utils import lenient_issubclass
from typing_extensions import TypeIs

from .consts import IS_PYDANTIC_V2

if IS_PYDANTIC_V2:
    from pydantic import BaseModel as BaseModelV2
    from pydantic import BaseModel as GenericModelV2
    from pydantic import PydanticUndefinedAnnotation as PydanticUndefinedAnnotationV2
    from pydantic.fields import FieldInfo as FieldV2
    from pydantic.fields import PydanticUndefined as UndefinedV2
else:

    class _DummyCls:
        pass

    BaseModelV2 = _DummyCls
    GenericModelV2 = _DummyCls
    FieldV2 = _DummyCls
    UndefinedV2 = _DummyCls

    class PydanticUndefinedAnnotationV2(Exception):
        pass


def is_pydantic_v2_model(model_cls: type[Any]) -> TypeIs[type[BaseModelV2]]:
    if not IS_PYDANTIC_V2:
        return False

    return lenient_issubclass(model_cls, BaseModelV2)


class ConfiguredBaseModelV2(BaseModelV2):
    model_config = {
        "arbitrary_types_allowed": True,
        "from_attributes": True,
        "populate_by_name": True,
    }
