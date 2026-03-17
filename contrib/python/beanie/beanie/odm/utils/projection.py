from typing import Dict, Optional, Type, TypeVar

from pydantic import BaseModel

from beanie.odm.interfaces.detector import ModelType
from beanie.odm.utils.pydantic import get_config_value, get_model_fields

ProjectionModelType = TypeVar("ProjectionModelType", bound=BaseModel)


def get_projection(
    model: Type[ProjectionModelType],
) -> Optional[Dict[str, int]]:
    if hasattr(model, "get_model_type") and (
        model.get_model_type() == ModelType.UnionDoc  # type: ignore
        or (  # type: ignore
            model.get_model_type() == ModelType.Document  # type: ignore
            and model._inheritance_inited  # type: ignore
        )
    ):  # type: ignore
        return None

    if hasattr(model, "Settings"):  # MyPy checks
        settings = getattr(model, "Settings")

        if hasattr(settings, "projection"):
            return getattr(settings, "projection")

    if get_config_value(model, "extra") == "allow":
        return None

    document_projection: Dict[str, int] = {}

    for name, field in get_model_fields(model).items():
        document_projection[field.alias or name] = 1
    return document_projection
