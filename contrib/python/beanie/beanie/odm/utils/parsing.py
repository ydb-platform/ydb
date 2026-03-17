from typing import TYPE_CHECKING, Any, Dict, Type, Union

from pydantic import BaseModel

from beanie.exceptions import (
    ApplyChangesException,
    DocWasNotRegisteredInUnionClass,
    UnionHasNoRegisteredDocs,
)
from beanie.odm.interfaces.detector import ModelType
from beanie.odm.utils.pydantic import get_config_value, parse_model

if TYPE_CHECKING:
    from beanie.odm.documents import Document


def merge_models(left: BaseModel, right: BaseModel) -> None:
    """
    Merge two models
    :param left: left model
    :param right: right model
    :return: None
    """
    from beanie.odm.fields import Link

    for k, right_value in right.__iter__():
        left_value = getattr(left, k)
        if isinstance(right_value, BaseModel) and isinstance(
            left_value, BaseModel
        ):
            if get_config_value(left_value, "frozen"):
                left.__setattr__(k, right_value)
            else:
                merge_models(left_value, right_value)
            continue
        if isinstance(right_value, list):
            links_found = False
            for i in right_value:
                if isinstance(i, Link):
                    links_found = True
                    break
            if links_found:
                continue
            left.__setattr__(k, right_value)
        elif not isinstance(right_value, Link):
            left.__setattr__(k, right_value)


def apply_changes(
    changes: Dict[str, Any], target: Union[BaseModel, Dict[str, Any]]
):
    for key, value in changes.items():
        if "." in key:
            key_parts = key.split(".")
            current_target = target
            try:
                for part in key_parts[:-1]:
                    if isinstance(current_target, dict):
                        current_target = current_target[part]
                    elif isinstance(current_target, BaseModel):
                        current_target = getattr(current_target, part)
                    else:
                        raise ApplyChangesException(
                            f"Unexpected type of target: {type(target)}"
                        )
                final_key = key_parts[-1]
                if isinstance(current_target, dict):
                    current_target[final_key] = value
                elif isinstance(current_target, BaseModel):
                    setattr(current_target, final_key, value)
                else:
                    raise ApplyChangesException(
                        f"Unexpected type of target: {type(target)}"
                    )
            except (KeyError, AttributeError) as e:
                raise ApplyChangesException(
                    f"Failed to apply change for key '{key}': {e}"
                )
        else:
            if isinstance(target, dict):
                target[key] = value
            elif isinstance(target, BaseModel):
                setattr(target, key, value)
            else:
                raise ApplyChangesException(
                    f"Unexpected type of target: {type(target)}"
                )


def save_state(item: BaseModel):
    if hasattr(item, "_save_state"):
        item._save_state()  # type: ignore


def parse_obj(
    model: Union[Type[BaseModel], Type["Document"]],
    data: Any,
    lazy_parse: bool = False,
) -> BaseModel:
    if (
        hasattr(model, "get_model_type")
        and model.get_model_type() == ModelType.UnionDoc  # type: ignore
    ):
        if model._document_models is None:  # type: ignore
            raise UnionHasNoRegisteredDocs

        if isinstance(data, dict):
            class_name = data[model.get_settings().class_id]  # type: ignore
        else:
            class_name = data._class_id

        if class_name not in model._document_models:  # type: ignore
            raise DocWasNotRegisteredInUnionClass
        return parse_obj(
            model=model._document_models[class_name],  # type: ignore
            data=data,
            lazy_parse=lazy_parse,
        )  # type: ignore
    if (
        hasattr(model, "get_model_type")
        and model.get_model_type() == ModelType.Document  # type: ignore
        and model._inheritance_inited  # type: ignore
    ):
        if isinstance(data, dict):
            class_name = data.get(model.get_settings().class_id)  # type: ignore
        elif hasattr(data, model.get_settings().class_id):  # type: ignore
            class_name = data._class_id
        else:
            class_name = None

        if model._children and class_name in model._children:  # type: ignore
            return parse_obj(
                model=model._children[class_name],  # type: ignore
                data=data,
                lazy_parse=lazy_parse,
            )  # type: ignore

    if (
        lazy_parse
        and hasattr(model, "get_model_type")
        and model.get_model_type() == ModelType.Document  # type: ignore
    ):
        o = model.lazy_parse(data, {"_id"})  # type: ignore
        o._saved_state = {"_id": o.id}
        return o
    result = parse_model(model, data)
    save_state(result)
    return result
