from typing import Union

from ..model_tools.definitions import BaseField, InputField, OutputField
from .location import InputFieldLoc, OutputFieldLoc


def input_field_to_loc(field: InputField) -> InputFieldLoc:
    return InputFieldLoc(
        type=field.type,
        field_id=field.id,
        default=field.default,
        metadata=field.metadata,
        is_required=field.is_required,
    )


def output_field_to_loc(field: OutputField) -> OutputFieldLoc:
    return OutputFieldLoc(
        type=field.type,
        field_id=field.id,
        default=field.default,
        metadata=field.metadata,
        accessor=field.accessor,
    )


def field_to_loc(field: BaseField) -> Union[InputFieldLoc, OutputFieldLoc]:
    if isinstance(field, InputField):
        return input_field_to_loc(field)
    if isinstance(field, OutputField):
        return output_field_to_loc(field)
    raise TypeError
