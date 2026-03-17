__all__ = ['TaskSpec']
from typing import Dict

from .field_spec import FieldSpec
from .view_spec import ViewSpec
from ..primitives.base import BaseTolokaObject


class TaskSpec(BaseTolokaObject):
    """Task interface description and input and output data specifications.

    Input and output data specifications are dictionaries.
    Field IDs are keys and field specifications are values.

    Attributes:
        input_spec: Input data specification.
        output_spec: Output data specification.
        view_spec: The description of the task interface.
    """

    input_spec: Dict[str, FieldSpec]
    output_spec: Dict[str, FieldSpec]
    view_spec: ViewSpec
