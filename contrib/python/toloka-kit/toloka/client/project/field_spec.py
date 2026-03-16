__all__ = [
    'FieldType',
    'FieldSpec',
    'BooleanSpec',
    'StringSpec',
    'IntegerSpec',
    'FloatSpec',
    'UrlSpec',
    'FileSpec',
    'CoordinatesSpec',
    'JsonSpec',
    'ArrayBooleanSpec',
    'ArrayStringSpec',
    'ArrayIntegerSpec',
    'ArrayFloatSpec',
    'ArrayUrlSpec',
    'ArrayFileSpec',
    'ArrayCoordinatesSpec'
]
from enum import unique
from typing import List

from ..primitives.base import BaseTolokaObject
from ...util._docstrings import inherit_docstrings
from ...util._extendable_enum import ExtendableStrEnum


@unique
class FieldType(ExtendableStrEnum):
    BOOLEAN = 'boolean'
    STRING = 'string'
    FLOAT = 'float'
    INTEGER = 'integer'
    URL = 'url'
    FILE = 'file'
    COORDINATES = 'coordinates'
    JSON = 'json'

    ARRAY_BOOLEAN = 'array_boolean'
    ARRAY_STRING = 'array_string'
    ARRAY_INTEGER = 'array_integer'
    ARRAY_FLOAT = 'array_float'
    ARRAY_URL = 'array_url'
    ARRAY_FILE = 'array_file'
    ARRAY_COORDINATES = 'array_coordinates'
    ARRAY_JSON = 'array_json'


class FieldSpec(BaseTolokaObject, spec_enum=FieldType, spec_field='type'):
    """A base class for field specifications used in project's `input_spec` and `output_spec` for input and response data validation.
    Use subclasses of this class to define the data type and set constraints such as maximum string length.

    Attributes:
        required: Whether a field is required. Default value: `True`.
        hidden: Whether to hide an input field from Tolokers. Output fields can't be hidden. Default value: `False`.
    """
    required: bool = True
    hidden: bool = False


@inherit_docstrings
class BooleanSpec(FieldSpec, spec_value=FieldType.BOOLEAN):
    """A boolean field specification.

    Attributes:
        allowed_values: A list of allowed values.
    """
    allowed_values: List[bool]


@inherit_docstrings
class StringSpec(FieldSpec, spec_value=FieldType.STRING):
    """A string field specification.

    Attributes:
        min_length: The minimum length of the string.
        max_length: The maximum length of the string.
        allowed_values: A list of allowed values.
    """
    min_length: int
    max_length: int
    allowed_values: List[str]


@inherit_docstrings
class IntegerSpec(FieldSpec, spec_value=FieldType.INTEGER):
    """An integer field specification.

    Attributes:
        min_value: The minimum value.
        max_value: The maximum value.
        allowed_values: A list of allowed values.
    """
    min_value: int
    max_value: int
    allowed_values: List[int]


@inherit_docstrings
class FloatSpec(FieldSpec, spec_value=FieldType.FLOAT):
    """A floating-point number field specification.

    Attributes:
        min_value: The minimum value.
        max_value: The maximum value.
    """
    min_value: float
    max_value: float


@inherit_docstrings
class UrlSpec(FieldSpec, spec_value=FieldType.URL):
    """A URL field specification.
    """


@inherit_docstrings
class FileSpec(FieldSpec, spec_value=FieldType.FILE):
    """A file field specification.

    `FileSpec` is used for output data only.
    """


@inherit_docstrings
class CoordinatesSpec(FieldSpec, spec_value=FieldType.COORDINATES):
    """Geographical coordinates field specification.

    `CoordinatesSpec` stores values like `“53.910236,27.531110`.

    Attributes:
        current_location: `True` — saves Toloker's current coordinates.
            The attribute can be used in tasks for the mobile application.
    """
    current_location: bool


@inherit_docstrings
class JsonSpec(FieldSpec, spec_value=FieldType.JSON):
    """A JSON object field specification.
    """


@inherit_docstrings
class ArrayBooleanSpec(BooleanSpec, spec_value=FieldType.ARRAY_BOOLEAN):
    """A boolean array field specification.

    Attributes:
        min_size: The minimum number of elements in the array.
        max_size: The maximum number of elements in the array.
    """
    min_size: int
    max_size: int


@inherit_docstrings
class ArrayStringSpec(StringSpec, spec_value=FieldType.ARRAY_STRING):
    """A string array field specification.

    Attributes:
        min_size: The minimum number of elements in the array.
        max_size: The maximum number of elements in the array.
    """
    min_size: int
    max_size: int


@inherit_docstrings
class ArrayIntegerSpec(IntegerSpec, spec_value=FieldType.ARRAY_INTEGER):
    """An integer array field specification.

    Attributes:
        min_size: The minimum number of elements in the array.
        max_size: The maximum number of elements in the array.
    """
    min_size: int
    max_size: int


@inherit_docstrings
class ArrayFloatSpec(FloatSpec, spec_value=FieldType.ARRAY_FLOAT):
    """A floating-point array field specification.

    Attributes:
        min_size: The minimum number of elements in the array.
        max_size: The maximum number of elements in the array.
    """
    min_size: int
    max_size: int


@inherit_docstrings
class ArrayUrlSpec(UrlSpec, spec_value=FieldType.ARRAY_URL):
    """A URL array field specification.

    Attributes:
        min_size: The minimum number of elements in the array.
        max_size: The maximum number of elements in the array.
    """
    min_size: int
    max_size: int


@inherit_docstrings
class ArrayFileSpec(FileSpec, spec_value=FieldType.ARRAY_FILE):
    """A file array field specification.

    `ArrayFileSpec` is used for output data only.

    Attributes:
        min_size: The minimum number of elements in the array.
        max_size: The maximum number of elements in the array.
    """
    min_size: int
    max_size: int


@inherit_docstrings
class ArrayCoordinatesSpec(CoordinatesSpec, spec_value=FieldType.ARRAY_COORDINATES):
    """Geographical coordinates array field specification.

    Attributes:
        min_size: The minimum number of elements in the array.
        max_size: The maximum number of elements in the array.
    """
    min_size: int
    max_size: int


@inherit_docstrings
class ArrayJsonSpec(JsonSpec, spec_value=FieldType.ARRAY_JSON):
    """An array of JSON objects.

    Attributes:
        min_size: The minimum number of elements in the array.
        max_size: The maximum number of elements in the array.
    """
    min_size: int
    max_size: int
