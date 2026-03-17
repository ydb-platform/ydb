"""The main function in this module is `create_model_from_schema`, which takes a JSON
schema as input and returns a dynamically created Pydantic model class. The function
supports various features such as nested objects, referenced definitions, custom
configurations, custom base classes, custom validators, and more.

Example usage:

    >>> from dydantic import create_model_from_schema
    >>> json_schema = {
    ...     "title": "Person",
    ...     "type": "object",
    ...     "properties": {
    ...         "name": {"type": "string"},
    ...         "age": {"type": "integer"},
    ...     },
    ...     "required": ["name"],
    ... }
    >>> Person = create_model_from_schema(json_schema)
    >>> person = Person(name="John", age=30)
    >>> person
    Person(name='John', age=30)

The module also includes helper functions such as `_json_schema_to_pydantic_field` and
`_json_schema_to_pydantic_type` that are used internally by `create_model_from_schema`
to convert JSON schema definitions to Pydantic fields and types.

Functions:
    - create_model_from_schema: Create a Pydantic model from a JSON schema.

For more detailed information and examples, refer to the docstring of the
`create_model_from_schema` function.
"""

from __future__ import annotations
import datetime
import logging
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union

from typing_extensions import Annotated
import uuid
from pydantic.networks import (
    IPv4Address,
    IPv6Address,
    IPvAnyAddress,
    IPvAnyInterface,
    IPvAnyNetwork,
)
from pydantic import (
    AnyUrl,
    Field,
    HttpUrl,
    Json,
    PostgresDsn,
    SecretBytes,
    SecretStr,
    StrictBytes,
    UUID1,
    UUID3,
    UUID4,
    UUID5,
    FileUrl,
    DirectoryPath,
    FilePath,
    NewPath,
    MongoDsn,
)
from pydantic import BaseModel, create_model as create_model_base, ConfigDict

logger = logging.getLogger(__name__)
if TYPE_CHECKING:
    from pydantic.main import AnyClassMethod
    from pydantic import EmailStr
else:
    try:
        from pydantic import EmailStr
    except ImportError:
        logger.warning(
            "EmailStr requires email_validator package to be installed. Will use str instead."
        )
        EmailStr = str


def create_model_from_schema(
    json_schema: Dict[str, Any],
    *,
    root_schema: Optional[Dict[str, Any]] = None,
    __config__: ConfigDict | None = None,
    __base__: None = None,
    __module__: str = __name__,
    __validators__: dict[str, AnyClassMethod] | None = None,
    __cls_kwargs__: dict[str, Any] | None = None,
) -> Type[BaseModel]:
    """Create a Pydantic model from a JSON schema.

    This function takes a JSON schema as input and dynamically creates a Pydantic model class
    based on the schema. It supports various JSON schema features such as nested objects,
    referenced definitions, custom configurations, custom base classes, custom validators, and more.

    Args:
        json_schema: A dictionary representing the JSON schema.
        root_schema: The root schema that contains the current schema. If not provided, the
            current schema will be treated as the root schema.
        __config__: Pydantic configuration for the generated model.
        __base__: Base class for the generated model. Defaults to `pydantic.BaseModel`.
        __module__: Module name for the generated model class. Defaults to current module.
        __validators__: A dictionary of custom validators for the generated model.
        __cls_kwargs__: Additional keyword arguments for the generated model class.

    Returns:
        A dynamically created Pydantic model class based on the provided JSON schema.

    **Examples:**

    1. Simple schema with string and integer properties:

            >>> json_schema = {
            ...     "title": "Person",
            ...     "type": "object",
            ...     "properties": {
            ...         "name": {"type": "string"},
            ...         "age": {"type": "integer"},
            ...     },
            ...     "required": ["name"],
            ... }
            >>> Person = create_model_from_schema(json_schema)
            >>> person = Person(name="John", age=30)
            >>> person
            Person(name='John', age=30)

    2. Schema with a nested object:

            >>> json_schema = {
            ...     "title": "Employee",
            ...     "type": "object",
            ...     "properties": {
            ...         "name": {"type": "string"},
            ...         "age": {"type": "integer"},
            ...         "address": {
            ...             "type": "object",
            ...             "properties": {
            ...                 "street": {"type": "string"},
            ...                 "city": {"type": "string"},
            ...             },
            ...         },
            ...     },
            ...     "required": ["name", "address"],
            ... }
            >>> Employee = create_model_from_schema(json_schema)
            >>> employee = Employee(
            ...     name="Alice", age=25, address={"street": "123 Main St", "city": "New York"}
            ... )
            >>> employee
            Employee(name='Alice', age=25, address=Address(street='123 Main St', city='New York'))

    3. Schema with a referenced definition:

            >>> json_schema = {
            ...     "title": "Student",
            ...     "type": "object",
            ...     "properties": {
            ...         "name": {"type": "string"},
            ...         "grade": {"type": "integer"},
            ...         "school": {"$ref": "#/$defs/School"},
            ...     },
            ...     "required": ["name", "school"],
            ...     "$defs": {
            ...         "School": {
            ...             "type": "object",
            ...             "properties": {
            ...                 "name": {"type": "string"},
            ...                 "address": {"type": "string"},
            ...             },
            ...             "required": ["name"],
            ...         },
            ...     },
            ... }
            >>> Student = create_model_from_schema(json_schema)
            >>> student = Student(
            ...     name="Bob", grade=10, school={"name": "ABC School", "address": "456 Elm St"}
            ... )
            >>> student
            Student(name='Bob', grade=10, school=School(name='ABC School', address='456 Elm St'))

    4. Schema with a custom configuration:

            >>> json_schema = {
            ...     "title": "User",
            ...     "type": "object",
            ...     "properties": {
            ...         "username": {"type": "string"},
            ...         "email": {"type": "string", "format": "email"},
            ...     },
            ...     "required": ["username", "email"],
            ... }
            >>> config = ConfigDict(extra="forbid")
            >>> User = create_model_from_schema(json_schema, __config__=config)
            >>> user = User(username="john_doe", email="john@example.com")
            >>> user
            User(username='john_doe', email='john@example.com')

    5. Schema with a custom base class:

            >>> class CustomBaseModel(BaseModel):
            ...     class Config:
            ...         frozen = True
            >>> json_schema = {
            ...     "title": "Product",
            ...     "type": "object",
            ...     "properties": {
            ...         "name": {"type": "string"},
            ...         "price": {"type": "number"},
            ...     },
            ...     "required": ["name", "price"],
            ... }
            >>> Product = create_model_from_schema(json_schema, __base__=CustomBaseModel)
            >>> product = Product(name="Phone", price=499.99)
            >>> product
            Product(name='Phone', price=499.99)
            >>> product.price = 599.99  # doctest: +IGNORE_EXCEPTION_DETAIL
            Traceback (most recent call last):
               ...
            pydantic_core._pydantic_core.ValidationError: 1 validation error for Product
            price
                Instance is frozen [type=frozen_instance, input_value=599.99, input_type=float]

    6. Schema with a custom validator:

            >>> from pydantic import field_validator
            >>> def price_validator(value):
            ...     if value <= 0:
            ...         raise ValueError("Price must be positive")
            ...     return value
            >>> json_schema = {
            ...     "title": "Item",
            ...     "type": "object",
            ...     "properties": {
            ...         "name": {"type": "string"},
            ...         "price": {"type": "number"},
            ...     },
            ...     "required": ["name", "price"],
            ... }
            >>> Item = create_model_from_schema(
            ...     json_schema,
            ...     __validators__={
            ...         "my_price_validator": field_validator("price")(price_validator)
            ...     },
            ... )
            >>> item = Item(name="Pen", price=-10)  # doctest: +IGNORE_EXCEPTION_DETAIL
            Traceback (most recent call last):
            ...
            pydantic_core._pydantic_core.ValidationError: 1 validation error for Item
            price
               ValueError: Price must be positive

    7. Schema with a union type using `anyOf`:

            >>> json_schema = {
            ...     "title": "SKU",
            ...     "type": "object",
            ...     "properties": {
            ...         "value": {
            ...             "title": "Value",
            ...             "anyOf": [
            ...                 {"type": "string"},
            ...                 {"type": "integer"},
            ...                 {"type": "boolean"},
            ...             ],
            ...         },
            ...     },
            ... }
            >>> Sku = create_model_from_schema(json_schema)
            >>> Sku(value="hello")
            SKU(value='hello')
            >>> Sku(value=42)
            SKU(value=42)
            >>> Sku(value=True)
            SKU(value=True)

    8. Schema with a string format:

            >>> json_schema = {
            ...     "title": "User",
            ...     "type": "object",
            ...     "properties": {
            ...         "username": {"type": "string"},
            ...         "email": {"type": "string", "format": "email"},
            ...         "password": {"type": "string", "format": "password"},
            ...     },
            ...     "required": ["username", "email", "password"],
            ... }
            >>> User = create_model_from_schema(json_schema)
            >>> user = User(username="john_doe", email="john@example.com", password="secret")
            >>> user
            User(username='john_doe', email='john@example.com', password=SecretStr('**********'))
            >>> user.password
            SecretStr('**********')

    9. Schema with an array of items:

            >>> json_schema = {
            ...     "title": "Numbers",
            ...     "type": "object",
            ...     "properties": {
            ...         "value": {
            ...             "type": "array",
            ...             "items": {"type": "integer"},
            ...         }
            ...     },
            ... }
            >>> Numbers = create_model_from_schema(json_schema)
            >>> numbers = Numbers(value=[1, 2, 3, 4, 5])
            >>> numbers
            Numbers(value=[1, 2, 3, 4, 5])

    10. Schema with a nested array of objects:

            >>> json_schema = {
            ...     "title": "Matrix",
            ...     "type": "object",
            ...     "properties": {
            ...         "value": {
            ...             "type": "array",
            ...             "items": {
            ...                 "type": "array",
            ...                 "items": {"type": "integer"},
            ...             },
            ...         },
            ...     },
            ... }
            >>> Matrix = create_model_from_schema(json_schema)
            >>> matrix = Matrix(value=[[1, 2, 3], [4, 5, 6], [7, 8, 9]])
            >>> matrix
            Matrix(value=[[1, 2, 3], [4, 5, 6], [7, 8, 9]])

    """  # noqa: E501
    model_name = json_schema.get("title", "DynamicModel")
    field_definitions = {
        name: _json_schema_to_pydantic_field(
            name, prop, json_schema.get("required", []), root_schema or json_schema
        )
        for name, prop in (json_schema.get("properties", {}) or {}).items()
    }

    return create_model_base(
        model_name,
        __config__=__config__,
        __base__=__base__,
        __module__=__module__,
        __validators__=__validators__,
        __cls_kwargs__=__cls_kwargs__,
        **field_definitions,
    )


FORMAT_TYPE_MAP: Dict[str, Type[Any]] = {
    "base64": Annotated[bytes, Field(json_schema_extra={"format": "base64"})],  # type: ignore[dict-item]
    "binary": StrictBytes,
    "date": datetime.date,
    "time": datetime.time,
    "date-time": datetime.datetime,
    "duration": datetime.timedelta,
    "directory-path": DirectoryPath,
    "email": EmailStr,
    "file-path": FilePath,
    "ipv4": IPv4Address,
    "ipv6": IPv6Address,
    "ipvanyaddress": IPvAnyAddress,
    "ipvanyinterface": IPvAnyInterface,
    "ipvanynetwork": IPvAnyNetwork,
    "json-string": Json,
    "multi-host-uri": Union[PostgresDsn, MongoDsn],  # type: ignore[dict-item]
    "password": SecretStr,
    "path": NewPath,
    "uri": AnyUrl,
    "uuid": uuid.UUID,
    "uuid1": UUID1,
    "uuid3": UUID3,
    "uuid4": UUID4,
    "uuid5": UUID5,
}


def _json_schema_to_pydantic_field(
    name: str,
    json_schema: Dict[str, Any],
    required: List[str],
    root_schema: Dict[str, Any],
) -> Any:
    type_ = _json_schema_to_pydantic_type(json_schema, root_schema, name_=name.title())
    description = json_schema.get("description")
    examples = json_schema.get("examples")
    is_required = name in required
    field_kwargs = {}
    if description:
        field_kwargs["description"] = description
    if examples:
        field_kwargs["examples"] = examples
    if not is_required:
        field_kwargs["default"] = None
    default = ... if is_required else None

    if isinstance(type_, type) and issubclass(type_, (int, float)):
        if "minimum" in json_schema:
            field_kwargs["ge"] = json_schema["minimum"]
        if "exclusiveMinimum" in json_schema:
            field_kwargs["gt"] = json_schema["exclusiveMinimum"]
        if "maximum" in json_schema:
            field_kwargs["le"] = json_schema["maximum"]
        if "exclusiveMaximum" in json_schema:
            field_kwargs["lt"] = json_schema["exclusiveMaximum"]
        if "multipleOf" in json_schema:
            field_kwargs["multiple_of"] = json_schema["multipleOf"]

    format_ = json_schema.get("format")
    if format_ in FORMAT_TYPE_MAP:
        pydantic_type = FORMAT_TYPE_MAP[format_]

        if format_ == "binary":
            field_kwargs["strict"] = True
        elif format_ == "password":
            if json_schema.get("writeOnly"):
                pydantic_type = SecretBytes
        elif format_ == "uri":
            allowed_schemes = json_schema.get("scheme")
            if allowed_schemes:
                if len(allowed_schemes) == 1 and allowed_schemes[0] == "http":
                    pydantic_type = HttpUrl
                elif len(allowed_schemes) == 1 and allowed_schemes[0] == "file":
                    pydantic_type = FileUrl
                else:
                    field_kwargs["allowed_schemes"] = allowed_schemes

        type_ = pydantic_type

    if isinstance(type_, type) and issubclass(type_, str):
        if "minLength" in json_schema:
            field_kwargs["min_length"] = json_schema["minLength"]
        if "maxLength" in json_schema:
            field_kwargs["max_length"] = json_schema["maxLength"]
    if not is_required:
        type_ = Optional[type_]
    return (type_, Field(default, json_schema_extra=field_kwargs))


def _json_schema_to_pydantic_type(
    json_schema: Dict[str, Any],
    root_schema: Dict[str, Any],
    *,
    name_: Optional[str] = None,
) -> Any:
    ref = json_schema.get("$ref")
    if ref:
        ref_path = ref.split("/")
        if ref.startswith("#/$defs/"):
            ref_schema = root_schema["$defs"]
            start_idx = 2
        else:
            ref_schema = root_schema
            start_idx = 1
        for path in ref_path[start_idx:]:
            ref_schema = ref_schema[path]
        return _json_schema_to_pydantic_type(ref_schema, root_schema, name_=name_)

    any_of_schemas = []
    if "anyOf" in json_schema or "oneOf" in json_schema:
        any_of_schemas = json_schema.get("anyOf", []) + json_schema.get("oneOf", [])
    if any_of_schemas:
        any_of_types = [
            _json_schema_to_pydantic_type(schema, root_schema)
            for schema in any_of_schemas
        ]
        return Union[tuple(any_of_types)]

    all_of_schemas = json_schema.get("allOf")
    if all_of_schemas:
        all_of_types = [
            _json_schema_to_pydantic_type(schema, root_schema)
            for schema in all_of_schemas
        ]
        if len(all_of_types) == 1:
            return all_of_types[0]
        return tuple(all_of_types)

    type_ = json_schema.get("type")

    if type_ == "string":
        return str
    elif type_ == "integer":
        return int
    elif type_ == "number":
        return float
    elif type_ == "boolean":
        return bool
    elif type_ == "array":
        items_schema = json_schema.get("items")
        if items_schema:
            item_type = _json_schema_to_pydantic_type(
                items_schema, root_schema, name_=name_
            )
            return List[item_type]  # type: ignore[valid-type]
        else:
            return List
    elif type_ == "object":
        properties = json_schema.get("properties")
        if properties:
            json_schema_ = json_schema.copy()
            if json_schema_.get("title") is None:
                json_schema_["title"] = name_
            nested_model = create_model_from_schema(
                json_schema_, root_schema=root_schema
            )
            return nested_model
        else:
            return Dict
    elif type_ == "null":
        return None
    elif type_ is None:
        return Any
    else:
        raise ValueError(f"Unsupported JSON schema type: {type_} from {json_schema}")


__all__ = ["create_model_from_schema"]
