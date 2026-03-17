from __future__ import annotations

import json
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional, Union
from unittest.mock import call

import pydantic
import pytest
import yaml

from datamodel_code_generator.imports import Import
from datamodel_code_generator.model import DataModelFieldBase
from datamodel_code_generator.parser.base import dump_templates
from datamodel_code_generator.parser.jsonschema import (
    JsonSchemaObject,
    JsonSchemaParser,
    get_model_by_path,
)
from datamodel_code_generator.types import DataType

if TYPE_CHECKING:
    from pytest_mock import MockerFixture

import yatest.common as yc
DATA_PATH: Path = Path(yc.source_path(__file__)).parents[1] / "data" / "jsonschema"

EXPECTED_JSONSCHEMA_PATH = Path(yc.source_path(__file__)).parents[1] / "data" / "expected" / "parser" / "jsonschema"


@pytest.mark.parametrize(
    ("schema", "path", "model"),
    [
        ({"foo": "bar"}, None, {"foo": "bar"}),
        ({"a": {"foo": "bar"}}, "a", {"foo": "bar"}),
        ({"a": {"b": {"foo": "bar"}}}, "a/b", {"foo": "bar"}),
        ({"a": {"b": {"c": {"foo": "bar"}}}}, "a/b", {"c": {"foo": "bar"}}),
        ({"a": {"b": {"c": {"foo": "bar"}}}}, "a/b/c", {"foo": "bar"}),
    ],
)
def test_get_model_by_path(schema: dict, path: str, model: dict) -> None:
    assert get_model_by_path(schema, path.split("/") if path else []) == model


def test_json_schema_object_ref_url_json(mocker: MockerFixture) -> None:
    parser = JsonSchemaParser("")
    obj = JsonSchemaObject.parse_obj({"$ref": "https://example.com/person.schema.json#/definitions/User"})
    mock_get = mocker.patch("httpx.get")
    mock_get.return_value.text = json.dumps(
        {
            "$id": "https://example.com/person.schema.json",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "definitions": {
                "User": {
                    "type": "object",
                    "properties": {
                        "name": {
                            "type": "string",
                        }
                    },
                }
            },
        },
    )

    parser.parse_ref(obj, ["Model"])
    assert (
        dump_templates(list(parser.results))
        == """class User(BaseModel):
    name: Optional[str] = None"""
    )
    parser.parse_ref(obj, ["Model"])
    mock_get.assert_has_calls([
        call(
            "https://example.com/person.schema.json",
            headers=None,
            verify=True,
            follow_redirects=True,
            params=None,
        ),
    ])


def test_json_schema_object_ref_url_yaml(mocker: MockerFixture) -> None:
    parser = JsonSchemaParser("")
    obj = JsonSchemaObject.parse_obj({"$ref": "https://example.org/schema.yaml#/definitions/User"})
    mock_get = mocker.patch("httpx.get")
    mock_get.return_value.text = yaml.safe_dump(json.load((DATA_PATH / "user.json").open()))

    parser.parse_ref(obj, ["User"])
    assert (
        dump_templates(list(parser.results))
        == """class User(BaseModel):
    name: Optional[str] = Field(None, example='ken')
    pets: List[User] = Field(default_factory=list)


class Pet(BaseModel):
    name: Optional[str] = Field(None, examples=['dog', 'cat'])"""
    )
    parser.parse_ref(obj, [])
    mock_get.assert_called_once_with(
        "https://example.org/schema.yaml",
        headers=None,
        verify=True,
        follow_redirects=True,
        params=None,
    )


def test_json_schema_object_cached_ref_url_yaml(mocker: MockerFixture) -> None:
    parser = JsonSchemaParser("")

    obj = JsonSchemaObject.parse_obj({
        "type": "object",
        "properties": {
            "pet": {"$ref": "https://example.org/schema.yaml#/definitions/Pet"},
            "user": {"$ref": "https://example.org/schema.yaml#/definitions/User"},
        },
    })
    mock_get = mocker.patch("httpx.get")
    mock_get.return_value.text = yaml.safe_dump(json.load((DATA_PATH / "user.json").open()))

    parser.parse_ref(obj, [])
    assert (
        dump_templates(list(parser.results))
        == """class Pet(BaseModel):
    name: Optional[str] = Field(None, examples=['dog', 'cat'])


class User(BaseModel):
    name: Optional[str] = Field(None, example='ken')
    pets: List[User] = Field(default_factory=list)"""
    )
    mock_get.assert_called_once_with(
        "https://example.org/schema.yaml",
        headers=None,
        verify=True,
        follow_redirects=True,
        params=None,
    )


def test_json_schema_ref_url_json(mocker: MockerFixture) -> None:
    parser = JsonSchemaParser("")
    obj = {
        "type": "object",
        "properties": {"user": {"$ref": "https://example.org/schema.json#/definitions/User"}},
    }
    mock_get = mocker.patch("httpx.get")
    mock_get.return_value.text = json.dumps(json.load((DATA_PATH / "user.json").open()))

    parser.parse_raw_obj("Model", obj, ["Model"])
    assert (
        dump_templates(list(parser.results))
        == """class Model(BaseModel):
    user: Optional[User] = None


class User(BaseModel):
    name: Optional[str] = Field(None, example='ken')
    pets: List[User] = Field(default_factory=list)


class Pet(BaseModel):
    name: Optional[str] = Field(None, examples=['dog', 'cat'])"""
    )
    mock_get.assert_called_once_with(
        "https://example.org/schema.json",
        headers=None,
        verify=True,
        follow_redirects=True,
        params=None,
    )


@pytest.mark.parametrize(
    ("source_obj", "generated_classes"),
    [
        (
            {
                "$id": "https://example.com/person.schema.json",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Person",
                "type": "object",
                "properties": {
                    "firstName": {
                        "type": "string",
                        "description": "The person's first name.",
                    },
                    "lastName": {
                        "type": "string",
                        "description": "The person's last name.",
                    },
                    "age": {
                        "description": "Age in years which must be equal to or greater than zero.",
                        "type": "integer",
                        "minimum": 0,
                    },
                },
            },
            """class Person(BaseModel):
    firstName: Optional[str] = None
    lastName: Optional[str] = None
    age: Optional[conint(ge=0)] = None""",
        ),
        (
            {
                "$id": "https://example.com/person.schema.json",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "person-object",
                "type": "object",
                "properties": {
                    "name": {
                        "type": "string",
                        "description": "The person's name.",
                    },
                    "home-address": {
                        "$ref": "#/definitions/home-address",
                        "description": "The person's home address.",
                    },
                },
                "definitions": {
                    "home-address": {
                        "type": "object",
                        "properties": {
                            "street-address": {"type": "string"},
                            "city": {"type": "string"},
                            "state": {"type": "string"},
                        },
                        "required": ["street_address", "city", "state"],
                    }
                },
            },
            """class Person(BaseModel):
    name: Optional[str] = None
    home_address: Optional[HomeAddress] = None""",
        ),
    ],
)
def test_parse_object(source_obj: dict[str, Any], generated_classes: str) -> None:
    parser = JsonSchemaParser(
        data_model_field_type=DataModelFieldBase,
        source="",
    )
    parser.parse_object("Person", JsonSchemaObject.parse_obj(source_obj), [])
    assert dump_templates(list(parser.results)) == generated_classes


@pytest.mark.parametrize(
    ("source_obj", "generated_classes"),
    [
        (
            {
                "$id": "https://example.com/person.schema.json",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "AnyJson",
                "description": "This field accepts any object",
                "discriminator": "type",
            },
            """class AnyObject(BaseModel):
    __root__: Any = Field(..., description='This field accepts any object', discriminator='type', title='AnyJson')""",
        )
    ],
)
def test_parse_any_root_object(source_obj: dict[str, Any], generated_classes: str) -> None:
    parser = JsonSchemaParser("")
    parser.parse_root_type("AnyObject", JsonSchemaObject.parse_obj(source_obj), [])
    assert dump_templates(list(parser.results)) == generated_classes


@pytest.mark.parametrize(
    ("source_obj", "generated_classes"),
    [
        (
            yaml.safe_load((DATA_PATH / "oneof.json").read_text()),
            (DATA_PATH / "oneof.json.snapshot").read_text(),
        )
    ],
)
def test_parse_one_of_object(source_obj: dict[str, Any], generated_classes: str) -> None:
    parser = JsonSchemaParser("")
    parser.parse_raw_obj("onOfObject", source_obj, [])
    assert dump_templates(list(parser.results)) == generated_classes


@pytest.mark.parametrize(
    ("source_obj", "generated_classes"),
    [
        (
            {
                "$id": "https://example.com/person.schema.json",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "defaults",
                "type": "object",
                "properties": {
                    "string": {
                        "type": "string",
                        "default": "default string",
                    },
                    "string_on_field": {
                        "type": "string",
                        "default": "default string",
                        "description": "description",
                    },
                    "number": {"type": "number", "default": 123},
                    "number_on_field": {
                        "type": "number",
                        "default": 123,
                        "description": "description",
                    },
                    "number_array": {"type": "array", "default": [1, 2, 3]},
                    "string_array": {"type": "array", "default": ["a", "b", "c"]},
                    "object": {"type": "object", "default": {"key": "value"}},
                },
            },
            """class Defaults(BaseModel):
    string: Optional[str] = 'default string'
    string_on_field: Optional[str] = Field('default string', description='description')
    number: Optional[float] = 123
    number_on_field: Optional[float] = Field(123, description='description')
    number_array: Optional[List] = [1, 2, 3]
    string_array: Optional[List] = ['a', 'b', 'c']
    object: Optional[Dict[str, Any]] = {'key': 'value'}""",
        )
    ],
)
def test_parse_default(source_obj: dict[str, Any], generated_classes: str) -> None:
    parser = JsonSchemaParser("")
    parser.parse_raw_obj("Defaults", source_obj, [])
    assert dump_templates(list(parser.results)) == generated_classes


def test_parse_array_schema() -> None:
    parser = JsonSchemaParser("")
    parser.parse_raw_obj("schema", {"type": "object", "properties": {"name": True}}, [])
    assert (
        dump_templates(list(parser.results))
        == """class Schema(BaseModel):
    name: Optional[Any] = None"""
    )


def test_parse_nested_array(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = JsonSchemaParser(
        DATA_PATH / "nested_array.json",
        data_model_field_type=DataModelFieldBase,
    )
    parser.parse()
    assert dump_templates(list(parser.results)) == (DATA_PATH / "nested_array.json.snapshot").read_text()


@pytest.mark.parametrize(
    ("schema_type", "schema_format", "result_type", "from_", "import_", "use_pendulum"),
    [
        ("integer", "int32", "int", None, None, False),
        ("integer", "int64", "int", None, None, False),
        ("integer", "date-time", "datetime", "datetime", "datetime", False),
        ("integer", "date-time", "DateTime", "pendulum", "DateTime", True),
        ("integer", "unix-time", "int", None, None, False),
        ("number", "float", "float", None, None, False),
        ("number", "double", "float", None, None, False),
        ("number", "time", "time", "datetime", "time", False),
        ("number", "time", "Time", "pendulum", "Time", True),
        ("number", "date-time", "datetime", "datetime", "datetime", False),
        ("number", "date-time", "DateTime", "pendulum", "DateTime", True),
        ("string", None, "str", None, None, False),
        ("string", "byte", "str", None, None, False),
        ("string", "binary", "bytes", None, None, False),
        ("boolean", None, "bool", None, None, False),
        ("string", "date", "date", "datetime", "date", False),
        ("string", "date", "Date", "pendulum", "Date", True),
        ("string", "date-time", "datetime", "datetime", "datetime", False),
        ("string", "date-time", "DateTime", "pendulum", "DateTime", True),
        ("string", "duration", "timedelta", "datetime", "timedelta", False),
        ("string", "duration", "Duration", "pendulum", "Duration", True),
        ("string", "path", "Path", "pathlib", "Path", False),
        ("string", "password", "SecretStr", "pydantic", "SecretStr", False),
        ("string", "email", "EmailStr", "pydantic", "EmailStr", False),
        ("string", "uri", "AnyUrl", "pydantic", "AnyUrl", False),
        ("string", "uri-reference", "str", None, None, False),
        ("string", "uuid", "UUID", "uuid", "UUID", False),
        ("string", "uuid1", "UUID1", "pydantic", "UUID1", False),
        ("string", "uuid2", "UUID2", "pydantic", "UUID2", False),
        ("string", "uuid3", "UUID3", "pydantic", "UUID3", False),
        ("string", "uuid4", "UUID4", "pydantic", "UUID4", False),
        ("string", "uuid5", "UUID5", "pydantic", "UUID5", False),
        ("string", "ipv4", "IPv4Address", "ipaddress", "IPv4Address", False),
        ("string", "ipv6", "IPv6Address", "ipaddress", "IPv6Address", False),
        ("string", "unknown-type", "str", None, None, False),
    ],
)
def test_get_data_type(
    schema_type: str,
    schema_format: str,
    result_type: str,
    from_: str | None,
    import_: str | None,
    use_pendulum: bool,
) -> None:
    if from_ and import_:
        import_: Import | None = Import(from_=from_, import_=import_)
    else:
        import_ = None

    parser = JsonSchemaParser("", use_pendulum=use_pendulum)
    assert (
        parser.get_data_type(JsonSchemaObject(type=schema_type, format=schema_format)).dict()
        == DataType(type=result_type, import_=import_).dict()
    )


@pytest.mark.parametrize(
    ("schema_types", "result_types"),
    [
        (["integer", "number"], ["int", "float"]),
        (["integer", "null"], ["int"]),
    ],
)
def test_get_data_type_array(schema_types: list[str], result_types: list[str]) -> None:
    parser = JsonSchemaParser("")
    assert parser.get_data_type(JsonSchemaObject(type=schema_types)) == parser.data_type(
        data_types=[
            parser.data_type(
                type=r,
            )
            for r in result_types
        ],
        is_optional="null" in schema_types,
    )


def test_additional_imports() -> None:
    """Test that additional imports are inside imports container."""
    new_parser = JsonSchemaParser(source="", additional_imports=["collections.deque"])
    assert len(new_parser.imports) == 1
    assert new_parser.imports["collections"] == {"deque"}


def test_no_additional_imports() -> None:
    """Test that not additional imports are not affecting imports container."""
    new_parser = JsonSchemaParser(
        source="",
    )
    assert len(new_parser.imports) == 0


@pytest.mark.parametrize(
    ("source_obj", "generated_classes"),
    [
        (
            {
                "$id": "https://example.com/person.schema.json",
                "$schema": "http://json-schema.org/draft-07/schema#",
                "title": "Person",
                "type": "object",
                "properties": {
                    "firstName": {
                        "type": "string",
                        "description": "The person's first name.",
                        "alt_type": "integer",
                    },
                    "lastName": {
                        "type": "string",
                        "description": "The person's last name.",
                        "alt_type": "integer",
                    },
                    "age": {
                        "description": "Age in years which must be equal to or greater than zero.",
                        "type": "integer",
                        "minimum": 0,
                        "alt_type": "number",
                    },
                    "real_age": {
                        "description": "Age in years which must be equal to or greater than zero.",
                        "type": "integer",
                        "minimum": 0,
                    },
                },
            },
            """class Person(BaseModel):
    firstName: Optional[int] = None
    lastName: Optional[int] = None
    age: Optional[confloat(ge=0.0)] = None
    real_age: Optional[conint(ge=0)] = None""",
        ),
    ],
)
@pytest.mark.skipif(pydantic.VERSION < "2.0.0", reason="Require Pydantic version 2.0.0 or later ")
def test_json_schema_parser_extension(source_obj: dict[str, Any], generated_classes: str) -> None:
    """
    Contrived example to extend the JsonSchemaParser to support an alt_type, which
    replaces the type if present.
    """

    class AltJsonSchemaObject(JsonSchemaObject):
        properties: Optional[dict[str, Union[AltJsonSchemaObject, bool]]] = None  # noqa: UP007, UP045
        alt_type: Optional[str] = None  # noqa: UP045

        def model_post_init(self, context: Any) -> None:  # noqa: ARG002
            if self.alt_type:
                self.type = self.alt_type

    class AltJsonSchemaParser(JsonSchemaParser):
        SCHEMA_OBJECT_TYPE = AltJsonSchemaObject

    parser = AltJsonSchemaParser(
        data_model_field_type=DataModelFieldBase,
        source="",
    )
    parser.parse_object("Person", AltJsonSchemaObject.parse_obj(source_obj), [])
    assert dump_templates(list(parser.results)) == generated_classes
