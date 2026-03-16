from __future__ import annotations

import os
import platform
from pathlib import Path
from typing import Any

import black
import pydantic
import pytest
from packaging import version

from datamodel_code_generator import OpenAPIScope, PythonVersionMin
from datamodel_code_generator.model import DataModelFieldBase
from datamodel_code_generator.model.pydantic import DataModelField
from datamodel_code_generator.parser.base import dump_templates
from datamodel_code_generator.parser.jsonschema import JsonSchemaObject
from datamodel_code_generator.parser.openapi import (
    MediaObject,
    OpenAPIParser,
    ParameterObject,
    RequestBodyObject,
    ResponseObject,
)

import yatest.common as yc
DATA_PATH: Path = Path(yc.source_path(__file__)).parents[1] / "data" / "openapi"

EXPECTED_OPEN_API_PATH = Path(yc.source_path(__file__)).parents[1] / "data" / "expected" / "parser" / "openapi"


def get_expected_file(
    test_name: str,
    with_import: bool,
    format_: bool,
    base_class: str | None = None,
    prefix: str | None = None,
) -> Path:
    params: list[str] = []
    if with_import:
        params.append("with_import")
    if format_:
        params.append("format")
    if base_class:
        params.append(base_class)
    file_name = "_".join(params or "output")

    return EXPECTED_OPEN_API_PATH / test_name / (prefix or "") / f"{file_name}.py"


@pytest.mark.parametrize(
    ("source_obj", "generated_classes"),
    [
        (
            {"properties": {"name": {"type": "string"}}},
            """class Pets(BaseModel):
    name: Optional[str] = None""",
        ),
        (
            {
                "properties": {
                    "kind": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    }
                }
            },
            """class Kind(BaseModel):
    name: Optional[str] = None


class Pets(BaseModel):
    kind: Optional[Kind] = None""",
        ),
        (
            {
                "properties": {
                    "Kind": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    }
                }
            },
            """class Kind(BaseModel):
    name: Optional[str] = None


class Pets(BaseModel):
    Kind: Optional[Kind] = None""",
        ),
        (
            {
                "properties": {
                    "pet_kind": {
                        "type": "object",
                        "properties": {"name": {"type": "string"}},
                    }
                }
            },
            """class PetKind(BaseModel):
    name: Optional[str] = None


class Pets(BaseModel):
    pet_kind: Optional[PetKind] = None""",
        ),
        (
            {
                "properties": {
                    "kind": {
                        "type": "array",
                        "items": [
                            {
                                "type": "object",
                                "properties": {"name": {"type": "string"}},
                            }
                        ],
                    }
                }
            },
            """class KindItem(BaseModel):
    name: Optional[str] = None


class Pets(BaseModel):
    kind: Optional[List[KindItem]] = None""",
        ),
        (
            {"properties": {"kind": {"type": "array", "items": []}}},
            """class Pets(BaseModel):
    kind: Optional[List] = None""",
        ),
    ],
)
def test_parse_object(source_obj: dict[str, Any], generated_classes: str) -> None:
    parser = OpenAPIParser("")
    parser.parse_object("Pets", JsonSchemaObject.parse_obj(source_obj), [])
    assert dump_templates(list(parser.results)) == generated_classes


@pytest.mark.parametrize(
    ("source_obj", "generated_classes"),
    [
        (
            {
                "type": "array",
                "items": {"type": "object", "properties": {"name": {"type": "string"}}},
            },
            """class Pet(BaseModel):
    name: Optional[str] = None


class Pets(BaseModel):
    __root__: List[Pet]""",
        ),
        (
            {
                "type": "array",
                "items": [{"type": "object", "properties": {"name": {"type": "string"}}}],
            },
            """class Pet(BaseModel):
    name: Optional[str] = None


class Pets(BaseModel):
    __root__: List[Pet]""",
        ),
        (
            {
                "type": "array",
                "items": {},
            },
            """class Pets(BaseModel):
    __root__: List""",
        ),
    ],
)
def test_parse_array(source_obj: dict[str, Any], generated_classes: str) -> None:
    parser = OpenAPIParser("")
    parser.parse_array("Pets", JsonSchemaObject.parse_obj(source_obj), [])
    assert dump_templates(list(parser.results)) == generated_classes


@pytest.mark.parametrize(
    ("with_import", "format_", "base_class"),
    [
        (
            True,
            True,
            None,
        ),
        (
            False,
            True,
            None,
        ),
        (
            True,
            False,
            None,
        ),
        (True, True, "custom_module.Base"),
    ],
)
def test_openapi_parser_parse(with_import: bool, format_: bool, base_class: str | None) -> None:
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source=Path(DATA_PATH / "api.yaml"),
        base_class=base_class,
    )
    expected_file = get_expected_file("openapi_parser_parse", with_import, format_, base_class)
    assert (
        parser.parse(with_import=with_import, format_=format_, settings_path=DATA_PATH.parent)
        == expected_file.read_text()
    )


@pytest.mark.parametrize(
    ("source_obj", "generated_classes"),
    [
        (
            {"type": "string", "nullable": True},
            """class Name(BaseModel):
    __root__: Optional[str] = None""",
        ),
        (
            {"type": "string", "nullable": False},
            """class Name(BaseModel):
    __root__: str""",
        ),
    ],
)
def test_parse_root_type(source_obj: dict[str, Any], generated_classes: str) -> None:
    parser = OpenAPIParser("")
    parser.parse_root_type("Name", JsonSchemaObject.parse_obj(source_obj), [])
    assert dump_templates(list(parser.results)) == generated_classes


def test_openapi_parser_parse_duplicate_models(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "duplicate_models.yaml"),
    )
    assert (
        parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_duplicate_models" / "output.py").read_text()
    )


def test_openapi_parser_parse_duplicate_model_with_simplify(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    raw = Path(DATA_PATH / "duplicate_model_simplify.yaml")
    parser = OpenAPIParser(raw)
    expected = (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_duplicate_models_simplify" / "output.py").read_text()
    got = parser.parse()
    assert got == expected


def test_openapi_parser_parse_resolved_models(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "resolved_models.yaml"),
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_resolved_models" / "output.py").read_text()


def test_openapi_parser_parse_lazy_resolved_models(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "lazy_resolved_models.yaml"),
    )
    assert (
        parser.parse()
        == """from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class Pet(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None


class Pets(BaseModel):
    __root__: List[Pet]


class Error(BaseModel):
    code: int
    message: str


class Event(BaseModel):
    name: Optional[str] = None
    event: Optional[Event] = None


class Events(BaseModel):
    __root__: List[Event]


class Results(BaseModel):
    envets: Optional[List[Events]] = None
    event: Optional[List[Event]] = None
"""
    )


def test_openapi_parser_parse_x_enum_varnames(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "x_enum_varnames.yaml"),
    )
    assert (
        parser.parse()
        == """from __future__ import annotations

from enum import Enum


class String(Enum):
    dog = 'dog'
    cat = 'cat'
    snake = 'snake'


class UnknownTypeString(Enum):
    dog = 'dog'
    cat = 'cat'
    snake = 'snake'


class NamedString(Enum):
    EQ = '='
    NE = '!='
    GT = '>'
    LT = '<'
    GE = '>='
    LE = '<='


class NamedNumber(Enum):
    one = 1
    two = 2
    three = 3


class Number(Enum):
    number_1 = 1
    number_2 = 2
    number_3 = 3


class UnknownTypeNumber(Enum):
    int_1 = 1
    int_2 = 2
    int_3 = 3
"""
    )


@pytest.mark.skipif(pydantic.VERSION < "1.9.0", reason="Require Pydantic version 1.9.0 or later ")
def test_openapi_parser_parse_enum_models() -> None:
    parser = OpenAPIParser(
        Path(DATA_PATH / "enum_models.yaml").read_text(encoding="utf-8"),
        target_python_version=PythonVersionMin,
    )
    expected_dir = EXPECTED_OPEN_API_PATH / "openapi_parser_parse_enum_models"
    assert parser.parse() == (expected_dir / "output.py").read_text()


def test_openapi_parser_parse_anyof(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "anyof.yaml"),
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_anyof" / "output.py").read_text()


def test_openapi_parser_parse_anyof_required(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "anyof_required.yaml"),
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_anyof_required" / "output.py").read_text()


def test_openapi_parser_parse_nested_anyof(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "nested_anyof.yaml").read_text(encoding="utf-8"),
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_nested_anyof" / "output.py").read_text()


def test_openapi_parser_parse_oneof(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "oneof.yaml"),
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_oneof" / "output.py").read_text()


def test_openapi_parser_parse_nested_oneof(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "nested_oneof.yaml").read_text(encoding="utf-8"),
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_nested_oneof" / "output.py").read_text()


def test_openapi_parser_parse_allof_ref(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "allof_same_prefix_with_ref.yaml"),
    )
    assert (
        parser.parse()
        == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_allof_same_prefix_with_ref" / "output.py").read_text()
    )


def test_openapi_parser_parse_allof(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "allof.yaml"),
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_allof" / "output.py").read_text()


def test_openapi_parser_parse_allof_required_fields(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "allof_required_fields.yaml"),
    )
    assert (
        parser.parse()
        == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_allof_required_fields" / "output.py").read_text()
    )


def test_openapi_parser_parse_alias(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        Path(DATA_PATH / "alias.yaml"),
    )
    delimiter = "\\" if platform.system() == "Windows" else "/"
    results = {delimiter.join(p): r for p, r in parser.parse().items()}
    openapi_parser_parse_alias_dir = EXPECTED_OPEN_API_PATH / "openapi_parser_parse_alias"
    for path in openapi_parser_parse_alias_dir.rglob("*.py"):
        key = str(path.relative_to(openapi_parser_parse_alias_dir))
        assert results.pop(key).body == path.read_text()


def test_openapi_parser_parse_modular(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(Path(DATA_PATH / "modular.yaml"), data_model_field_type=DataModelFieldBase)
    modules = parser.parse()
    main_modular_dir = EXPECTED_OPEN_API_PATH / "openapi_parser_parse_modular"

    for paths, result in modules.items():
        expected = main_modular_dir.joinpath(*paths).read_text()
        assert result.body == expected


@pytest.mark.parametrize(
    ("with_import", "format_", "base_class"),
    [
        (
            True,
            True,
            None,
        ),
        (
            False,
            True,
            None,
        ),
        (
            True,
            False,
            None,
        ),
        (
            True,
            True,
            "custom_module.Base",
        ),
    ],
)
def test_openapi_parser_parse_additional_properties(with_import: bool, format_: bool, base_class: str | None) -> None:
    parser = OpenAPIParser(
        Path(DATA_PATH / "additional_properties.yaml").read_text(encoding="utf-8"),
        base_class=base_class,
        data_model_field_type=DataModelFieldBase,
    )

    assert (
        parser.parse(with_import=with_import, format_=format_, settings_path=DATA_PATH.parent)
        == get_expected_file(
            "openapi_parser_parse_additional_properties",
            with_import,
            format_,
            base_class,
        ).read_text()
    )


@pytest.mark.skip(reason="flaky")
def test_openapi_parser_parse_array_enum(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(source=Path(DATA_PATH / "array_enum.yaml"))
    expected_file = get_expected_file("openapi_parser_parse_array_enum", True, True)
    assert parser.parse() == expected_file.read_text()


@pytest.mark.xfail
def test_openapi_parser_parse_remote_ref(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source=(DATA_PATH / "refs.yaml").read_text(),
        http_ignore_tls=bool(os.environ.get("HTTP_IGNORE_TLS")),
    )
    expected_file = get_expected_file("openapi_parser_parse_remote_ref", True, True)

    assert parser.parse() == expected_file.read_text()


def test_openapi_parser_parse_required_null(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(source=Path(DATA_PATH / "required_null.yaml"))
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_required_null" / "output.py").read_text()


def test_openapi_model_resolver(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(source=(DATA_PATH / "api.yaml"))
    parser.parse()

    references = {
        k: v.dict(
            exclude={"source", "module_name", "actual_module_name"},
        )
        for k, v in parser.model_resolver.references.items()
    }
    assert references == {
        "api.yaml#/components/schemas/Error": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Error",
            "original_name": "Error",
            "path": "api.yaml#/components/schemas/Error",
        },
        "api.yaml#/components/schemas/Event": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Event",
            "original_name": "Event",
            "path": "api.yaml#/components/schemas/Event",
        },
        "api.yaml#/components/schemas/Id": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Id",
            "original_name": "Id",
            "path": "api.yaml#/components/schemas/Id",
        },
        "api.yaml#/components/schemas/Pet": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Pet",
            "original_name": "Pet",
            "path": "api.yaml#/components/schemas/Pet",
        },
        "api.yaml#/components/schemas/Pets": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Pets",
            "original_name": "Pets",
            "path": "api.yaml#/components/schemas/Pets",
        },
        "api.yaml#/components/schemas/Result": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Result",
            "original_name": "Result",
            "path": "api.yaml#/components/schemas/Result",
        },
        "api.yaml#/components/schemas/Rules": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Rules",
            "original_name": "Rules",
            "path": "api.yaml#/components/schemas/Rules",
        },
        "api.yaml#/components/schemas/Users": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Users",
            "original_name": "Users",
            "path": "api.yaml#/components/schemas/Users",
        },
        "api.yaml#/components/schemas/Users/Users/0#-datamodel-code-generator-#-object-#-special-#": {
            "duplicate_name": None,
            "loaded": True,
            "name": "User",
            "original_name": "Users",
            "path": "api.yaml#/components/schemas/Users/Users/0#-datamodel-code-generator-#-object-#-special-#",
        },
        "api.yaml#/components/schemas/apis": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Apis",
            "original_name": "apis",
            "path": "api.yaml#/components/schemas/apis",
        },
        "api.yaml#/components/schemas/apis/apis/0#-datamodel-code-generator-#-object-#-special-#": {
            "duplicate_name": None,
            "loaded": True,
            "name": "Api",
            "original_name": "apis",
            "path": "api.yaml#/components/schemas/apis/apis/0#-datamodel-code-generator-#-object-#-special-#",
        },
    }


def test_openapi_parser_parse_any(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source=Path(DATA_PATH / "any.yaml"),
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_parse_any" / "output.py").read_text()


def test_openapi_parser_responses_without_content(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source=Path(DATA_PATH / "body_and_parameters.yaml"),
        openapi_scopes=[OpenAPIScope.Paths],
        allow_responses_without_content=True,
    )
    assert (
        parser.parse()
        == (EXPECTED_OPEN_API_PATH / "openapi_parser_responses_without_content" / "output.py").read_text()
    )


def test_openapi_parser_responses_with_tag(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(tmp_path)
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source=Path(DATA_PATH / "body_and_parameters.yaml"),
        openapi_scopes=[OpenAPIScope.Tags, OpenAPIScope.Schemas, OpenAPIScope.Paths],
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_responses_with_tag" / "output.py").read_text()


@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
def test_openapi_parser_with_query_parameters() -> None:
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source=Path(DATA_PATH / "query_parameters.yaml"),
        openapi_scopes=[
            OpenAPIScope.Parameters,
            OpenAPIScope.Schemas,
            OpenAPIScope.Paths,
        ],
    )
    assert parser.parse() == (EXPECTED_OPEN_API_PATH / "openapi_parser_with_query_parameters" / "output.py").read_text()


@pytest.mark.skipif(
    black.__version__.split(".")[0] >= "24",
    reason="Installed black doesn't support the old style",
)
def test_openapi_parser_with_include_path_parameters() -> None:
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source=Path(DATA_PATH / "query_parameters.yaml"),
        openapi_scopes=[
            OpenAPIScope.Parameters,
            OpenAPIScope.Schemas,
            OpenAPIScope.Paths,
        ],
        include_path_parameters=True,
    )
    assert (
        parser.parse()
        == (EXPECTED_OPEN_API_PATH / "openapi_parser_with_query_parameters" / "with_path_params.py").read_text()
    )


def test_parse_all_parameters_duplicate_names_exception() -> None:
    parser = OpenAPIParser("", include_path_parameters=True)
    parameters = [
        ParameterObject.parse_obj({"name": "duplicate_param", "in": "path", "schema": {"type": "string"}}),
        ParameterObject.parse_obj({"name": "duplicate_param", "in": "query", "schema": {"type": "integer"}}),
    ]

    with pytest.raises(Exception) as exc_info:  # noqa: PT011
        parser.parse_all_parameters("TestModel", parameters, ["test", "path"])

    assert "Parameter name 'duplicate_param' is used more than once." in str(exc_info.value)


@pytest.mark.skipif(
    version.parse(pydantic.VERSION) < version.parse("2.9.0"),
    reason="Require Pydantic version 2.0.0 or later ",
)
def test_openapi_parser_array_called_fields_with_one_of_items() -> None:
    parser = OpenAPIParser(
        data_model_field_type=DataModelField,
        source=Path(DATA_PATH / "array_called_fields_with_oneOf_items.yaml"),
        openapi_scopes=[
            OpenAPIScope.Parameters,
            OpenAPIScope.Schemas,
            OpenAPIScope.Paths,
        ],
        field_constraints=True,
    )
    assert (
        parser.parse()
        == (
            EXPECTED_OPEN_API_PATH / "openapi_parser_parse_array_called_fields_with_oneOf_items" / "output.py"
        ).read_text()
    )


def test_additional_imports() -> None:
    """Test that additional imports are inside imports container."""
    new_parser = OpenAPIParser(source="", additional_imports=["collections.deque"])
    assert len(new_parser.imports) == 1
    assert new_parser.imports["collections"] == {"deque"}


def test_no_additional_imports() -> None:
    """Test that not additional imports are not affecting imports container."""
    new_parser = OpenAPIParser(
        source="",
    )
    assert len(new_parser.imports) == 0


@pytest.mark.parametrize(
    ("request_body_data", "expected_type_hints"),
    [
        pytest.param(
            {"application/json": {"schema": {"type": "object", "properties": {"name": {"type": "string"}}}}},
            {"application/json": "TestRequest"},
            id="object_with_properties",
        ),
        pytest.param(
            {
                "application/json": {"schema": {"type": "object", "properties": {"name": {"type": "string"}}}},
                "text/plain": {"schema": {"type": "string"}},
            },
            {"application/json": "TestRequest", "text/plain": "str"},
            id="multiple_media_types",
        ),
        pytest.param(
            {"application/json": {"schema": {"$ref": "#/components/schemas/RequestRef"}}},
            {"application/json": "RequestRef"},
            id="schema_reference",
        ),
        pytest.param(
            {"application/json": {}},  # MediaObject with no schema
            {},  # Should result in empty dict since no schema to process
            id="missing_schema",
        ),
    ],
)
def test_parse_request_body_return(request_body_data: dict[str, Any], expected_type_hints: dict[str, str]) -> None:
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source="",
        use_standard_collections=True,
    )
    result = parser.parse_request_body(
        "TestRequest",
        RequestBodyObject(
            content={
                media_type: MediaObject.parse_obj(media_data) for media_type, media_data in request_body_data.items()
            }
        ),
        ["test", "path"],
    )

    assert isinstance(result, dict)
    assert len(result) == len(expected_type_hints)
    for media_type, expected_hint in expected_type_hints.items():
        assert media_type in result
        assert result[media_type].type_hint == expected_hint


@pytest.mark.parametrize(
    ("parameters_data", "expected_type_hint"),
    [
        pytest.param([], None, id="no_parameters"),
        pytest.param(
            [{"name": "search", "in": "query", "required": False, "schema": {"type": "string"}}],
            "TestParametersQuery",
            id="with_query_parameters",
        ),
        pytest.param(
            [{"name": "userId", "in": "path", "required": True, "schema": {"type": "string"}}],
            None,
            id="path_parameter_only",
        ),
    ],
)
def test_parse_all_parameters_return(parameters_data: list[dict[str, Any]], expected_type_hint: str | None) -> None:
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source="",
        openapi_scopes=[OpenAPIScope.Parameters],
    )
    result = parser.parse_all_parameters(
        "TestParametersQuery",
        [ParameterObject.parse_obj(param_data) for param_data in parameters_data],
        ["test", "path"],
    )
    if expected_type_hint is None:
        assert result is None
    else:
        assert result is not None
        assert result.type_hint == expected_type_hint


@pytest.mark.parametrize(
    ("responses_data", "expected_type_hints"),
    [
        pytest.param(
            {
                "200": {
                    "description": "Success",
                    "content": {"application/json": {"schema": {"type": "string"}}},
                }
            },
            {"200": {"application/json": "str"}},
            id="simple_response_with_schema",
        ),
        pytest.param(
            {
                "200": {
                    "description": "Success",
                    "content": {
                        "application/json": {"schema": {"type": "object", "properties": {"name": {"type": "string"}}}},
                        "text/plain": {"schema": {"type": "string"}},
                    },
                },
                "400": {
                    "description": "Bad Request",
                    "content": {"text/plain": {"schema": {"type": "string"}}},
                },
            },
            {"200": {"application/json": "TestResponse", "text/plain": "str"}, "400": {"text/plain": "str"}},
            id="multiple_status_codes_and_content_types",
        ),
        pytest.param(
            {
                "200": {
                    "description": "Success",
                    "content": {"application/json": {}},  # Content but no schema
                }
            },
            {},  # Should skip since no schema in content
            id="response_with_no_schema",
        ),
    ],
)
def test_parse_responses_return(
    responses_data: dict[str, dict[str, Any]],
    expected_type_hints: dict[str, dict[str, str]],
) -> None:
    parser = OpenAPIParser(
        data_model_field_type=DataModelFieldBase,
        source="",
    )

    result = parser.parse_responses(
        "TestResponse",
        {status_code: ResponseObject.parse_obj(response_data) for status_code, response_data in responses_data.items()},
        ["test", "path"],
    )

    assert isinstance(result, dict)
    assert len(result) == len(expected_type_hints)
    for status_code, expected_content_types in expected_type_hints.items():
        assert status_code in result
        assert len(result[status_code]) == len(expected_content_types)
        for content_type, expected_type_hint in expected_content_types.items():
            assert content_type in result[status_code]
            assert result[status_code][content_type].type_hint == expected_type_hint
