from __future__ import annotations

from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any

import pytest

from datamodel_code_generator.model.base import (
    DataModel,
    DataModelFieldBase,
    TemplateBase,
    get_module_path,
    sanitize_module_name,
)
from datamodel_code_generator.reference import Reference
from datamodel_code_generator.types import DataType, Types


class A(TemplateBase):
    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def template_file_path(self) -> Path:
        return self._path

    def render(self) -> str:  # noqa: PLR6301
        return ""


class B(DataModel):
    @classmethod
    def get_data_type(cls, types: Types, **kwargs: Any) -> DataType:
        pass

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

    TEMPLATE_FILE_PATH = ""


class C(DataModel):
    @classmethod
    def get_data_type(cls, types: Types, **kwargs: Any) -> DataType:
        pass


template: str = """{%- for decorator in decorators -%}
{{ decorator }}
{%- endfor %}
@dataclass
class {{ class_name }}:
{%- for field in fields -%}
    {%- if field.required %}
    {{ field.name }}: {{ field.type_hint }}
    {%- else %}
    {{ field.name }}: {{ field.type_hint }} = {{field.default}}
    {%- endif %}
{%- endfor -%}"""


@pytest.mark.xfail(reason="Relative paths in testing")
def test_template_base() -> None:
    with NamedTemporaryFile("w", delete=False, encoding="utf-8") as dummy_template:
        dummy_template.write("abc")
        dummy_template.seek(0)
        dummy_template.close()
        a: TemplateBase = A(Path(dummy_template.name))
    assert str(a.template_file_path) == dummy_template.name
    assert a._render() == "abc"
    assert not str(a)


@pytest.mark.xfail(reason="Relative paths in testing")
def test_data_model() -> None:
    field = DataModelFieldBase(name="a", data_type=DataType(type="str"), default="abc", required=True)

    with NamedTemporaryFile("w", delete=False, encoding="utf-8") as dummy_template:
        dummy_template.write(template)
        dummy_template.seek(0)
        dummy_template.close()
        B.TEMPLATE_FILE_PATH = dummy_template.name
        data_model = B(
            fields=[field],
            decorators=["@validate"],
            base_classes=[Reference(path="base", original_name="base", name="Base")],
            reference=Reference(path="test_model", name="test_model"),
        )

    assert data_model.name == "test_model"
    assert data_model.fields == [field]
    assert data_model.decorators == ["@validate"]
    assert data_model.base_class == "Base"
    assert data_model.render() == "@validate\n@dataclass\nclass test_model:\n    a: str"


def test_data_model_exception() -> None:
    field = DataModelFieldBase(name="a", data_type=DataType(type="str"), default="abc", required=True)
    with pytest.raises(Exception, match="TEMPLATE_FILE_PATH is undefined"):
        C(
            fields=[field],
            reference=Reference(path="abc", original_name="abc", name="abc"),
        )


def test_data_field() -> None:
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(is_list=True),
        required=True,
        is_list=True,
        is_union=True,
    )
    assert field.type_hint == "List"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(is_list=True),
        required=True,
        is_list=True,
        is_union=False,
    )
    assert field.type_hint == "List"
    field = DataModelFieldBase(name="a", data_type=DataType(), required=False)
    assert field.type_hint == "None"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(is_list=True),
        required=False,
        is_list=True,
        is_union=True,
    )
    assert field.type_hint == "Optional[List]"
    field = DataModelFieldBase(name="a", data_type=DataType(), required=False, is_list=False, is_union=True)
    assert field.type_hint == "None"
    field = DataModelFieldBase(name="a", data_type=DataType(), required=False, is_list=False, is_union=False)
    assert field.type_hint == "None"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(is_list=True),
        required=False,
        is_list=True,
        is_union=False,
    )
    assert field.type_hint == "Optional[List]"
    field = DataModelFieldBase(name="a", data_type=DataType(type="str"), required=True)
    assert field.type_hint == "str"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(type="str", is_list=True),
        required=True,
    )
    assert field.type_hint == "List[str]"
    field = DataModelFieldBase(name="a", data_type=DataType(type="str"), required=True)
    assert field.type_hint == "str"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(type="str"),
        required=True,
    )
    assert field.type_hint == "str"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(type="str", is_list=True),
        required=True,
    )
    assert field.type_hint == "List[str]"
    field = DataModelFieldBase(name="a", data_type=DataType(type="str"), required=False)
    assert field.type_hint == "Optional[str]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(
            type="str",
            is_list=True,
        ),
        required=False,
    )
    assert field.type_hint == "Optional[List[str]]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(type="str"),
        required=False,
    )
    assert field.type_hint == "Optional[str]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(type="str"),
        required=False,
    )
    assert field.type_hint == "Optional[str]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(
            type="str",
            is_list=True,
        ),
        required=False,
    )
    assert field.type_hint == "Optional[List[str]]"

    field = DataModelFieldBase(
        name="a",
        data_type=DataType(data_types=[DataType(type="str"), DataType(type="int")]),
        required=True,
    )
    assert field.type_hint == "Union[str, int]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(
            data_types=[DataType(type="str"), DataType(type="int")],
            is_list=True,
        ),
        required=True,
    )
    assert field.type_hint == "List[Union[str, int]]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(data_types=[DataType(type="str"), DataType(type="int")]),
        required=True,
    )
    assert field.type_hint == "Union[str, int]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(data_types=[DataType(type="str"), DataType(type="int")]),
        required=True,
    )
    assert field.type_hint == "Union[str, int]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(data_types=[DataType(type="str"), DataType(type="int")], is_list=True),
        required=True,
    )
    assert field.type_hint == "List[Union[str, int]]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(data_types=[DataType(type="str"), DataType(type="int")]),
        required=False,
    )
    assert field.type_hint == "Optional[Union[str, int]]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(
            data_types=[DataType(type="str"), DataType(type="int")],
            is_list=True,
        ),
        required=False,
    )
    assert field.type_hint == "Optional[List[Union[str, int]]]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(data_types=[DataType(type="str"), DataType(type="int")]),
        required=False,
    )
    assert field.type_hint == "Optional[Union[str, int]]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(data_types=[DataType(type="str"), DataType(type="int")]),
        required=False,
    )
    assert field.type_hint == "Optional[Union[str, int]]"
    field = DataModelFieldBase(
        name="a",
        data_type=DataType(data_types=[DataType(type="str"), DataType(type="int")], is_list=True),
        required=False,
    )
    assert field.type_hint == "Optional[List[Union[str, int]]]"

    field = DataModelFieldBase(name="a", data_type=DataType(is_list=True), required=False)
    assert field.type_hint == "Optional[List]"


@pytest.mark.parametrize(
    ("name", "expected_true", "expected_false"),
    [
        ("array-commons.schema", "array_commons.schema", "array_commons_schema"),
        ("123filename", "_123filename", "_123filename"),
        ("normal_filename", "normal_filename", "normal_filename"),
        ("file!name", "file_name", "file_name"),
        ("", "", ""),
    ],
)
@pytest.mark.parametrize("treat_dot_as_module", [True, False])
def test_sanitize_module_name(name: str, expected_true: str, expected_false: str, treat_dot_as_module: bool) -> None:
    expected = expected_true if treat_dot_as_module else expected_false
    assert sanitize_module_name(name, treat_dot_as_module=treat_dot_as_module) == expected


@pytest.mark.parametrize(
    ("treat_dot_as_module", "expected"),
    [
        (True, ["inputs", "array_commons.schema", "array-commons"]),
        (False, ["inputs", "array_commons_schema", "array-commons"]),
    ],
)
def test_get_module_path_with_file_path(treat_dot_as_module: bool, expected: list[str]) -> None:
    file_path = Path("inputs/array-commons.schema.json")
    result = get_module_path("array-commons.schema", file_path, treat_dot_as_module=treat_dot_as_module)
    assert result == expected


@pytest.mark.parametrize("treat_dot_as_module", [True, False])
def test_get_module_path_without_file_path(treat_dot_as_module: bool) -> None:
    result = get_module_path("my_module.submodule", None, treat_dot_as_module=treat_dot_as_module)
    expected = ["my_module"]
    assert result == expected


@pytest.mark.parametrize(
    ("treat_dot_as_module", "name", "expected"),
    [
        (True, "a.b.c", ["a", "b"]),
        (True, "simple", []),
        (True, "with.dot", ["with"]),
        (False, "a.b.c", ["a", "b"]),
        (False, "simple", []),
        (False, "with.dot", ["with"]),
    ],
)
def test_get_module_path_without_file_path_parametrized(
    treat_dot_as_module: bool, name: str, expected: list[str]
) -> None:
    result = get_module_path(name, None, treat_dot_as_module=treat_dot_as_module)
    assert result == expected
