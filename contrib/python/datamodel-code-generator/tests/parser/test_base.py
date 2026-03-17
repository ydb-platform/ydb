from __future__ import annotations

from collections import OrderedDict
from typing import Any

import pytest

from datamodel_code_generator.model import DataModel, DataModelFieldBase
from datamodel_code_generator.model.pydantic import BaseModel, DataModelField
from datamodel_code_generator.parser.base import (
    Parser,
    escape_characters,
    exact_import,
    relative,
    sort_data_models,
)
from datamodel_code_generator.reference import Reference, snake_to_upper_camel
from datamodel_code_generator.types import DataType


class A(DataModel):
    pass


class B(DataModel):
    pass


class C(Parser):
    def parse_raw(self, name: str, raw: dict[str, Any]) -> None:
        pass

    def parse(self) -> str:  # noqa: PLR6301
        return "parsed"


def test_parser() -> None:
    c = C(
        data_model_type=D,
        data_model_root_type=B,
        data_model_field_type=DataModelFieldBase,
        base_class="Base",
        source="",
    )
    assert c.data_model_type == D
    assert c.data_model_root_type == B
    assert c.data_model_field_type == DataModelFieldBase
    assert c.base_class == "Base"


def test_sort_data_models() -> None:
    reference_a = Reference(path="A", original_name="A", name="A")
    reference_b = Reference(path="B", original_name="B", name="B")
    reference_c = Reference(path="C", original_name="C", name="C")
    data_type_a = DataType(reference=reference_a)
    data_type_b = DataType(reference=reference_b)
    data_type_c = DataType(reference=reference_c)
    reference = [
        BaseModel(
            fields=[
                DataModelField(data_type=data_type_a),
                DataModelFieldBase(data_type=data_type_c),
            ],
            reference=reference_a,
        ),
        BaseModel(
            fields=[DataModelField(data_type=data_type_b)],
            reference=reference_b,
        ),
        BaseModel(
            fields=[DataModelField(data_type=data_type_b)],
            reference=reference_c,
        ),
    ]

    unresolved, resolved, require_update_action_models = sort_data_models(reference)
    expected = OrderedDict()
    expected["B"] = reference[1]
    expected["C"] = reference[2]
    expected["A"] = reference[0]

    assert resolved == expected
    assert unresolved == []
    assert require_update_action_models == ["B", "A"]


def test_sort_data_models_unresolved() -> None:
    reference_a = Reference(path="A", original_name="A", name="A")
    reference_b = Reference(path="B", original_name="B", name="B")
    reference_c = Reference(path="C", original_name="C", name="C")
    reference_d = Reference(path="D", original_name="D", name="D")
    reference_v = Reference(path="V", original_name="V", name="V")
    reference_z = Reference(path="Z", original_name="Z", name="Z")
    data_type_a = DataType(reference=reference_a)
    data_type_b = DataType(reference=reference_b)
    data_type_c = DataType(reference=reference_c)
    data_type_v = DataType(reference=reference_v)
    data_type_z = DataType(reference=reference_z)
    reference = [
        BaseModel(
            fields=[
                DataModelField(data_type=data_type_a),
                DataModelFieldBase(data_type=data_type_c),
            ],
            reference=reference_a,
        ),
        BaseModel(
            fields=[DataModelField(data_type=data_type_b)],
            reference=reference_b,
        ),
        BaseModel(
            fields=[DataModelField(data_type=data_type_b)],
            reference=reference_c,
        ),
        BaseModel(
            fields=[
                DataModelField(data_type=data_type_a),
                DataModelField(data_type=data_type_c),
                DataModelField(data_type=data_type_z),
            ],
            reference=reference_d,
        ),
        BaseModel(
            fields=[DataModelField(data_type=data_type_v)],
            reference=reference_z,
        ),
    ]

    with pytest.raises(Exception):  # noqa: B017, PT011
        sort_data_models(reference)


def test_sort_data_models_unresolved_raise_recursion_error() -> None:
    reference_a = Reference(path="A", original_name="A", name="A")
    reference_b = Reference(path="B", original_name="B", name="B")
    reference_c = Reference(path="C", original_name="C", name="C")
    reference_d = Reference(path="D", original_name="D", name="D")
    reference_v = Reference(path="V", original_name="V", name="V")
    reference_z = Reference(path="Z", original_name="Z", name="Z")
    data_type_a = DataType(reference=reference_a)
    data_type_b = DataType(reference=reference_b)
    data_type_c = DataType(reference=reference_c)
    data_type_v = DataType(reference=reference_v)
    data_type_z = DataType(reference=reference_z)
    reference = [
        BaseModel(
            fields=[
                DataModelField(data_type=data_type_a),
                DataModelFieldBase(data_type=data_type_c),
            ],
            reference=reference_a,
        ),
        BaseModel(
            fields=[DataModelField(data_type=data_type_b)],
            reference=reference_b,
        ),
        BaseModel(
            fields=[DataModelField(data_type=data_type_b)],
            reference=reference_c,
        ),
        BaseModel(
            fields=[
                DataModelField(data_type=data_type_a),
                DataModelField(data_type=data_type_c),
                DataModelField(data_type=data_type_z),
            ],
            reference=reference_d,
        ),
        BaseModel(
            fields=[DataModelField(data_type=data_type_v)],
            reference=reference_z,
        ),
    ]

    with pytest.raises(Exception):  # noqa: B017, PT011
        sort_data_models(reference, recursion_count=100000)


@pytest.mark.parametrize(
    ("current_module", "reference", "val"),
    [
        ("", "Foo", ("", "")),
        ("a", "a.Foo", ("", "")),
        ("a", "a.b.Foo", (".", "b")),
        ("a.b", "a.Foo", (".", "Foo")),
        ("a.b.c", "a.Foo", ("..", "Foo")),
        ("a.b.c", "Foo", ("...", "Foo")),
    ],
)
def test_relative(current_module: str, reference: str, val: tuple[str, str]) -> None:
    assert relative(current_module, reference) == val


@pytest.mark.parametrize(
    ("from_", "import_", "name", "val"),
    [
        (".", "mod", "Foo", (".mod", "Foo")),
        ("..", "mod", "Foo", ("..mod", "Foo")),
        (".a", "mod", "Foo", (".a.mod", "Foo")),
        ("..a", "mod", "Foo", ("..a.mod", "Foo")),
        ("..a.b", "mod", "Foo", ("..a.b.mod", "Foo")),
    ],
)
def test_exact_import(from_: str, import_: str, name: str, val: tuple[str, str]) -> None:
    assert exact_import(from_, import_, name) == val


@pytest.mark.parametrize(
    ("word", "expected"),
    [
        (
            "_hello",
            "_Hello",
        ),  # In case a name starts with a underline, we should keep it.
        ("hello_again", "HelloAgain"),  # regular snake case
        ("hello__again", "HelloAgain"),  # handles double underscores
        (
            "hello___again_again",
            "HelloAgainAgain",
        ),  # handles double and single underscores
        ("hello_again_", "HelloAgain"),  # handles trailing underscores
        ("hello", "Hello"),  # no underscores
        ("____", "_"),  # degenerate case, but this is the current expected behavior
    ],
)
def test_snake_to_upper_camel(word: str, expected: str) -> None:
    """Tests the snake to upper camel function."""
    actual = snake_to_upper_camel(word)
    assert actual == expected


class D(DataModel):
    def __init__(self, filename: str, data: str, fields: list[DataModelFieldBase]) -> None:  # noqa: ARG002
        super().__init__(fields=fields, reference=Reference(""))
        self._data = data

    def render(self) -> str:
        return self._data


def test_additional_imports() -> None:
    """Test that additional imports are inside imports container."""
    new_parser = C(
        source="",
        additional_imports=["collections.deque"],
    )
    assert len(new_parser.imports) == 1
    assert new_parser.imports["collections"] == {"deque"}


def test_no_additional_imports() -> None:
    """Test that not additional imports are not affecting imports container."""
    new_parser = C(
        source="",
    )
    assert len(new_parser.imports) == 0


@pytest.mark.parametrize(
    ("input_data", "expected"),
    [
        (
            {
                ("folder1", "module1.py"): "content1",
                ("folder1", "module2.py"): "content2",
                ("folder1", "__init__.py"): "init_content",
            },
            {
                ("folder1", "module1.py"): "content1",
                ("folder1", "module2.py"): "content2",
                ("folder1", "__init__.py"): "init_content",
            },
        ),
        (
            {
                ("folder1.module", "file.py"): "content1",
                ("folder1.module", "__init__.py"): "init_content",
            },
            {
                ("folder1", "module", "file.py"): "content1",
                ("folder1", "__init__.py"): "init_content",
                ("folder1", "module", "__init__.py"): "init_content",
            },
        ),
    ],
)
def test_postprocess_result_modules(input_data: Any, expected: Any) -> None:
    result = Parser._Parser__postprocess_result_modules(input_data)
    assert result == expected


def test_find_member_with_integer_enum() -> None:
    """Test find_member method with integer enum values"""
    from datamodel_code_generator.model.enum import Enum  # noqa: PLC0415
    from datamodel_code_generator.model.pydantic.base_model import DataModelField  # noqa: PLC0415
    from datamodel_code_generator.reference import Reference  # noqa: PLC0415
    from datamodel_code_generator.types import DataType  # noqa: PLC0415

    # Create test Enum with integer values
    enum = Enum(
        reference=Reference(path="test_path", original_name="TestEnum", name="TestEnum"),
        fields=[
            DataModelField(
                name="VALUE_1000",
                default="1000",
                data_type=DataType(type="int"),
                required=True,
            ),
            DataModelField(
                name="VALUE_100",
                default="100",
                data_type=DataType(type="int"),
                required=True,
            ),
            DataModelField(
                name="VALUE_0",
                default="0",
                data_type=DataType(type="int"),
                required=True,
            ),
        ],
    )

    # Test finding members with integer values
    assert enum.find_member(1000).field.name == "VALUE_1000"
    assert enum.find_member(100).field.name == "VALUE_100"
    assert enum.find_member(0).field.name == "VALUE_0"

    # Test with string representations
    assert enum.find_member("1000").field.name == "VALUE_1000"
    assert enum.find_member("100").field.name == "VALUE_100"
    assert enum.find_member("0").field.name == "VALUE_0"

    # Test with non-existent values
    assert enum.find_member(999) is None
    assert enum.find_member("999") is None


def test_find_member_with_string_enum() -> None:
    from datamodel_code_generator.model.enum import Enum  # noqa: PLC0415
    from datamodel_code_generator.model.pydantic.base_model import DataModelField  # noqa: PLC0415
    from datamodel_code_generator.reference import Reference  # noqa: PLC0415
    from datamodel_code_generator.types import DataType  # noqa: PLC0415

    enum = Enum(
        reference=Reference(path="test_path", original_name="TestEnum", name="TestEnum"),
        fields=[
            DataModelField(
                name="VALUE_A",
                default="'value_a'",
                data_type=DataType(type="str"),
                required=True,
            ),
            DataModelField(
                name="VALUE_B",
                default="'value_b'",
                data_type=DataType(type="str"),
                required=True,
            ),
        ],
    )

    member = enum.find_member("value_a")
    assert member is not None
    assert member.field.name == "VALUE_A"

    member = enum.find_member("value_b")
    assert member is not None
    assert member.field.name == "VALUE_B"

    member = enum.find_member("'value_a'")
    assert member is not None
    assert member.field.name == "VALUE_A"


def test_find_member_with_mixed_enum() -> None:
    from datamodel_code_generator.model.enum import Enum  # noqa: PLC0415
    from datamodel_code_generator.model.pydantic.base_model import DataModelField  # noqa: PLC0415
    from datamodel_code_generator.reference import Reference  # noqa: PLC0415
    from datamodel_code_generator.types import DataType  # noqa: PLC0415

    enum = Enum(
        reference=Reference(path="test_path", original_name="TestEnum", name="TestEnum"),
        fields=[
            DataModelField(
                name="INT_VALUE",
                default="100",
                data_type=DataType(type="int"),
                required=True,
            ),
            DataModelField(
                name="STR_VALUE",
                default="'value_a'",
                data_type=DataType(type="str"),
                required=True,
            ),
        ],
    )

    member = enum.find_member(100)
    assert member is not None
    assert member.field.name == "INT_VALUE"

    member = enum.find_member("100")
    assert member is not None
    assert member.field.name == "INT_VALUE"

    member = enum.find_member("value_a")
    assert member is not None
    assert member.field.name == "STR_VALUE"

    member = enum.find_member("'value_a'")
    assert member is not None
    assert member.field.name == "STR_VALUE"


@pytest.fixture
def escape_map() -> dict[str, str]:
    return {
        "\u0000": r"\x00",  # Null byte
        "'": r"\'",
        "\b": r"\b",
        "\f": r"\f",
        "\n": r"\n",
        "\r": r"\r",
        "\t": r"\t",
        "\\": r"\\",
    }


@pytest.mark.parametrize(
    ("input_str", "expected"),
    [
        ("\u0000", r"\x00"),  # Test null byte
        ("'", r"\'"),  # Test single quote
        ("\b", r"\b"),  # Test backspace
        ("\f", r"\f"),  # Test form feed
        ("\n", r"\n"),  # Test newline
        ("\r", r"\r"),  # Test carriage return
        ("\t", r"\t"),  # Test tab
        ("\\", r"\\"),  # Test backslash
    ],
)
def test_character_escaping(input_str: str, expected: str) -> None:
    assert input_str.translate(escape_characters) == expected


@pytest.mark.parametrize("flag", [True, False])
def test_use_non_positive_negative_number_constrained_types(flag: bool) -> None:
    instance = C(source="", use_non_positive_negative_number_constrained_types=flag)

    assert instance.data_type_manager.use_non_positive_negative_number_constrained_types == flag
