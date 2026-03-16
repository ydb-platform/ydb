from __future__ import annotations

from typing import Any

import pytest

from datamodel_code_generator.model.pydantic import DataTypeManager
from datamodel_code_generator.model.pydantic.base_model import BaseModel, DataModelField
from datamodel_code_generator.reference import Reference
from datamodel_code_generator.types import DataType, Types


def test_base_model() -> None:
    field = DataModelField(name="a", data_type=DataType(type="str"), required=True)

    base_model = BaseModel(
        fields=[field],
        reference=Reference(name="test_model", path="test_model"),
    )

    assert base_model.name == "test_model"
    assert base_model.fields == [field]
    assert base_model.decorators == []
    assert base_model.render() == "class test_model(BaseModel):\n    a: str"


def test_base_model_optional() -> None:
    field = DataModelField(name="a", data_type=DataType(type="str"), default="abc", required=False)

    base_model = BaseModel(
        fields=[field],
        reference=Reference(name="test_model", path="test_model"),
    )

    assert base_model.name == "test_model"
    assert base_model.fields == [field]
    assert base_model.decorators == []
    assert base_model.render() == "class test_model(BaseModel):\n    a: Optional[str] = 'abc'"


def test_base_model_nullable_required() -> None:
    field = DataModelField(
        name="a",
        data_type=DataType(type="str"),
        default="abc",
        required=True,
        nullable=True,
    )

    base_model = BaseModel(
        fields=[field],
        reference=Reference(name="test_model", path="test_model"),
    )

    assert base_model.name == "test_model"
    assert base_model.fields == [field]
    assert base_model.decorators == []
    assert base_model.render() == "class test_model(BaseModel):\n    a: Optional[str] = Field(...)"


def test_base_model_strict_non_nullable_required() -> None:
    field = DataModelField(
        name="a",
        data_type=DataType(type="str"),
        default="abc",
        required=True,
        nullable=False,
    )

    base_model = BaseModel(
        fields=[field],
        reference=Reference(name="test_model", path="test_model"),
    )

    assert base_model.name == "test_model"
    assert base_model.fields == [field]
    assert base_model.decorators == []
    assert base_model.render() == "class test_model(BaseModel):\n    a: str"


def test_base_model_decorator() -> None:
    field = DataModelField(name="a", data_type=DataType(type="str"), default="abc", required=False)

    base_model = BaseModel(
        fields=[field],
        decorators=["@validate"],
        base_classes=[Reference(name="Base", original_name="Base", path="Base")],
        reference=Reference(name="test_model", path="test_model"),
    )

    assert base_model.name == "test_model"
    assert base_model.fields == [field]
    assert base_model.base_class == "Base"
    assert base_model.decorators == ["@validate"]
    assert base_model.render() == "@validate\nclass test_model(Base):\n    a: Optional[str] = 'abc'"


def test_base_model_get_data_type() -> None:
    data_type_manager = DataTypeManager()
    assert data_type_manager.get_data_type(Types.integer) == data_type_manager.data_type(type="int")


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"required": True}, None),
        ({"required": True, "example": "example"}, None),
        ({"example": "example"}, None),
        (
            {"required": True, "default": 123, "example": "example"},
            None,
        ),
        (
            {"required": False, "default": 123, "example": "example"},
            None,
        ),
        ({"description": "description"}, None),
        ({"title": "title"}, None),
        ({"alias": "alias"}, "Field(None, alias='alias')"),
        ({"example": True}, None),
        ({"examples": True}, None),
        (
            {
                "example": True,
                "description": "description",
                "title": "title",
                "alias": "alias",
            },
            "Field(None, alias='alias')",
        ),
        ({"examples": [1, 2, 3]}, None),
        (
            {"examples": {"name": "dog", "age": 1}},
            None,
        ),
        ({"default": "abc", "title": "title"}, None),
        ({"default": 123, "title": "title"}, None),
    ],
)
def test_data_model_field(kwargs: dict[str, Any], expected: str | None) -> None:
    assert DataModelField(**kwargs, data_type=DataType()).field == expected
