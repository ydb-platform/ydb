from __future__ import annotations

from datamodel_code_generator.model import DataModelFieldBase
from datamodel_code_generator.model.pydantic import DataTypeManager
from datamodel_code_generator.model.pydantic.dataclass import DataClass
from datamodel_code_generator.reference import Reference
from datamodel_code_generator.types import DataType, Types


def test_data_class() -> None:
    field = DataModelFieldBase(name="a", data_type=DataType(type="str"), required=True)

    data_class = DataClass(
        fields=[field],
        reference=Reference(name="test_model", path="test_model"),
    )

    assert data_class.name == "test_model"
    assert data_class.fields == [field]
    assert data_class.decorators == []
    assert data_class.render() == "@dataclass\nclass test_model:\n    a: str"


def test_data_class_base_class() -> None:
    field = DataModelFieldBase(name="a", data_type=DataType(type="str"), required=True)

    data_class = DataClass(
        fields=[field],
        base_classes=[Reference(name="Base", original_name="Base", path="Base")],
        reference=Reference(name="test_model", path="test_model"),
    )

    assert data_class.name == "test_model"
    assert data_class.fields == [field]
    assert data_class.decorators == []
    assert data_class.render() == "@dataclass\nclass test_model(Base):\n    a: str"


def test_data_class_optional() -> None:
    field = DataModelFieldBase(name="a", data_type=DataType(type="str"), default="'abc'", required=True)

    data_class = DataClass(
        fields=[field],
        reference=Reference(name="test_model", path="test_model"),
    )

    assert data_class.name == "test_model"
    assert data_class.fields == [field]
    assert data_class.decorators == []
    assert data_class.render() == "@dataclass\nclass test_model:\n    a: str = 'abc'"


def test_data_class_get_data_type() -> None:
    data_type_manager = DataTypeManager()
    assert data_type_manager.get_data_type(Types.integer) == data_type_manager.data_type(type="int")
