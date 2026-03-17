from __future__ import annotations

from datamodel_code_generator.model import DataModelFieldBase
from datamodel_code_generator.model.pydantic_v2.root_model import RootModel
from datamodel_code_generator.reference import Reference
from datamodel_code_generator.types import DataType


def test_root_model() -> None:
    root_model = RootModel(
        fields=[
            DataModelFieldBase(
                name="a",
                data_type=DataType(type="str"),
                default="abc",
                required=False,
            )
        ],
        reference=Reference(name="TestRootModel", path="test_root_model"),
    )

    assert root_model.name == "TestRootModel"
    assert root_model.fields == [
        DataModelFieldBase(
            name="a",
            data_type=DataType(type="str"),
            default="abc",
            required=False,
            parent=root_model,
        )
    ]

    assert root_model.base_class == "RootModel"
    assert root_model.custom_base_class is None
    assert root_model.render() == ("class TestRootModel(RootModel[Optional[str]]):\n    root: Optional[str] = 'abc'")


def test_root_model_custom_base_class_is_ignored() -> None:
    """Verify that passing a custom_base_class is ignored."""

    root_model = RootModel(
        custom_base_class="test.Test",
        fields=[
            DataModelFieldBase(
                name="a",
                data_type=DataType(type="str"),
                default="abc",
                required=False,
            )
        ],
        reference=Reference(name="TestRootModel", path="test_root_model"),
    )

    assert root_model.name == "TestRootModel"
    assert root_model.fields == [
        DataModelFieldBase(
            name="a",
            data_type=DataType(type="str"),
            default="abc",
            required=False,
            parent=root_model,
        )
    ]

    assert root_model.base_class == "RootModel"
    assert root_model.custom_base_class is None  # make sure it's ignored
    assert root_model.render() == ("class TestRootModel(RootModel[Optional[str]]):\n    root: Optional[str] = 'abc'")
