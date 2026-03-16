import pytest

from marshmallow import Schema, fields

from apispec.ext.marshmallow.common import (
    make_schema_key,
    get_unique_schema_name,
    get_fields,
)
from tests.schemas import PetSchema, SampleSchema


class TestMakeSchemaKey:
    def test_raise_if_schema_class_passed(self):
        with pytest.raises(TypeError, match="based on a Schema instance"):
            make_schema_key(PetSchema)

    def test_same_schemas_instances_equal(self):
        assert make_schema_key(PetSchema()) == make_schema_key(PetSchema())

    @pytest.mark.parametrize("structure", (list, set))
    def test_same_schemas_instances_unhashable_modifiers_equal(self, structure):
        modifier = [str(i) for i in range(1000)]
        assert make_schema_key(
            PetSchema(load_only=structure(modifier))
        ) == make_schema_key(PetSchema(load_only=structure(modifier[::-1])))

    def test_different_schemas_not_equal(self):
        assert make_schema_key(PetSchema()) != make_schema_key(SampleSchema())

    def test_instances_with_different_modifiers_not_equal(self):
        assert make_schema_key(PetSchema()) != make_schema_key(PetSchema(partial=True))


class TestUniqueName:
    def test_unique_name(self, spec):
        properties = {
            "id": {"type": "integer", "format": "int64"},
            "name": {"type": "string", "example": "doggie"},
        }

        name = get_unique_schema_name(spec.components, "Pet")
        assert name == "Pet"

        spec.components.schema("Pet", properties=properties)
        with pytest.warns(
            UserWarning, match="Multiple schemas resolved to the name Pet"
        ):
            name_1 = get_unique_schema_name(spec.components, "Pet")
        assert name_1 == "Pet1"

        spec.components.schema("Pet1", properties=properties)
        with pytest.warns(
            UserWarning, match="Multiple schemas resolved to the name Pet"
        ):
            name_2 = get_unique_schema_name(spec.components, "Pet")
        assert name_2 == "Pet2"


class TestGetFields:
    @pytest.mark.parametrize("exclude_type", (tuple, list))
    @pytest.mark.parametrize("dump_only_type", (tuple, list))
    def test_get_fields_meta_exclude_dump_only_as_list_and_tuple(
        self, exclude_type, dump_only_type
    ):
        class ExcludeSchema(Schema):
            field1 = fields.Int()
            field2 = fields.Int()
            field3 = fields.Int()
            field4 = fields.Int()
            field5 = fields.Int()

            class Meta:
                ordered = True
                exclude = exclude_type(("field1", "field2"))
                dump_only = dump_only_type(("field3", "field4"))

        assert list(get_fields(ExcludeSchema).keys()) == ["field3", "field4", "field5"]
        assert list(get_fields(ExcludeSchema, exclude_dump_only=True).keys()) == [
            "field5"
        ]
