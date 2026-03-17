import datetime as dt
import re

import pytest
from marshmallow import fields, validate

from apispec.ext.marshmallow.openapi import MARSHMALLOW_VERSION_INFO

from tests.schemas import CategorySchema, CustomList, CustomStringField, CustomIntegerField
from tests.utils import build_ref


def test_field2choices_preserving_order(openapi):
    choices = ["a", "b", "c", "aa", "0", "cc"]
    field = fields.String(validate=validate.OneOf(choices))
    assert openapi.field2choices(field) == {"enum": choices}


@pytest.mark.parametrize(
    ("FieldClass", "jsontype"),
    [
        (fields.Integer, "integer"),
        (fields.Number, "number"),
        (fields.Float, "number"),
        (fields.String, "string"),
        (fields.Str, "string"),
        (fields.Boolean, "boolean"),
        (fields.Bool, "boolean"),
        (fields.UUID, "string"),
        (fields.DateTime, "string"),
        (fields.Date, "string"),
        (fields.Time, "string"),
        (fields.Email, "string"),
        (fields.URL, "string"),
        # Custom fields inherit types from their parents
        (CustomStringField, "string"),
        (CustomIntegerField, "integer"),
    ],
)
def test_field2property_type(FieldClass, jsontype, spec_fixture):
    field = FieldClass()
    res = spec_fixture.openapi.field2property(field)
    assert res["type"] == jsontype


@pytest.mark.parametrize("FieldClass", [fields.Field, fields.Raw])
def test_field2property_no_type_(FieldClass, spec_fixture):
    field = FieldClass()
    res = spec_fixture.openapi.field2property(field)
    assert "type" not in res


@pytest.mark.parametrize("ListClass", [fields.List, CustomList])
def test_formatted_field_translates_to_array(ListClass, spec_fixture):
    field = ListClass(fields.String)
    res = spec_fixture.openapi.field2property(field)
    assert res["type"] == "array"
    assert res["items"] == spec_fixture.openapi.field2property(fields.String())


@pytest.mark.parametrize(
    ("FieldClass", "expected_format"),
    [
        (fields.Integer, "int32"),
        (fields.Float, "float"),
        (fields.UUID, "uuid"),
        (fields.DateTime, "date-time"),
        (fields.Date, "date"),
        (fields.Email, "email"),
        (fields.URL, "url"),
    ],
)
def test_field2property_formats(FieldClass, expected_format, spec_fixture):
    field = FieldClass()
    res = spec_fixture.openapi.field2property(field)
    assert res["format"] == expected_format


def test_field_with_description(spec_fixture):
    field = fields.Str(description="a username")
    res = spec_fixture.openapi.field2property(field)
    assert res["description"] == "a username"


def test_field_with_missing(spec_fixture):
    field = fields.Str(default="foo", missing="bar")
    res = spec_fixture.openapi.field2property(field)
    assert res["default"] == "bar"


def test_boolean_field_with_false_missing(spec_fixture):
    field = fields.Boolean(default=None, missing=False)
    res = spec_fixture.openapi.field2property(field)
    assert res["default"] is False


def test_datetime_field_with_missing(spec_fixture):
    if MARSHMALLOW_VERSION_INFO[0] < 3:
        field = fields.Date(missing=dt.date(2014, 7, 18).isoformat())
    else:
        field = fields.Date(missing=dt.date(2014, 7, 18))
    res = spec_fixture.openapi.field2property(field)
    assert res["default"] == dt.date(2014, 7, 18).isoformat()


def test_field_with_missing_callable(spec_fixture):
    field = fields.Str(missing=lambda: "dummy")
    res = spec_fixture.openapi.field2property(field)
    assert "default" not in res


def test_field_with_doc_default(spec_fixture):
    field = fields.Str(doc_default="Manual default")
    res = spec_fixture.openapi.field2property(field)
    assert res["default"] == "Manual default"


def test_field_with_doc_default_and_missing(spec_fixture):
    field = fields.Int(doc_default=42, missing=12)
    res = spec_fixture.openapi.field2property(field)
    assert res["default"] == 42


def test_field_with_choices(spec_fixture):
    field = fields.Str(validate=validate.OneOf(["freddie", "brian", "john"]))
    res = spec_fixture.openapi.field2property(field)
    assert set(res["enum"]) == {"freddie", "brian", "john"}


def test_field_with_equal(spec_fixture):
    field = fields.Str(validate=validate.Equal("only choice"))
    res = spec_fixture.openapi.field2property(field)
    assert res["enum"] == ["only choice"]


def test_only_allows_valid_properties_in_metadata(spec_fixture):
    field = fields.Str(
        missing="foo",
        description="foo",
        enum=["red", "blue"],
        allOf=["bar"],
        not_valid="lol",
    )
    res = spec_fixture.openapi.field2property(field)
    assert res["default"] == field.missing
    assert "description" in res
    assert "enum" in res
    assert "allOf" in res
    assert "not_valid" not in res


def test_field_with_choices_multiple(spec_fixture):
    field = fields.Str(
        validate=[
            validate.OneOf(["freddie", "brian", "john"]),
            validate.OneOf(["brian", "john", "roger"]),
        ]
    )
    res = spec_fixture.openapi.field2property(field)
    assert set(res["enum"]) == {"brian", "john"}


def test_field_with_additional_metadata(spec_fixture):
    field = fields.Str(minLength=6, maxLength=100)
    res = spec_fixture.openapi.field2property(field)
    assert res["maxLength"] == 100
    assert res["minLength"] == 6


def test_field_with_allow_none(spec_fixture):
    field = fields.Str(allow_none=True)
    res = spec_fixture.openapi.field2property(field)
    if spec_fixture.openapi.openapi_version.major < 3:
        assert res["x-nullable"] is True
    else:
        assert res["nullable"] is True


def test_field_with_str_regex(spec_fixture):
    regex_str = "^[a-zA-Z0-9]$"
    field = fields.Str(validate=validate.Regexp(regex_str))
    ret = spec_fixture.openapi.field2property(field)
    assert ret["pattern"] == regex_str


def test_field_with_pattern_obj_regex(spec_fixture):
    regex_str = "^[a-zA-Z0-9]$"
    field = fields.Str(validate=validate.Regexp(re.compile(regex_str)))
    ret = spec_fixture.openapi.field2property(field)
    assert ret["pattern"] == regex_str


def test_field_with_no_pattern(spec_fixture):
    field = fields.Str()
    ret = spec_fixture.openapi.field2property(field)
    assert "pattern" not in ret


def test_field_with_multiple_patterns(recwarn, spec_fixture):
    regex_validators = [validate.Regexp("winner"), validate.Regexp("loser")]
    field = fields.Str(validate=regex_validators)
    with pytest.warns(UserWarning, match="More than one regex validator"):
        ret = spec_fixture.openapi.field2property(field)
    assert ret["pattern"] == "winner"


def test_field2property_nested_spec_metadatas(spec_fixture):
    spec_fixture.spec.components.schema("Category", schema=CategorySchema)
    category = fields.Nested(
        CategorySchema,
        description="A category",
        invalid_property="not in the result",
        x_extension="A great extension",
    )
    result = spec_fixture.openapi.field2property(category)
    assert result == {
        "allOf": [build_ref(spec_fixture.spec, "schema", "Category")],
        "description": "A category",
        "x-extension": "A great extension",
    }


def test_field2property_nested_spec(spec_fixture):
    spec_fixture.spec.components.schema("Category", schema=CategorySchema)
    category = fields.Nested(CategorySchema)
    assert spec_fixture.openapi.field2property(category) == build_ref(
        spec_fixture.spec, "schema", "Category"
    )


def test_field2property_nested_many_spec(spec_fixture):
    spec_fixture.spec.components.schema("Category", schema=CategorySchema)
    category = fields.Nested(CategorySchema, many=True)
    ret = spec_fixture.openapi.field2property(category)
    assert ret["type"] == "array"
    assert ret["items"] == build_ref(spec_fixture.spec, "schema", "Category")


def test_field2property_nested_ref(spec_fixture):
    category = fields.Nested(CategorySchema)
    ref = spec_fixture.openapi.field2property(category)
    assert ref == build_ref(spec_fixture.spec, "schema", "Category")


def test_field2property_nested_many(spec_fixture):
    categories = fields.Nested(CategorySchema, many=True)
    res = spec_fixture.openapi.field2property(categories)
    assert res["type"] == "array"
    assert res["items"] == build_ref(spec_fixture.spec, "schema", "Category")


def test_nested_field_with_property(spec_fixture):
    category_1 = fields.Nested(CategorySchema)
    category_2 = fields.Nested(CategorySchema, dump_only=True)
    category_3 = fields.Nested(CategorySchema, many=True)
    category_4 = fields.Nested(CategorySchema, many=True, dump_only=True)
    spec_fixture.spec.components.schema("Category", schema=CategorySchema)

    assert spec_fixture.openapi.field2property(category_1) == build_ref(
        spec_fixture.spec, "schema", "Category"
    )
    assert spec_fixture.openapi.field2property(category_2) == {
        "allOf": [build_ref(spec_fixture.spec, "schema", "Category")],
        "readOnly": True,
    }
    assert spec_fixture.openapi.field2property(category_3) == {
        "items": build_ref(spec_fixture.spec, "schema", "Category"),
        "type": "array",
    }
    assert spec_fixture.openapi.field2property(category_4) == {
        "items": build_ref(spec_fixture.spec, "schema", "Category"),
        "readOnly": True,
        "type": "array",
    }


def test_custom_properties_for_custom_fields(spec_fixture):
    def custom_string2properties(self, field, **kwargs):
        ret = {}
        if isinstance(field, CustomStringField):
            if self.openapi_version.major == 2:
                ret["x-customString"] = True
            else:
                ret["x-customString"] = False
        return ret

    spec_fixture.marshmallow_plugin.converter.add_attribute_function(
        custom_string2properties
    )
    properties = spec_fixture.marshmallow_plugin.converter.field2property(
        CustomStringField()
    )
    assert properties["x-customString"] == (
        spec_fixture.openapi.openapi_version == "2.0"
    )


def test_field2property_with_non_string_metadata_keys(spec_fixture):
    class _DesertSentinel:
        pass

    field = fields.Boolean(description="A description")
    field.metadata[_DesertSentinel()] = "to be ignored"
    result = spec_fixture.openapi.field2property(field)
    assert result == {"description": "A description", "type": "boolean"}
