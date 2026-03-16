import pytest

import fastjsonschema
from fastjsonschema import JsonSchemaValueException


def _composition_example(composition="oneOf"):
    return {
        "definitions": {
            "multiple-of-3": {"type": "number", "multipleOf": 3},
            "multiple-of-5": {"type": "number", "multipleOf": 5},
        },
        composition: [
            {"$ref": "#/definitions/multiple-of-3"},
            {"$ref": "#/definitions/multiple-of-5"},
        ],
    }


@pytest.mark.parametrize(
    "composition, value", [("oneOf", 10), ("allOf", 15), ("anyOf", 9)]
)
def test_composition(asserter, composition, value):
    asserter(_composition_example(composition), value, value)


@pytest.mark.parametrize(
    "composition, value", [("oneOf", 2), ("anyOf", 2), ("allOf", 3)]
)
def test_ref_is_expanded_on_composition_error(composition, value):
    with pytest.raises(JsonSchemaValueException) as exc:
        fastjsonschema.validate(_composition_example(composition), value)

    if composition in exc.value.definition:
        assert exc.value.definition[composition] == [
            {"type": "number", "multipleOf": 3},
            {"type": "number", "multipleOf": 5},
        ]
    else:
        # allOf will fail on the first invalid item,
        # so the error message will not refer to the entire composition
        assert composition == "allOf"
        assert "$ref" not in exc.value.definition
        assert exc.value.definition["type"] == "number"


@pytest.mark.parametrize(
    "composition, value", [("oneOf", 2), ("anyOf", 2), ("allOf", 3)]
)
def test_ref_is_expanded_with_resolver(composition, value):
    repo = {
        "sch://multiple-of-3": {"type": "number", "multipleOf": 3},
        "sch://multiple-of-5": {"type": "number", "multipleOf": 5},
    }
    schema = {
        composition: [
            {"$ref": "sch://multiple-of-3"},
            {"$ref": "sch://multiple-of-5"},
        ]
    }

    with pytest.raises(JsonSchemaValueException) as exc:
        fastjsonschema.validate(schema, value, handlers={"sch": repo.__getitem__})

    if composition in exc.value.definition:
        assert exc.value.definition[composition] == [
            {"type": "number", "multipleOf": 3},
            {"type": "number", "multipleOf": 5},
        ]
    else:
        # allOf will fail on the first invalid item,
        # so the error message will not refer to the entire composition
        assert composition == "allOf"
        assert "$ref" not in exc.value.definition
        assert exc.value.definition["type"] == "number"


def test_ref_in_conditional():
    repo = {
        "sch://USA": {"properties": {"country": {"const": "United States of America"}}},
        "sch://USA-post-code": {
            "properties": {"postal_code": {"pattern": "[0-9]{5}(-[0-9]{4})?"}}
        },
        "sch://general-post-code": {
            "properties": {
                "postal_code": {"pattern": "[A-Z][0-9][A-Z] [0-9][A-Z][0-9]"}
            }
        },
    }
    schema = {
        "type": "object",
        "properties": {
            "street_address": {"type": "string"},
            "country": {
                "default": "United States of America",
                "enum": ["United States of America", "Canada"],
            },
        },
        "if": {"allOf": [{"$ref": "sch://USA"}]},
        "then": {"oneOf": [{"$ref": "sch://USA-post-code"}]},
        "else": {"anyOf": [{"$ref": "sch://general-post-code"}]},
    }
    invalid = {
        "street_address": "1600 Pennsylvania Avenue NW",
        "country": "United States of America",
        "postal_code": "BS12 3FG",
    }

    with pytest.raises(JsonSchemaValueException) as exc:
        fastjsonschema.validate(schema, invalid, handlers={"sch": repo.__getitem__})

    assert exc.value.definition["oneOf"] == [
        {"properties": {"postal_code": {"pattern": "[0-9]{5}(-[0-9]{4})?"}}}
    ]
