"""Test suite for prance.util.formats ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


import pytest

from prance.util import formats


def test_format_info_yaml():
    ctype, ext = formats.format_info("yaml")
    assert "yaml" in ctype
    assert "yaml" in ext


def test_format_info_json():
    ctype, ext = formats.format_info("json")
    assert "json" in ctype
    assert "json" in ext


def test_format_info_bad_format():
    ctype, ext = formats.format_info("foo")
    assert ctype is None
    assert ext is None


def test_parse_details_yaml():
    yaml = """---
foo: bar
"""
    parsed, ctype, ext = formats.parse_spec_details(yaml, "foo.yaml")

    assert parsed["foo"] == "bar", "Did not parse with explicit YAML"
    assert "yaml" in ctype
    assert "yaml" in ext


def test_parse_yaml():
    yaml = """---
foo: bar
"""
    parsed = formats.parse_spec(yaml, "foo.yaml")
    assert parsed["foo"] == "bar", "Did not parse with explicit YAML"

    parsed = formats.parse_spec(yaml)
    assert parsed["foo"] == "bar", "Did not parse with implicit YAML"


def test_parse_yaml_ctype():
    yaml = """---
foo: bar
"""
    parsed = formats.parse_spec(yaml, None, content_type="text/yaml")
    assert parsed["foo"] == "bar", "Did not parse with explicit YAML"


def test_parse_json():
    json = '{ "foo": "bar" }'

    parsed = formats.parse_spec(json, "foo.js")
    assert parsed["foo"] == "bar", "Did not parse with explicit JSON"


def test_parse_json_ctype():
    json = '{ "foo": "bar" }'

    parsed = formats.parse_spec(json, None, content_type="application/json")
    assert parsed["foo"] == "bar", "Did not parse with explicit JSON"


def test_parse_unknown():
    with pytest.raises(formats.ParseError):
        formats.parse_spec("{-")


def test_parse_unknown_ext():
    with pytest.raises(formats.ParseError):
        formats.parse_spec("{-", "asdf.xml")


def test_parse_unknown_ctype():
    with pytest.raises(formats.ParseError):
        formats.parse_spec("{-", None, content_type="text/xml")


def test_serialize_json():
    specs = {
        "foo": "bar",
        "baz": [1, 2, 3],
    }

    # With no further information given, the specs must be in JSON
    serialized = formats.serialize_spec(specs)
    assert serialized.startswith("{")

    # The same must be the case if we provide a JSON file name
    serialized = formats.serialize_spec(specs, "foo.json")
    assert serialized.startswith("{")


def test_serialize_yaml():
    specs = {
        "foo": "bar",
        "baz": [1, 2, 3],
    }

    # Provide a YAML file name
    serialized = formats.serialize_spec(specs, "foo.yml")
    assert "foo: bar" in serialized


def test_serialize_json_ctype():
    specs = {
        "foo": "bar",
        "baz": [1, 2, 3],
    }

    # Force JSON with content type
    serialized = formats.serialize_spec(specs, None, content_type="application/json")
    assert serialized.startswith("{")


def test_serialize_yaml_ctype():
    specs = {
        "foo": "bar",
        "baz": [1, 2, 3],
    }

    # Force YAML with content type
    serialized = formats.serialize_spec(specs, None, content_type="text/yaml")
    assert "foo: bar" in serialized
