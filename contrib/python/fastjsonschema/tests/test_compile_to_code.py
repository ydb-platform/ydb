import os
import pytest
import shutil

from fastjsonschema import JsonSchemaValueException
from fastjsonschema import compile_to_code, compile as compile_spec


def test_compile_to_code(tmp_path, monkeypatch):
    code = compile_to_code({
        'properties': {
            'a': {'type': 'string'},
            'b': {'type': 'integer'},
            'c': {'format': 'hostname'},  # Test generation of regex patterns to the file.
        }
    })
    with open(tmp_path / 'schema_1.py', 'w') as f:
        f.write(code)
    with monkeypatch.context() as m:
        m.syspath_prepend(tmp_path)
        from schema_1 import validate
    assert validate({
        'a': 'a',
        'b': 1, 
        'c': 'example.com',
    }) == {
        'a': 'a',
        'b': 1,
        'c': 'example.com',
    }

def test_compile_to_code_ipv6_regex(tmp_path, monkeypatch):
    code = compile_to_code({
        'properties': {
            'ip': {'format': 'ipv6'},
        }
    })
    with open(tmp_path / 'schema_2.py', 'w') as f:
        f.write(code)
    with monkeypatch.context() as m:
        m.syspath_prepend(tmp_path)
        from schema_2 import validate
    assert validate({
        'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334'
    }) == {
        'ip': '2001:0db8:85a3:0000:0000:8a2e:0370:7334'
    }

# https://github.com/horejsek/python-fastjsonschema/issues/74
def test_compile_complex_one_of_all_of():
    compile_spec({
        "oneOf": [
            {
                "required": [
                    "schema"
                ]
            },
            {
                "required": [
                    "content"
                ],
                "allOf": [
                    {
                        "not": {
                            "required": [
                                "style"
                            ]
                        }
                    },
                    {
                        "not": {
                            "required": [
                                "explode"
                            ]
                        }
                    }
                ]
            }
        ]
    })


def test_compile_to_code_custom_format(tmp_path, monkeypatch):
    formats = {'my-format': str.isidentifier}
    code = compile_to_code({'type': 'string', 'format': 'my-format'}, formats=formats)
    with open(tmp_path / 'schema_3.py', 'w') as f:
        f.write(code)
    with monkeypatch.context() as m:
        m.syspath_prepend(tmp_path)
        from schema_3 import validate
    assert validate("valid", formats) == "valid"
    with pytest.raises(JsonSchemaValueException) as exc:
        validate("not-valid", formats)
    assert exc.value.message == "data must be my-format"


def test_compile_to_code_custom_format_with_refs(tmp_path, monkeypatch):
    schema = {
        'type': 'object',
        'properties': {
            'a': {'$ref': '#/definitions/a'}
        },
        'definitions': {
            'a': {
                '$id': '#/definitions/a',
                'type': 'array',
                'items': {'type': 'string', 'format': 'my-format'}
            }
        }
    }
    formats = {'my-format': str.isidentifier}
    code = compile_to_code(schema, formats=formats)

    (tmp_path / "schema_4.py").write_text(code, encoding="utf-8")
    with monkeypatch.context() as m:
        m.syspath_prepend(tmp_path)
        from schema_4 import validate

    assert validate({"a": ["identifier"]}, formats) is not None
    with pytest.raises(JsonSchemaValueException) as exc:
        validate({"a": ["identifier", "not-valid"]}, formats)
    assert exc.value.message == "data.a[1] must be my-format"
