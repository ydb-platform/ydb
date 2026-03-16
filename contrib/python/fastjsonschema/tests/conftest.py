import os
import sys

current_dir = os.path.abspath(os.path.dirname(__file__))
sys.path.insert(0, os.path.join(current_dir, os.pardir))


from pprint import pprint

import pytest

from fastjsonschema import JsonSchemaValueException, compile
from fastjsonschema.draft07 import CodeGeneratorDraft07


def pytest_configure(config):
    config.addinivalue_line("markers", "none")
    config.addinivalue_line("markers", "benchmark")


@pytest.fixture
def asserter():
    def f(definition, value, expected, formats={}, use_formats=True):
        # When test fails, it will show up code.
        code_generator = CodeGeneratorDraft07(definition, formats=formats, use_formats=use_formats)
        print(code_generator.func_code)
        pprint(code_generator.global_state)

        # By default old tests are written for draft-04.
        definition.setdefault('$schema', 'http://json-schema.org/draft-04/schema')

        validator = compile(definition, formats=formats, use_formats=use_formats)
        if isinstance(expected, JsonSchemaValueException):
            with pytest.raises(JsonSchemaValueException) as exc:
                validator(value)
            if expected.message is not any:
                assert exc.value.message == expected.message
            assert exc.value.value == (value if expected.value == '{data}' else expected.value)
            assert exc.value.name == expected.name
            assert exc.value.definition == (definition if expected.definition == '{definition}' else expected.definition)
            assert exc.value.rule == expected.rule
        else:
            assert validator(value) == expected
    return f
