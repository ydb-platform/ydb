import pytest

from .utils import template_test, resolve_param_values_and_ids


def pytest_generate_tests(metafunc):
    param_values, param_ids = resolve_param_values_and_ids(
        schema_version='http://json-schema.org/draft-07/schema',
        suite_dir='JSON-Schema-Test-Suite/tests/draft7',
        ignored_suite_files=[
            # Requires local server.
            'refRemote.json',
            'cross-draft.json',

            # Optional.
            'ecmascript-regex.json',
            'float-overflow.json',
            'idn-hostname.json',
            'iri.json',
            'unknown.json',
            'unknownKeyword.json',
            'date-time.json',
            'date.json',

            # TODO: fix const with booleans to not match numbers
            'const.json',
            'enum.json',

            # TODO: fix formats
            'ipv6.json',
            'time.json',

            # TODO: fix ref
            'ref.json',
            'id.json',
        ],
    )
    metafunc.parametrize(['schema_version', 'schema', 'data', 'is_valid'], param_values, ids=param_ids)


# Real test function to be used with parametrization by previous hook function.
test = template_test
