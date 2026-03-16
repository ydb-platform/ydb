import pytest

from flex.loading.schema.paths.path_item.operation.parameters import (
    parameters_validator,
)
from flex.constants import (
    PATH,
    INTEGER,
)
from tests.factories import SchemaFactory
from flex.paths import (
    PARAMETER_REGEX,
    escape_regex_special_chars,
    match_path_to_api_path,
    path_to_regex,
    path_to_pattern,
)
from flex.constants import (
    INTEGER,
    NUMBER,
    STRING,
)


@pytest.mark.parametrize(
    'input_,expected',
    (
        ('/get/{id}', set(['{id}'])),
        ('/get/{main_id}/nested/{id}', set(['{main_id}', '{id}'])),
    )
)
def test_parameter_regex(input_, expected):
    """
    Ensure that the regex used to match parametrized path components correctly
    matches the expected parts in a path.
    """
    actual = PARAMETER_REGEX.findall(input_)

    assert expected == set(actual)


@pytest.mark.parametrize(
    'input_,expected',
    (
        ('/something.json', '/something\.json'),
    ),
)
def test_regex_character_escaping(input_, expected):
    """
    Test that the expected characters get escaped.
    """
    actual = escape_regex_special_chars(input_)
    assert actual == expected


def test_path_to_pattern_with_single_parameter():
    input_ = '/get/{id}'
    expected = '^/get/(?P<id>[^/]+)$'

    parameters = parameters_validator([{
        'required': True,
        'type': STRING,
        'name': 'id',
        'in': 'path',
    }])
    actual = path_to_pattern(input_, parameters=parameters)

    assert actual == expected


def test_path_to_pattern_with_multiple_parameters():
    input_ = '/get/{first_id}/then/{second_id}/'
    expected = '^/get/(?P<first_id>[^/]+)/then/(?P<second_id>[^/]+)/$'

    parameters = parameters_validator([
        {'required': True, 'name': 'first_id', 'in': 'path', 'type': STRING},
        {'required': True, 'name': 'second_id', 'in': 'path',  'type': STRING},
    ])
    actual = path_to_pattern(input_, parameters=parameters)

    assert actual == expected


@pytest.mark.parametrize(
    'path',
    (
        '/basic_path',
        '/something.json',
        '/path-with-dashes',
        '/a/deeper/path/',
    ),
)
def test_path_to_regex_conversion_for_non_parametrized_paths(path):
    regex = path_to_regex(path, [])
    assert regex.match(path)


@pytest.mark.parametrize(
    'path,bad_path',
    (
        ('/something.json', '/something_json'),
        ('/something', '/something-with-extra'),
    ),
)
def test_path_to_regex_does_not_overmatch(path, bad_path):
    regex = path_to_regex(path, [])
    assert not regex.match(bad_path)


@pytest.mark.parametrize(
    'path,schema_path',
    (
        ('/api/path', '/path'),
        ('/api/path.json', '/path.json'),
        ('/api/get/1', '/get/{id}'),
    ),
)
def test_match_target_path_to_api_path(path, schema_path):
    schema = SchemaFactory(
        paths={
            '/path': {},
            '/path.json': {},
            '/get/{id}': {
                'parameters': [{
                    'name': 'id',
                    'in': PATH,
                    'type': INTEGER,
                    'required': True,
                }],
            },
            '/post': {},
        },
        basePath='/api',
    )
    paths = schema['paths']
    base_path = schema['basePath']

    path = match_path_to_api_path(
        path_definitions=paths,
        target_path=path,
        base_path=base_path,
    )
    assert path == schema_path


def test_match_target_path_missing_base_path():
    target_path = '/path'
    base_path = '/api'
    schema = SchemaFactory(
        paths={
            target_path: {},
        },
        basePath=base_path,
    )

    with pytest.raises(LookupError):
        match_path_to_api_path(
            path_definitions=schema['paths'],
            target_path=target_path,
            base_path=base_path,
        )


def test_match_path_with_parameter_defined_in_operation():
    schema = SchemaFactory(
        paths={
            '/get/{id}': {
                'get': {
                    'parameters': [{
                        'name': 'id',
                        'in': PATH,
                        'type': INTEGER,
                        'required': True,
                    }],
                },
            },
        },
    )
    paths = schema['paths']

    path = match_path_to_api_path(
        path_definitions=paths,
        target_path='/get/1234',
    )
    assert path == '/get/{id}'


def test_matching_with_nested_path():
    schema = SchemaFactory(
        paths={
            '/get/{id}': {
                'get': {
                    'parameters': [{
                        'name': 'id',
                        'in': PATH,
                        'type': STRING,
                        'required': True,
                    }],
                },
            },
            '/get/{id}/nested/{other_id}': {
                'get': {
                    'parameters': [
                        {
                            'name': 'id',
                            'in': PATH,
                            'type': STRING,
                            'required': True,
                        },
                        {
                            'name': 'other_id',
                            'in': PATH,
                            'type': STRING,
                            'required': True,
                        },
                    ],
                },
            },
        },
    )
    paths = schema['paths']

    path = match_path_to_api_path(
        path_definitions=paths,
        target_path='/get/1234/nested/5678',
    )
    assert path == '/get/{id}/nested/{other_id}'


def test_matching_with_full_nested_parametrized_resource():
    schema = SchemaFactory(
        paths={
            '/get/main/':{
                'get':{},
            },
            '/get/main/{id}': {
                'get': {
                    'parameters': [{
                        'name': 'id',
                        'in': PATH,
                        'type': STRING,
                        'required': True,
                    }],
                },
            },
            '/get/main/{id}/nested/{other_id}': {
                'get': {
                    'parameters': [
                        {
                            'name': 'id',
                            'in': PATH,
                            'type': STRING,
                            'required': True,
                        },
                        {
                            'name': 'other_id',
                            'in': PATH,
                            'type': STRING,
                            'required': True,
                        },
                    ],
                },
            },
        },
    )
    paths = schema['paths']

    path = match_path_to_api_path(
        path_definitions=paths,
        target_path='/get/main/1234/nested/5678',
    )
    assert path == '/get/main/{id}/nested/{other_id}'


def test_matching_with_full_nested_list_resource():
    schema = SchemaFactory(
        paths={
            '/get/main/':{
                'get': {},
            },
            '/get/main/{id}': {
                'get': {
                    'parameters': [{
                        'name': 'id',
                        'in': PATH,
                        'type': STRING,
                        'required': True,
                    }],
                },
            },
            '/get/main/{id}/nested/': {
                'get': {
                    'parameters': [
                        {
                            'name': 'id',
                            'in': PATH,
                            'type': STRING,
                            'required': True,
                        },
                    ],
                },
            },
        },
    )
    paths = schema['paths']

    path = match_path_to_api_path(
        path_definitions=paths,
        target_path='/get/main/1234/nested/',
    )
    assert path == '/get/main/{id}/nested/'


def test_matching_with_baseline_path_being_slash():
    schema = SchemaFactory(
        base_path='/',
        paths={
            '/get/main': {
                'get': {},
            },
        }
    )

    path = match_path_to_api_path(
        path_definitions=schema['paths'],
        target_path='/get/main',
        base_path=schema['base_path'],
    )
    assert path == '/get/main'


def test_matching_with_no_path_when_base_path_is_not_just_a_slash():
    # Since the base_path isn't /, we do expect the specification
    # to be correct. So, this is expected to fail
    # The target path that should work is /foo/foo/get/main
    schema = SchemaFactory(
        base_path='/foo',
        paths={
            '/foo/get/main': {
                'get': {},
            },
        }
    )

    with pytest.raises(LookupError):
        match_path_to_api_path(
            path_definitions=schema['paths'],
            target_path='/foo/get/main',
            base_path=schema['base_path'],
        )


def test_matching_target_path_with_multiple_slashes():
    schema = SchemaFactory(
        base_path='/',
        paths={
            '/get/main': {
                'get': {},
            },
        }
    )

    path = match_path_to_api_path(
        path_definitions=schema['paths'],
        target_path='/get/////main',
        base_path=schema['base_path'],
    )
    assert path == '/get/main'

def test_matching_target_path_with_simple_comparison_proceeding():
    schema = SchemaFactory(
        base_path='/',
        paths={
            '/api/v1/search/' : {
                'get': {},
            },
            '/api/v1/{longer_id}/' : {
                'post': {},
            }
        }
    )

    path = match_path_to_api_path(
        path_definitions=schema['paths'],
        target_path='/api/v1/search/',
        base_path=schema['base_path']
    )
    assert path == '/api/v1/search/'

