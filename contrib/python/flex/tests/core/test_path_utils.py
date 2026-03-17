import re

from flex.loading.schema.paths.path_item.operation.parameters import (
    parameters_validator,
)
from flex.paths import (
    get_parameter_names_from_path,
    path_to_pattern,
)
from flex.constants import (
    INTEGER,
    STRING,
    PATH,
)


ID_IN_PATH = {
    'name': 'id', 'in': PATH, 'description': 'id', 'type': INTEGER, 'required': True,
}
USERNAME_IN_PATH = {
    'name': 'username', 'in': PATH, 'description': 'username', 'type': STRING, 'required': True
}


#
# get_parameter_names_from_path tests
#
def test_non_parametrized_path_returns_empty():
    path = "/get/with/no-parameters"
    names = get_parameter_names_from_path(path)
    assert len(names) == 0


def test_getting_names_from_parametrized_path():
    path = "/get/{username}/also/{with_underscores}/and/{id}"
    names = get_parameter_names_from_path(path)
    assert len(names) == 3
    assert ("username", "with_underscores", "id") == names


#
# path_to_pattern tests
#
def test_undeclared_api_path_parameters_are_skipped():
    """
    Test that parameters that are declared in the path string but do not appear
    in the parameter definitions are ignored.
    """
    path = '/get/{username}/posts/{id}/'
    parameters = parameters_validator([ID_IN_PATH])
    pattern = path_to_pattern(path, parameters)
    assert pattern == r'^/get/\{username\}/posts/(?P<id>\d+)/$'

def test_params_do_not_match_across_slashes():
    path = '/get/{username}/posts/{id}'
    parameters = parameters_validator([USERNAME_IN_PATH, ID_IN_PATH])
    pattern = path_to_pattern(path, parameters)

    assert re.match(pattern, '/get/simon/posts/123')

    # {id} should not expand to "/123/unread".
    assert not re.match(pattern, '/get/simon/posts/123/unread')
