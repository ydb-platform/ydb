import six

from flex.loading.schema.paths.path_item.operation.parameters import (
    parameters_validator,
)
from flex.validation.parameter import (
    get_path_parameter_values,
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
#  get_path_parameter_values tests
#
def test_getting_parameter_values_from_path():
    parameters = parameters_validator([
        ID_IN_PATH,
        USERNAME_IN_PATH,
    ])

    values = get_path_parameter_values(
        target_path='/get/fernando/posts/1234/',
        api_path='/get/{username}/posts/{id}/',
        path_parameters=parameters,
        context={},
    )
    assert len(values) == 2
    assert 'username' in values
    assert 'id' in values
    assert isinstance(values['username'], six.string_types)
    assert isinstance(values['id'], int)
