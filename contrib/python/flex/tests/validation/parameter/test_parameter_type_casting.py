import pytest

from flex.loading.schema.paths.path_item.operation.parameters import (
    parameters_validator,
)
from flex.constants import (
    INTEGER,
    NUMBER,
    BOOLEAN,
    ARRAY,
    QUERY,
    CSV,
    SSV,
    TSV,
    PIPES,
)
from flex.validation.parameter import (
    type_cast_parameters,
)


#
# type_cast_parameters tests
#
def test_integer_type_casting():
    parameters = parameters_validator([{
        'type': INTEGER,
        'in': QUERY,
        'description': 'id',
        'name': 'id',
    }])
    parameter = {'id': '123'}
    actual = type_cast_parameters(parameter, parameters, {})
    assert actual['id'] == 123


def test_number_type_casting():
    parameters = parameters_validator([{
        'type': NUMBER,
        'in': QUERY,
        'description': 'id',
        'name': 'id',
    }])
    parameter = {'id': '12.5'}
    actual = type_cast_parameters(parameter, parameters, {})
    assert actual['id'] == 12.5


@pytest.mark.parametrize(
    'input_,expected',
    (
        ('true', True),
        ('True', True),
        ('1', True),
        ('false', False),
        ('False', False),
        ('0', False),
        ('', False),
    )
)
def test_boolean_type_casting(input_, expected):
    parameters = parameters_validator([{
        'type': BOOLEAN,
        'in': QUERY,
        'description': 'id',
        'name': 'id',
    }])
    parameter = {'id': input_}
    actual = type_cast_parameters(parameter, parameters, {})
    assert actual['id'] == expected


@pytest.mark.parametrize(
    'format_,value',
    (
        (CSV, '1,2,3'),
        (CSV, '1, 2, 3'),
        (SSV, '1 2 3'),
        (SSV, '1 2  3'),
        (TSV, '1\t2\t3'),
        (TSV, '1\t 2\t 3'),
        (PIPES, '1|2|3'),
        (PIPES, '1| 2| 3'),
    )
)
def test_array_type_casting(format_, value):
    parameters = parameters_validator([{
        'type': ARRAY,
        'collectionFormat': format_,
        'in': QUERY,
        'description': 'id',
        'name': 'id',
        'items': {'type': INTEGER},
    }])
    parameter = {'id': value}
    actual = type_cast_parameters(parameter, parameters, {})
    assert actual['id'] == [1, 2, 3]
