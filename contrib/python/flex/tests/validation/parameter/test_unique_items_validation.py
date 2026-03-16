from collections import OrderedDict

import pytest

from flex.exceptions import ValidationError
from flex.loading.schema.paths.path_item.operation.parameters import (
    parameters_validator,
)
from flex.validation.parameter import (
    validate_parameters,
)
from flex.constants import (
    PATH,
    ARRAY,
    STRING,
    NUMBER,
    BOOLEAN,
    OBJECT
)
from flex.error_messages import MESSAGES

from tests.utils import assert_message_in_errors


#
# unique_items validation tests
#
@pytest.mark.parametrize(
    'value',
    (
        [1, 2, 3, 1],
        ['a', 'b', 'c', 'z', 'c'],
        [True, False, True],
        [OrderedDict([('a', 1), ('b', 2)]), OrderedDict([('b', 2), ('a', 1)])]
    ),
)
def test_unique_items_validation_with_duplicates(value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': ARRAY,
            'required': True,
            'uniqueItems': True,
            'items': {'type': [STRING, NUMBER, BOOLEAN, OBJECT]},
        },
    ])
    parameter_values = {
        'id': value,
    }

    with pytest.raises(ValidationError) as err:
        validate_parameters(parameter_values, parameters, {})

    assert_message_in_errors(
        MESSAGES['unique_items']['invalid'],
        err.value.detail,
        'id.uniqueItems',
    )


@pytest.mark.parametrize(
    'value',
    (
        [True, 1, '1'],
        [False, 0, ''],
        ['1', 1],
        ['a', 'b', 'A', 'B'],
        [{'a': 'b'}, {'A': 'B'}]
    ),
)
def test_unique_items_validation_with_no_duplicates(value):
    parameters = parameters_validator([
        {
            'name': 'id',
            'in': PATH,
            'description':'id',
            'type': ARRAY,
            'required': True,
            'uniqueItems': True,
            'items': {'type': [STRING, NUMBER, BOOLEAN, OBJECT]},
        },
    ])
    parameter_values = {
        'id': value,
    }

    validate_parameters(parameter_values, parameters, {})
