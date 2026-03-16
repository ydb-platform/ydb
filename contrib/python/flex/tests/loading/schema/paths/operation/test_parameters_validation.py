import pytest

from flex.exceptions import (
    ValidationError,
)
from flex.loading.schema.paths.path_item.operation import (
    operation_validator,
)


def test_parameters_is_not_required(msg_assertions):
    try:
        operation_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('parameters', errors)


@pytest.mark.parametrize(
    'value',
    (None, True, 1, 1.1, {'a': 1}, 'abcd'),
)
def test_parameters_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        operation_validator({'parameters': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'parameters.type',
    )


def test_parameters_with_valid_value(msg_assertions):
    try:
        operation_validator({'parameters': []})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('parameters', errors)
