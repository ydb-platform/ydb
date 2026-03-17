import pytest

from flex.exceptions import (
    ValidationError,
)
from flex.loading.schema.paths.path_item.operation import (
    operation_validator,
)


def test_responses_is_not_required(msg_assertions):
    try:
        operation_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('responses', errors)


@pytest.mark.parametrize(
    'value',
    (None, True, 1, 1.1, [1, 2, 3], 'abcd'),
)
def test_responses_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        operation_validator({'responses': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'responses.type',
    )


def test_responses_with_valid_value(msg_assertions):
    try:
        operation_validator({'responses': {}})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('responses', errors)
