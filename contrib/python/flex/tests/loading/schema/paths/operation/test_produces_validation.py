import pytest

from flex.exceptions import (
    ValidationError,
)
from flex.loading.schema.paths.path_item.operation import (
    operation_validator,
)


def test_produces_is_not_required(msg_assertions):
    try:
        operation_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('produces', errors)


@pytest.mark.parametrize(
    'value',
    (None, True, 1, 1.1, 'abc', [1, 2, 3], {'a': 1}),
)
def test_produces_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        operation_validator({'produces': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'produces.type',
    )


def test_produces_with_valid_value(msg_assertions):
    try:
        operation_validator({'produces': ['application/json']})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('produces', errors)
