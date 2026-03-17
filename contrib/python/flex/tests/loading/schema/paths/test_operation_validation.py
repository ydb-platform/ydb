import itertools
import pytest

from flex.constants import (
    REQUEST_METHODS,
)
from flex.exceptions import (
    ValidationError,
)
from flex.loading.schema.paths.path_item import (
    path_item_validator,
)


@pytest.mark.parametrize(
    'method',
    REQUEST_METHODS,
)
def test_operations_are_not_required(method, msg_assertions):
    try:
        path_item_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors(method, errors)


@pytest.mark.parametrize(
    'value,method',
    itertools.product(
        (None, True, 1, 1.1, [1, 2, 3], 'abcd'),
        REQUEST_METHODS,
    ),
)
def test_operation_type_validation(value, method, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        path_item_validator({method: value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        '{0}.type'.format(method),
    )


@pytest.mark.parametrize(
    'method',
    REQUEST_METHODS,
)
def test_operation_with_valid_value(method, msg_assertions):
    try:
        path_item_validator({'get': {}})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors(method, errors)
