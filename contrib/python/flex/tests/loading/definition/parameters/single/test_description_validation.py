import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.parameters import (
    single_parameter_validator,
)

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_description_is_not_required():
    context = {'deferred_references': set()}
    try:
        single_parameter_validator({}, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'description',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    ([1, 2], None, {'a': 1}, True, 1, 1.1),
)
def test_description_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        single_parameter_validator({'description': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'description.type',
    )


def test_in_with_valid_values():
    try:
        single_parameter_validator({'description': 'The description'})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'description',
        errors,
    )
