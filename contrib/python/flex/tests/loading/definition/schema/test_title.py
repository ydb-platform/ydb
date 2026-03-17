import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_title_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('title', errors)


@pytest.mark.parametrize(
    'value',
    ([1, 2], None, {'a': 1}, True, 1, 1.1),
)
def test_title_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'title': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'title.type',
    )


def test_title_for_valid_title():
    try:
        schema_validator({'title': 'uuid'})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('title', errors)
