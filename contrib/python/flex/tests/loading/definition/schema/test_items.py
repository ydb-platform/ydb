import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_items_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('items', errors)


@pytest.mark.parametrize(
    'value',
    (None, 1, 1.1, True),
)
def test_items_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'items': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'items.type',
    )
