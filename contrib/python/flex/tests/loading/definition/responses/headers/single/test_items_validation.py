import pytest

from flex.constants import (
    ARRAY,
)
from flex.exceptions import (
    ValidationError,
)
from flex.loading.definitions.responses.single.headers.single import (
    single_header_validator,
)


def test_items_is_not_required(msg_assertions):
    try:
        single_header_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('headers', errors)


@pytest.mark.parametrize(
    'value',
    (None, True, 1, 1.1),
)
def test_items_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        single_header_validator({'items': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'items.type',
    )


def test_items_is_required_if_type_array(msg_assertions, MESSAGES):
    with pytest.raises(ValidationError) as err:
        single_header_validator({'type': ARRAY})

    msg_assertions.assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'items',
    )
