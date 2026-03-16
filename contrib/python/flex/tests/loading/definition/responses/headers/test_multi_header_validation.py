import pytest

from flex.exceptions import (
    ValidationError,
)
from flex.error_messages import (
    MESSAGES,
)
from flex.loading.definitions.responses.single import (
    single_response_validator,
)

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_headers_is_not_required():
    try:
        single_response_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('headers', errors)


@pytest.mark.parametrize(
    'value',
    ('abc', 1, 1.1, True, None, [1, 2]),
)
def test_headers_validation_with_invalid_types(value):
    context = {'deferred_references': set()}

    with pytest.raises(ValidationError) as err:
        single_response_validator({'headers': value}, context=context)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'headers.type',
    )


def test_headers_validation_with_valid_type():
    try:
        single_response_validator({'headers': {}})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('headers', errors)
