import pytest

from flex.exceptions import (
    ValidationError,
)
from flex.loading.definitions.responses.single.headers.single import (
    single_header_validator,
)


def test_schema_is_not_required(msg_assertions):
    try:
        single_header_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('headers', errors)


@pytest.mark.parametrize(
    'value',
    (None, True, 1, 1.1, [1, 2, 3], 'abc'),
)
def test_schema_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        single_header_validator({'schema': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'schema.type',
    )

