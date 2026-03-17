import pytest

from flex.exceptions import (
    ValidationError,
)
from flex.loading.schema.paths.path_item.operation import (
    operation_validator,
)


def test_tags_is_not_required(msg_assertions):
    try:
        operation_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('tags', errors)


@pytest.mark.parametrize(
    'value',
    (None, True, 1, 1.1, 'abc', {'a': 'b'}),
)
def test_tags_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        operation_validator({'tags': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'tags.type',
    )


def test_tags_with_valid_value(msg_assertions):
    try:
        operation_validator({'tags': ['abc', 'def']})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('tags', errors)
