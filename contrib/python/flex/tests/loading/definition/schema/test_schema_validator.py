import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_message_in_errors,
)


@pytest.mark.parametrize(
    'value',
    (1, 1.0, 'abc', [1, 2], None),
)
def test_schema_for_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator(value)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'type',
    )
