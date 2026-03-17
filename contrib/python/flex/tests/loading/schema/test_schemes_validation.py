import itertools
import pytest

from flex.loading.schema import (
    swagger_schema_validator,
)
from flex.loading.schema.host import decompose_hostname
from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES

from tests.utils import (
    assert_message_in_errors,
    assert_path_not_in_errors,
    assert_path_in_errors,
)
from tests.factories import (
    RawSchemaFactory,
)


def test_schemes_is_not_required():
    """
    Test that the info field is required for overall schema validation.
    """
    raw_schema = RawSchemaFactory()
    raw_schema.pop('schemes', None)

    assert 'schemes' not in raw_schema

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'schemes',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    ('http', 'https', 1, 1.1, {'a': 1}, None),
)
def test_schemes_invalid_for_non_array_value(value):
    raw_schema = RawSchemaFactory(schemes=value)

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'schemes.type',
    )


def test_schemes_invalid_for_invalid_schemes_value():
    raw_schema = RawSchemaFactory(schemes=['invalid-scheme'])

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['schemes']['invalid'],
        err.value.detail,
        'schemes',
    )
