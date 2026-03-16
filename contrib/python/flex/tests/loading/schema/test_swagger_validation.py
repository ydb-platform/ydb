import pytest

from flex.loading.schema import (
    swagger_schema_validator,
)
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


NON_STRING_VALUES = (1, 1.1, True, ['a', 'b'], {'a': 'b'}, None)


def test_swagger_field_is_required():
    """
    Test that the info field is required for overall schema validation.
    """
    raw_schema = RawSchemaFactory()
    raw_schema.pop('swagger', None)

    assert 'swagger' not in raw_schema

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'required.swagger',
    )


def test_swagger_field_cannot_be_null():
    raw_schema = RawSchemaFactory(swagger=None)

    assert raw_schema['swagger'] is None

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_path_in_errors(
        'swagger',
        err.value.detail,
    )


def test_swagger_field_with_valid_version():
    raw_schema = RawSchemaFactory(swagger='2.0')

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'swagger',
        errors
    )


def test_swagger_field_with_invalid_version():
    raw_schema = RawSchemaFactory(swagger='not-valid')

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['enum']['invalid'],
        err.value.detail,
        'swagger.enum',
    )


@pytest.mark.parametrize(
    'value',
    (1, 1.1, True, ['a', 'b'], {'a': 'b'}, None),
)
def test_version_must_be_a_string(value):
    data = RawSchemaFactory(
        swagger=value,
    )
    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(data)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'swagger.type',
    )
