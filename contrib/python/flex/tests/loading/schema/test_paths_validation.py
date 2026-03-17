import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.schema import (
    swagger_schema_validator,
)

from tests.utils import (
    assert_message_in_errors,
)
from tests.factories import (
    RawSchemaFactory,
)


def test_paths_is_required():
    raw_schema = RawSchemaFactory()
    raw_schema.pop('paths', None)

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'required.paths',
    )


@pytest.mark.parametrize(
    'value',
    ('abc', 1, 1.1, True, ['a', 'b'], None),
)
def test_paths_with_invalid_types(value):
    raw_schema = RawSchemaFactory(paths=value)

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'paths.type',
    )


def test_paths_dynamically_validates_paths():
    raw_schema = RawSchemaFactory(paths={'does-not-start-with-slash': None})

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['path']['must_start_with_slash'],
        err.value.detail,
        'paths.does-not-start-with-slash',
    )
