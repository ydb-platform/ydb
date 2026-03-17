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


def test_base_path_is_not_required():
    """
    Test that the info field is required for overall schema validation.
    """
    raw_schema = RawSchemaFactory()
    raw_schema.pop('basePath', None)

    assert 'basePath' not in raw_schema

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'basePath',
        errors,
    )


def test_base_path_must_begin_with_slash():
    raw_schema = RawSchemaFactory(basePath='api/v3/')

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['path']['must_start_with_slash'],
        err.value.detail,
        'basePath',
    )


def test_base_path_with_valid_path():
    raw_schema = RawSchemaFactory(basePath='/api/v3/')

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'basePath',
        errors,
    )
