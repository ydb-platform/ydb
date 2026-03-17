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


#
# produces tests
#
def test_produces_not_required():
    raw_schema = RawSchemaFactory()
    raw_schema.pop('produces', None)

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'produces',
        errors,
    )


def test_produces_with_invalid_mimetype():
    raw_schema = RawSchemaFactory(produces=['invalid-mimetype'])

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['mimetype']['invalid'],
        err.value.detail,
        'produces',
    )


def test_produces_with_valid():
    raw_schema = RawSchemaFactory(produces=['application/json'])

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'produces',
        errors,
    )


#
# consumes tests
#
def test_consumes_not_required():
    raw_schema = RawSchemaFactory()
    raw_schema.pop('consumes', None)

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'consumes',
        errors,
    )


def test_consumes_with_invalid_mimetype():
    raw_schema = RawSchemaFactory(consumes=['invalid-mimetype'])

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['mimetype']['invalid'],
        err.value.detail,
        'consumes',
    )


def test_consumes_with_valid():
    raw_schema = RawSchemaFactory(consumes=['application/json'])

    try:
        swagger_schema_validator(raw_schema)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'consumes',
        errors,
    )
