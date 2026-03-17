import pytest

from flex.loading.schema import (
    info_validator,
    swagger_schema_validator,
)
from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES

from tests.utils import (
    assert_message_in_errors,
    assert_path_not_in_errors,
)
from tests.factories import (
    RawSchemaFactory,
)


NON_STRING_VALUES = (1, 1.1, True, ['a', 'b'], None, {'a': 'b'})
NON_OBJECT_VALUES = NON_STRING_VALUES[:-1] + ('a',)


def test_info_field_is_required():
    """
    Test that the info field is required for overall schema validation.
    """
    raw_schema = RawSchemaFactory()
    raw_schema.pop('info', None)

    assert 'info' not in raw_schema

    with pytest.raises(ValidationError) as err:
        swagger_schema_validator(raw_schema)

    assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'required.info',
    )


#
# title
#
def test_title_is_required():
    data = {}
    with pytest.raises(ValidationError) as err:
        info_validator(data)

    assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'required.title',
    )


@pytest.mark.parametrize(
    'value',
    NON_STRING_VALUES,
)
def test_title_type_must_be_string(value):
    data = {
        'title': value,
    }
    with pytest.raises(ValidationError) as err:
        info_validator(data)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'title.type',
    )


#
# description
#
def test_description_is_not_required():
    data = {}
    try:
        info_validator(data)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'description.required',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    NON_STRING_VALUES,
)
def test_description_must_be_a_string(value):
    data = {
        'description': value,
    }
    with pytest.raises(ValidationError) as err:
        info_validator(data)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'description.type',
    )


#
# termsOfService
#
def test_terms_of_service_is_not_required():
    data = {}
    try:
        info_validator(data)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'termsOfService.required',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    NON_STRING_VALUES,
)
def test_terms_of_service_must_be_a_string(value):
    data = {
        'termsOfService': value,
    }
    with pytest.raises(ValidationError) as err:
        info_validator(data)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'termsOfService.type',
    )


#
# contact
#
def test_contact_is_not_required():
    data = {}
    try:
        info_validator(data)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'contact.required',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    NON_OBJECT_VALUES,
)
def test_contact_must_be_an_object(value):
    data = {
        'contact': value,
    }
    with pytest.raises(ValidationError) as err:
        info_validator(data)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'contact.type',
    )


#
# license
#
def test_license_is_not_required():
    data = {}
    try:
        info_validator(data)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'license.required',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    NON_OBJECT_VALUES,
)
def test_license_must_be_an_object(value):
    data = {
        'license': value,
    }
    with pytest.raises(ValidationError) as err:
        info_validator(data)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'license.type',
    )


#
# version
#
def test_version_is_not_required():
    data = {}
    try:
        info_validator(data)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'version.required',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    NON_STRING_VALUES,
)
def test_version_must_be_a_string(value):
    data = {
        'version': value,
    }
    with pytest.raises(ValidationError) as err:
        info_validator(data)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'version.type',
    )
