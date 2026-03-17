import pytest

from flex.constants import (
    STRING,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError

from tests.utils import (
    assert_message_in_errors,
    assert_path_not_in_errors,
)

from flex.loading.common.reference import (
    reference_object_validator,
)




def test_ref_is_required():
    ref_schema = {}

    with pytest.raises(ValidationError) as err:
        reference_object_validator(ref_schema)

    assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'required.$ref',
    )


@pytest.mark.parametrize(
    'value',
    ({'a': 'abc'}, 1, 1.1, True, ['a', 'b'], None),
)
def test_ref_with_invalid_types(value):
    schema = {'$ref': value}

    with pytest.raises(ValidationError) as err:
        reference_object_validator(schema)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        '$ref.type',
    )


def test_valid_reference():
    schema = {'$ref': '#/definitions/SomeReference'}
    context = {
        'definitions': {
            'SomeReference': {
                'type': STRING,
            }
        }
    }

    try:
        reference_object_validator(schema, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        '$ref',
        errors,
    )
