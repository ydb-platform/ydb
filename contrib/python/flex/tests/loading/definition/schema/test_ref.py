import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.schema import schema_validator

from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_ref_is_not_required():
    try:
        schema_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('$ref', errors)


@pytest.mark.parametrize(
    'value',
    (None, {'a': 1}, True, 1, 1.1, [1, 2]),
)
def test_ref_with_invalid_types(value):
    with pytest.raises(ValidationError) as err:
        schema_validator({'$ref': value})

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        '$ref.type',
    )


def test_references_are_deferred():
    context = {
        'deferred_references': set(),
    }
    try:
        schema_validator({'$ref': 'TestReference'}, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors('$ref', errors)
    assert 'TestReference' in context['deferred_references']
