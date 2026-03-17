import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions import (
    definitions_validator,
)

from tests.factories import (
    ResponseDefinitionFactory,
)
from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_responses_definitions_are_not_required():
    context = {'deferred_references': set()}
    try:
        definitions_validator({}, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'responses',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    ('abc', 1, 1.1, True, None, [1, 2]),
)
def test_responses_definitions_type_validation_for_invalid_types(value):
    context = {'deferred_references': set()}
    with pytest.raises(ValidationError) as err:
        definitions_validator({'responses': value}, context=context)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'responses',
    )


def test_responses_definitions_with_valid_response_definition():
    context = {'deferred_references': set()}
    response_definition = ResponseDefinitionFactory()
    try:
        definitions_validator({
            'responses': {
                'SomeResponse': response_definition,
            },
        }, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'responses',
        errors,
    )


def test_responses_are_validated():
    """
    Sanity check that the individual response objects are validated.
    """
    context = {'deferred_references': set()}

    with pytest.raises(ValidationError) as err:
        definitions_validator({
            'responses': {
                'WrongType': [],
                'AlsoWrongType': None,
            },
        }, context=context)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'responses.WrongType',
    )
    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'responses.AlsoWrongType',
    )
