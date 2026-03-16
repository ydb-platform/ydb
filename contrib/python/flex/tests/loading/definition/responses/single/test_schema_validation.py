import pytest

from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.responses.single import (
    single_response_validator,
)

from tests.factories import (
    ResponseDefinitionFactory,
)
from tests.utils import (
    assert_path_not_in_errors,
    assert_message_in_errors,
)


def test_schema_is_not_required():
    context = {'deferred_references': set()}
    response_definition = ResponseDefinitionFactory()
    response_definition.pop('schema', None)
    try:
        single_response_validator(response_definition, context=context)
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    assert_path_not_in_errors(
        'schema',
        errors,
    )


@pytest.mark.parametrize(
    'value',
    ('abc', 1, 1.1, True, None, [1, 2]),
)
def test_schema_validation_with_invalid_types(value):
    context = {'deferred_references': set()}

    response_definition = ResponseDefinitionFactory(schema=value)
    with pytest.raises(ValidationError) as err:
        single_response_validator(response_definition, context=context)

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'schema.type',
    )
