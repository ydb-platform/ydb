import pytest

from flex.constants import (
    OBJECT,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.schema.paths.path_item.operation.responses.single.schema import (
    schema_validator,
)


@pytest.mark.parametrize(
    'value',
    (1, 1.1, True, None, ['a', 'b'], {'a': 'b'}),
)
def test_reference_type_validation(value, msg_assertions):
    with pytest.raises(ValidationError) as err:
        schema_validator({'$ref': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        '$ref',
    )


def test_reference_not_found_in_definitions(msg_assertions):
    with pytest.raises(ValidationError) as err:
        schema_validator(
            {'$ref': '#/definitions/UnknownReference'},
            context={'definitions': set()},
        )

    msg_assertions.assert_message_in_errors(
        MESSAGES['reference']['undefined'],
        err.value.detail,
        '$ref',
    )


def test_with_valid_reference(msg_assertions):
    try:
        schema_validator(
            {'$ref': '#/definitions/SomeReference'},
            context={
                'definitions': {
                    'SomeReference': {'type': OBJECT},
                },
            },
        )
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors(
        '$ref', errors,
    )
