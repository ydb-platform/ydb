import pytest

from flex.exceptions import (
    ValidationError,
)
from flex.loading.schema.paths.path_item.operation import (
    operation_validator,
)


def test_external_docs_is_not_required(msg_assertions):
    try:
        operation_validator({})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('externalDocs', errors)


@pytest.mark.parametrize(
    'value',
    (None, True, 1, 1.1, 'abc', [1, 2, 3]),
)
def test_external_docs_type_validation(value, MESSAGES, msg_assertions):
    with pytest.raises(ValidationError) as err:
        operation_validator({'externalDocs': value})

    msg_assertions.assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'externalDocs.type',
    )


def test_external_docs_with_valid_value(msg_assertions):
    try:
        operation_validator({'consumes': {'url': 'http://example.com'}})
    except ValidationError as err:
        errors = err.detail
    else:
        errors = {}

    msg_assertions.assert_path_not_in_errors('externalDocs', errors)
