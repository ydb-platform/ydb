import pytest

from flex.loading.schema.paths.path_item.operation.responses.single.headers.single import (
    single_header_validator,
)
from flex.constants import (
    INTEGER,
)
from flex.exceptions import ValidationError
from flex.validation.common import (
    validate_object,
)
from flex.validation.header import (
    construct_header_validators
)
from flex.error_messages import MESSAGES

from tests.utils import assert_error_message_equal


@pytest.mark.parametrize(
    'type_,value',
    (
        (INTEGER, '1'),
    )
)
def test_header_type_validation_for_invalid_values(type_, value):
    header_definition = single_header_validator({
        'type': type_,
    })
    validators = construct_header_validators(header_definition=header_definition, context={})

    with pytest.raises(ValidationError) as err:
        validate_object(value, validators)

    assert 'type' in err.value.detail
    assert_error_message_equal(
        err.value.detail['type'][0],
        MESSAGES['type']['invalid'],
    )
