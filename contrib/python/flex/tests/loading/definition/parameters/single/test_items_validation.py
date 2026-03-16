import pytest

from flex.constants import (
    ARRAY,
)
from flex.error_messages import MESSAGES
from flex.exceptions import ValidationError
from flex.loading.definitions.parameters import (
    single_parameter_validator,
)

from tests.factories import ParameterFactory
from tests.utils import (
    assert_message_in_errors,
)


def test_that_when_type_is_array_items_is_required():
    parameter = ParameterFactory(type=ARRAY)
    with pytest.raises(ValidationError) as err:
        single_parameter_validator(parameter)

    assert_message_in_errors(
        MESSAGES['items']['items_required_for_type_array'],
        err.value.detail,
        'items',
    )
