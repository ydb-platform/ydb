import pytest

from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    OBJECT,
    STRING,
)
from flex.loading.definitions import (
    definitions_validator,
)
from tests.utils import (
    assert_message_in_errors,
)


def test_references_end_up_in_deferred_referrences():
    deferred_references = set()
    context = {'deferred_references': deferred_references}

    definitions = {
        'definitions': {
            'SomeReference': {
                'type': OBJECT,
                'properties': {
                    'address': {
                        '$ref': '#/definitions/Address',
                    }
                }
            },
            'Address': {
                'type': STRING,
            }
        }
    }
    definitions_validator(definitions, context=context)
    assert '#/definitions/Address' in deferred_references


def test_deferred_references_are_validated():
    deferred_references = set()
    context = {'deferred_references': deferred_references}

    definitions = {
        'definitions': {
            'SomeReference': {
                'type': OBJECT,
                'properties': {
                    'address': {
                        '$ref': '#/definitions/Address',
                    }
                }
            },
        }
    }
    with pytest.raises(ValidationError) as err:
        definitions_validator(definitions, context=context)

    assert_message_in_errors(
        MESSAGES['reference']['undefined'],
        err.value.detail,
        'definitions.Address',
    )
