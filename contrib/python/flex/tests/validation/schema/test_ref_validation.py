import pytest

from flex.exceptions import ValidationError
from flex.error_messages import (
    MESSAGES,
)
from flex.loading.definitions.schema_definitions import (
    schema_definitions_validator,
)
from flex.constants import (
    STRING,
    EMPTY,
)
from flex.error_messages import MESSAGES

from tests.utils import (
    generate_validator_from_schema,
    assert_message_in_errors,
)


#
# reference validation tests
#
@pytest.mark.parametrize(
    'zipcode',
    (
        '80205',
        '80205-1234',
        '80205 1234',
    ),
)
def test_reference_with_valid_values(zipcode):
    schema = {
        '$ref': '#/definitions/ZipCode',
    }
    context = {
        'definitions': {
            'ZipCode': {
                'type': STRING,
                'minLength': 5,
                'maxLength': 10,
            },
        },
    }
    validator = generate_validator_from_schema(schema, context=context)

    validator(zipcode)


@pytest.mark.parametrize(
    'zipcode',
    (
        '8020',  # too short
        80205,  # not a string
        '80205-12345', # too long
    ),
)
def test_reference_with_invalid_values(zipcode):
    schema = {
        '$ref': '#/definitions/ZipCode',
    }
    context = {
        'definitions': {
            'ZipCode': {
                'type': STRING,
                'minLength': 5,
                'maxLength': 10,
            },
        },
    }
    validator = generate_validator_from_schema(schema, context=context)

    with pytest.raises(ValidationError):
        validator(zipcode)


@pytest.mark.parametrize(
    'name',
    (
        'Piper',
        'Matthew',
        'Lindsey',
        'Lynn',
    ),
)
def test_reference_with_additional_validators_and_valid_value(name):
    schema = {
        '$ref': '#/definitions/Name',
        'pattern': '^[A-Z][a-z]*$',
    }
    context = {
        'definitions': {
            'Name': {
                'type': STRING,
                'minLength': 4,
                'maxLength': 7,
            },
        },
    }
    validator = generate_validator_from_schema(schema, context=context)

    validator(name)


@pytest.mark.parametrize(
    'name',
    (
        'Joe', # too short
        'piper',  # not capitalized
        'Jennifer',  # too long
    ),
)
def test_reference_with_additional_validators_and_invalid_value(name):
    schema = {
        '$ref': '#/definitions/Name',
        'pattern': '^[A-Z][a-z]*$',
    }
    context = {
        'definitions': {
            'Name': {
                'type': STRING,
                'minLength': 4,
                'maxLength': 7,
            },
        },
    }
    validator = generate_validator_from_schema(schema, context=context)

    with pytest.raises(ValidationError):
        validator(name)


def test_reference_is_noop_when_not_required_and_not_provided():
    schema = {
        'properties': {
            'name': {'$ref': '#/definitions/Name'},
        },
    }
    context = {
        'definitions': {
            'Name': {
                'type': STRING,
                'minLength': 4,
                'maxLength': 7,
            },
        },
    }
    validator = generate_validator_from_schema(schema, context=context)

    validator({})


def test_non_required_circular_reference():
    """
    A schema which references itself is allowable, as long as the self
    reference is not required.  This test ensures that such a case is handled.
    """
    schema = {
        '$ref': '#/definitions/Node',
    }
    definitions = schema_definitions_validator(
        {
            'Node': {
                'properties': {
                    'parent': {'$ref': '#/definitions/Node'},
                    'value': {'type': STRING},
                },
            },
        },
        context={'deferred_references': set()},
    )

    validator = generate_validator_from_schema(
        schema,
        context={'definitions': definitions},
    )


def test_required_circular_reference():
    """
    A schema which references itself and has the self reference as a required
    field can never be valid, since the schema would have to go infinitely
    deep.  This test ensures that we can handle that case without ending up in
    an infinite recursion situation.
    """
    schema = {
        '$ref': '#/definitions/Node',
    }
    definitions = schema_definitions_validator(
        {
            'Node': {
                'required': ['parent'],
                'properties': {
                    'parent': {'$ref': '#/definitions/Node'},
                },
            },
        },
        context={'deferred_references': set()},
    )

    validator = generate_validator_from_schema(
        schema,
        context={'definitions': definitions},
    )

    with pytest.raises(ValidationError) as err:
        validator({
            'parent': {
                'parent': {
                    'parent': {
                        'parent': {
                        },
                    },
                },
            },
        })

    assert_message_in_errors(
        MESSAGES['required']['required'],
        err.value.detail,
        'parent.parent.parent.parent.required.parent',
    )


def test_nested_references_are_validated():
    schema = {
        '$ref': '#/definitions/Node',
    }
    definitions = schema_definitions_validator(
        {
            'Node': {
                'properties': {
                    'parent': {'$ref': '#/definitions/Node'},
                    'value': {'type': STRING},
                },
            },
        },
        context={'deferred_references': set()},
    )

    validator = generate_validator_from_schema(
        schema,
        context={'definitions': definitions},
    )

    with pytest.raises(ValidationError) as err:
        validator({
            'parent': {
                'value': 'bar',
                'parent': {
                    'value': 1234,
                    'parent': {
                        'value': 'baz',
                        'parent': {
                            'value': 54321,
                        },
                    },
                },
            },
        })

    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'parent.parent.value',
    )
    assert_message_in_errors(
        MESSAGES['type']['invalid'],
        err.value.detail,
        'parent.parent.parent.parent.value',
    )
