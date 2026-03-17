import pytest

from flex.exceptions import ValidationError
from flex.constants import (
    ARRAY,
    INTEGER,
    STRING,
)

from tests.utils import (
    generate_validator_from_schema,
    assert_path_in_errors,
)


@pytest.mark.parametrize(
    'items',
    (
        [1, 2, 3, 4, -1, -2, 8, 9],  # -1 and -2 are less than minimum
        [1, 2, 3, 4, 15, 16, 5, 6, 7],  # 15 and 16 are greater than maximum
        [1, 2, 3, '4', '5', 6],  # string not allowed.
    )
)
def test_invalid_values_against_single_schema(items):
    schema = {
        'type': ARRAY,
        'items': {
            'type': INTEGER,
            'minimum': 0,
            'maximum': 10,
        }
    }

    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(items)

    assert_path_in_errors('items', err.value.detail)


@pytest.mark.parametrize(
    'items',
    (
        [1, 2, 3, 4, -1, -2, 8, 9],  # -1 and -2 are less than minimum
        [1, 2, 3, 4, 15, 16, 5, 6, 7],  # 15 and 16 are greater than maximum
        [1, 2, 3, '4', '5', 6],  # string not allowed.
    )
)
def test_invalid_values_against_schema_reference(items):
    schema = {
        'type': ARRAY,
        'items': {
            '$ref': '#/definitions/SomeReference',
        },
    }
    context = {
        'definitions': {
            'SomeReference': {
                'type': INTEGER,
                'minimum': 0,
                'maximum': 10,
            },
        },
    }

    validator = generate_validator_from_schema(schema, context=context)

    with pytest.raises(ValidationError) as err:
        validator(items, context=context)

    assert_path_in_errors('items', err.value.detail)


def test_invalid_values_against_list_of_schemas():
    schema = {
        'type': ARRAY,
        'items': [
            {'type': INTEGER, 'minimum': 0, 'maximum': 10},
            {'type': STRING, 'minLength': 3, 'maxLength': 5},
            {'type': INTEGER, 'minimum': 0, 'maximum': 10},
            {'type': STRING, 'minLength': 3, 'maxLength': 5},
            {'type': INTEGER, 'minimum': 0, 'maximum': 10},
        ],
    }

    validator = generate_validator_from_schema(schema)

    with pytest.raises(ValidationError) as err:
        validator(
            [11, 'abc-abc', -5, 'ab', 'wrong-type'],
        )

    assert_path_in_errors('items.0.maximum', err.value.detail)
    assert_path_in_errors('items.1.maxLength', err.value.detail)
    assert_path_in_errors('items.2.minimum', err.value.detail)
    assert_path_in_errors('items.3.minLength', err.value.detail)
    assert_path_in_errors('items.4.type', err.value.detail)


def test_items_past_the_number_of_schemas_provided_are_skipped():
    schema = {
        'type': ARRAY,
        'items': [
            {'type': INTEGER, 'minimum': 0, 'maximum': 10},
            {'type': INTEGER, 'minimum': 0, 'maximum': 10},
            {'type': INTEGER, 'minimum': 0, 'maximum': 10},
        ],
    }

    validator = generate_validator_from_schema(schema)

    validator(
        [0, 5, 10, 20, 30, 40],
        # 20, 30, and 40 don't conform, but are beyond the declared number of schemas.
    )
