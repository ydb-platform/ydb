from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.constants import (
    ARRAY,
    EMPTY,
)
from flex.utils import (
    pluralize,
)
from flex.decorators import (
    pull_keys_from_obj,
    suffix_reserved_words,
    skip_if_any_kwargs_empty,
)


@pull_keys_from_obj('type', 'items')
@skip_if_any_kwargs_empty('type')
@suffix_reserved_words
def validate_items_required_if_array_type(type_, items, **kwargs):
    types = pluralize(type_)

    if ARRAY in types and items is EMPTY:
        raise ValidationError(MESSAGES['items']['items_required_for_type_array'])
