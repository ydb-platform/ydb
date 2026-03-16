import six

from flex.exceptions import ValidationError
from flex.error_messages import MESSAGES
from flex.decorators import (
    pull_keys_from_obj,
    suffix_reserved_words,
    skip_if_any_kwargs_empty,
)
from flex.utils import is_value_of_any_type


@pull_keys_from_obj('type', 'default')
@suffix_reserved_words
@skip_if_any_kwargs_empty('default', 'type_')
def validate_default_is_of_one_of_declared_types(default, type_, **kwargs):
    if isinstance(type_, six.string_types):
        type_ = [type_]

    if not is_value_of_any_type(default, type_):
        raise ValidationError(
            MESSAGES['default']['invalid_type'].format(default, type_),
        )
