import math
import numbers

from six.moves import urllib_parse as urlparse
import six

import jsonpointer

from flex._compat import Mapping, Sequence
from flex.constants import (
    PRIMITIVE_TYPES,
    NULL,
    BOOLEAN,
    INTEGER,
    NUMBER,
    STRING,
    ARRAY,
    OBJECT,
    TRUE_VALUES,
    FALSE_VALUES,
)
from flex.error_messages import MESSAGES

from flex.functional import chain_reduce_partial as _chain_reduce_partial


def chain_reduce_partial(*args, **kwargs):
    import warnings
    warnings.warn("moved to `flex.functional.chain_reduce_partial`", DeprecationWarning)
    return _chain_reduce_partial(*args, **kwargs)


def is_any_string_type(value):
    if six.PY2:
        string_types = six.string_types
    else:
        string_types = (six.binary_type, six.text_type)
    return isinstance(value, string_types)


def is_non_string_iterable(value):
    return not is_any_string_type(value) and hasattr(value, '__iter__')


def pluralize(value):
    if is_non_string_iterable(value) and not isinstance(value, Mapping):
        return value
    return [value]


def is_value_of_type(value, type_):
    if type_ not in PRIMITIVE_TYPES:
        raise ValueError("Unknown type: {0}".format(type_))

    if type_ == ARRAY and is_value_of_type(value, STRING):
        return False

    if type_ in (INTEGER, NUMBER) and is_value_of_type(value, BOOLEAN):
        return False

    return isinstance(value, PRIMITIVE_TYPES[type_])


def is_value_of_any_type(value, types):
    return any(is_value_of_type(value, type_) for type_ in types)


def deep_equal(a, b):
    """
    Because of things in python like:
        >>> 1 == 1.0
        True
        >>> 1 == True
        True
        >>> b'test' == 'test'  # python3
        False
    """
    if is_any_string_type(a) and is_any_string_type(b):
        if isinstance(a, six.binary_type):
            a = six.text_type(a, encoding='utf-8')
        if isinstance(b, six.binary_type):
            b = six.text_type(b, encoding='utf-8')
        return a == b
    return a == b and isinstance(a, type(b)) and isinstance(b, type(a))


def cast_value_to_type(value, type_):
    if type_ == STRING:
        return six.text_type(value)
    elif type_ == INTEGER:
        return int(value)
    elif type_ == NUMBER:
        return float(value)
    elif type_ == ARRAY:
        return list(value)
    elif type_ == OBJECT:
        return dict(value)
    elif type_ == BOOLEAN:
        if value in TRUE_VALUES:
            return True
        elif value in FALSE_VALUES:
            return False
        else:
            raise TypeError("Invalid value for boolean: `{0}`".format(repr(value)))
    # TODO: the only thing left is null type.
    return PRIMITIVE_TYPES[type_][0](value)


def get_type_for_value(value):
    if value is None:
        return NULL
    if isinstance(value, PRIMITIVE_TYPES[BOOLEAN]):
        return BOOLEAN
    elif isinstance(value, PRIMITIVE_TYPES[INTEGER]):
        return INTEGER
    elif isinstance(value, PRIMITIVE_TYPES[NUMBER]):
        return NUMBER
    elif isinstance(value, PRIMITIVE_TYPES[STRING]):
        return STRING
    elif isinstance(value, PRIMITIVE_TYPES[ARRAY]):
        return ARRAY
    elif isinstance(value, PRIMITIVE_TYPES[OBJECT]):
        return OBJECT
    else:
        raise ValueError("Unable to identify type of {0}".format(repr(value)))


def is_single_item_iterable(value):
    if is_non_string_iterable(value):
        if isinstance(value, Sequence):
            if len(value) == 1:
                return True
    return False


def indent_message(message, indent, prefix='', suffix=''):
    return "{indent}{prefix}{message}{suffix}".format(
        indent=' ' * indent,
        prefix=prefix,
        message=message,
        suffix=suffix,
    )


SINGULAR_TYPES = six.string_types + (numbers.Number,)


def format_errors(errors, indent=0, prefix='', suffix=''):
    """
    string: "example"

        "example"

    dict:
        "example":
            -

    """
    if is_single_item_iterable(errors):
        errors = errors[0]
    if isinstance(errors, SINGULAR_TYPES):
        yield indent_message(repr(errors), indent, prefix=prefix, suffix=suffix)

    elif isinstance(errors, Mapping):
        for key, value in errors.items():
            assert isinstance(key, SINGULAR_TYPES), type(key)
            if isinstance(value, SINGULAR_TYPES):
                message = "{0}: {1}".format(repr(key), repr(value))
                yield indent_message(message, indent, prefix=prefix, suffix=suffix)
            else:
                yield indent_message(repr(key), indent, prefix=prefix, suffix=':')
                for message in format_errors(value, indent + 4, prefix='- '):
                    yield message

    elif is_non_string_iterable(errors):
        # for making the rhs of the numbers line up
        extra_indent = int(math.ceil(math.log10(len(errors)))) + 2
        for index, value in enumerate(errors):
            list_prefix = "{0}. ".format(index)
            messages = format_errors(
                value,
                indent=indent + extra_indent - len(list_prefix),
                prefix=list_prefix,
            )
            for message in messages:
                yield message
    else:
        assert False, "should not be possible"


def prettify_errors(errors):
    return '\n'.join(format_errors(errors))


def dereference_reference(reference, context):
    parts = urlparse.urlparse(reference)
    if any((parts.scheme, parts.netloc, parts.path, parts.params, parts.query)):
        raise ValueError(
            MESSAGES['reference']['unsupported'].format(reference),
        )
    return jsonpointer.resolve_pointer(context, parts.fragment)
