from jsonschema._types import (
    TypeChecker, is_array, is_bool, is_integer,
    is_object, is_number,
)
from six import text_type, binary_type


def is_string(checker, instance):
    return isinstance(instance, (text_type, binary_type))


oas30_type_checker = TypeChecker(
    {
        u"string": is_string,
        u"number": is_number,
        u"integer": is_integer,
        u"boolean": is_bool,
        u"array": is_array,
        u"object": is_object,
    },
)
