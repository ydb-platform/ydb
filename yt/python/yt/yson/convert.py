from .yson_types import (
    YsonType, YsonString, YsonUnicode, YsonBoolean, YsonInt64, YsonUint64, YsonDouble,
    YsonList, YsonMap, YsonEntity)
from .common import YsonError

try:
    from yt.packages.six import text_type, binary_type, integer_types, iteritems, PY3
    from yt.packages.six.moves import map as imap
except ImportError:
    from six import text_type, binary_type, integer_types, iteritems, PY3
    from six.moves import map as imap

import copy


def to_yson_type(value, attributes=None, always_create_attributes=True, encoding="utf-8"):
    """Wraps value with YSON type."""
    if not always_create_attributes and attributes is None:
        if isinstance(value, text_type) and not PY3:
            return value.encode("utf-8")
        return value

    if isinstance(value, YsonType):
        if attributes is not None:
            value = copy.deepcopy(value)
            value.attributes = attributes
        return value

    if isinstance(value, text_type):
        if PY3:
            result = YsonUnicode(value)
        else:  # COMPAT
            result = YsonString(value.encode("utf-8"))
    elif isinstance(value, binary_type):
        result = YsonString(value)
    elif value is False or value is True:
        result = YsonBoolean(value)
    elif isinstance(value, integer_types):
        if value < -2 ** 63 or value >= 2 ** 64:
            raise TypeError("Integer {0} cannot be represented in YSON "
                            "since it is out of range [-2^63, 2^64 - 1])".format(value))
        greater_than_max_int64 = value >= 2 ** 63
        if greater_than_max_int64 or isinstance(value, YsonUint64):
            result = YsonUint64(value)
        else:
            result = YsonInt64(value)
    elif isinstance(value, float):
        result = YsonDouble(value)
    elif isinstance(value, list):
        result = YsonList(value)
    elif isinstance(value, dict):
        result = YsonMap(value)
    else:
        result = YsonEntity()

    if attributes is not None:
        result.attributes = attributes
    else:
        result.attributes = {}

    return result


# TODO(ignat): Should we make auto-detection for use_byte_strings?
def json_to_yson(json_tree, use_byte_strings=None):
    """Converts json representation to YSON representation."""
    def to_literal(string):
        if use_byte_strings:
            return string.encode("ascii")
        else:
            return string

    def decode_key(string):
        # In yt wrapper we expect here correct keys, but other usages in arcadia could not give this guarantee.
        # TODO(ignat): fix this usages.
        if use_byte_strings:
            if not isinstance(string, binary_type):
                string = string.encode("ascii")
        else:
            if not isinstance(string, text_type):
                string = string.decode("ascii")

        if string.startswith(to_literal("$")):
            if not string.startswith(to_literal("$$")):
                raise YsonError("Keys should not start with single dollar sign")
            string = string[1:]
        return string

    if use_byte_strings is None:
        use_byte_strings = not PY3

    has_attrs = isinstance(json_tree, dict) and to_literal("$value") in json_tree
    value = json_tree[to_literal("$value")] if has_attrs else json_tree
    if isinstance(value, text_type):
        result = YsonUnicode(value)
    elif isinstance(value, binary_type):
        result = YsonString(value)
    elif value is False or value is True:
        result = YsonBoolean(value)
    elif isinstance(value, integer_types):
        greater_than_max_int64 = value >= 2 ** 63
        if greater_than_max_int64:
            result = YsonUint64(value)
        else:
            result = YsonInt64(value)
    elif isinstance(value, float):
        result = YsonDouble(value)
    elif isinstance(value, list):
        result = YsonList(imap(lambda item: json_to_yson(item, use_byte_strings=use_byte_strings), value))
    elif isinstance(value, dict):
        result = YsonMap((decode_key(k), json_to_yson(v, use_byte_strings=use_byte_strings)) for k, v in iteritems(YsonMap(value)))
    elif value is None:
        result = YsonEntity()
    else:
        raise YsonError("Unknown type:", type(value))

    if has_attrs and json_tree.get(to_literal("$attributes"), {}):
        result.attributes = json_to_yson(json_tree[to_literal("$attributes")], use_byte_strings=use_byte_strings)
    return result


def _yson_to_json(yson_tree, print_attributes=True, attributes_printed=False, annotate_with_types=False):
    should_annotate_with_types = (
        annotate_with_types and
        not isinstance(yson_tree, list) and
        not isinstance(yson_tree, dict) and
        not isinstance(yson_tree, YsonEntity) and
        yson_tree is not None
    )

    def encode_key(key):
        if isinstance(key, binary_type):
            key = key.decode("ascii")
        if key and key[0] == "$":
            return "$" + key
        return key

    def process_dict(d):
        return dict(
            (
                encode_key(k),
                _yson_to_json(v, print_attributes=print_attributes, annotate_with_types=annotate_with_types),
            ) for k, v in iteritems(d)
        )

    def get_type_name():
        if isinstance(yson_tree, YsonType):
            yson_type_str = yson_tree.get_yson_type_str()

            if yson_type_str is not None:
                return yson_type_str

        if isinstance(yson_tree, bool):
            return "bool"
        elif isinstance(yson_tree, int):
            return "int64"
        elif isinstance(yson_tree, float):
            return "double"
        elif isinstance(yson_tree, str) or isinstance(yson_tree, binary_type):
            return "string"
        else:
            raise RuntimeError("Failed to perform yson to json conversion of {!r}, unknown type {!r} to annotate with types".format(
                yson_tree,
                type(yson_tree)
            ))

    def do_annotate_with_types(value):
        return {"$type": get_type_name(), "$value": value} if should_annotate_with_types else value

    if hasattr(yson_tree, "attributes") and yson_tree.attributes and print_attributes and not attributes_printed:
        # If value is primitive do not pass annotate with types.
        value_annotate_with_types = False if should_annotate_with_types else annotate_with_types

        result = {
            "$attributes": process_dict(yson_tree.attributes),
            "$value": _yson_to_json(yson_tree, print_attributes=print_attributes, attributes_printed=True, annotate_with_types=value_annotate_with_types),
        }

        if should_annotate_with_types:
            result["$type"] = get_type_name()

        return result

    if isinstance(yson_tree, list):
        return [_yson_to_json(element, print_attributes=print_attributes, annotate_with_types=annotate_with_types) for element in yson_tree]
    elif isinstance(yson_tree, dict):
        return process_dict(yson_tree)
    elif isinstance(yson_tree, YsonEntity):
        return None
    elif isinstance(yson_tree, YsonString) or isinstance(yson_tree, binary_type):
        return do_annotate_with_types(yson_tree.decode("utf-8"))
    elif isinstance(yson_tree, bool) or isinstance(yson_tree, YsonBoolean):
        return do_annotate_with_types(True if yson_tree else False)
    else:
        bases = type(yson_tree).__bases__
        while len(bases) == 1 and YsonType not in bases:
            bases = bases[0].__bases__

        if YsonType in bases:
            other_types = list(set(bases) - set([YsonType]))
            if not other_types:
                raise RuntimeError("Failed to perform yson to json conversion of {!r}".format(yson_tree))
            other = other_types[0]
            return do_annotate_with_types(other(yson_tree))
        return do_annotate_with_types(yson_tree)


def yson_to_json(yson_tree, print_attributes=True, annotate_with_types=False):
    return _yson_to_json(yson_tree, print_attributes=print_attributes, annotate_with_types=annotate_with_types)
