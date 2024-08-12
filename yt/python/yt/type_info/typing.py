from . import type_base

# Backward compatibility
from .type_base import (  # noqa
    is_valid_type, validate_type, Type
)

try:
    import yt.yson
    _TI_SERIALIZATION_AVAILABLE = True
except ImportError:
    _TI_SERIALIZATION_AVAILABLE = False

import six


@six.python_2_unicode_compatible
class _SingleArgumentGenericAlias(type_base.Type):
    REQUIRED_ATTRS = type_base.Type.REQUIRED_ATTRS + ["item"]

    def __str__(self):
        return "{}<{}>".format(self.name, self.item)

    def to_yson_type(self):
        return {"type_name": self.name.lower(), "item": self.item.to_yson_type()}


class _SingleArgumentGeneric(type_base.Generic):
    def __getitem__(self, param):
        type_base.validate_type(param)

        attrs = {
            "name": self.name,
            "yt_type_name": self.yt_type_name,
            "item": param,
        }

        return _SingleArgumentGenericAlias(attrs)

    def from_dict(self, type_):
        _validate_contains(type_, "item")
        param = _parse_type(type_["item"])
        return self.__getitem__(param)


def _make_tuple_attrs(items, type_name, yt_type_name):
    for item in items:
        type_base.validate_type(item)

    attrs = {
        "name": type_name,
        "yt_type_name": yt_type_name,
        "items": items,
    }

    return attrs


@six.python_2_unicode_compatible
class _TupleAlias(type_base.Type):
    REQUIRED_ATTRS = type_base.Type.REQUIRED_ATTRS + ["items"]

    def __str__(self):
        return "{}<{}>".format(self.name, ", ".join(str(x) for x in self.items))

    def to_yson_type(self):
        yson_repr = {
            "type_name": self.yt_type_name,
            "elements": [{"type": item.to_yson_type()} for item in self.items]
        }
        return yson_repr


class _GenericTuple(type_base.Generic):
    def __getitem__(self, params):
        if not isinstance(params, tuple):
            params = (params,)
        return _TupleAlias(_make_tuple_attrs(params, self.name, self.yt_type_name))

    def from_dict(self, type_):
        _validate_contains(type_, "elements")

        elements = type_["elements"]
        _validate(isinstance(elements, list), "\"elements\" must contain a list")
        _validate(all(isinstance(elem, dict) for elem in elements), "\"elements\" must contain a list of maps")

        params = []
        for elem in elements:
            _validate_contains(elem, "type")
            params.append(_parse_type(elem["type"]))

        return self.__getitem__(tuple(params))


@six.python_2_unicode_compatible
class _DictAlias(type_base.Type):
    REQUIRED_ATTRS = type_base.Type.REQUIRED_ATTRS + ["key", "value"]

    def __str__(self):
        return "{}<{}, {}>".format(self.name, self.key, self.value)

    def to_yson_type(self):
        return {"type_name": self.yt_type_name, "key": self.key.to_yson_type(), "value": self.value.to_yson_type()}


class _GenericDict(type_base.Generic):
    def __getitem__(self, params):
        if not isinstance(params, tuple) or len(params) != 2:
            raise ValueError("Expected two types in Dict, but got {}".format(type_base._with_type(params)))
        key, value = params
        type_base.validate_type(key)
        type_base.validate_type(value)

        attrs = {
            "name": self.name,
            "yt_type_name": self.yt_type_name,
            "key": key,
            "value": value,
        }

        return _DictAlias(attrs)

    def from_dict(self, type_):
        _validate_contains(type_, "key")
        _validate_contains(type_, "value")
        key = _parse_type(type_["key"])
        value = _parse_type(type_["value"])
        return self.__getitem__((key, value))


def _validate_struct_fields(items):
    names = set()
    for item_name, type_ in items:
        if not type_base._is_utf8(item_name):
            raise ValueError("Name of struct field must be UTF-8, got {}".format(type_base._with_type(item_name)))

        if not item_name:
            raise ValueError("Empty field name is not allowed")

        if item_name in names:
            raise ValueError("Duplicate fields are not allowed: {}".format(item_name))

        names.add(item_name)


def _make_struct_attrs(params, type_name, yt_type_name):
    for x in params:
        if not (
            isinstance(x, slice) and
            type_base._is_utf8(x.start) and
            type_base.is_valid_type(x.stop) and
            x.step is None
        ):
            raise ValueError("Expected slice in form of field:type, but got {}".format(type_base._with_type(x)))

    items = tuple((type_base._as_utf8(x.start), x.stop) for x in params)
    _validate_struct_fields(items)

    attrs = {
        "name": type_name,
        "yt_type_name": yt_type_name,
        "items": items,
    }

    return attrs


@six.python_2_unicode_compatible
class _StructAlias(type_base.Type):
    REQUIRED_ATTRS = type_base.Type.REQUIRED_ATTRS + ["items"]

    def __str__(self):
        params_str = ", ".join(
            u"{}: {}".format(type_base.quote_string(name), str(type_))
            for name, type_ in self.items
        )
        return u"{}<{}>".format(self.name, params_str)

    def to_yson_type(self):
        yson_repr = {
            "type_name": self.yt_type_name,
            "members": [
                {"name": name, "type": type_.to_yson_type()}
                for name, type_ in self.items
            ]
        }
        return yson_repr


class _GenericStruct(type_base.Generic):
    def __getitem__(self, params):
        if not isinstance(params, tuple):
            params = (params,)
        return _StructAlias(_make_struct_attrs(params, self.name, self.yt_type_name))

    def from_dict(self, type_):
        _validate_contains(type_, "members")

        members = type_["members"]
        _validate(isinstance(members, list), "\"members\" must contain a list")
        _validate(all(isinstance(member, dict) for member in members), "\"members\" must contain a list of maps")

        params = []
        for member in members:
            _validate_contains(member, "name")
            _validate_contains(member, "type")

            _validate(type_base._is_utf8(member["name"]), "\"name\" must contain a string")
            params.append(slice(member["name"], _parse_type(member["type"])))

        return self.__getitem__(tuple(params))


class _GenericVariant(type_base.Generic):
    def __getitem__(self, params):
        if not isinstance(params, tuple):
            params = (params,)

        if isinstance(params[0], slice):
            attrs = _make_struct_attrs(params, self.name, self.yt_type_name)
            attrs["underlying"] = "Struct"
            return _StructAlias(attrs)
        else:
            attrs = _make_tuple_attrs(params, self.name, self.yt_type_name)
            attrs["underlying"] = "Tuple"
            return _TupleAlias(attrs)

    def from_dict(self, type_):
        _validate("elements" in type_ or "members" in type_, "missing both keys \"members\" and \"elements\"")
        _validate(not ("elements" in type_ and "members" in type_), "both keys \"members\" and \"elements\" are present")

        if "elements" in type_:
            cls = Tuple.from_dict(type_)
            cls.underlying = Tuple.name
        else:
            cls = Struct.from_dict(type_)
            cls.underlying = Struct.name

        cls.name = self.name
        cls.yt_type_name = self.yt_type_name
        return cls


@six.python_2_unicode_compatible
class _TaggedAlias(type_base.Type):
    REQUIRED_ATTRS = type_base.Type.REQUIRED_ATTRS + ["item", "tag"]

    def __str__(self):
        return u"{}<{}, {}>".format(self.name, str(self.item), type_base.quote_string(self.tag))

    def to_yson_type(self):
        yson_repr = {
            "type_name": self.yt_type_name,
            "item": self.item.to_yson_type(),
            "tag": self.tag,
        }
        return yson_repr


class _GenericTagged(type_base.Generic):
    def __getitem__(self, params):
        if not (
            isinstance(params, tuple) and
            len(params) == 2 and
            type_base.is_valid_type(params[0]) and
            (isinstance(params[1], six.string_types) or isinstance(params[1], bytes))
        ):
            raise ValueError("Expected type and tag, but got {}".format(type_base._with_type(params)))
        item, tag = params
        if not type_base._is_utf8(tag):
            raise ValueError("Tag must be UTF-8, got {}".format(type_base._with_type(tag)))
        tag = type_base._as_utf8(tag)

        attrs = {
            "name": self.name,
            "yt_type_name": self.yt_type_name,
            "item": item,
            "tag": tag,
        }

        return _TaggedAlias(attrs)

    def from_dict(self, type_):
        _validate_contains(type_, "tag")
        _validate_contains(type_, "item")

        tag = type_["tag"]
        _validate(type_base._is_utf8(tag), "\"tag\" must contain a string")

        item = _parse_type(type_["item"])
        return self.__getitem__((item, tag,))


@six.python_2_unicode_compatible
class _DecimalAlias(type_base.Type):
    REQUIRED_ATTRS = type_base.Type.REQUIRED_ATTRS + ["precision", "scale"]

    def __str__(self):
        return "{}({}, {})".format(self.name, self.precision, self.scale)

    def to_yson_type(self):
        yson_repr = {
            "type_name": self.yt_type_name,
            "precision": self.precision,
            "scale": self.scale,
        }
        return yson_repr


class _GenericDecimal(type_base.Generic):
    def _create_alias(self, precision, scale):
        if not isinstance(precision, six.integer_types):
            raise ValueError("Expected integer, but got {}".format(type_base._with_type(precision)))

        if not isinstance(scale, six.integer_types):
            raise ValueError("Expected integer, but got {}".format(type_base._with_type(scale)))

        attrs = {
            "name": self.name,
            "yt_type_name": self.yt_type_name,
            "precision": precision,
            "scale": scale,
        }

        return _DecimalAlias(attrs)

    def __call__(self, precision, scale):
        return self._create_alias(precision, scale)

    def __getitem__(self, params):
        if not isinstance(params, tuple) or len(params) != 2:
            raise ValueError("Expected precision and scale integer parameters")

        precision, scale = params
        return self._create_alias(precision, scale)

    def from_dict(self, type_):
        _validate_contains(type_, "precision")
        _validate_contains(type_, "scale")

        return self.__getitem__((type_["precision"], type_["scale"],))


Bool = type_base.make_primitive_type("Bool", yt_type_name_v1="boolean")
Yson = type_base.make_primitive_type("Yson", yt_type_name_v1="any")

Int8 = type_base.make_primitive_type("Int8")
Uint8 = type_base.make_primitive_type("Uint8")
Int16 = type_base.make_primitive_type("Int16")
Uint16 = type_base.make_primitive_type("Uint16")
Int32 = type_base.make_primitive_type("Int32")
Uint32 = type_base.make_primitive_type("Uint32")
Int64 = type_base.make_primitive_type("Int64")
Uint64 = type_base.make_primitive_type("Uint64")
Float = type_base.make_primitive_type("Float")
Double = type_base.make_primitive_type("Double")
String = type_base.make_primitive_type("String")
Utf8 = type_base.make_primitive_type("Utf8")
Json = type_base.make_primitive_type("Json")
Uuid = type_base.make_primitive_type("Uuid")
Date = type_base.make_primitive_type("Date")
Datetime = type_base.make_primitive_type("Datetime")
Timestamp = type_base.make_primitive_type("Timestamp")
Interval = type_base.make_primitive_type("Interval")
TzDate = type_base.make_primitive_type("TzDate", yt_type_name="tz_date")
TzDatetime = type_base.make_primitive_type("TzDatetime", yt_type_name="tz_datetime")
TzTimestamp = type_base.make_primitive_type("TzTimestamp", yt_type_name="tz_timestamp")

Void = type_base.make_primitive_type("Void")
Null = type_base.make_primitive_type("Null")

Optional = _SingleArgumentGeneric("Optional")
List = _SingleArgumentGeneric("List")
Tuple = _GenericTuple("Tuple")
Dict = _GenericDict("Dict")
Struct = _GenericStruct("Struct")
Variant = _GenericVariant("Variant")
Tagged = _GenericTagged("Tagged")
Decimal = _GenericDecimal("Decimal")


EmptyTuple = Tuple.__getitem__(tuple())
EmptyStruct = Struct.__getitem__(tuple())

PRIMITIVES_V1 = {type_.yt_type_name_v1: type_ for type_ in locals().values() if isinstance(type_, type_base.Primitive)}
PRIMITIVES_V3 = {type_.yt_type_name: type_ for type_ in locals().values() if isinstance(type_, type_base.Primitive)}
GENERICS = {type_.yt_type_name: type_ for type_ in locals().values() if isinstance(type_, type_base.Generic)}


def _validate(condition, *error_args):
    if not condition:
        raise ValueError(*error_args)


def _validate_contains(dict, key):
    _validate(key in dict, "missing required key \"{}\"".format(key))


def _parse_type(type_description):
    if isinstance(type_description, six.string_types):
        _validate(type_description in PRIMITIVES_V3, "unknown type \"{}\"".format(type_description))
        return PRIMITIVES_V3[type_description]

    _validate(isinstance(type_description, dict),
              "type must be either a string or a map, got {}".format(type_base._with_type(type_description)))

    _validate_contains(type_description, "type_name")

    type_name = type_description["type_name"]
    _validate(isinstance(type_name, six.string_types), "\"type_name\" must contain a string")

    _validate(type_name in PRIMITIVES_V3 or type_name in GENERICS, "unknown type \"{}\"".format(type_name))

    if type_name in PRIMITIVES_V3:
        return PRIMITIVES_V3[type_name]

    return GENERICS[type_name].from_dict(type_description)


def _parse_type_v1(type_description, required):
    if required:
        _validate(isinstance(type_description, six.string_types), "\"type_description\" must be a string for v1 type")
        _validate(type_description in PRIMITIVES_V1, "unknown type \"{}\"".format(type_description))
        return PRIMITIVES_V1[type_description]
    else:
        return Optional[_parse_type_v1(type_description, True)]


def _check_serialization_available():
    if not _TI_SERIALIZATION_AVAILABLE:
        raise ImportError("Module `yt.yson` is required to use type_info serialization. "
                          "Make sure you have yt/python/yt/yson in your PEERDIR")


def serialize_yson(type_, human_readable=False):
    _check_serialization_available()

    if not type_base.is_valid_type(type_):
        raise TypeError("serialize_yson can be called only for types")

    return yt.yson.dumps(type_, yson_format="pretty" if human_readable else "binary")


def deserialize_yson(yson):
    _check_serialization_available()

    if type(yson) is six.text_type and six.PY3:
        yson = yson.encode("utf-8")

    try:
        type_description = yt.yson.loads(yson)
        return _parse_type(type_description)
    except (ValueError, yt.yson.YsonError) as e:
        six.raise_from(ValueError("deserialization failed: {}".format(e)), e)


def deserialize_yson_v1(yson, required):
    _check_serialization_available()

    if type(yson) is six.text_type and six.PY3:
        yson = yson.encode("utf-8")

    try:
        type_description = yt.yson.loads(yson)
        return _parse_type_v1(type_description, required)
    except (ValueError, yt.yson.YsonError) as e:
        six.raise_from(ValueError("deserialization failed: {}".format(e)), e)
