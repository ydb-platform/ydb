import datetime
import decimal
import uuid
from enum import Enum
from inspect import isclass
import typing
import importlib.metadata

from marshmallow import fields, missing, Schema, validate
from marshmallow.class_registry import get_class
from marshmallow.decorators import post_dump
from marshmallow.utils import _Missing
from marshmallow import INCLUDE, EXCLUDE, RAISE
from packaging.version import Version


def marshmallow_version_supports_native_enums() -> bool:
    """
    returns true if and only if the version of marshmallow installed supports enums natively
    """
    return Version(importlib.metadata.version("marshmallow")) >= Version("3.18")


try:
    from marshmallow_union import Union

    ALLOW_UNIONS = True
except ImportError:
    ALLOW_UNIONS = False

try:
    from marshmallow_enum import EnumField as MarshmallowEnumEnumField, LoadDumpOptions

    ALLOW_MARSHMALLOW_ENUM_ENUMS = True
except ImportError:
    ALLOW_MARSHMALLOW_ENUM_ENUMS = False

ALLOW_MARSHMALLOW_NATIVE_ENUMS = marshmallow_version_supports_native_enums()
if ALLOW_MARSHMALLOW_NATIVE_ENUMS:
    from marshmallow.fields import Enum as MarshmallowNativeEnumField

from .exceptions import UnsupportedValueError
from .validation import (
    handle_equal,
    handle_length,
    handle_one_of,
    handle_range,
    handle_regexp,
)

__all__ = ("JSONSchema",)

PY_TO_JSON_TYPES_MAP = {
    dict: {"type": "object"},
    list: {"type": "array"},
    datetime.time: {"type": "string", "format": "time"},
    datetime.timedelta: {
        # TODO explore using 'range'?
        "type": "string"
    },
    datetime.datetime: {"type": "string", "format": "date-time"},
    datetime.date: {"type": "string", "format": "date"},
    uuid.UUID: {"type": "string", "format": "uuid"},
    str: {"type": "string"},
    bytes: {"type": "string"},
    decimal.Decimal: {"type": "number", "format": "decimal"},
    set: {"type": "array"},
    tuple: {"type": "array"},
    float: {"type": "number", "format": "float"},
    int: {"type": "integer"},
    bool: {"type": "boolean"},
    Enum: {"type": "string"},
}

# We use these pairs to get proper python type from marshmallow type.
# We can't use mapping as earlier Python versions might shuffle dict contents
# and then `fields.Number` might end up before `fields.Integer`.
# As we perform sequential subclass check to determine proper Python type,
# we can't let that happen.
MARSHMALLOW_TO_PY_TYPES_PAIRS = [
    # This part of a mapping is carefully selected from marshmallow source code,
    # see marshmallow.BaseSchema.TYPE_MAPPING.
    (fields.UUID, uuid.UUID),
    (fields.String, str),
    (fields.Float, float),
    (fields.Raw, str),
    (fields.Boolean, bool),
    (fields.Integer, int),
    (fields.Time, datetime.time),
    (fields.Date, datetime.date),
    (fields.TimeDelta, datetime.timedelta),
    (fields.DateTime, datetime.datetime),
    (fields.Decimal, decimal.Decimal),
    # These are some mappings that generally make sense for the rest
    # of marshmallow fields.
    (fields.Email, str),
    (fields.Dict, dict),
    (fields.Url, str),
    (fields.List, list),
    (fields.Number, decimal.Decimal),
    (fields.IP, str),
    (fields.IPInterface, str),
    # This one is here just for completeness sake and to check for
    # unknown marshmallow fields more cleanly.
    (fields.Nested, dict),
]

if ALLOW_MARSHMALLOW_NATIVE_ENUMS:
    MARSHMALLOW_TO_PY_TYPES_PAIRS.append((MarshmallowNativeEnumField, Enum))
if ALLOW_MARSHMALLOW_ENUM_ENUMS:
    # We currently only support loading enum's from their names. So the possible
    # values will always map to string in the JSONSchema
    MARSHMALLOW_TO_PY_TYPES_PAIRS.append((MarshmallowEnumEnumField, Enum))


FIELD_VALIDATORS = {
    validate.Equal: handle_equal,
    validate.Length: handle_length,
    validate.OneOf: handle_one_of,
    validate.Range: handle_range,
    validate.Regexp: handle_regexp,
}


def _resolve_additional_properties(cls) -> bool:
    meta = cls.Meta

    additional_properties = getattr(meta, "additional_properties", None)
    if additional_properties is not None:
        if additional_properties in (True, False):
            return additional_properties
        else:
            raise UnsupportedValueError(
                "`additional_properties` must be either True or False"
            )

    unknown = getattr(meta, "unknown", None)
    if unknown is None:
        return False
    elif unknown in (RAISE, EXCLUDE):
        return False
    elif unknown == INCLUDE:
        return True
    else:
        raise UnsupportedValueError("Unknown value %s for `unknown`" % unknown)


class JSONSchema(Schema):
    """Converts to JSONSchema as defined by http://json-schema.org/."""

    properties = fields.Method("get_properties")
    type = fields.Constant("object")
    required = fields.Method("get_required")

    def __init__(self, *args, **kwargs) -> None:
        """Setup internal cache of nested fields, to prevent recursion.

        :param bool props_ordered: if `True` order of properties will be save as declare in class,
                                   else will using sorting, default is `False`.
                                   Note: For the marshmallow scheme, also need to enable
                                   ordering of fields too (via `class Meta`, attribute `ordered`).
        """
        self._nested_schema_classes: typing.Dict[str, typing.Dict[str, typing.Any]] = {}
        self.nested = kwargs.pop("nested", False)
        self.props_ordered = kwargs.pop("props_ordered", False)
        setattr(self.opts, "ordered", self.props_ordered)
        super().__init__(*args, **kwargs)

    def get_properties(self, obj) -> typing.Dict[str, typing.Dict[str, typing.Any]]:
        """Fill out properties field."""
        properties = self.dict_class()

        if self.props_ordered:
            fields_items_sequence = obj.fields.items()
        else:
            fields_items_sequence = sorted(obj.fields.items())

        for field_name, field in fields_items_sequence:
            schema = self._get_schema_for_field(obj, field)
            properties[
                field.metadata.get("name") or field.data_key or field.name
            ] = schema

        return properties

    def get_required(self, obj) -> typing.Union[typing.List[str], _Missing]:
        """Fill out required field."""
        required = []

        for field_name, field in sorted(obj.fields.items()):
            if field.required:
                required.append(field.data_key or field.name)

        return required or missing

    def _from_python_type(self, obj, field, pytype) -> typing.Dict[str, typing.Any]:
        """Get schema definition from python type."""
        json_schema = {"title": field.attribute or field.name or ""}

        for key, val in PY_TO_JSON_TYPES_MAP[pytype].items():
            json_schema[key] = val

        if field.dump_only:
            json_schema["readOnly"] = True

        if field.default is not missing and not callable(field.default):
            json_schema["default"] = field.default

        if ALLOW_MARSHMALLOW_NATIVE_ENUMS and isinstance(field, MarshmallowNativeEnumField):
            json_schema["enum"] = self._get_marshmallow_native_enum_values(field)
        elif ALLOW_MARSHMALLOW_ENUM_ENUMS and isinstance(field, MarshmallowEnumEnumField):
            json_schema["enum"] = self._get_marshmallow_enum_enum_values(field)

        if field.allow_none:
            previous_type = json_schema["type"]
            json_schema["type"] = [previous_type, "null"]

        # NOTE: doubled up to maintain backwards compatibility
        metadata = field.metadata.get("metadata", {})
        metadata.update(field.metadata)

        for md_key, md_val in metadata.items():
            if md_key in ("metadata", "name"):
                continue
            json_schema[md_key] = md_val

        if isinstance(field, fields.List):
            json_schema["items"] = self._get_schema_for_field(obj, field.inner)

        if isinstance(field, fields.Dict):
            json_schema["additionalProperties"] = (
                self._get_schema_for_field(obj, field.value_field)
                if field.value_field
                else {}
            )
        return json_schema

    def _get_marshmallow_enum_enum_values(self, field) -> typing.List[str]:
        assert ALLOW_MARSHMALLOW_ENUM_ENUMS and isinstance(field, MarshmallowEnumEnumField)

        if field.load_by == LoadDumpOptions.value:
            # Python allows enum values to be almost anything, so it's easier to just load from the
            # names of the enum's which will have to be strings.
            raise NotImplementedError(
                "Currently do not support JSON schema for enums loaded by value"
            )

        return [value.name for value in field.enum]
    def _get_marshmallow_native_enum_values(self, field) -> typing.List[str]:
        assert ALLOW_MARSHMALLOW_NATIVE_ENUMS and isinstance(field, MarshmallowNativeEnumField)

        if field.by_value:
            # Python allows enum values to be almost anything, so it's easier to just load from the
            # names of the enum's which will have to be strings.
            raise NotImplementedError(
                "Currently do not support JSON schema for enums loaded by value"
            )

        return [value.name for value in field.enum]

    def _from_union_schema(
        self, obj, field
    ) -> typing.Dict[str, typing.List[typing.Any]]:
        """Get a union type schema. Uses anyOf to allow the value to be any of the provided sub fields"""
        assert ALLOW_UNIONS and isinstance(field, Union)

        return {
            "anyOf": [
                self._get_schema_for_field(obj, sub_field)
                for sub_field in field._candidate_fields
            ]
        }

    def _get_python_type(self, field):
        """Get python type based on field subclass"""
        for map_class, pytype in MARSHMALLOW_TO_PY_TYPES_PAIRS:
            if issubclass(field.__class__, map_class):
                return pytype

        raise UnsupportedValueError("unsupported field type %s" % field)

    def _get_schema_for_field(self, obj, field):
        """Get schema and validators for field."""
        if hasattr(field, "_jsonschema_type_mapping"):
            schema = field._jsonschema_type_mapping()
        elif "_jsonschema_type_mapping" in field.metadata:
            schema = field.metadata["_jsonschema_type_mapping"]
        else:
            if isinstance(field, fields.Nested):
                # Special treatment for nested fields.
                schema = self._from_nested_schema(obj, field)
            elif ALLOW_UNIONS and isinstance(field, Union):
                schema = self._from_union_schema(obj, field)
            else:
                pytype = self._get_python_type(field)
                schema = self._from_python_type(obj, field, pytype)
        # Apply any and all validators that field may have
        for validator in field.validators:
            if validator.__class__ in FIELD_VALIDATORS:
                schema = FIELD_VALIDATORS[validator.__class__](
                    schema, field, validator, obj
                )
            else:
                base_class = getattr(
                    validator, "_jsonschema_base_validator_class", None
                )
                if base_class is not None and base_class in FIELD_VALIDATORS:
                    schema = FIELD_VALIDATORS[base_class](schema, field, validator, obj)
        return schema

    def _from_nested_schema(self, obj, field):
        """Support nested field."""
        if isinstance(field.nested, (str, bytes)):
            nested = get_class(field.nested)
        else:
            nested = field.nested

        if isclass(nested) and issubclass(nested, Schema):
            name = nested.__name__
            only = field.only
            exclude = field.exclude
            nested_cls = nested
            nested_instance = nested(only=only, exclude=exclude)
        else:
            nested_cls = nested.__class__
            name = nested_cls.__name__
            nested_instance = nested

        outer_name = obj.__class__.__name__
        # If this is not a schema we've seen, and it's not this schema (checking this for recursive schemas),
        # put it in our list of schema defs
        if name not in self._nested_schema_classes and name != outer_name:
            wrapped_nested = self.__class__(nested=True)
            wrapped_dumped = wrapped_nested.dump(nested_instance)

            wrapped_dumped["additionalProperties"] = _resolve_additional_properties(
                nested_cls
            )

            self._nested_schema_classes[name] = wrapped_dumped

            self._nested_schema_classes.update(wrapped_nested._nested_schema_classes)

        # and the schema is just a reference to the def
        schema = {"type": "object", "$ref": "#/definitions/{}".format(name)}

        # NOTE: doubled up to maintain backwards compatibility
        metadata = field.metadata.get("metadata", {})
        metadata.update(field.metadata)

        for md_key, md_val in metadata.items():
            if md_key in ("metadata", "name"):
                continue
            schema[md_key] = md_val

        if field.default is not missing and not callable(field.default):
            schema["default"] = nested_instance.dump(field.default)

        if field.many:
            schema = {
                "type": "array" if field.required else ["array", "null"],
                "items": schema,
            }

        return schema

    def dump(self, obj, **kwargs):
        """Take obj for later use: using class name to namespace definition."""
        self.obj = obj
        return super().dump(obj, **kwargs)

    @post_dump
    def wrap(self, data, **_) -> typing.Dict[str, typing.Any]:
        """Wrap this with the root schema definitions."""
        if self.nested:  # no need to wrap, will be in outer defs
            return data

        cls = self.obj.__class__
        name = cls.__name__

        data["additionalProperties"] = _resolve_additional_properties(cls)

        self._nested_schema_classes[name] = data
        root = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "definitions": self._nested_schema_classes,
            "$ref": "#/definitions/{name}".format(name=name),
        }
        return root
