"""
A simple instance validation module for Discovery schemas.
Unfrtunately, Google uses a slightly modified version of JSONschema draft3 (mix of jsonschema and protobuf).
As a result, using an external library to validate Discovery schemas will raise lots of errors.
I tried to modify the popular: https://github.com/Julian/jsonschema to make it work with Google's version,
but it was just too complicated for the relatively simple task on our hands.
If you face any problems with aiogoogle's validation, you can always turn it off by either passing: ``method.__call__(validate=False)``
or turning it off for the whole API by passing aiogoogle.discover(validate=False)

This module misses a lot of the features provided with more advanced jsonschema validators e.g.

1. collecting all validation errors and raising them all at once
2. more descriptive errors with nice tracebacks
3. Validating repeatable instances
4. additionalItems

and more: https://tools.ietf.org/html/draft-zyp-json-schema-03
"""


__all__ = ["validate"]

import base64
import datetime
import warnings
import re
import rfc3339
from functools import wraps

from .excs import ValidationError


# ------- MAPPINGS -------#

JSON_PYTHON_TYPE_MAPPING = {
    "number": (float, int),
    "integer": (int),
    "string": (str),
    "object": (dict),
    "array": (list, set, tuple),
    "boolean": (bool),
    "null": (),  # Not used
    "any": (float, int, str, dict, list, set, tuple, bool),
}

KNOWN_FORMATS = [
    "int32",
    "uint32",
    "double",
    "float",
    "null",
    "byte",
    "date",
    "date-time",
    "int64",
    "uint64",
    "google-fieldmask",
    "google-duration",
    "google-datetime",
]

# After writing a format check for any of those formats, remove the format you wrote the checker for.
IGNORABLE_FORMATS = ["google-fieldmask", "google-duration", "google-datetime"]

TYPE_FORMAT_MAPPING = {
    # Given this type, if None then don't check for additional "format" property in the schema, else, format might be any of the mapped values
    # Those are kept here for reference. They aren't used by any of the validators below. Instead validators check directly if there's any format requirements
    "any": [],
    "array": [],
    "boolean": [],
    "integer": ["int32", "unint32"],
    "number": ["double", "float"],
    "object": [],
    "string": ["null", "byte", "date", "date-time", "int64", "uint64"],
}

# ------ Helpers -------#


def make_validation_error_msg(checked_value, correct_criteria, schema_name=None):
    return f'\n\n Invalid instance: "{str(schema_name)}"\n\n{checked_value} isn\'t valid. Expected a value that meets the following criteria: {correct_criteria}'


def handle_type_and_value_errors(fn):
    @wraps(fn)
    def wrapper(*args, schema_name=None):
        try:
            return fn(*args, schema_name=schema_name)
        except (ValueError, TypeError) as e:
            raise ValidationError(
                f"{e.__class__.__name__}:\nValue: {args[0]}\nSchema name: {schema_name}\nValidator's name: {fn.__name__}\nError: {str(e)}"
            )

    return wrapper


# -------- VALIDATORS ---------#

# Type validators (JSON schema)


@handle_type_and_value_errors
def any_validator(value, schema_name=None):
    # req_types = JSON_PYTHON_TYPE_MAPPING['any']
    # if not isinstance(value, req_types):
    #    raise ValidationError(make_validation_error_msg(value, str(req_types), schema_name))
    pass


@handle_type_and_value_errors
def array_validator(value, schema_name=None):
    req_types = JSON_PYTHON_TYPE_MAPPING["array"]
    if not isinstance(value, req_types):
        raise ValidationError(
            make_validation_error_msg(value, str(req_types), schema_name)
        )


@handle_type_and_value_errors
def boolean_validator(value, schema_name=None):
    req_types = JSON_PYTHON_TYPE_MAPPING["boolean"]
    if not isinstance(value, req_types):
        raise ValidationError(
            make_validation_error_msg(value, str(req_types), schema_name)
        )


@handle_type_and_value_errors
def integer_validator(value, schema_name=None):
    req_types = JSON_PYTHON_TYPE_MAPPING["integer"]
    if not isinstance(value, req_types):
        raise ValidationError(
            make_validation_error_msg(value, str(req_types), schema_name)
        )


@handle_type_and_value_errors
def number_validator(value, schema_name=None):
    req_types = JSON_PYTHON_TYPE_MAPPING["number"]
    if not isinstance(value, req_types):
        raise ValidationError(
            make_validation_error_msg(value, str(req_types), schema_name)
        )


@handle_type_and_value_errors
def object_validator(value, schema_name=None):
    req_types = JSON_PYTHON_TYPE_MAPPING["object"]
    if not isinstance(value, req_types):
        raise ValidationError(
            make_validation_error_msg(value, str(req_types), schema_name)
        )


@handle_type_and_value_errors
def string_validator(value, schema_name=None):
    req_types = JSON_PYTHON_TYPE_MAPPING["string"]
    if not isinstance(value, req_types):
        raise ValidationError(
            make_validation_error_msg(value, str(req_types), schema_name)
        )


# Format validators (Discovery Specific)  https://developers.google.com/discovery/v1/type-format


@handle_type_and_value_errors
def int32_validator(value, schema_name=None):
    if (int(value) > 2147483648) or (int(value) < -2147483648):
        raise ValidationError(
            make_validation_error_msg(
                value, "Integer between -2147483648 and 2147483648", schema_name
            )
        )


@handle_type_and_value_errors
def uint32_validator(value, schema_name=None):
    if (int(value) < 0) or (int(value) > 4294967295):
        raise ValidationError(
            make_validation_error_msg(
                value, "Integer between 0 and 4294967295", schema_name
            )
        )


@handle_type_and_value_errors
def int64_validator(value, schema_name=None):
    if (int(value) > 9223372036854775807) or (int(value) < -9223372036854775807):
        raise ValidationError(
            make_validation_error_msg(
                value,
                "Integer between -9,223,372,036,854,775,807 and -9,223,372,036,854,775,807",
                schema_name,
            )
        )


@handle_type_and_value_errors
def uint64_validator(value, schema_name=None):
    if (int(value) > 9223372036854775807 * 2) or (int(value) < 0):
        raise ValidationError(
            make_validation_error_msg(
                value,
                "Integer between 0 and 9,223,372,036,854,775,807 * 2",
                schema_name,
            )
        )


@handle_type_and_value_errors
def double_validator(value, schema_name=None):
    if not isinstance(value, float):
        raise ValidationError(
            make_validation_error_msg(value, "Double type", schema_name)
        )


@handle_type_and_value_errors
def float_validator(value, schema_name=None):
    if not isinstance(value, float):
        raise ValidationError(
            make_validation_error_msg(value, "Float type", schema_name)
        )


@handle_type_and_value_errors
def byte_validator(value, schema_name=None):
    base64.urlsafe_b64decode(value)


@handle_type_and_value_errors
def date_validator(value, schema_name=None):
    msg = make_validation_error_msg(
        value,
        "JSON date value. Hint: use datetime.date.isoformat(), instead of datetime.date",
        schema_name,
    )
    try:
        pvalue = rfc3339.parse_date(value)
        # pvalue = datetime.date.fromisoformat(value)
    except Exception as e:
        raise ValidationError(str(e) + msg)
    if not isinstance(pvalue, datetime.date):
        raise ValidationError(msg)


@handle_type_and_value_errors
def datetime_validator(value, schema_name=None):
    msg = make_validation_error_msg(
        value,
        "JSON datetime value. Hint: use datetime.datetime.isoformat(), instead of datetime.datetime",
        schema_name,
    )
    try:
        pvalue = rfc3339.parse_datetime(value)
        # pvalue = datetime.datetime.fromisoformat(value)
    except Exception as e:
        raise ValidationError(str(e), msg)
    if not isinstance(pvalue, datetime.datetime):
        raise ValidationError(msg)


@handle_type_and_value_errors
def null_validator(value, schema_name=None):
    if value != "null":
        raise ValidationError(
            make_validation_error_msg(value, "'null' NOT None", schema_name)
        )


# Other Validators


@handle_type_and_value_errors
def minimum_validator(value, minimum, schema_name=None):
    if float(value) < float(minimum):
        raise ValidationError(
            make_validation_error_msg(value, f"Not less than {minimum}", schema_name)
        )


@handle_type_and_value_errors
def maximum_validator(value, maximum, schema_name=None):
    if float(value) > float(maximum):
        raise ValidationError(
            make_validation_error_msg(value, f"Not less than {maximum}", schema_name)
        )


# -- Sub validators ---------------------------


def validate_type(instance, schema, schema_name=None):
    type_validator_name = schema["type"]
    # Check if type in list of possible types to avoid calling globals() maliciously
    if type_validator_name not in JSON_PYTHON_TYPE_MAPPING:
        warnings.warn(
            f"""\n\nUnknown type: {type_validator_name} found.\n\nSkipping type checks for {instance}
            against this schema:\n\n{schema}\n\nPlease create an
            issue @ https://github.com/omarryhan/aiogoogle and report this warning msg."""
        )
        return
    type_validator = globals()[type_validator_name + "_validator"]
    type_validator(instance, schema_name=schema_name)


def validate_format(instance, schema, schema_name=None):
    if schema.get("format"):
        # Exception for format: mostly protobuf formats
        if schema["format"] in IGNORABLE_FORMATS:
            return
        # /Exception
        format_validator_name = schema["format"]
        # Check if type in list of possible types to avoid calling globals() maliciously
        if format_validator_name not in KNOWN_FORMATS:
            warnings.warn(
                f"""\n\nUnknown format: {format_validator_name} found.\n\nSkipping format checks for {instance}
            against this schema:\n\n{schema}\n\nPlease create an
            issue @ https://github.com/omarryhan/aiogoogle and report this warning msg."""
            )
            return
        if format_validator_name == "date-time":
            format_validator_name = "datetime"
        format_validator = globals()[format_validator_name + "_validator"]
        format_validator(instance, schema_name=schema_name)


def validate_range(instance, schema, schema_name=None):
    if schema.get("minimum"):
        minimum_validator(instance, schema["minimum"], schema_name=schema_name)
    if schema.get("maximum"):
        maximum_validator(instance, schema["maximum"], schema_name=schema_name)


def validate_pattern(instance, schema, schema_name=None):
    pattern = schema.get("pattern")
    if pattern is not None:
        match = re.match(pattern, instance)
        if match is None:
            raise ValidationError(
                make_validation_error_msg(
                    instance,
                    f'Match this pattern: r"{pattern}"',
                    schema_name=schema_name,
                )
            )


def validate_enum(instance, schema, schema_name=None):
    options = schema.get("enum")
    if options is not None and instance not in options:
        option_string = ', '.join(f'"{o}"' for o in options)
        raise ValidationError(
            make_validation_error_msg(
                instance,
                f'Must be one of the following: {option_string}',
                schema_name=schema_name
            )
        )

# -- Main Validator ---------------


def validate_all(instance, schema, schema_name=None):
    validate_type(instance, schema, schema_name)
    validate_format(instance, schema, schema_name)
    validate_range(instance, schema, schema_name)
    validate_pattern(instance, schema, schema_name)
    validate_enum(instance, schema, schema_name)


# -- API --------------------


def validate(instance, schema, schemas=None, schema_name=None):
    """
    Arguments:

        Instance: Instance to validate

        schema: schema to validate instance against

        schemas: Full schamas dict to resolve refs if any

        schema_name: Name of the schema (Useful if you want more meaningful errors)
    """

    def resolve(schema):
        """
        Resolves schema from schemas
        if no $ref was found, returns original schema
        """
        if "$ref" in schema:
            if schemas is None:
                raise ValidationError(
                    f"Attempted to resolve a {str(schema)}, but no schemas ref were found to resolve from"
                )
            try:
                schema = schemas[schema["$ref"]]
            except KeyError:
                raise ValidationError(
                    f"Attempted to resolve {schema['$ref']}, but no results found."
                )
        return schema

    def validate_object():
        # Validate instance is an object
        object_validator(instance)  # dict validator, nothing jsonschema related

        # 1. Resolve additional properties
        additional_properties = schema.get("additionalProperties")
        # Jsonschema draft 3
        if additional_properties is False or (
            isinstance(additional_properties, dict) and len(additional_properties) == 0
        ):
            additional_properties = None
        if not isinstance(additional_properties, dict):
            # Typically, additionalProperties should be either False, dict(schema) or empty.
            # Empty will default to None. And False, as shown above, also defaults to None.
            # Sometimes, it's a string, but strings shouldn't be schemas. Strings will be ignored.
            # There are too many of them to raise an error whenever we find one (around 75). So they'll just be ignored
            # raise ValidationError(f'Invalid type of addiotional properties. Shoudl be either a dict or False')
            additional_properties = None

        # 2. Resolve properties
        if not schema.get("properties"):
            if not additional_properties:
                raise ValidationError(
                    f"""
                    Invalid Schema: {str(schema)}.
                    Neither properties nor addiotional properties found in this schema"""
                )
            props = {}
        else:
            props = schema["properties"]

        # 3. Raise warnings or fail on passed dict keys that aren't mentioned in the schema
        for k, _ in instance.items():
            if k not in props:
                # If there's a schema for additional properties validate
                if additional_properties is not None:
                    # some additional properties are misused and
                    # have many properties instead of just one.
                    # Basically, additionalProperties is sort of like **kwargs,
                    # you should only use one per statement/schema
                    if "properties" not in additional_properties:
                        validate(
                            instance[k],
                            additional_properties,
                            schema_name=str(schema_name) + "/additionalProperties",
                        )  # instace, schema for any additional props
                else:
                    warnings.warn(
                        f"""
                        Item {k} was passed, but not mentioned in the
                        following schema {schema.get('id')}.\n\n
                        It will probably be discarded by the API you're using"""
                    )

        # 4. Validate existing properties
        for k, v in props.items():
            # Check if there's a ref to resolve
            v = resolve(v)
            # Check if instance has the property, if not, check if it's required
            if k not in instance:
                if v.get("required") is True:
                    raise ValidationError(f"Instance {k} is required")
            else:
                validate(instance[k], v, schemas, schema_name=k)

    def validate_array():
        array_validator(instance)
        schema_ = resolve(schema["items"])
        # Check if instance has the property, if not, check if it's required
        for item in instance:
            validate(item, schema_, schemas, schema_name=schema_name)

    # Check schema and schemas are dicts.
    # These errors shouldn't normally be raised, unless there's some messed up schema(s) being passed/reffed
    if not isinstance(schema, dict):
        raise TypeError("Schema must be a dict")
    if schemas is not None:
        if not isinstance(schemas, dict):
            raise TypeError("Schemas must be a dict")
    # Preliminary resolvement.
    schema = resolve(schema)
    # If object (Dict): iterate over each entry and recursively validate
    if schema["type"] == "object":
        validate_object()
    # If array (list or tuple) iterate over each item and recursively validate
    elif schema["type"] == "array":
        # Validate instance is an array
        validate_array()
    # Else we reached the lowest level of a schema, validate
    else:
        validate_all(instance, schema, schema_name)
