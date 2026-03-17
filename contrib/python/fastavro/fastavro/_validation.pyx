# cython: language_level=3

import array
import numbers
from collections.abc import Mapping, Sequence

from . import const
from ._schema import (
    extract_record_type, extract_logical_type, schema_name, parse_schema
)
from ._logical_writers import LOGICAL_WRITERS
from ._schema_common import UnknownType
from ._validate_common import ValidationError, ValidationErrorData

ctypedef int int32
ctypedef unsigned int uint32
ctypedef unsigned long long ulong64
ctypedef long long long64

cdef int32 INT_MIN_VALUE = const.INT_MIN_VALUE
cdef int32 INT_MAX_VALUE = const.INT_MAX_VALUE
cdef long64 LONG_MIN_VALUE = const.LONG_MIN_VALUE
cdef long64 LONG_MAX_VALUE = const.LONG_MAX_VALUE

NoValue = object()


cdef inline bint validate_null(datum):
    return datum is None


cdef inline bint validate_boolean(datum):
    return isinstance(datum, bool)


cdef inline bint validate_string(datum):
    return isinstance(datum, str)


cdef inline bint validate_bytes(datum):
    return isinstance(datum, (bytes, bytearray))


cdef inline bint validate_int(datum):
    return (
        isinstance(datum, (int, numbers.Integral))
        and INT_MIN_VALUE <= datum <= INT_MAX_VALUE
        and not isinstance(datum, bool)
    )


cdef inline bint validate_long(datum):
    return (
        isinstance(datum, (int, numbers.Integral))
        and LONG_MIN_VALUE <= datum <= LONG_MAX_VALUE
        and not isinstance(datum, bool)
    )


cdef inline bint validate_float(datum):
    return (
        isinstance(datum, (int, float, numbers.Real))
        and not isinstance(datum, bool)
    )


cdef inline bint validate_fixed(datum, dict schema):
    return (
        (isinstance(datum, bytes) or isinstance(datum, bytearray))
        and len(datum) == schema["size"]
    )


cdef inline bint validate_enum(datum, dict schema):
    return datum in schema["symbols"]


cdef inline bint validate_array(
    datum,
    dict schema,
    dict named_schemas,
    str parent_ns,
    bint raise_errors,
    dict options,
) except -1:
    if not isinstance(datum, (Sequence, array.array)) or isinstance(datum, str):
        return False

    for d in datum:
        if not _validate(
            datum=d,
            schema=schema["items"],
            named_schemas=named_schemas,
            field=parent_ns,
            raise_errors=raise_errors,
            options=options,
        ):
            return False
    return True


cdef inline bint validate_map(
    object datum,
    dict schema,
    dict named_schemas,
    str parent_ns,
    bint raise_errors,
    dict options,
) except -1:
    # initial checks for map type
    if not isinstance(datum, Mapping):
        return False
    for k in datum:
        if not isinstance(k, str):
            return False

    for v in datum.values():
        if not _validate(
            datum=v,
            schema=schema["values"],
            named_schemas=named_schemas,
            field=parent_ns,
            raise_errors=raise_errors,
            options=options,
        ):
            return False
    return True


cdef inline bint validate_record(
    object datum,
    dict schema,
    dict named_schemas,
    str parent_ns,
    bint raise_errors,
    dict options,
) except -1:
    if not isinstance(datum, Mapping):
        return False
    _, fullname = schema_name(schema, parent_ns)
    if "-type" in datum and datum["-type"] != fullname:
        return False

    for f in schema["fields"]:
        datum_value = datum.get(f["name"], f.get("default", NoValue))
        if datum_value is NoValue and options.get("strict"):
            return False
        elif datum_value is NoValue:
            datum_value = None

        if not _validate(
            datum=datum_value,
            schema=f["type"],
            named_schemas=named_schemas,
            field=f"{fullname}.{f['name']}",
            raise_errors=raise_errors,
            options=options,
        ):
            return False
    return True


cdef inline bint validate_union(
    object datum,
    list schema,
    dict named_schemas,
    str parent_ns,
    bint raise_errors,
    dict options,
) except -1:
    if isinstance(datum, tuple) and not options.get("disable_tuple_notation"):
        (name, datum) = datum
        for candidate in schema:
            if extract_record_type(candidate) == "record":
                schema_name = candidate["name"]
            else:
                schema_name = candidate
            if schema_name == name:
                return _validate(
                    datum,
                    schema=candidate,
                    named_schemas=named_schemas,
                    field=parent_ns,
                    raise_errors=raise_errors,
                    options=options,
                )
        else:
            return False

    cdef list errors = []
    for s in schema:
        try:
            ret = _validate(
                datum,
                schema=s,
                named_schemas=named_schemas,
                field=parent_ns,
                raise_errors=raise_errors,
                options=options,
            )
            if ret:
                # We exit on the first passing type in Unions
                return True
        except ValidationError as e:
            errors.extend(e.errors)
    if raise_errors:
        raise ValidationError(*errors)
    return False


cpdef _validate(
    object datum,
    object schema,
    dict named_schemas,
    str field,
    bint raise_errors,
    dict options,
):
    record_type = extract_record_type(schema)
    result = None

    logical_type = extract_logical_type(schema)
    if logical_type:
        prepare = LOGICAL_WRITERS.get(logical_type)
        if prepare:
            datum = prepare(datum, schema)

    # explicit, so that cython is faster, but only for Base Validators
    if record_type == "null":
        result = validate_null(datum)
    elif record_type == "boolean":
        result = validate_boolean(datum)
    elif record_type == "string":
        result = validate_string(datum)
    elif record_type == "int":
        result = validate_int(datum)
    elif record_type == "long":
        result = validate_long(datum)
    elif record_type in ("float", "double"):
        result = validate_float(datum)
    elif record_type == "bytes":
        result = validate_bytes(datum)
    elif record_type == "fixed":
        result = validate_fixed(datum, schema=schema)
    elif record_type == "enum":
        result = validate_enum(datum, schema=schema)
    elif record_type == "array":
        result = validate_array(
            datum,
            schema=schema,
            named_schemas=named_schemas,
            parent_ns=field,
            raise_errors=raise_errors,
            options=options,
        )
    elif record_type == "map":
        result = validate_map(
            datum,
            schema=schema,
            named_schemas=named_schemas,
            parent_ns=field,
            raise_errors=raise_errors,
            options=options,
        )
    elif record_type in ("union", "error_union"):
        result = validate_union(
            datum,
            schema=schema,
            named_schemas=named_schemas,
            parent_ns=field,
            raise_errors=raise_errors,
            options=options,
        )
    elif record_type in ("record", "error", "request"):
        result = validate_record(
            datum,
            schema=schema,
            named_schemas=named_schemas,
            parent_ns=field,
            raise_errors=raise_errors,
            options=options,
        )
    elif record_type in named_schemas:
        result = _validate(
            datum,
            schema=named_schemas[record_type],
            named_schemas=named_schemas,
            field=field,
            raise_errors=raise_errors,
            options=options,
        )
    else:
        raise UnknownType(record_type)

    if raise_errors and result is False:
        raise ValidationError(ValidationErrorData(datum, schema, field))

    return bool(result)


cpdef validate(
    object datum,
    object schema,
    str field="",
    bint raise_errors=True,
    bint strict=False,
    bint disable_tuple_notation=False,
):
    named_schemas = {}
    parsed_schema = parse_schema(schema, named_schemas)
    return _validate(
        datum,
        parsed_schema,
        named_schemas,
        field,
        raise_errors,
        options={"strict": strict, "disable_tuple_notation": disable_tuple_notation},
    )


cpdef validate_many(
    object records,
    object schema,
    bint raise_errors=True,
    bint strict=False,
    bint disable_tuple_notation=False,
):
    cdef bint result
    cdef list errors = []
    cdef list results = []
    named_schemas = {}
    parsed_schema = parse_schema(schema, named_schemas)
    for record in records:
        try:
            result = _validate(
                record,
                parsed_schema,
                named_schemas,
                field="",
                raise_errors=raise_errors,
                options={
                    "strict": strict,
                    "disable_tuple_notation": disable_tuple_notation,
                }
            )
            results.append(result)
        except ValidationError as e:
            errors.extend(e.errors)
    if raise_errors and errors:
        raise ValidationError(*errors)
    return all(results)
