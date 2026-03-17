# cython: language_level=3

import hashlib
from io import StringIO
from os import path
from copy import deepcopy
import json
from libc.math cimport floor, log10
import re

from .repository import FlatDictRepository, SchemaRepositoryError
from .const import AVRO_TYPES
from ._schema_common import (
    PRIMITIVES,
    UnknownType,
    SchemaParseException,
    RESERVED_PROPERTIES,
    OPTIONAL_FIELD_PROPERTIES,
    RESERVED_FIELD_PROPERTIES,
    JAVA_FINGERPRINT_MAPPING,
    FINGERPRINT_ALGORITHMS,
    RABIN_64,
    rabin_fingerprint,
)

SYMBOL_REGEX = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
NO_DEFAULT = object()


cpdef inline is_nullable_union(schema):
    count = 0
    for s in schema:
        extracted_type = extract_record_type(s)
        if extracted_type not in AVRO_TYPES or extracted_type == "record":
            count += 1

    return count == 1


cpdef inline _get_name_and_record_counts_from_union(schema):
    record_type_count = 0
    named_type_count = 0
    for s in schema:
        extracted_type = extract_record_type(s)
        if extracted_type == "record":
            record_type_count += 1
            named_type_count += 1
        elif extracted_type == "enum" or extracted_type == "fixed":
            named_type_count += 1
        elif extracted_type not in AVRO_TYPES:
            named_type_count += 1
            # There should probably be extra checks to see if this simple name
            # is actually a record, but the current behavior doesn't do the
            # check and just assumes it is or could be a record
            record_type_count += 1

    return named_type_count, record_type_count


cpdef inline is_single_record_union(schema):
    return _get_name_and_record_counts_from_union(schema)[1] == 1


cpdef inline is_single_name_union(schema):
    return _get_name_and_record_counts_from_union(schema)[0] == 1


cpdef inline extract_record_type(schema):
    if isinstance(schema, dict):
        return schema["type"]

    if isinstance(schema, list):
        return "union"

    return schema


cpdef inline str extract_logical_type(schema):
    if not isinstance(schema, dict):
        return None
    rt = schema["type"]
    lt = schema.get("logicalType")
    if lt:
        # TODO: Building this string every time is going to be relatively slow.
        return f"{rt}-{lt}"
    return None


cpdef fullname(schema):
    return schema_name(schema, "")[1]


cpdef schema_name(schema, parent_ns):
    try:
        name = schema["name"]
    except KeyError:
        raise SchemaParseException(
            f'"name" is a required field missing from the schema: {schema}'
        )

    namespace = schema.get("namespace", parent_ns)
    if "." in name:
        return name.rsplit(".", 1)[0], name
    elif namespace:
        return namespace, f"{namespace}.{name}"
    else:
        return "", name


cpdef expand_schema(schema):
    return parse_schema(schema, expand=True, _write_hint=False)


def parse_schema(
    schema,
    named_schemas=None,
    *,
    expand=False,
    _write_hint=True,
    _force=False,
    _ignore_default_error=False,
):
    if named_schemas is None:
        named_schemas = {}

    if isinstance(schema, dict) and "__fastavro_parsed" in schema:
        if "__named_schemas" in schema:
            for key, value in schema["__named_schemas"].items():
                named_schemas[key] = value
        else:
            # Some old schemas might only have __fastavro_parsed and not
            # __named_schemas since that came later. For these schemas, we need
            # to re-parse the schema to handle named types
            return _parse_schema(
                schema,
                "",
                expand,
                _write_hint,
                set(),
                named_schemas,
                NO_DEFAULT,
                _ignore_default_error,
            )

    if _force or expand:
        return _parse_schema(
            schema,
            "",
            expand,
            _write_hint,
            set(),
            named_schemas,
            NO_DEFAULT,
            _ignore_default_error,
        )
    elif isinstance(schema, dict) and "__fastavro_parsed" in schema:
        return schema
    elif isinstance(schema, list):
        # If we are given a list we should make sure that the immediate sub
        # schemas have the hint in them
        return [
            parse_schema(
                s,
                named_schemas,
                expand=expand,
                _write_hint=_write_hint,
                _force=_force,
                _ignore_default_error=_ignore_default_error,
            )
            for s in schema
        ]
    else:
        return _parse_schema(
            schema,
            "",
            expand,
            _write_hint,
            set(),
            named_schemas,
            NO_DEFAULT,
            _ignore_default_error,
        )


cdef _raise_default_value_error(
    default, schema_type, ignore_default_error
):
    if ignore_default_error:
        return
    elif isinstance(schema_type, list):
        text = f"a schema in union with type: {schema_type}"
    else:
        text = f"schema type: {schema_type}"

    raise SchemaParseException(f"Default value <{default}> must match {text}")


cdef _maybe_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        return value


cdef _default_matches_schema(default, schema):
    if (
        (schema == "null" and default is not None)
        or (schema == "boolean" and not isinstance(default, bool))
        or (schema == "string" and not isinstance(default, str))
        or (schema == "bytes" and not isinstance(default, str))
        or (schema == "double" and not isinstance(_maybe_float(default), float))
        or (schema == "float" and not isinstance(_maybe_float(default), float))
        or (schema == "int" and not isinstance(default, int))
        or (schema == "long" and not isinstance(default, int))
    ):
        return False
    return True


cdef _parse_schema(
    schema,
    namespace,
    expand,
    _write_hint,
    names,
    named_schemas,
    default,
    ignore_default_error,
):
    # union schemas
    if isinstance(schema, list):
        parsed_schemas = [
            _parse_schema(
                s,
                namespace,
                expand,
                False,
                names,
                named_schemas,
                NO_DEFAULT,
                ignore_default_error,
            )
            for s in schema
        ]
        if default is not NO_DEFAULT:
            for s in parsed_schemas:
                if _default_matches_schema(default, s):
                    break
            else:
                _raise_default_value_error(default, schema, ignore_default_error)
        return parsed_schemas

    # string schemas; this could be either a named schema or a primitive type
    elif not isinstance(schema, dict):
        if schema in PRIMITIVES:
            if default is not NO_DEFAULT:
                if not _default_matches_schema(default, schema):
                    _raise_default_value_error(
                        default, schema, ignore_default_error
                    )
            return schema

        if "." not in schema and namespace:
            schema = namespace + "." + schema

        if schema not in named_schemas:
            raise UnknownType(schema)

        if expand and "name" in named_schemas[schema]:
            # If `name` is in the schema, it has been fully resolved and so we
            # can include the full schema. If `name` is not in the schema yet,
            # then we are still recursing that schema and must use the named
            # schema or else we will have infinite recursion when printing the
            # final schema
            return named_schemas[schema]
        return schema

    else:
        # Remaining valid schemas must be dict types
        schema_type = schema["type"]

        parsed_schema = {
            key: value
            for key, value in schema.items()
            if key not in RESERVED_PROPERTIES
        }
        parsed_schema["type"] = schema_type

        if "doc" in schema:
            parsed_schema["doc"] = schema["doc"]

        # Correctness checks for logical types
        logical_type = parsed_schema.get("logicalType")
        if logical_type == "decimal":
            scale = parsed_schema.get("scale")
            if scale and (not isinstance(scale, int) or scale < 0):
                raise SchemaParseException(
                    f"decimal scale must be a positive integer, not {scale}"
                )

            precision = parsed_schema.get("precision")
            if precision:
                if not isinstance(precision, int) or precision <= 0:
                    raise SchemaParseException(
                        "decimal precision must be a positive integer, "
                        + f"not {precision}"
                    )
                if schema_type == "fixed":
                    # https://avro.apache.org/docs/current/spec.html#Decimal
                    size = schema["size"]
                    max_precision = int(floor(log10(2) * (8 * size - 1)))
                    if precision > max_precision:
                        raise SchemaParseException(
                            f"decimal precision of {precision} doesn't fit "
                            + f"into array of length {size}"
                        )

            if scale and precision and precision < scale:
                raise SchemaParseException(
                    "decimal scale must be less than or equal to "
                    + f"the precision of {precision}"
                )

        if schema_type == "array":
            parsed_schema["items"] = _parse_schema(
                schema["items"],
                namespace,
                expand,
                False,
                names,
                named_schemas,
                NO_DEFAULT,
                ignore_default_error,
            )
            if default is not NO_DEFAULT and not isinstance(default, list):
                _raise_default_value_error(
                    default, schema_type, ignore_default_error
                )

        elif schema_type == "map":
            parsed_schema["values"] = _parse_schema(
                schema["values"],
                namespace,
                expand,
                False,
                names,
                named_schemas,
                NO_DEFAULT,
                ignore_default_error,
            )
            if default is not NO_DEFAULT and not isinstance(default, dict):
                _raise_default_value_error(
                    default, schema_type, ignore_default_error
                )

        elif schema_type == "enum":
            _, fullname = schema_name(schema, namespace)
            if fullname in names:
                raise SchemaParseException(f"redefined named type: {fullname}")
            names.add(fullname)

            _validate_enum_symbols(schema)

            if default is not NO_DEFAULT and not isinstance(default, str):
                _raise_default_value_error(
                    default, schema_type, ignore_default_error
                )

            named_schemas[fullname] = parsed_schema

            parsed_schema["name"] = fullname
            parsed_schema["symbols"] = schema["symbols"]

        elif schema_type == "fixed":
            _, fullname = schema_name(schema, namespace)
            if fullname in names:
                raise SchemaParseException(f"redefined named type: {fullname}")
            names.add(fullname)

            if default is not NO_DEFAULT and not isinstance(default, str):
                _raise_default_value_error(
                    default, schema_type, ignore_default_error
                )

            named_schemas[fullname] = parsed_schema

            parsed_schema["name"] = fullname
            parsed_schema["size"] = schema["size"]

        elif schema_type == "record" or schema_type == "error":
            # records
            namespace, fullname = schema_name(schema, namespace)
            if fullname in names:
                raise SchemaParseException(f"redefined named type: {fullname}")
            names.add(fullname)

            if default is not NO_DEFAULT and not isinstance(default, dict):
                _raise_default_value_error(
                    default, schema_type, ignore_default_error
                )

            named_schemas[fullname] = parsed_schema

            fields = []
            for field in schema.get("fields", []):
                fields.append(
                    parse_field(
                        field,
                        namespace,
                        expand,
                        names,
                        named_schemas,
                        ignore_default_error,
                    )
                )

            parsed_schema["name"] = fullname
            parsed_schema["fields"] = fields

            # Hint that we have parsed the record
            if _write_hint:
                # Make a copy of parsed_schema so that we don't have a cyclical
                # reference. Using deepcopy is pretty slow, and we don't need a
                # true deepcopy so this works good enough
                named_schemas[fullname] = {
                    k: v for k, v in parsed_schema.items()
                }

                parsed_schema["__fastavro_parsed"] = True
                parsed_schema["__named_schemas"] = named_schemas

        elif schema_type in PRIMITIVES:
            parsed_schema["type"] = schema_type
            if default is not NO_DEFAULT:
                if (
                    (schema_type == "null" and default is not None)
                    or (schema_type == "boolean" and not isinstance(default, bool))
                    or (schema_type == "string" and not isinstance(default, str))
                    or (schema_type == "bytes" and not isinstance(default, str))
                    or (schema_type == "double" and not isinstance(default, float))
                    or (schema_type == "float" and not isinstance(default, float))
                    or (schema_type == "int" and not isinstance(default, int))
                    or (schema_type == "long" and not isinstance(default, int))
                ):
                    _raise_default_value_error(
                        default, schema_type, ignore_default_error
                    )

        else:
            raise UnknownType(schema)

        return parsed_schema


cdef parse_field(field, namespace, expand, names, named_schemas, ignore_default_error):
    parsed_field = {
        key: value
        for key, value in field.items()
        if key not in RESERVED_FIELD_PROPERTIES
    }

    for prop in OPTIONAL_FIELD_PROPERTIES:
        if prop in field:
            parsed_field[prop] = field[prop]

    # Aliases must be a list
    aliases = parsed_field.get("aliases", [])
    if not isinstance(aliases, list):
        msg = f"aliases must be a list, not {aliases}"
        raise SchemaParseException(msg)

    default = field.get("default", NO_DEFAULT)

    parsed_field["name"] = field["name"]
    parsed_field["type"] = _parse_schema(
        field["type"],
        namespace,
        expand,
        False,
        names,
        named_schemas,
        default,
        ignore_default_error,
    )

    return parsed_field


def load_schema(
    schema_path,
    *,
    repo=None,
    named_schemas=None,
    _write_hint=True,
    _injected_schemas=None,
):
    schema_name = schema_path
    if repo is None:
        file_dir, file_name = path.split(schema_path)
        schema_name, _file_ext = path.splitext(file_name)
        repo = FlatDictRepository(file_dir)

    if named_schemas is None:
        named_schemas = {}

    if _injected_schemas is None:
        _injected_schemas = set()

    return _load_schema(
        schema_name, repo, named_schemas, _write_hint, _injected_schemas
    )


cdef _load_schema(schema_name, repo, named_schemas, write_hint, injected_schemas):
    try:
        schema = repo.load(schema_name)
        return _parse_schema_with_repo(
            schema,
            repo,
            named_schemas,
            write_hint,
            injected_schemas,
        )
    except SchemaRepositoryError as error:
        raise error


cdef _parse_schema_with_repo(
    schema,
    repo,
    named_schemas,
    write_hint,
    injected_schemas,
):
    try:
        schema_copy = deepcopy(named_schemas)
        return parse_schema(schema, named_schemas=named_schemas, _write_hint=write_hint)
    except UnknownType as error:
        missing_subject = error.name
        try:
            sub_schema = _load_schema(
                missing_subject,
                repo,
                named_schemas=schema_copy,
                write_hint=False,
                injected_schemas=injected_schemas,
            )
        except SchemaRepositoryError:
            raise error

        if sub_schema["name"] not in injected_schemas:
            injected_schema = _inject_schema(schema, sub_schema)
            if isinstance(schema, str) or isinstance(schema, list):
                schema = injected_schema[0]
            injected_schemas.add(sub_schema["name"])
        return _parse_schema_with_repo(
            schema, repo, schema_copy, write_hint, injected_schemas
        )


cdef _inject_schema(outer_schema, inner_schema, ns="", is_injected=False):
    namespace = ns  # Avoids a conflict with a C++ keyword in Cythonized path.
    # Once injected, we can stop checking to see if we need to inject since it
    # should only be done once at most
    if is_injected is True:
        return outer_schema, is_injected

    # union schemas
    if isinstance(outer_schema, list):
        union = []
        for each_schema in outer_schema:
            if is_injected:
                union.append(each_schema)
            else:
                return_schema, injected = _inject_schema(
                    each_schema, inner_schema, namespace, is_injected
                )
                union.append(return_schema)
                if injected is True:
                    is_injected = injected
        return union, is_injected

    # string schemas; this could be either a named schema or a primitive type
    elif not isinstance(outer_schema, dict):
        if outer_schema in PRIMITIVES:
            return outer_schema, is_injected

        if "." not in outer_schema and namespace:
            outer_schema = namespace + "." + outer_schema

        if outer_schema == inner_schema["name"]:
            return inner_schema, True
        else:
            # Hit a named schema that has already been loaded previously. Return
            # the outer_schema so we keep looking
            return outer_schema, is_injected
    else:
        # Remaining valid schemas must be dict types
        schema_type = outer_schema["type"]

        if schema_type == "array":
            return_schema, injected = _inject_schema(
                outer_schema["items"], inner_schema, namespace, is_injected
            )
            outer_schema["items"] = return_schema
            return outer_schema, injected

        elif schema_type == "map":
            return_schema, injected = _inject_schema(
                outer_schema["values"], inner_schema, namespace, is_injected
            )
            outer_schema["values"] = return_schema
            return outer_schema, injected

        elif schema_type == "enum":
            return outer_schema, is_injected

        elif schema_type == "fixed":
            return outer_schema, is_injected

        elif schema_type == "record" or schema_type == "error":
            # records
            namespace, _ = schema_name(outer_schema, namespace)
            fields = []
            for field in outer_schema.get("fields", []):
                if is_injected:
                    fields.append(field)
                else:
                    return_schema, injected = _inject_schema(
                        field["type"], inner_schema, namespace, is_injected
                    )
                    field["type"] = return_schema
                    fields.append(field)

                    if injected is True:
                        is_injected = injected
            if fields:
                outer_schema["fields"] = fields

            return outer_schema, is_injected

        elif schema_type in PRIMITIVES:
            return outer_schema, is_injected

        else:
            raise Exception(
                "Internal error; "
                + "You should raise an issue in the fastavro github repository"
            )


def load_schema_ordered(ordered_schemas, *, _write_hint=True):
    loaded_schemas = []
    named_schemas = {}
    for idx, schema_path in enumerate(ordered_schemas):
        # _write_hint is always False except maybe the outer most schema
        _last = _write_hint if idx + 1 == len(ordered_schemas) else False
        schema = load_schema(
            schema_path, named_schemas=named_schemas, _write_hint=_last
        )
        loaded_schemas.append(schema)

    top_first_order = loaded_schemas[::-1]
    outer_schema = top_first_order.pop(0)

    while top_first_order:
        sub_schema = top_first_order.pop(0)
        _inject_schema(outer_schema, sub_schema)

    return outer_schema


def to_parsing_canonical_form(schema):
    fo = StringIO()
    _to_parsing_canonical_form(parse_schema(schema), fo)
    return fo.getvalue()


cdef _to_parsing_canonical_form(schema, fo):
    # union schemas
    if isinstance(schema, list):
        fo.write("[")
        for idx, s in enumerate(schema):
            if idx != 0:
                fo.write(",")
            _to_parsing_canonical_form(s, fo)
        fo.write("]")

    # string schemas; this could be either a named schema or a primitive type
    elif not isinstance(schema, dict):
        fo.write(f'"{schema}"')

    else:
        # Remaining valid schemas must be dict types
        schema_type = schema["type"]

        if schema_type == "array":
            fo.write(f'{{"type":"{schema_type}","items":')
            _to_parsing_canonical_form(schema["items"], fo)
            fo.write("}")

        elif schema_type == "map":
            fo.write(f'{{"type":"{schema_type}","values":')
            _to_parsing_canonical_form(schema["values"], fo)
            fo.write("}")

        elif schema_type == "enum":
            name = schema["name"]
            fo.write(f'{{"name":"{name}","type":"{schema_type}","symbols":[')

            for idx, symbol in enumerate(schema["symbols"]):
                if idx != 0:
                    fo.write(",")
                fo.write(f'"{symbol}"')
            fo.write("]}")

        elif schema_type == "fixed":
            name = schema["name"]
            size = schema["size"]
            fo.write(f'{{"name":"{name}","type":"{schema_type}","size":{size}}}')

        elif schema_type == "record" or schema_type == "error":
            name = schema["name"]
            fo.write(f'{{"name":"{name}","type":"record","fields":[')

            for idx, field in enumerate(schema["fields"]):
                if idx != 0:
                    fo.write(",")
                name = field["name"]
                fo.write(f'{{"name":"{name}","type":')
                _to_parsing_canonical_form(field["type"], fo)
                fo.write("}")
            fo.write("]}")

        elif schema_type in PRIMITIVES:
            fo.write(f'"{schema_type}"')


def fingerprint(parsing_canonical_form, algorithm):
    if algorithm not in FINGERPRINT_ALGORITHMS:
        raise ValueError(
            f"Unknown schema fingerprint algorithm {algorithm}. "
            + f"Valid values include: {FINGERPRINT_ALGORITHMS}"
        )

    # Fix Java names
    algorithm = JAVA_FINGERPRINT_MAPPING.get(algorithm, algorithm)

    if algorithm == RABIN_64:
        return rabin_fingerprint(parsing_canonical_form.encode())

    h = hashlib.new(algorithm, parsing_canonical_form.encode())
    return h.hexdigest()


def _validate_enum_symbols(schema):
    symbols = schema["symbols"]
    for symbol in symbols:
        if not isinstance(symbol, str) or not SYMBOL_REGEX.fullmatch(symbol):
            raise SchemaParseException(
                "Every symbol must match the regular expression [A-Za-z_][A-Za-z0-9_]*"
            )
    if len(symbols) != len(set(symbols)):
        raise SchemaParseException("All symbols in an enum must be unique")

    if "default" in schema and schema["default"] not in symbols:
        raise SchemaParseException("Default value for enum must be in symbols list")
