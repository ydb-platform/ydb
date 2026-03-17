#
# Copyright (C) 2015 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""JSON schema validator."""
from __future__ import annotations

import typing
import warnings

import jsonschema

from ... import utils
from ..datatypes import ValidationError, InDataT

if typing.TYPE_CHECKING:
    try:
        from typing import TypeGuard
    except ImportError:
        from typing_extensions import TypeGuard

    from .datatypes import (
        InDataExT, ResultT,
    )


def is_valid_schema_object(maybe_scm: InDataExT) -> TypeGuard[InDataT]:
    """Determine given object ``maybe_scm`` is an expected schema object."""
    return maybe_scm and utils.is_dict_like(maybe_scm)


def _validate_all(
    data: InDataExT, schema: InDataT, **_options: typing.Any,
) -> ResultT:
    """Do all of the validation checks.

    See the description of :func:`validate` for more details of parameters and
    return value.

    :seealso: https://python-jsonschema.readthedocs.io/en/latest/validate/,
    a section of 'iter_errors' especially
    """
    vldtr = jsonschema.Draft7Validator(schema)  # :raises: SchemaError, ...
    errors = list(vldtr.iter_errors(data))

    return (not errors, [err.message for err in errors])


def _validate(
    data: InDataExT, schema: InDataT, *,
    ac_schema_safe: bool = True,
    **options: typing.Any,
) -> ResultT:
    """Validate ``data`` with ``schema``.

    See the description of :func:`validate` for more details of parameters and
    return value.

    Validate target object 'data' with given schema object.
    """
    try:
        jsonschema.validate(data, schema, **options)
    except (jsonschema.ValidationError, jsonschema.SchemaError,
            Exception) as exc:
        if ac_schema_safe:
            return (False, str(exc))  # Validation was failed.
        raise

    return (True, "")


def validate(
    data: InDataExT, schema: InDataExT, *,
    ac_schema_safe: bool = True,
    ac_schema_errors: bool = False,
    **options: typing.Any,
) -> ResultT:
    """Validate target object with given schema object.

    See also: https://python-jsonschema.readthedocs.org/en/latest/validate/

    :parae data: Target object (a dict or a dict-like object) to validate
    :param schema: Schema object (a dict or a dict-like object)
        instantiated from schema JSON file or schema JSON string
    :param options: Other keyword options such as:

        - ac_schema_safe: Exception (jsonschema.ValidationError or
          jsonschema.SchemaError or others) will be thrown during validation
          process due to any validation or related errors. However, these will
          be catched by default, and will be re-raised if this value is False.

        - ac_schema_errors: Lazily yield each of the validation errors and
          returns all of them if validation fails.

    :return: (True if validation succeeded else False, error message[s])
    """
    if not is_valid_schema_object(schema):
        return (False, f"Invalid schema object: {schema!r}")

    options = utils.filter_options(("cls", ), options)
    if ac_schema_errors:
        return _validate_all(data, typing.cast("InDataT", schema), **options)

    return _validate(data, schema, ac_schema_safe=ac_schema_safe, **options)


def is_valid(
    data: InDataExT, schema: InDataExT, *,
    ac_schema_safe: bool = True,
    ac_schema_errors: bool = False,
    **options: typing.Any,
) -> bool:
    """Raise ValidationError if ``data`` was invalidated by schema `schema`."""
    if not is_valid_schema_object(schema):
        return True

    (_success, error_or_errors) = validate(
        data, schema, ac_schema_safe=True,
        ac_schema_errors=ac_schema_errors, **options,
    )
    if error_or_errors:
        msg = f"scm={schema!s}, err={error_or_errors!s}"
        if ac_schema_safe:
            warnings.warn(msg, stacklevel=2)
            return False

        raise ValidationError(msg)

    return True
