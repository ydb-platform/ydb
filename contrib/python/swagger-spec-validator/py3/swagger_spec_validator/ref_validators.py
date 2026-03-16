from __future__ import annotations

import contextlib
import functools
import logging
from collections.abc import Mapping
from typing import Any
from typing import Callable
from typing import Generator
from typing import TYPE_CHECKING

from jsonschema import validators
from jsonschema.validators import Draft4Validator
from jsonschema.validators import RefResolver

if TYPE_CHECKING:
    from jsonschema.exceptions import _Error
    from jsonschema.validators import _Validator

from swagger_spec_validator import common


log = logging.getLogger(__name__)


default_handlers = {
    "http": common.read_url,
    "https": common.read_url,
    "file": common.read_url,
}


def validate(
    instance: object,
    schema: Mapping[str, Any],
    instance_cls: type[_Validator],
    cls: type[_Validator] | None = None,
    *args: Any,
    **kwargs: Any,
) -> None:
    """This is a carbon-copy of :method:`jsonschema.validate` except that it
    takes two validator classes instead of just one. In the jsonschema
    implementation, `cls` is used to validate both the schema and the
    instance. This changes the behavior to have a separate validator for
    each of schema and instance. Schema should not be validated with the
    custom validator returned by :method:`create_dereffing_validator` because
    it follows $refs.

    :param instance: the instance to validate
    :param schema: the schema to validate with
    :param instance_cls: Validator class to validate instance.
    :param cls: Validator class to validate schema.

    :raises:
        :exc:`ValidationError` if the instance is invalid
        :exc:`SchemaError` if the schema itself is invalid
    """
    if cls is None:
        cls = validators.validator_for(schema)
    cls.check_schema(schema)
    instance_cls(schema, *args, **kwargs).validate(instance)


def create_dereffing_validator(instance_resolver: RefResolver) -> type[_Validator]:
    """Create a customized Draft4Validator that follows $refs in the schema
    being validated (the Swagger spec for a service). This is not to be
    confused with $refs that are in the schema that describes the Swagger 2.0
    specification.

    :param instance_resolver: resolver for the swagger service's spec
    :type instance_resolver: :class:`jsonschema.RefResolver`

    :rtype: Its complicated. See jsonschema.validators.create()
    """
    visited_refs: dict[str, str] = {}

    validators_to_bound = {
        "$ref",
        "additionalProperties",
        "allOf",
        "anyOf",
        "dependencies",
        "maxProperties",
        "minProperties",
        "not",
        "oneOf",
        "patternProperties",
        "properties",
        "required",
        "type",
    }

    bound_validators = {
        k: functools.partial(
            validator_wrapper,
            instance_resolver=instance_resolver,
            visited_refs=visited_refs,
            default_validator_callable=v,
        )
        if k in validators_to_bound
        else v
        for k, v in Draft4Validator.VALIDATORS.items()
    }

    return validators.extend(Draft4Validator, bound_validators)


def validate_schema_value(
    schema: Mapping[str, Any],
    value: Any,
    swagger_resolver: RefResolver | None = None,
) -> None:
    # pass resolver to avoid to refetch schema files
    if swagger_resolver is None:
        swagger_resolver = RefResolver.from_schema(schema)
    create_dereffing_validator(swagger_resolver)(
        schema, resolver=swagger_resolver
    ).validate(value)


@contextlib.contextmanager
def visiting(visited_refs: dict[str, str], ref: str) -> Generator[None, None, None]:
    """Context manager that keeps track of $refs that we've seen during
    validation.

    :param visited_refs: dict of $refs
    :param ref: string $ref value
    """
    visited_refs[ref] = ref
    try:
        yield
    finally:
        del visited_refs[ref]


def validator_wrapper(
    validator: type[_Validator],
    schema_element: Any,
    instance: dict[str, Any],
    schema: Mapping[str, Any],
    instance_resolver: RefResolver,
    visited_refs: dict[str, str],
    default_validator_callable: Callable,
) -> Generator[_Error, None, None]:
    """Generator function that parameterizes default_validator_callable.

    :type validator: :class:`jsonschema.validators.Validator`
    :param schema_element: The schema element that is passed in to each
        specific validator callable aka the 2nd arg in each
        jsonschema._validators.* callable.
    :param instance: The fragment of the swagger service spec that is being
        validated.
    :param schema: The fragment of the swagger jsonschema spec that describes
        is used for validation.
    :param instance_resolver: Resolves refs in the swagger service spec
    :param visited_refs: Keeps track of visisted refs during validation of
        the swagger service spec.
    :param default_validator_callable: jsonschema._validators.* callable
    """
    yield from deref_and_validate(
        validator,
        schema_element,
        instance,
        schema,
        instance_resolver,
        visited_refs,
        default_validator_callable,
    )


def deref_and_validate(
    validator: type[_Validator],
    schema_element: Any,
    instance: dict[str, Any],
    schema: Mapping[str, Any],
    instance_resolver: RefResolver,
    visited_refs: dict[str, str],
    default_validator_callable: Callable,
) -> Generator[_Error, None, None]:
    """Generator function that dereferences instance if it is a $ref before
    passing it downstream for actual validation. When a cyclic ref is detected,
    short-circuit and return.

    :type validator: :class:`jsonschema.validators.Validator`
    :param schema_element: The schema element that is passed in to each
        specific validator callable aka the 2nd arg in each
        jsonschema._validators.* callable.
    :param instance: The fragment of the swagger service spec that is being
        validated.
    :param schema: The fragment of the swagger jsonschema spec that describes
        is used for validation.
    :param instance_resolver: Resolves refs in the swagger service spec
    :param visited_refs: Keeps track of visisted refs during validation of
        the swagger service spec.
    :param default_validator_callable: jsonschema._validators.* callable
    """
    if (
        isinstance(instance, dict)
        and "$ref" in instance
        and isinstance(instance["$ref"], str)
    ):
        ref = instance["$ref"]
        # Annotate $ref dict with scope - used by custom validations
        # We still need to attach the scope even if this is a cycle, as otherwise there are cases
        # with specs split into multiple files where it can't be dereferenced properly
        attach_scope(instance, instance_resolver)

        if ref in visited_refs:
            log.debug("Found cycle in %s", ref)
            return

        with visiting(visited_refs, ref):
            with instance_resolver.resolving(ref) as target:
                yield from default_validator_callable(
                    validator, schema_element, target, schema
                )

    else:
        yield from default_validator_callable(
            validator, schema_element, instance, schema
        )


def attach_scope(ref_dict: dict[str, Any], instance_resolver: RefResolver) -> None:
    """Attach scope to each $ref we encounter so that the $ref can be
    resolved by custom validations done outside the scope of jsonscema
    validations.

    :param ref_dict: dict with $ref key
    :type instance_resolver: :class:`jsonschema.RefResolver`
    """
    if "x-scope" in ref_dict:
        log.debug("Ref %s already has scope attached", ref_dict["$ref"])
        return
    log.debug("Attaching x-scope to %s", ref_dict)
    ref_dict["x-scope"] = list(instance_resolver._scopes_stack)  # type: ignore


@contextlib.contextmanager
def in_scope(
    resolver: RefResolver, ref_dict: dict[str, Any]
) -> Generator[None, None, None]:
    """Context manager to assume the given scope for the passed in resolver.

    The resolver's original scope is restored when exiting the context manager.

    :type resolver: :class:`jsonschema.RefResolver
    :type ref_dict: dict
    """
    if "x-scope" not in ref_dict:
        yield
    else:
        saved_scope_stack = resolver._scopes_stack  # type: ignore
        try:
            resolver._scopes_stack = ref_dict["x-scope"]  # type: ignore
            yield
        finally:
            resolver._scopes_stack = saved_scope_stack  # type: ignore
