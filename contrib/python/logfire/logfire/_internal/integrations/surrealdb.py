from __future__ import annotations

import functools
import inspect
import uuid
from inspect import Signature
from typing import Any, Union, get_args, get_origin

from surrealdb.connections.async_template import AsyncTemplate
from surrealdb.connections.sync_template import SyncTemplate
from surrealdb.data.types.record_id import RecordIdType
from surrealdb.data.types.table import Table
from surrealdb.types import Value

from logfire._internal.main import Logfire
from logfire._internal.scrubbing import BaseScrubber
from logfire._internal.utils import handle_internal_errors


def _is_complex_type(tp: type | type[Value]) -> bool:
    """Return false if values of this type are small and thus worth including in the log message."""
    origin = get_origin(tp)
    if origin in {list, dict, set, tuple}:
        return True
    if tp in {Value}:
        return True
    if tp in (str, bool, int, float, type(None), uuid.UUID, Table, RecordIdType):
        return False
    if origin is Union:  # pragma: no branch
        args = get_args(tp)
        return any(_is_complex_type(arg) for arg in args)
    return True  # pragma: no cover


def _get_all_subclasses(cls: type) -> set[type]:
    subclasses: set[type] = set()
    for subclass in cls.__subclasses__():
        subclasses.add(subclass)
        subclasses.update(_get_all_subclasses(subclass))
    return subclasses


def get_all_surrealdb_classes() -> set[type]:
    return _get_all_subclasses(SyncTemplate) | _get_all_subclasses(AsyncTemplate)


def instrument_surrealdb(
    obj: SyncTemplate | AsyncTemplate | type[SyncTemplate] | type[AsyncTemplate] | None, logfire_instance: Logfire
):
    logfire_instance = logfire_instance.with_settings(custom_scope_suffix='surrealdb')
    if obj is None:
        for cls in get_all_surrealdb_classes():
            instrument_surrealdb(cls, logfire_instance)
        return

    for name, template_method in inspect.getmembers(SyncTemplate):
        if not (
            inspect.isfunction(template_method)
            and not name.startswith('_')
            and SyncTemplate.__dict__.get(name) == template_method
        ):
            continue
        patch_method(obj, name, logfire_instance)


@handle_internal_errors
def patch_method(obj: Any, method_name: str, logfire_instance: Logfire):
    original_method = getattr(obj, method_name, None)
    if not original_method or hasattr(original_method, '_logfire_template'):
        return  # already patched

    sig = inspect.signature(original_method)
    template = span_name = f'surrealdb {method_name}'
    # Keep the span name clean and simple.
    template += _get_params_template(logfire_instance.config.scrubber, sig)

    def get_attributes(*args: Any, **kwargs: Any) -> dict[str, Any]:
        bound = sig.bind(*args, **kwargs)
        params = bound.arguments
        params.pop('self', None)
        params.pop('token', None)  # not scrubbed by default, definitely sensitive in this context
        return params

    if inspect.isgeneratorfunction(original_method) or inspect.isasyncgenfunction(original_method):
        # Don't create a span around a generator: https://logfire.pydantic.dev/docs/reference/advanced/generators/
        @functools.wraps(original_method)
        def wrapped_method(*args: Any, **kwargs: Any) -> Any:
            logfire_instance.info(template, **get_attributes(*args, **kwargs))
            return original_method(*args, **kwargs)

    elif inspect.iscoroutinefunction(original_method):

        @functools.wraps(original_method)
        async def wrapped_method(*args: Any, **kwargs: Any) -> Any:  # pyright: ignore[reportRedeclaration]
            with logfire_instance.span(template, **get_attributes(*args, **kwargs), _span_name=span_name):
                return await original_method(*args, **kwargs)

    else:

        @functools.wraps(original_method)
        def wrapped_method(*args: Any, **kwargs: Any) -> Any:
            with logfire_instance.span(template, **get_attributes(*args, **kwargs), _span_name=span_name):
                return original_method(*args, **kwargs)

    # Mark the method as patched to avoid double patching. Also useful for testing.
    wrapped_method._logfire_template = template  # type: ignore

    setattr(obj, method_name, wrapped_method)


def _get_params_template(scrubber: BaseScrubber, sig: Signature) -> str:
    """Returns a string template for simple non-sensitive parameters in the signature."""
    template_params: list[str] = []
    for param_name, param in sig.parameters.items():
        if param_name == 'self':
            continue
        _, scrubbed = scrubber.scrub_value(path=(param_name,), value=None)
        if (
            param.annotation is not inspect.Parameter.empty
            and not _is_complex_type(param.annotation)
            and not scrubbed
            # not scrubbed by default, definitely sensitive in this context
            and 'token' not in param_name
        ):
            template_params.append(param_name)
    if len(template_params) == 1:
        # e.g. " {param}"
        return f' {{{template_params[0]}}}'
    elif len(template_params) > 1:
        # e.g. " param1 = {param1}, param2 = {param2}"
        return ' ' + ', '.join(f'{p} = {{{p}}}' for p in template_params)
    else:
        return ''
