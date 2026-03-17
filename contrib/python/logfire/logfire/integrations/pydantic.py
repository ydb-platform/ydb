"""Integration for instrumenting Pydantic models."""

from __future__ import annotations

import functools
import inspect
import os
import re
from dataclasses import dataclass
from functools import lru_cache
from typing import TYPE_CHECKING, Any, Callable, Literal, TypedDict, TypeVar

import pydantic
from typing_extensions import ParamSpec

import logfire
from logfire import LogfireSpan

from .._internal.config import GLOBAL_CONFIG, PydanticPlugin
from .._internal.utils import get_version

if TYPE_CHECKING:  # pragma: no cover
    from pydantic import ValidationError
    from pydantic.plugin import SchemaKind, SchemaTypePath
    from pydantic_core import CoreConfig, CoreSchema

METER = GLOBAL_CONFIG._meter_provider.get_meter('logfire.pydantic')  # type: ignore
validation_counter = METER.create_counter('pydantic.validations')


class PluginSettings(TypedDict, total=False):
    """A typed dict for the Pydantic plugin settings.

    This is how you can use the [`PluginSettings`][logfire.integrations.pydantic.PluginSettings]
    with a Pydantic model:

    ```py
    from pydantic import BaseModel

    from logfire.integrations.pydantic import PluginSettings


    class Model(BaseModel, plugin_settings=PluginSettings(logfire={'record': 'all'})):
        a: int
    ```
    """

    logfire: LogfireSettings
    """Settings for the logfire integration."""


class LogfireSettings(TypedDict, total=False):
    """Settings for the logfire integration."""

    trace_sample_rate: float
    """The sample rate to use for tracing."""
    tags: list[str]
    """Tags to add to the spans."""
    record: Literal['all', 'failure', 'metrics']
    """What to record.

    The following values are supported:

    * `all`: Record all validation events.
    * `failure`: Record only validation failures.
    * `metrics`: Record only validation metrics.
    """


class _ValidateWrapper:
    """Decorator factory for one schema validator method."""

    __slots__ = (
        'validation_method',
        'schema_name',
        '_record',
        '_logfire',
    )

    def __init__(
        self,
        validation_method: Literal['validate_python', 'validate_json', 'validate_strings'],
        schema: CoreSchema,
        _config: CoreConfig | None,
        _plugin_settings: PluginSettings | dict[str, Any],
        schema_type_path: SchemaTypePath,
        record: Literal['all', 'failure', 'metrics'],
    ) -> None:
        self.validation_method = validation_method

        # We accept the schema, config, and plugin_settings in the init since these are the things
        # that are currently exposed by the plugin to potentially configure the validation handlers.
        self.schema_name = get_schema_name(schema)
        self._record = record

        self._logfire = logfire.DEFAULT_LOGFIRE_INSTANCE
        # trace_sample_rate = _plugin_settings.get('logfire', {}).get('trace_sample_rate')
        # if trace_sample_rate:
        #     self._logfire = self._logfire.with_trace_sample_rate(float(trace_sample_rate))

        tags = _plugin_settings.get('logfire', {}).get('tags')
        if tags:
            if isinstance(tags, str):
                tags = [tags]
            self._logfire = self._logfire.with_tags(*tags)

    def __call__(self, validator: Any) -> Any:
        """Decorator which wraps a schema validator method with instrumentation."""
        from pydantic import ValidationError

        # This branching is a bit over-optimised, it can be relaxed as more conditions are added.
        if self._record == 'all':

            @functools.wraps(validator)
            def wrapped_validator(input_data: Any, *args: Any, **kwargs: Any) -> Any:
                if not GLOBAL_CONFIG._initialized:  # type: ignore
                    # These wrappers should be created when the model is defined if the plugin is activated
                    # by env vars even if logfire.configure() hasn't been called yet,
                    # but we don't want to actually record anything until logfire.configure() has been called.
                    # For example it would be annoying if the user didn't want to send data to logfire
                    # but validation ran into an error about not being authenticated.
                    return validator(input_data, *args, **kwargs)

                # If we get a validation error, we want to let it bubble through.
                # If we used `with span:` this would set the log level to 'error' and export it,
                # but we want the log level to be 'warn', so we end the span manually.
                span = self._on_enter(input_data)
                try:
                    result = validator(input_data, *args, **kwargs)
                except ValidationError as error:
                    self._on_error_span(span, error)
                    self._count_validation(success=False)
                    raise
                except Exception as exception:
                    self._on_exception_span(span, exception)
                    self._count_validation(success=False)
                    raise
                else:
                    self._on_success(span, result)
                    self._count_validation(success=True)
                    return result

        elif self._record == 'failure':
            # Don't open a span at the beginning, only log errors/exceptions.

            @functools.wraps(validator)
            def wrapped_validator(input_data: Any, *args: Any, **kwargs: Any) -> Any:
                if not GLOBAL_CONFIG._initialized:  # type: ignore
                    # Only start recording after logfire has been configured.
                    return validator(input_data, *args, **kwargs)

                try:
                    result = validator(input_data, *args, **kwargs)
                except ValidationError as error:
                    self._count_validation(success=False)
                    self._on_error_log(error)
                    raise
                except Exception as exception:
                    self._count_validation(success=False)
                    self._on_exception_log(exception)
                    raise
                else:
                    self._count_validation(success=True)
                    return result
        else:
            assert self._record == 'metrics'

            @functools.wraps(validator)
            def wrapped_validator(input_data: Any, *args: Any, **kwargs: Any) -> Any:
                if not GLOBAL_CONFIG._initialized:  # type: ignore
                    # Only start recording after logfire has been configured.
                    return validator(input_data, *args, **kwargs)

                try:
                    result = validator(input_data, *args, **kwargs)
                except Exception:
                    self._count_validation(success=False)
                    raise
                else:
                    self._count_validation(success=True)
                    return result

        return wrapped_validator

    def _on_enter(self, input_data: Any):
        return self._logfire.span(
            'Pydantic {schema_name} {validation_method}',
            schema_name=self.schema_name,
            validation_method=self.validation_method,
            input_data=input_data,
            _level='info',
            _span_name=f'pydantic.{self.validation_method}',
        ).__enter__()

    def _on_success(self, span: LogfireSpan, result: Any):
        self._set_span_attributes(
            span,
            success=True,
            status='succeeded',
            result=result,
        )
        span.__exit__(None, None, None)

    def _on_error_log(self, error: ValidationError):
        self._logfire.log(
            level='warn',
            msg_template='Validation on {schema_name} failed',
            attributes={
                'schema_name': self.schema_name,
                'error_count': error.error_count(),
                'errors': error.errors(include_url=False),
            },
        )

    def _on_error_span(self, span: LogfireSpan, error: ValidationError):
        self._set_span_attributes(
            span,
            success=False,
            status='failed',
            error_count=error.error_count(),
            errors=error.errors(include_url=False),
        )
        span.set_level('warn')
        span.__exit__(None, None, None)

    def _on_exception_log(self, exception: Exception):
        self._logfire.log(
            level='error',
            msg_template='Validation on {schema_name} raised {exception_type}',
            attributes={
                'schema_name': self.schema_name,
                'exception_type': type(exception).__name__,
            },
            exc_info=exception,
        )

    def _on_exception_span(self, span: LogfireSpan, exception: Exception):
        self._set_span_attributes(
            span,
            success=False,
            status=f'raised {type(exception).__name__}',
        )
        span.__exit__(type(exception), exception, exception.__traceback__)

    def _set_span_attributes(self, span: LogfireSpan, *, status: str, success: bool, **attributes: Any) -> None:
        if span.is_recording():
            span.set_attributes({'success': success, **attributes})
            span.message += ' ' + status

    def _count_validation(self, *, success: bool) -> None:
        validation_counter.add(
            1, {'success': success, 'schema_name': self.schema_name, 'validation_method': self.validation_method}
        )


def get_schema_name(schema: CoreSchema) -> str:
    """Find the best name to use for a schema.

    The follow rules are used:
    * If the schema represents a model or dataclass, use the name of the class.
    * If the root schema is a wrap/before/after validator, look at its `schema` property.
    * Otherwise use the schema's `type` property.

    Args:
        schema: The schema to get the name for.

    Returns:
        The name of the schema.
    """
    if schema['type'] in {'model', 'dataclass'}:
        return schema['cls'].__name__  # type: ignore
    elif schema['type'] in {'function-after', 'function-before', 'function-wrap'}:
        return get_schema_name(schema['schema'])  # type: ignore
    elif schema['type'] == 'definitions':
        inner_schema = schema['schema']
        if inner_schema['type'] == 'definition-ref':
            schema_ref: str = inner_schema['schema_ref']  # type: ignore
            [schema_definition] = [
                definition
                for definition in schema['definitions']
                if definition['ref'] == schema_ref  # type: ignore
            ]
            return get_schema_name(schema_definition)
        else:
            return get_schema_name(inner_schema)
    else:
        return schema['type']


@dataclass
class LogfirePydanticPlugin:
    """Implements a new API for pydantic plugins.

    Patches Pydantic to accept this new API shape.

    Set the `LOGFIRE_PYDANTIC_RECORD` environment variable to `"off"` to disable the plugin, or
    `PYDANTIC_DISABLE_PLUGINS` to `true` to disable all Pydantic plugins.
    """

    if get_version(pydantic.__version__) < get_version('2.5.0') or os.environ.get('LOGFIRE_PYDANTIC_RECORD') == 'off':

        def new_schema_validator(  # type: ignore[reportRedeclaration]
            self, *_: Any, **__: Any
        ) -> tuple[_ValidateWrapper, ...] | tuple[None, ...]:
            """Backwards compatibility for Pydantic < 2.5.0.

            This method is called every time a new `SchemaValidator` is created, and is a NO-OP for Pydantic < 2.5.0.
            """
            return None, None, None
    else:

        def new_schema_validator(
            self,
            schema: CoreSchema,
            schema_type: Any,
            schema_type_path: SchemaTypePath,
            schema_kind: SchemaKind,
            config: CoreConfig | None,
            plugin_settings: dict[str, Any],
        ) -> tuple[_ValidateWrapper, ...] | tuple[None, ...]:
            """This method is called every time a new `SchemaValidator` is created.

            Args:
                schema: The schema to validate against.
                schema_type: The original type which the schema was created from, e.g. the model class.
                schema_type_path: Path defining where `schema_type` was defined, or where `TypeAdapter` was called.
                schema_kind: The kind of schema to validate against.
                config: The config to use for validation.
                plugin_settings: The plugin settings.

            Returns:
                A tuple of decorator factories for each of the three validation methods -
                    `validate_python`, `validate_json`, `validate_strings` or a tuple of
                    three `None` if recording is `off`.
            """
            # Patch a bug that occurs even if the plugin is disabled.
            _patch_PluggableSchemaValidator()

            logfire_settings = plugin_settings.get('logfire')
            if logfire_settings and 'record' in logfire_settings:
                record = logfire_settings['record']
            else:
                record = get_pydantic_plugin_config().record

            if record == 'off':
                return None, None, None

            if _include_model(schema_type_path):
                _patch_build_wrapper()
                return (
                    _ValidateWrapper('validate_python', schema, config, plugin_settings, schema_type_path, record),
                    _ValidateWrapper('validate_json', schema, config, plugin_settings, schema_type_path, record),
                    _ValidateWrapper('validate_strings', schema, config, plugin_settings, schema_type_path, record),
                )

            return None, None, None


plugin = LogfirePydanticPlugin()

# set of modules to ignore completely
IGNORED_MODULES: tuple[str, ...] = 'fastapi', 'logfire_backend', 'fastui'
IGNORED_MODULE_PREFIXES: tuple[str, ...] = tuple(f'{module}.' for module in IGNORED_MODULES)

_pydantic_plugin_config_value: PydanticPlugin | None = None


def get_pydantic_plugin_config() -> PydanticPlugin:
    """Get the Pydantic plugin config."""
    if _pydantic_plugin_config_value is not None:
        return _pydantic_plugin_config_value
    else:
        return GLOBAL_CONFIG.param_manager.pydantic_plugin


def set_pydantic_plugin_config(plugin_config: PydanticPlugin | None) -> None:
    """Set the pydantic plugin config."""
    global _pydantic_plugin_config_value
    _pydantic_plugin_config_value = plugin_config


def _include_model(schema_type_path: SchemaTypePath) -> bool:
    """Check whether a model should be instrumented."""
    config = get_pydantic_plugin_config()
    include = config.include
    exclude = config.exclude

    # check if the model is in ignored model
    module = schema_type_path.module
    if module.startswith(IGNORED_MODULE_PREFIXES) or module in IGNORED_MODULES:  # pragma: no cover
        return False

    # check if the model is in exclude models
    if exclude and any(re.search(f'{pattern}$', f'{module}::{schema_type_path.name}') for pattern in exclude):
        return False

    # check if the model is in include models
    if include:
        return any(re.search(f'{pattern}$', f'{module}::{schema_type_path.name}') for pattern in include)
    return True


@lru_cache  # only patch once
def _patch_build_wrapper():
    """The old pydantic plugin API required managing state between event handler methods.

    This was messy, especially in a way that handles concurrency and nested validation:
    see https://pydantic.slack.com/archives/C05AF4A4WRM/p1710503400257589.
    The 'new API' simply requires decorating the validation methods, returning a new wrapper function.
    At the time of writing, this API doesn't actually exist yet in pydantic.
    We patch it here so that this will also work for older versions of pydantic without needing to support both APIs.
    """
    from pydantic.plugin import _schema_validator

    _schema_validator.build_wrapper = _build_wrapper


@lru_cache  # only patch once
def _patch_PluggableSchemaValidator():
    """Patch a 'bug' in PluggableSchemaValidator.

    Getting an attribute before proper initializing (e.g. when using cloudpickle)
    leads to infinite recursion trying to get _schema_validator.
    """
    from pydantic.plugin._schema_validator import PluggableSchemaValidator

    if (  # pragma: no branch
        inspect.getsource(PluggableSchemaValidator.__getattr__).strip()
        # Check that we're replacing the code that's known to be buggy.
        == """
    def __getattr__(self, name: str) -> Any:
        return getattr(self._schema_validator, name)
    """.strip()
    ):

        def __getattr__(self: Any, name: str) -> Any:
            # Add these two lines to the above.
            # The exact error or return value is not important, as long as the end result
            # is an AttributeError rather than infinite recursion.
            if name == '_schema_validator':
                raise AttributeError(name)

            return getattr(self._schema_validator, name)

        PluggableSchemaValidator.__getattr__ = __getattr__


P = ParamSpec('P')
R = TypeVar('R')


def _build_wrapper(func: Callable[P, R], event_handlers: list[Any]) -> Callable[P, R]:
    for handler in event_handlers:
        # Check for the old event handler methods (on_enter etc.) to continue supporting other plugins.
        # Note that this patching also changes the order in which the event handlers are called
        # in the case of multiple plugins, but probably in a good way.
        old_wrapped = _wrap_with_old_handler(func, handler)
        if old_wrapped:
            func = old_wrapped
        elif callable(handler):  # no event handler methods found
            # Use the new API, especially _ValidateWrapper.__call__
            func = handler(func)  # type: ignore

    return func


def _noop(*_: Any, **__: Any):
    return None


def _wrap_with_old_handler(func: Callable[P, R], handler: Any) -> Callable[P, R] | None:
    from pydantic import ValidationError

    on_enter = _get_handler_method(handler, 'on_enter')
    on_success = _get_handler_method(handler, 'on_success')
    on_error = _get_handler_method(handler, 'on_error')
    on_exception = _get_handler_method(handler, 'on_exception')

    if on_enter is on_success is on_error is on_exception is _noop:
        # No old event handlers were found, return None to indicate that the new API should be used instead.
        return None

    @functools.wraps(func)
    def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        on_enter(*args, **kwargs)
        try:
            result = func(*args, **kwargs)
        except ValidationError as error:
            on_error(error)
            raise
        except Exception as exception:
            on_exception(exception)
            raise
        else:
            on_success(result)
            return result

    return wrapper


def _get_handler_method(handler: Any, method_name: str) -> Callable[..., None]:
    handler = getattr(handler, method_name, None)
    if handler is None:
        return _noop
    elif handler.__module__ == 'pydantic.plugin':
        # this is the original handler, from the protocol due to runtime inheritance.
        return _noop
    else:
        return handler
