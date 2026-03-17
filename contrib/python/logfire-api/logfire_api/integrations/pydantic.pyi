from .._internal.config import GLOBAL_CONFIG as GLOBAL_CONFIG, PydanticPlugin as PydanticPlugin
from .._internal.utils import get_version as get_version
from _typeshed import Incomplete
from dataclasses import dataclass
from logfire import LogfireSpan as LogfireSpan
from pydantic.plugin import SchemaKind, SchemaTypePath
from pydantic_core import CoreConfig, CoreSchema
from typing import Any, Literal, TypeVar, TypedDict
from typing_extensions import ParamSpec

METER: Incomplete
validation_counter: Incomplete

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

class LogfireSettings(TypedDict, total=False):
    """Settings for the logfire integration."""
    trace_sample_rate: float
    tags: list[str]
    record: Literal['all', 'failure', 'metrics']

class _ValidateWrapper:
    """Decorator factory for one schema validator method."""
    validation_method: Incomplete
    schema_name: Incomplete
    def __init__(self, validation_method: Literal['validate_python', 'validate_json', 'validate_strings'], schema: CoreSchema, _config: CoreConfig | None, _plugin_settings: PluginSettings | dict[str, Any], schema_type_path: SchemaTypePath, record: Literal['all', 'failure', 'metrics']) -> None: ...
    def __call__(self, validator: Any) -> Any:
        """Decorator which wraps a schema validator method with instrumentation."""

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

@dataclass
class LogfirePydanticPlugin:
    '''Implements a new API for pydantic plugins.

    Patches Pydantic to accept this new API shape.

    Set the `LOGFIRE_PYDANTIC_RECORD` environment variable to `"off"` to disable the plugin, or
    `PYDANTIC_DISABLE_PLUGINS` to `true` to disable all Pydantic plugins.
    '''
    def new_schema_validator(self, *_: Any, **__: Any) -> tuple[_ValidateWrapper, ...] | tuple[None, ...]:
        """Backwards compatibility for Pydantic < 2.5.0.

            This method is called every time a new `SchemaValidator` is created, and is a NO-OP for Pydantic < 2.5.0.
            """
    def new_schema_validator(self, schema: CoreSchema, schema_type: Any, schema_type_path: SchemaTypePath, schema_kind: SchemaKind, config: CoreConfig | None, plugin_settings: dict[str, Any]) -> tuple[_ValidateWrapper, ...] | tuple[None, ...]:
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

plugin: Incomplete
IGNORED_MODULES: tuple[str, ...]
IGNORED_MODULE_PREFIXES: tuple[str, ...]

def get_pydantic_plugin_config() -> PydanticPlugin:
    """Get the Pydantic plugin config."""
def set_pydantic_plugin_config(plugin_config: PydanticPlugin | None) -> None:
    """Set the pydantic plugin config."""
P = ParamSpec('P')
R = TypeVar('R')
