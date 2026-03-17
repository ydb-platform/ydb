from . import config as config
from .constants import LevelName as LevelName
from .exporters.console import ConsoleColorsValues as ConsoleColorsValues
from .utils import read_toml_file as read_toml_file
from _typeshed import Incomplete
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from logfire.exceptions import LogfireConfigError as LogfireConfigError
from pathlib import Path
from typing import Any, Callable, TypeVar

T = TypeVar('T')
slots_true: Incomplete
PydanticPluginRecordValues: Incomplete

@dataclass(**slots_true)
class ConfigParam:
    """A parameter that can be configured for a Logfire instance."""
    env_vars: list[str]
    allow_file_config: bool = ...
    default: Any = ...
    tp: Any = ...

@dataclass
class _DefaultCallback:
    """A default value that is computed at runtime.

    A good example is when we want to check if we are running under pytest and set a default value based on that.
    """
    callback: Callable[[], Any]

SEND_TO_LOGFIRE: Incomplete
MIN_LEVEL: Incomplete
TOKEN: Incomplete
API_KEY: Incomplete
SERVICE_NAME: Incomplete
SERVICE_VERSION: Incomplete
ENVIRONMENT: Incomplete
CREDENTIALS_DIR: Incomplete
CONSOLE: Incomplete
CONSOLE_COLORS: Incomplete
CONSOLE_SPAN_STYLE: Incomplete
CONSOLE_INCLUDE_TIMESTAMP: Incomplete
CONSOLE_INCLUDE_TAGS: Incomplete
CONSOLE_VERBOSE: Incomplete
CONSOLE_MIN_LOG_LEVEL: Incomplete
CONSOLE_SHOW_PROJECT_LINK: Incomplete
PYDANTIC_PLUGIN_RECORD: Incomplete
PYDANTIC_PLUGIN_INCLUDE: Incomplete
PYDANTIC_PLUGIN_EXCLUDE: Incomplete
TRACE_SAMPLE_RATE: Incomplete
INSPECT_ARGUMENTS: Incomplete
IGNORE_NO_CONFIG: Incomplete
BASE_URL: Incomplete
DISTRIBUTED_TRACING: Incomplete
HTTPX_CAPTURE_ALL: Incomplete
AIOHTTP_CLIENT_CAPTURE_ALL: Incomplete
CONFIG_PARAMS: Incomplete

@dataclass
class ParamManager:
    """Manage parameters for a Logfire instance."""
    config_from_file: dict[str, Any]
    @classmethod
    def create(cls, config_dir: Path | None = None) -> ParamManager: ...
    def load_param(self, name: str, runtime: Any = None) -> Any:
        """Load a parameter given its name.

        The parameter is loaded in the following order:
        1. From the runtime argument, if provided.
        2. From the environment variables.
        3. From the config file, if allowed.

        If none of the above is found, the default value is returned.

        Args:
            name: Name of the parameter.
            runtime: Value provided at runtime.

        Returns:
            The value of the parameter.
        """
    @cached_property
    def pydantic_plugin(self): ...

def extract_list_of_str(value: str | Sequence[str]) -> list[str] | None:
    """Extract a list of strings from a string, sequence, or None.

    If value is a comma-separated string, split it into a list of non-empty trimmed strings.
    If value is a sequence, convert to list.
    If value is None, return None.
    """
def normalize_token(value: str | Sequence[str] | None) -> str | list[str] | None:
    """Normalize a token value to str (single token), list[str] (multiple tokens), or None.

    If there's exactly one token, return it as a string.
    If there are multiple tokens, return them as a list.
    If there are no tokens, return None.
    """
