from __future__ import annotations as _annotations

import os
import sys
from collections.abc import Sequence
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Callable, Literal, TypeVar, Union

from opentelemetry.sdk.environment_variables import OTEL_SERVICE_NAME
from typing_extensions import get_args, get_origin

from logfire.exceptions import LogfireConfigError

from . import config
from .constants import LevelName
from .exporters.console import ConsoleColorsValues
from .utils import read_toml_file

T = TypeVar('T')

slots_true = {'slots': True} if sys.version_info >= (3, 10) else {}

PydanticPluginRecordValues = Literal['off', 'all', 'failure', 'metrics']
"""Possible values for the `pydantic_plugin_record` parameter."""


@dataclass(**slots_true)
class ConfigParam:
    """A parameter that can be configured for a Logfire instance."""

    env_vars: list[str]
    """Environment variables to check for the parameter."""
    allow_file_config: bool = False
    """Whether the parameter can be set in the config file."""
    default: Any = None
    """Default value if no other value is found."""
    tp: Any = str
    """Type of the parameter."""


@dataclass
class _DefaultCallback:
    """A default value that is computed at runtime.

    A good example is when we want to check if we are running under pytest and set a default value based on that.
    """

    callback: Callable[[], Any]


_send_to_logfire_default = _DefaultCallback(lambda: 'PYTEST_VERSION' not in os.environ)
"""When running under pytest, don't send spans to Logfire by default."""

# fmt: off
SEND_TO_LOGFIRE = ConfigParam(env_vars=['LOGFIRE_SEND_TO_LOGFIRE'], allow_file_config=True, default=_send_to_logfire_default, tp=Union[bool, Literal['if-token-present']])
"""Whether to send spans to Logfire."""
MIN_LEVEL = ConfigParam(env_vars=['LOGFIRE_MIN_LEVEL'], allow_file_config=True, default=None, tp=LevelName)
"""Minimum log level for logs and spans to be created. By default, all logs and spans are created."""
TOKEN = ConfigParam(env_vars=['LOGFIRE_TOKEN'], tp=list[str])
"""Token for sending application telemetry data to Logfire, also known as a "write token". Can be a comma-separated list for multi-project export."""
API_KEY = ConfigParam(env_vars=['LOGFIRE_API_KEY'])
"""API key for Logfire API access (used for managed variables and other public APIs)."""
SERVICE_NAME = ConfigParam(env_vars=['LOGFIRE_SERVICE_NAME', OTEL_SERVICE_NAME], allow_file_config=True, default='')
"""Name of the service emitting spans. For further details, please refer to the [Service section](https://opentelemetry.io/docs/specs/semconv/resource/#service)."""
SERVICE_VERSION = ConfigParam(env_vars=['LOGFIRE_SERVICE_VERSION', 'OTEL_SERVICE_VERSION'], allow_file_config=True)
"""Version number of the service emitting spans. For further details, please refer to the [Service section](https://opentelemetry.io/docs/specs/semconv/resource/#service)."""
ENVIRONMENT = ConfigParam(env_vars=['LOGFIRE_ENVIRONMENT'], allow_file_config=True)
"""Environment in which the service is running. For further details, please refer to the [Deployment section](https://opentelemetry.io/docs/specs/semconv/resource/deployment-environment/)."""
CREDENTIALS_DIR = ConfigParam(env_vars=['LOGFIRE_CREDENTIALS_DIR'], allow_file_config=True, default='.logfire', tp=Path)
"""The directory where to store the configuration file."""
CONSOLE = ConfigParam(env_vars=['LOGFIRE_CONSOLE'], allow_file_config=True, default=True, tp=bool)
"""Whether to enable/disable the console exporter."""
CONSOLE_COLORS = ConfigParam(env_vars=['LOGFIRE_CONSOLE_COLORS'], allow_file_config=True, default='auto', tp=ConsoleColorsValues)
"""Whether to use colors in the console."""
CONSOLE_SPAN_STYLE = ConfigParam(env_vars=['LOGFIRE_CONSOLE_SPAN_STYLE'], allow_file_config=True, default='show-parents', tp=Literal['simple', 'indented', 'show-parents'])
"""How spans are shown in the console.

* `'simple'`: Spans are shown as a flat list, not indented.
* `'indented'`: Spans are shown as a tree, indented based on how many parents they have.
* `'show-parents'`: Spans are shown intended, when spans are interleaved parent spans are printed again to
  give the best context."""
CONSOLE_INCLUDE_TIMESTAMP = ConfigParam(env_vars=['LOGFIRE_CONSOLE_INCLUDE_TIMESTAMP'], allow_file_config=True, default=True, tp=bool)
"""Whether to include the timestamp in the console."""
CONSOLE_INCLUDE_TAGS = ConfigParam(env_vars=['LOGFIRE_CONSOLE_INCLUDE_TAGS'], allow_file_config=True, default=True, tp=bool)
"""Whether to include tags in the console."""
CONSOLE_VERBOSE = ConfigParam(env_vars=['LOGFIRE_CONSOLE_VERBOSE'], allow_file_config=True, default=False, tp=bool)
"""Whether to log in verbose mode in the console."""
CONSOLE_MIN_LOG_LEVEL = ConfigParam(env_vars=['LOGFIRE_CONSOLE_MIN_LOG_LEVEL'], allow_file_config=True, default='info', tp=LevelName)
"""Minimum log level to show in the console."""
CONSOLE_SHOW_PROJECT_LINK = ConfigParam(env_vars=['LOGFIRE_CONSOLE_SHOW_PROJECT_LINK', 'LOGFIRE_SHOW_SUMMARY'], allow_file_config=True, default=True, tp=bool)
"""Whether to enable/disable the console exporter."""
PYDANTIC_PLUGIN_RECORD = ConfigParam(env_vars=['LOGFIRE_PYDANTIC_PLUGIN_RECORD'], allow_file_config=True, default='off', tp=PydanticPluginRecordValues)
"""Whether instrument Pydantic validation.."""
PYDANTIC_PLUGIN_INCLUDE = ConfigParam(env_vars=['LOGFIRE_PYDANTIC_PLUGIN_INCLUDE'], allow_file_config=True, default=set(), tp=set[str])
"""Set of items that should be included in Logfire Pydantic plugin instrumentation."""
PYDANTIC_PLUGIN_EXCLUDE = ConfigParam(env_vars=['LOGFIRE_PYDANTIC_PLUGIN_EXCLUDE'], allow_file_config=True, default=set(), tp=set[str])
"""Set of items that should be excluded from Logfire Pydantic plugin instrumentation."""
TRACE_SAMPLE_RATE = ConfigParam(env_vars=['LOGFIRE_TRACE_SAMPLE_RATE', 'OTEL_TRACES_SAMPLER_ARG'], allow_file_config=True, default=1.0, tp=float)
"""Head sampling rate for traces."""
INSPECT_ARGUMENTS = ConfigParam(env_vars=['LOGFIRE_INSPECT_ARGUMENTS'], allow_file_config=True, default=sys.version_info[:2] >= (3, 11), tp=bool)
"""Whether to enable the f-string magic feature. On by default for Python 3.11 and above."""
IGNORE_NO_CONFIG = ConfigParam(env_vars=['LOGFIRE_IGNORE_NO_CONFIG'], allow_file_config=True, default=False, tp=bool)
"""Whether to show a warning message if logfire if used without calling logfire.configure()"""
BASE_URL = ConfigParam(env_vars=['LOGFIRE_BASE_URL'], allow_file_config=True, default=None, tp=str)
"""The base URL of the Logfire backend. Primarily for testing purposes."""
DISTRIBUTED_TRACING = ConfigParam(env_vars=['LOGFIRE_DISTRIBUTED_TRACING'], allow_file_config=True, default=None, tp=bool)
"""Whether to extract incoming trace context. By default, will extract but warn about it."""

# Instrumentation packages parameters
HTTPX_CAPTURE_ALL = ConfigParam(env_vars=['LOGFIRE_HTTPX_CAPTURE_ALL'], allow_file_config=True, default=False, tp=bool)
"""Whether to capture all HTTP headers, request and response bodies when using `logfire.instrument_httpx()`"""
AIOHTTP_CLIENT_CAPTURE_ALL = ConfigParam(env_vars=['LOGFIRE_AIOHTTP_CLIENT_CAPTURE_ALL'], allow_file_config=True, default=False, tp=bool)
"""Whether to capture all HTTP headers, request and response bodies when using `logfire.instrument_aiohttp_client()`"""
# fmt: on

CONFIG_PARAMS = {
    'base_url': BASE_URL,
    'send_to_logfire': SEND_TO_LOGFIRE,
    'min_level': MIN_LEVEL,
    'token': TOKEN,
    'api_key': API_KEY,
    'service_name': SERVICE_NAME,
    'service_version': SERVICE_VERSION,
    'environment': ENVIRONMENT,
    'trace_sample_rate': TRACE_SAMPLE_RATE,
    'data_dir': CREDENTIALS_DIR,
    'console': CONSOLE,
    'console_colors': CONSOLE_COLORS,
    'console_span_style': CONSOLE_SPAN_STYLE,
    'console_include_timestamp': CONSOLE_INCLUDE_TIMESTAMP,
    'console_include_tags': CONSOLE_INCLUDE_TAGS,
    'console_verbose': CONSOLE_VERBOSE,
    'console_min_log_level': CONSOLE_MIN_LOG_LEVEL,
    'console_show_project_link': CONSOLE_SHOW_PROJECT_LINK,
    'pydantic_plugin_record': PYDANTIC_PLUGIN_RECORD,
    'pydantic_plugin_include': PYDANTIC_PLUGIN_INCLUDE,
    'pydantic_plugin_exclude': PYDANTIC_PLUGIN_EXCLUDE,
    'inspect_arguments': INSPECT_ARGUMENTS,
    'ignore_no_config': IGNORE_NO_CONFIG,
    'distributed_tracing': DISTRIBUTED_TRACING,
    # Instrumentation packages parameters
    'httpx_capture_all': HTTPX_CAPTURE_ALL,
    'aiohttp_client_capture_all': AIOHTTP_CLIENT_CAPTURE_ALL,
}


@dataclass
class ParamManager:
    """Manage parameters for a Logfire instance."""

    config_from_file: dict[str, Any]
    """Config loaded from the config file."""

    @classmethod
    def create(cls, config_dir: Path | None = None) -> ParamManager:
        config_dir = Path(config_dir or os.getenv('LOGFIRE_CONFIG_DIR') or '.')
        config_from_file = _load_config_from_file(config_dir)
        return ParamManager(config_from_file=config_from_file)

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
        if runtime is not None and runtime != '':
            return runtime

        param = CONFIG_PARAMS[name]
        for env_var in param.env_vars:
            value = os.getenv(env_var)
            # `None` (unset) and `''` (empty string) are generally considered the same
            if value:
                return self._cast(value, name, param.tp)

        if param.allow_file_config:
            value = self.config_from_file.get(name)
            if value is not None:
                return self._cast(value, name, param.tp)

        if isinstance(param.default, _DefaultCallback):
            return self._cast(param.default.callback(), name, param.tp)
        return self._cast(param.default, name, param.tp)

    @cached_property
    def pydantic_plugin(self):
        return config.PydanticPlugin(
            record=self.load_param('pydantic_plugin_record'),
            include=self.load_param('pydantic_plugin_include'),
            exclude=self.load_param('pydantic_plugin_exclude'),
        )

    def _cast(self, value: Any, name: str, tp: type[T]) -> T | None:
        if tp is str:
            return value
        if get_origin(tp) is Literal:
            return _check_literal(value, name, tp)
        if get_origin(tp) is Union:
            for arg in get_args(tp):
                try:
                    return self._cast(value, name, arg)
                except LogfireConfigError:
                    pass
            raise LogfireConfigError(f'Expected {name} to be an instance of one of {get_args(tp)}, got {value!r}')
        if tp is bool:
            return _check_bool(value, name)  # type: ignore
        if tp is float:
            return float(value)  # type: ignore
        if tp is Path:
            return Path(value)  # type: ignore
        if get_origin(tp) is set and get_args(tp) == (str,):  # pragma: no branch
            return _extract_set_of_str(value)  # type: ignore
        if get_origin(tp) is list and get_args(tp) == (str,):
            return extract_list_of_str(value)  # type: ignore
        raise RuntimeError(f'Unexpected type {tp}')  # pragma: no cover


def _check_literal(value: Any, name: str, tp: type[T]) -> T | None:
    if value is None:  # pragma: no cover
        return None
    literals = get_args(tp)
    if value not in literals:
        raise LogfireConfigError(f'Expected {name} to be one of {literals}, got {value!r}')
    return value


def _check_bool(value: Any, name: str) -> bool | None:
    if value is None:  # pragma: no cover
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, str):  # pragma: no branch
        if value.lower() in ('1', 'true', 't'):
            return True
        if value.lower() in ('0', 'false', 'f'):  # pragma: no branch
            return False
    raise LogfireConfigError(f'Expected {name} to be a boolean, got {value!r}')  # pragma: no cover


def _extract_set_of_str(value: str | set[str]) -> set[str]:
    return set(map(str.strip, value.split(','))) if isinstance(value, str) else value


def extract_list_of_str(value: str | Sequence[str]) -> list[str] | None:
    """Extract a list of strings from a string, sequence, or None.

    If value is a comma-separated string, split it into a list of non-empty trimmed strings.
    If value is a sequence, convert to list.
    If value is None, return None.
    """
    if isinstance(value, str):
        tokens = [t.strip() for t in value.split(',') if t.strip()]
        return tokens if tokens else None
    return [v for v in value if v] if value else None


def normalize_token(value: str | Sequence[str] | None) -> str | list[str] | None:
    """Normalize a token value to str (single token), list[str] (multiple tokens), or None.

    If there's exactly one token, return it as a string.
    If there are multiple tokens, return them as a list.
    If there are no tokens, return None.
    """
    if not value:
        # This covers `''`, `None`, and empty sequences
        return None
    # Now we must have a non-empty string or sequence
    tokens = extract_list_of_str(value)
    if tokens is None or not tokens:
        return None
    if len(tokens) == 1:
        return tokens[0]
    return tokens


def _load_config_from_file(config_dir: Path) -> dict[str, Any]:
    config_file = config_dir / 'pyproject.toml'
    if not config_file.exists():
        return {}
    try:
        data = read_toml_file(config_file)
        return data.get('tool', {}).get('logfire', {})
    except Exception as exc:
        raise LogfireConfigError(f'Invalid config file: {config_file}') from exc
