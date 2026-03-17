import enum
import logging
from dataclasses import dataclass
from typing import Dict, Optional, Union

from pyhanko.config.errors import ConfigurationError
from pyhanko.pdf_utils.misc import get_and_apply

__all__ = ['StdLogOutput', 'LogConfig', 'parse_logging_config']


class StdLogOutput(enum.Enum):
    STDERR = enum.auto()
    STDOUT = enum.auto()


@dataclass(frozen=True)
class LogConfig:
    level: Union[int, str]
    """
    Logging level, should be one of the levels defined in the logging module.
    """

    output: Union[StdLogOutput, str]
    """
    Name of the output file, or a standard one.
    """

    @staticmethod
    def parse_output_spec(spec) -> Union[StdLogOutput, str]:
        if not isinstance(spec, str):
            raise ConfigurationError(
                "Log output must be specified as a string."
            )
        spec_l = spec.lower()
        if spec_l == 'stderr':
            return StdLogOutput.STDERR
        elif spec_l == 'stdout':
            return StdLogOutput.STDOUT
        else:
            return spec


DEFAULT_ROOT_LOGGER_LEVEL = logging.INFO


def _retrieve_log_level(settings_dict, key, default=None) -> Union[int, str]:
    try:
        level_spec = settings_dict[key]
    except KeyError:
        if default is not None:
            return default
        raise ConfigurationError(
            f"Logging config for '{key}' does not define a log level."
        )
    if not isinstance(level_spec, (int, str)):
        raise ConfigurationError(
            f"Log levels must be int or str, not {type(level_spec)}"
        )
    return level_spec


def parse_logging_config(log_config_spec) -> Dict[Optional[str], LogConfig]:
    if not isinstance(log_config_spec, dict):
        raise ConfigurationError('logging config should be a dictionary')

    root_logger_level = _retrieve_log_level(
        log_config_spec, 'root-level', default=DEFAULT_ROOT_LOGGER_LEVEL
    )

    root_logger_output = get_and_apply(
        log_config_spec,
        'root-output',
        LogConfig.parse_output_spec,
        default=StdLogOutput.STDERR,
    )

    log_config: Dict[Optional[str], LogConfig] = {
        None: LogConfig(root_logger_level, root_logger_output)
    }

    logging_by_module = log_config_spec.get('by-module', {})
    if not isinstance(logging_by_module, dict):
        raise ConfigurationError('logging.by-module should be a dict')

    for module, module_logging_settings in logging_by_module.items():
        if not isinstance(module, str):
            raise ConfigurationError(
                "Keys in logging.by-module should be strings"
            )
        level_spec = _retrieve_log_level(module_logging_settings, 'level')
        output_spec = get_and_apply(
            module_logging_settings,
            'output',
            LogConfig.parse_output_spec,
            default=StdLogOutput.STDERR,
        )
        log_config[module] = LogConfig(level=level_spec, output=output_spec)

    return log_config
