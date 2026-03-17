"""
argclass - Declarative argument parser using classes.

A wrapper around the standard `argparse` module that allows you to describe
argument parsers declaratively using classes with type annotations.
"""

from enum import EnumMeta

from .actions import (
    ConfigAction,
    INIConfigAction,
    JSONConfigAction,
    TOMLConfigAction,
)
from .defaults import (
    AbstractDefaultsParser,
    INIDefaultsParser,
    JSONDefaultsParser,
    TOMLDefaultsParser,
    UnexpectedConfigValue,
    ValueKind,
)
from .exceptions import (
    ArgclassError,
    ArgumentDefinitionError,
    ComplexTypeError,
    ConfigurationError,
    EnumValueError,
    TypeConversionError,
)
from .factory import (
    Argument,
    ArgumentSequence,
    ArgumentSingle,
    Config,
    EnumArgument,
    LogLevel,
    Secret,
)
from .parser import Base, Destination, Group, Meta, Parser
from .secret import SecretString
from .store import (
    AbstractGroup,
    AbstractParser,
    ArgumentBase,
    ConfigArgument,
    INIConfig,
    JSONConfig,
    TOMLConfig,
    Store,
    StoreMeta,
    TypedArgument,
)
from .types import (
    Actions,
    ConverterType,
    LogLevelEnum,
    MetavarType,
    Nargs,
    NargsType,
)
from .utils import parse_bool, read_ini_configs

# Alias for backward compatibility
read_configs = read_ini_configs

# For backward compatibility
EnumType = EnumMeta

__all__ = [
    # Exceptions
    "ArgclassError",
    "ArgumentDefinitionError",
    "ComplexTypeError",
    "ConfigurationError",
    "EnumValueError",
    "TypeConversionError",
    "UnexpectedConfigValue",
    # Types and enums
    "Actions",
    "Nargs",
    "LogLevelEnum",
    "ConverterType",
    "MetavarType",
    "NargsType",
    # Classes
    "SecretString",
    "Store",
    "StoreMeta",
    "TypedArgument",
    "ArgumentBase",
    "ConfigArgument",
    "INIConfig",
    "JSONConfig",
    "TOMLConfig",
    "AbstractGroup",
    "AbstractParser",
    "Group",
    "Parser",
    "Base",
    "Meta",
    "Destination",
    # Actions
    "ConfigAction",
    "INIConfigAction",
    "JSONConfigAction",
    "TOMLConfigAction",
    # Defaults parsers (for config_files parameter)
    "AbstractDefaultsParser",
    "INIDefaultsParser",
    "JSONDefaultsParser",
    "TOMLDefaultsParser",
    "ValueKind",
    "UnexpectedConfigValue",
    # Factory functions
    "Argument",
    "ArgumentSingle",
    "ArgumentSequence",
    "EnumArgument",
    "Secret",
    "Config",
    "LogLevel",
    # Utilities
    "read_ini_configs",
    "parse_bool",
]
