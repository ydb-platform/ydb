"""Type definitions, enums, and constants for argclass."""

from enum import Enum, IntEnum
from typing import Any, Callable, Tuple, Union, Literal

# Type aliases
ConverterType = Callable[[Any], Any]
NargsType = Union[int, str, "Nargs", Literal["?", "*", "+"]]
MetavarType = Union[str, Tuple[str, ...]]
NoneType = type(None)
UnionClass = Union[None, int].__class__


class Actions(str, Enum):
    """Argparse action types."""

    STORE = "store"
    STORE_CONST = "store_const"
    STORE_TRUE = "store_true"
    STORE_FALSE = "store_false"
    APPEND = "append"
    APPEND_CONST = "append_const"
    COUNT = "count"
    HELP = "help"
    VERSION = "version"

    @classmethod
    def default(cls) -> "Actions":
        return cls.STORE


class Nargs(Enum):
    """Argparse nargs values."""

    ZERO_OR_ONE = "?"
    ZERO_OR_MORE = "*"
    ONE_OR_MORE = "+"


class LogLevelEnum(IntEnum):
    """Standard logging levels."""

    CRITICAL = 50
    FATAL = 50
    ERROR = 40
    WARNING = 30
    WARN = 30
    INFO = 20
    DEBUG = 10
    NOTSET = 0


# Container types for automatic nargs handling
CONTAINER_TYPES: Tuple[type, ...] = (list, set, frozenset, tuple)


# Boolean string values
TEXT_TRUE_VALUES = frozenset(
    (
        "y",
        "yes",
        "true",
        "t",
        "enable",
        "enabled",
        "1",
        "on",
    )
)
