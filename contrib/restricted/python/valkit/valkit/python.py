import logging

from typing import Any

from . import raise_validator
from . import not_none_string
from . import check_re_match
from . import add_validator_magic


# =====
@add_validator_magic
def valid_object_name(
    arg: Any,
    strip: bool=False,
) -> str:

    return check_re_match(arg, "Python object name", r"^[a-zA-Z_][a-zA-Z0-9_]*$", strip)


@add_validator_magic
def valid_object_path(
    arg: Any,
    strip: bool=False,
) -> str:

    pattern = r"^([a-zA-Z_][a-zA-Z0-9_]*\.)*[a-zA-Z_][a-zA-Z0-9_]*$"
    return check_re_match(arg, "Python object path", pattern, strip)


@add_validator_magic
def valid_logging_level(
    arg: Any,
    up: bool=True,
    strip: bool=False,
) -> int:

    name = "logging level"
    try:
        arg = int(arg)
        if arg not in logging._levelToName:  # pylint: disable=protected-access
            raise_validator(arg, name)
        return arg
    except ValueError:
        arg = not_none_string(arg, name, strip)
        try:
            return logging._nameToLevel[arg.upper() if up else arg]  # pylint: disable=protected-access
        except KeyError:
            raise_validator(arg, name)
