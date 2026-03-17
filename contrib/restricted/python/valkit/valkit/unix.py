from typing import Any

from . import check_re_match
from . import add_validator_magic


# =====
@add_validator_magic
def valid_username(
    arg: Any,
    strip: bool=False,
) -> str:

    return check_re_match(arg, "UNIX username", r"^[a-z_][a-z0-9_-]*$", strip)


@add_validator_magic
def valid_groupname(
    arg: Any,
    strip: bool=False,
) -> str:

    return check_re_match(arg, "UNIX groupname", r"^[a-z_][a-z0-9_-]*$", strip)
