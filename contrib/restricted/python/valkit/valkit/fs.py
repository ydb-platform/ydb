import os

from typing import Any

from . import raise_validator
from . import not_none_string
from . import check_re_match
from . import add_validator_magic


# =====
@add_validator_magic
def valid_path(
    arg: Any,
    expanduser: bool=False,
    abspath: bool=False,
    f_ok: bool=False,
    strip: bool=False,
) -> str:

    name = ("accessible path" if f_ok else "path")
    if len(str(arg).strip()) == 0:
        arg = None
    arg = not_none_string(arg, name, strip)
    if expanduser:
        arg = os.path.expanduser(arg)
    if abspath:
        arg = os.path.abspath(arg)
    if f_ok and not os.access(arg, os.F_OK):
        raise_validator(arg, name)
    return arg


@add_validator_magic
def valid_filename(
    arg: Any,
    strip: bool=False,
) -> str:

    # http://en.wikipedia.org/wiki/Filename#Comparison_of_filename_limitations
    assert os.name == "posix", "This validator is not implemented for %s" % (os.name)
    name = "filename"
    arg = os.path.normpath(not_none_string(arg, name, strip))
    if arg in [".", ".."]:
        raise_validator(arg, name)
    return check_re_match(arg, name, r"^[^/\0]+$")
