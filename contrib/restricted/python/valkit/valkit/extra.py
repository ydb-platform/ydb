import json

from typing import Any

from . import raise_validator
from . import not_none_string
from . import check_re_match
from . import add_validator_magic


# =====
@add_validator_magic
def valid_hex_string(
    arg: Any,
    strip: bool=False,
) -> str:

    return check_re_match(arg, "hex string", r"^[0-9a-fA-F]+$", strip)


@add_validator_magic
def valid_uuid_string(
    arg: Any,
    strip: bool=False,
) -> str:

    pattern = r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
    return check_re_match(arg, "UUID string", pattern, strip)


@add_validator_magic
def valid_json_string(
    arg: Any,
    strip: bool=False,
) -> str:

    arg = not_none_string(arg, "JSON string", strip)
    try:
        json.dumps(json.loads(arg))
        return arg
    except Exception as err:
        raise raise_validator(arg, "JSON string", str(err))
