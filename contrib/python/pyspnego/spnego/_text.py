# Copyright: (c) 2020, Jordan Borean (@jborean93) <jborean93@gmail.com>
# MIT License (see LICENSE or https://opensource.org/licenses/MIT)

import typing


def _obj_str(
    obj: typing.Any,
    default: str,
) -> str:
    # First try to get the str() then repr() before falling back to the default value.
    to_str_funcs: typing.List[typing.Callable] = [str, repr]
    for func in to_str_funcs:
        try:
            obj = func(obj)
        except (UnicodeError, TypeError):
            continue
        else:
            return obj
    else:
        return default


def to_bytes(
    obj: typing.Any,
    encoding: str = "utf-8",
    errors: str = "strict",
    nonstring: str = "str",
) -> typing.Any:
    if isinstance(obj, bytes):
        return obj
    elif isinstance(obj, str):
        return obj.encode(encoding, errors)

    if nonstring == "str":
        return to_bytes(_obj_str(obj, ""), encoding=encoding, errors=errors)
    elif nonstring == "passthru":
        return obj
    elif nonstring == "empty":
        return b""
    else:
        raise ValueError("Invalid nonstring value '%s', expecting str, passthru, or empty" % nonstring)


def to_text(
    obj: typing.Any,
    encoding: str = "utf-8",
    errors: str = "strict",
    nonstring: str = "str",
) -> typing.Any:
    if isinstance(obj, str):
        return obj
    elif isinstance(obj, bytes):
        return obj.decode(encoding, errors)

    if nonstring == "str":
        try:
            obj = obj.__unicode__()
        except (AttributeError, UnicodeError):
            obj = _obj_str(obj, "")

        return to_text(obj, errors=errors, encoding=encoding)
    elif nonstring == "passthru":
        return obj
    elif nonstring == "empty":
        return ""
    else:
        raise ValueError("Invalid nonstring value '%s', expecting repr, passthru, or empty" % nonstring)
