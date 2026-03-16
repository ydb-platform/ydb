__all__ = 'plain_repr', 'PlainRepr', 'Omit', 'get_dict_arg'

from typing import Any


class PlainRepr:
    """
    Hack to allow repr of string without quotes.
    """

    def __init__(self, v: str):
        self.v = v

    def __repr__(self) -> str:
        return self.v


def plain_repr(v: str) -> PlainRepr:
    return PlainRepr(v)


# used to omit arguments from repr
Omit = object()


def get_dict_arg(
    name: str, expected_args: tuple[dict[Any, Any], ...], expected_kwargs: dict[str, Any]
) -> dict[Any, Any]:
    """
    Used to enforce init logic similar to `dict(...)`.
    """
    if expected_kwargs:
        value = expected_kwargs
        if expected_args:
            raise TypeError(f'{name} requires either a single argument or kwargs, not both')
    elif not expected_args:
        value = {}
    elif len(expected_args) == 1:
        value = expected_args[0]

        if not isinstance(value, dict):
            raise TypeError(f'expected_values must be a dict, got {type(value)}')
    else:
        raise TypeError(f'{name} expected at most 1 argument, got {len(expected_args)}')

    return value
