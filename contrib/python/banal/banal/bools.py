from typing import Any

BOOL_TRUEISH = ["1", "yes", "y", "t", "true", "on", "enabled"]


def as_bool(value: Any, default: bool = False) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    value = str(value).strip().lower()
    if not len(value):
        return default
    return value in BOOL_TRUEISH
