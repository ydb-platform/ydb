from typing import TYPE_CHECKING, Any, NoReturn, Union

try:
    from types import NoneType, UnionType

    UNION_TYPES = {UnionType, Union}
except ImportError:
    UNION_TYPES = {Union}

    NoneType = type(None)  # type: ignore[misc]


class Frozendict(dict):
    def _immutable_error(self, *_: Any, **__: Any) -> NoReturn:
        msg = f"Unable to mutate {type(self).__name__}"
        raise TypeError(msg)

    # Override all mutation methods to prevent changes
    if not TYPE_CHECKING:
        __setitem__ = _immutable_error
        __delitem__ = _immutable_error
        clear = _immutable_error
        pop = _immutable_error
        popitem = _immutable_error
        update = _immutable_error

    def __hash__(self) -> int:  # type: ignore[override]
        return hash(tuple(self.items()))


__all__ = ("UNION_TYPES", "Frozendict", "NoneType")
