from __future__ import annotations

from collections import OrderedDict
from typing import TYPE_CHECKING, Any, TypeVar

from icalendar.parser_tools import to_unicode

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

KT = TypeVar("KT")
VT = TypeVar("VT")


def canonsort_keys(
    keys: Iterable[KT], canonical_order: Iterable[KT] | None = None
) -> list[KT]:
    """Sorts leading keys according to canonical_order.  Keys not specified in
    canonical_order will appear alphabetically at the end.
    """
    canonical_map = {k: i for i, k in enumerate(canonical_order or [])}
    head = [k for k in keys if k in canonical_map]
    tail = [k for k in keys if k not in canonical_map]
    return sorted(head, key=lambda k: canonical_map[k]) + sorted(tail)


def canonsort_items(
    dict1: Mapping[KT, VT], canonical_order: Iterable[KT] | None = None
) -> list[tuple[KT, VT]]:
    """Returns a list of items from dict1, sorted by canonical_order."""
    return [(k, dict1[k]) for k in canonsort_keys(dict1.keys(), canonical_order)]


class CaselessDict(OrderedDict):
    """A dictionary that isn't case sensitive, and only uses strings as keys.
    Values retain their case.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        """Set keys to upper for initial dict."""
        super().__init__(*args, **kwargs)
        for key, value in self.items():
            key_upper = to_unicode(key).upper()
            if key != key_upper:
                super().__delitem__(key)
                self[key_upper] = value

    __hash__ = None

    def __getitem__(self, key: Any) -> Any:
        key = to_unicode(key)
        return super().__getitem__(key.upper())

    def __setitem__(self, key: Any, value: Any) -> None:
        key = to_unicode(key)
        super().__setitem__(key.upper(), value)

    def __delitem__(self, key: Any) -> None:
        key = to_unicode(key)
        super().__delitem__(key.upper())

    def __contains__(self, key: Any) -> bool:
        key = to_unicode(key)
        return super().__contains__(key.upper())

    def get(self, key: Any, default: Any = None) -> Any:
        key = to_unicode(key)
        return super().get(key.upper(), default)

    def setdefault(self, key: Any, value: Any = None) -> Any:
        key = to_unicode(key)
        return super().setdefault(key.upper(), value)

    def pop(self, key: Any, default: Any = None) -> Any:
        key = to_unicode(key)
        return super().pop(key.upper(), default)

    def popitem(self) -> tuple[Any, Any]:
        return super().popitem()

    def has_key(self, key: Any) -> bool:
        key = to_unicode(key)
        return super().__contains__(key.upper())

    def update(self, *args: Any, **kwargs: Any) -> None:
        # Multiple keys where key1.upper() == key2.upper() will be lost.
        mappings = list(args) + [kwargs]
        for mapping in mappings:
            if hasattr(mapping, "items"):
                mapping = iter(mapping.items())  # noqa: PLW2901
            for key, value in mapping:
                self[key] = value

    def copy(self) -> Self:
        return type(self)(super().copy())

    def __repr__(self) -> str:
        return f"{type(self).__name__}({dict(self)})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, dict):
            return NotImplemented
        return self is other or dict(self.items()) == dict(other.items())

    def __ne__(self, other: object) -> bool:
        return not self == other

    # A list of keys that must appear first in sorted_keys and sorted_items;
    # must be uppercase.
    canonical_order = None

    def sorted_keys(self) -> list[str]:
        """Sorts keys according to the canonical_order for the derived class.
        Keys not specified in canonical_order will appear at the end.
        """
        return canonsort_keys(self.keys(), self.canonical_order)

    def sorted_items(self) -> list[tuple[Any, Any]]:
        """Sorts items according to the canonical_order for the derived class.
        Items not specified in canonical_order will appear at the end.
        """
        return canonsort_items(self, self.canonical_order)


__all__ = ["CaselessDict", "canonsort_items", "canonsort_keys"]
