#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Multimap module."""

from collections import OrderedDict
from typing import Any, List, Optional


class Multimap(object):
    """Multimap class."""

    def __init__(self) -> None:
        """Make new multimap."""
        # maybe defaultdict(set) is better
        self._map: OrderedDict[Optional[str], List[Any]] = OrderedDict()

    def set(self, key: Optional[str], value: Any) -> None:
        """Set value."""
        _set = self._map.get(key)
        if not _set:
            _set = list()
            self._map[key] = _set
        if value not in _set:
            _set.append(value)

    def get(self, key: Optional[str]) -> List[Any]:
        """Get values."""
        return self._map.get(key, list())

    def has(self, key: Optional[str]) -> bool:
        """Check key is in this map."""
        return key in self._map

    def hasValue(self, key: Optional[str], value: Any) -> bool:
        """Check value is in this map."""
        _set = self._map.get(key, list())
        return value in _set

    def size(self) -> int:
        """Length of this map."""
        return len(self._map)

    def delete(self, key: Optional[str], value: Any) -> bool:
        """Delete value from key."""
        values = self.get(key)
        result = value in values
        if result:
            values.remove(value)
        if len(values) == 0:
            self._map.pop(key)
        return result

    def deleteAll(self, key: Optional[str]) -> None:
        """Delete all value of the key."""
        self._map.pop(key, None)

    def firstValue(self, key: Optional[str]) -> Any:
        """Get first value of the key."""
        _set = self._map.get(key)
        if not _set:
            return None
        return _set[0]

    def firstKey(self) -> Optional[str]:
        """Get first key."""
        return next(iter(self._map.keys()))

    def valuesArray(self) -> List[Any]:
        """Get all values as list."""
        result: List[Any] = list()
        for values in self._map.values():
            result.extend(values)
        return result

    def clear(self) -> None:
        """Clear all entries of this map."""
        self._map.clear()
