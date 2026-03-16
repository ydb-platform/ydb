import sys
from typing import Iterable, Optional, Any, Dict, Union, Set

from fakeredis import _msgs as msgs
from fakeredis._helpers import current_time

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ExpiringMembersSet:
    DECODE_ERROR = msgs.INVALID_HASH_MSG
    redis_type = b"set"

    def __init__(self, values: Dict[bytes, Optional[int]] = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._values: Dict[bytes, Optional[int]] = values or dict()

    def _expire_members(self) -> None:
        removed = []
        now = current_time()
        for k in self._values:
            if self._values[k] is not None and self._values[k] < now:
                self._values.pop(k, None)
                removed.append(k)

    def set_member_expireat(self, key: bytes, when_ms: int) -> int:
        now = current_time()
        if when_ms <= now:
            self._values.pop(key, None)
            return 2
        self._values[key] = when_ms
        return 1

    def clear_key_expireat(self, key: bytes) -> bool:
        return self._values.pop(key, None) is not None

    def get_key_expireat(self, key: bytes) -> Optional[int]:
        self._expire_members()
        return self._values.get(key, None)

    def __contains__(self, key: bytes) -> bool:
        self._expire_members()
        return self._values.__contains__(key)

    def __delitem__(self, key: bytes) -> None:
        self._values.pop(key, None)

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterable[bytes]:
        return iter({k for k in self._values if self._values[k] is None or self._values[k] >= current_time()})

    def __get__(self, instance, owner=None) -> Set[bytes]:
        self._expire_members()
        return set(self._values.keys())

    def __sub__(self, other: Self) -> Self:
        return ExpiringMembersSet({k: v for k, v in self._values.items() if k not in other._values})

    def __and__(self, other: Self) -> Self:
        return ExpiringMembersSet({k: v for k, v in self._values.items() if k in other._values})

    def __or__(self, other: Self) -> Self:
        return ExpiringMembersSet({k: v for k, v in self._values.items()}).update(other)

    def update(self, other: Union[Self, Iterable[bytes]]) -> Self:
        self._expire_members()
        if isinstance(other, ExpiringMembersSet):
            self._values.update(other._values)
            return self
        for value in other:
            self._values[value] = None
        return self

    def discard(self, key: bytes) -> None:
        self._values.pop(key, None)

    def remove(self, key: bytes) -> None:
        self._values.pop(key)

    def add(self, key: bytes) -> None:
        self._values[key] = None

    def copy(self) -> Self:
        return ExpiringMembersSet(self._values.copy())
