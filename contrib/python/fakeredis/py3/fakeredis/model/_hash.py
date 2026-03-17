from typing import Iterable, Tuple, Optional, Any, Dict

from fakeredis import _msgs as msgs
from fakeredis._helpers import current_time


class Hash:
    DECODE_ERROR = msgs.INVALID_HASH_MSG
    redis_type = b"hash"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._expirations: Dict[bytes, int] = dict()
        self._values: Dict[bytes, Any] = dict()

    def _expire_keys(self) -> None:
        removed = []
        now = current_time()
        for k in self._expirations:
            if self._expirations[k] < now:
                self._values.pop(k, None)
                removed.append(k)
        for k in removed:
            self._expirations.pop(k, None)

    def set_key_expireat(self, key: bytes, when_ms: int) -> int:
        now = current_time()
        if when_ms <= now:
            self._values.pop(key, None)
            self._expirations.pop(key, None)
            return 2
        self._expirations[key] = when_ms
        return 1

    def clear_key_expireat(self, key: bytes) -> bool:
        return self._expirations.pop(key, None) is not None

    def get_key_expireat(self, key: bytes) -> Optional[int]:
        self._expire_keys()
        return self._expirations.get(key, None)

    def __getitem__(self, key: bytes) -> Any:
        self._expire_keys()
        return self._values.get(key)

    def __contains__(self, key: bytes) -> bool:
        self._expire_keys()
        return self._values.__contains__(key)

    def __setitem__(self, key: bytes, value: Any) -> None:
        self._expirations.pop(key, None)
        self._values[key] = value

    def __delitem__(self, key: bytes) -> None:
        self._values.pop(key, None)
        self._expirations.pop(key, None)

    def __len__(self) -> int:
        return len(self._values)

    def __iter__(self) -> Iterable[bytes]:
        return iter(self._values)

    def get(self, key: bytes, default: Any = None) -> Any:
        return self._values.get(key, default)

    def keys(self) -> Iterable[bytes]:
        self._expire_keys()
        return self._values.keys()

    def values(self) -> Iterable[Any]:
        return [v for k, v in self.items()]

    def items(self) -> Iterable[Tuple[bytes, Any]]:
        self._expire_keys()
        return self._values.items()

    def update(self, values: Dict[bytes, Any]) -> None:
        self._expire_keys()
        self._values.update(values)
