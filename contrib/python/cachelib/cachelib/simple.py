import typing as _t
from time import time

from cachelib.base import BaseCache
from cachelib.serializers import SimpleSerializer


class SimpleCache(BaseCache):
    """Simple memory cache for single process environments.  This class exists
    mainly for the development server and is not 100% thread safe.  It tries
    to use as many atomic operations as possible and no locks for simplicity
    but it could happen under heavy load that keys are added multiple times.

    :param threshold: the maximum number of items the cache stores before
                      it starts deleting some.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    """

    serializer = SimpleSerializer()

    def __init__(
        self,
        threshold: int = 500,
        default_timeout: int = 300,
    ):
        BaseCache.__init__(self, default_timeout)
        self._cache: _t.Dict[str, _t.Any] = {}
        self._threshold = threshold or 500  # threshold = 0

    def _over_threshold(self) -> bool:
        return len(self._cache) > self._threshold

    def _remove_expired(self, now: float) -> None:
        toremove = [k for k, (expires, _) in self._cache.items() if expires < now]
        for k in toremove:
            self._cache.pop(k, None)

    def _remove_older(self) -> None:
        k_ordered = (
            k for k, v in sorted(self._cache.items(), key=lambda item: item[1][0])
        )
        for k in k_ordered:
            self._cache.pop(k, None)
            if not self._over_threshold():
                break

    def _prune(self) -> None:
        if self._over_threshold():
            now = time()
            self._remove_expired(now)
        # remove older items if still over threshold
        if self._over_threshold():
            self._remove_older()

    def _normalize_timeout(self, timeout: _t.Optional[int]) -> int:
        timeout = BaseCache._normalize_timeout(self, timeout)
        if timeout > 0:
            timeout = int(time()) + timeout
        return timeout

    def get(self, key: str) -> _t.Any:
        try:
            expires, value = self._cache[key]
            if expires == 0 or expires > time():
                return self.serializer.loads(value)
        except KeyError:
            return None

    def set(
        self, key: str, value: _t.Any, timeout: _t.Optional[int] = None
    ) -> _t.Optional[bool]:
        expires = self._normalize_timeout(timeout)
        self._prune()
        self._cache[key] = (expires, self.serializer.dumps(value))
        return True

    def add(self, key: str, value: _t.Any, timeout: _t.Optional[int] = None) -> bool:
        expires = self._normalize_timeout(timeout)
        self._prune()
        item = (expires, self.serializer.dumps(value))
        if key in self._cache:
            return False
        self._cache.setdefault(key, item)
        return True

    def delete(self, key: str) -> bool:
        return self._cache.pop(key, None) is not None

    def has(self, key: str) -> bool:
        try:
            expires, value = self._cache[key]
            return bool(expires == 0 or expires > time())
        except KeyError:
            return False

    def clear(self) -> bool:
        self._cache.clear()
        return not bool(self._cache)
