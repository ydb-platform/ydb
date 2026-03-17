import re
import threading
import time
import weakref
from collections import defaultdict
from collections.abc import MutableMapping
from typing import Any, Set, Callable, Dict, Optional, Iterator


class SimpleString:
    def __init__(self, value: bytes) -> None:
        assert isinstance(value, bytes)
        self.value = value

    @classmethod
    def decode(cls, value: bytes) -> bytes:
        return value


class SimpleError(Exception):
    """Exception that will be turned into a frontend-specific exception."""

    def __init__(self, value: str) -> None:
        assert isinstance(value, str)
        self.value = value


class NoResponse:
    """Returned by pub/sub commands to indicate that no response should be returned"""

    pass


OK = SimpleString(b"OK")
QUEUED = SimpleString(b"QUEUED")
BGSAVE_STARTED = SimpleString(b"Background saving started")


def current_time() -> int:
    return int(time.time() * 1000)


def null_terminate(s: bytes) -> bytes:
    # Redis uses C functions on some strings, which means they stop at the
    # first NULL.
    ind = s.find(b"\0")
    if ind > -1:
        return s[:ind].lower()
    return s.lower()


def casematch(a: bytes, b: bytes) -> bool:
    return null_terminate(a) == null_terminate(b)


def decode_command_bytes(s: bytes) -> str:
    return s.decode(encoding="utf-8", errors="replace").lower()


def compile_pattern(pattern_bytes: bytes) -> re.Pattern:  # type: ignore
    """Compile a glob pattern (e.g., for keys) to a `bytes` regex.

    `fnmatch.fnmatchcase` doesn't work for this because it uses different
    escaping rules to redis, uses ! instead of ^ to negate a character set,
    and handles invalid cases (such as a [ without a ]) differently. This
    implementation was written by studying the redis implementation.
    """
    # It's easier to work with text than bytes, because indexing bytes
    # doesn't behave the same in Python 3. Latin-1 will round-trip safely.
    pattern: str = pattern_bytes.decode(
        "latin-1",
    )
    parts = ["^"]
    i = 0
    pattern_len = len(pattern)
    while i < pattern_len:
        c = pattern[i]
        i += 1
        if c == "?":
            parts.append(".")
        elif c == "*":
            parts.append(".*")
        elif c == "\\":
            if i == pattern_len:
                i -= 1
            parts.append(re.escape(pattern[i]))
            i += 1
        elif c == "[":
            parts.append("[")
            if i < pattern_len and pattern[i] == "^":
                i += 1
                parts.append("^")
            parts_len = len(parts)  # To detect if anything was added
            while i < pattern_len:
                if pattern[i] == "\\" and i + 1 < pattern_len:
                    i += 1
                    parts.append(re.escape(pattern[i]))
                elif pattern[i] == "]":
                    i += 1
                    break
                elif i + 2 < pattern_len and pattern[i + 1] == "-":
                    start = pattern[i]
                    end = pattern[i + 2]
                    if start > end:
                        start, end = end, start
                    parts.append(re.escape(start) + "-" + re.escape(end))
                    i += 2
                else:
                    parts.append(re.escape(pattern[i]))
                i += 1
            if len(parts) == parts_len:
                if parts[-1] == "[":
                    # Empty group - will never match
                    parts[-1] = "(?:$.)"
                else:
                    # Negated empty group - matches any character
                    assert parts[-1] == "^"
                    parts.pop()
                    parts[-1] = "."
            else:
                parts.append("]")
        else:
            parts.append(re.escape(c))
    parts.append("\\Z")
    regex: bytes = "".join(parts).encode("latin-1")
    return re.compile(regex, flags=re.S)


class Database(MutableMapping):  # type: ignore
    def __init__(self, lock: Optional[threading.Lock], *args: Any, **kwargs: Any) -> None:
        self._dict: Dict[bytes, Any] = dict(*args, **kwargs)
        self.time = 0.0
        # key to the set of connections
        self._watches: Dict[bytes, weakref.WeakSet[Any]] = defaultdict(weakref.WeakSet)
        self.condition = threading.Condition(lock)
        self._change_callbacks: Set[Callable[[], None]] = set()

    def swap(self, other: "Database") -> None:
        self._dict, other._dict = other._dict, self._dict
        self.time, other.time = other.time, self.time

    def notify_watch(self, key: bytes) -> None:
        for sock in self._watches.get(key, set()):
            sock.notify_watch()
        self.condition.notify_all()
        for callback in self._change_callbacks:
            callback()

    def add_watch(self, key: bytes, sock: Any) -> None:
        self._watches[key].add(sock)

    def remove_watch(self, key: bytes, sock: Any) -> None:
        watches = self._watches[key]
        watches.discard(sock)
        if not watches:
            del self._watches[key]

    def add_change_callback(self, callback: Callable[[], None]) -> None:
        self._change_callbacks.add(callback)

    def remove_change_callback(self, callback: Callable[[], None]) -> None:
        self._change_callbacks.remove(callback)

    def clear(self) -> None:
        for key in self:
            self.notify_watch(key)
        self._dict.clear()

    def expired(self, item: Any) -> bool:
        return item.expireat is not None and item.expireat < self.time

    def _remove_expired(self) -> None:
        for key in list(self._dict):
            item = self._dict[key]
            if self.expired(item):
                del self._dict[key]

    def __getitem__(self, key: bytes) -> Any:
        item = self._dict[key]
        if self.expired(item):
            del self._dict[key]
            raise KeyError(key)
        return item

    def __setitem__(self, key: bytes, value: Any) -> None:
        self._dict[key] = value

    def __delitem__(self, key: bytes) -> None:
        del self._dict[key]

    def __iter__(self) -> Iterator[bytes]:
        self._remove_expired()
        return iter(self._dict)

    def __len__(self) -> int:
        self._remove_expired()
        return len(self._dict)

    def __hash__(self) -> int:
        return hash(super(object, self))

    def __eq__(self, other: object) -> bool:
        return super(object, self) == other


def valid_response_type(value: Any, nested: bool = False) -> bool:
    if isinstance(value, NoResponse) and not nested:
        return True
    if value is not None and not isinstance(value, (bytes, SimpleString, SimpleError, float, int, list)):
        return False
    if isinstance(value, list):
        if any(not valid_response_type(item, True) for item in value):
            return False
    return True


class FakeSelector(object):
    def __init__(self, sock: Any):
        self.sock = sock

    def check_can_read(self, timeout: Optional[float]) -> bool:
        if self.sock.responses.qsize():
            return True
        if timeout is not None and timeout <= 0:
            return False

        # A sleep/poll loop is easier to mock out than messing with condition
        # variables.
        start = time.time()
        while True:
            if self.sock.responses.qsize():
                return True
            time.sleep(0.01)
            now = time.time()
            if timeout is not None and now > start + timeout:
                return False

    @staticmethod
    def check_is_ready_for_command(_: Any) -> bool:
        return True
