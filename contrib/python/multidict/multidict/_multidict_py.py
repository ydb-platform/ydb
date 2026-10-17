import enum
import functools
import reprlib
import sys
import threading
from array import array
from collections.abc import (
    Callable,
    ItemsView,
    Iterable,
    Iterator,
    KeysView,
    Mapping,
    ValuesView,
)
from dataclasses import dataclass
from typing import (
    TYPE_CHECKING,
    Any,
    ClassVar,
    Generic,
    NoReturn,
    TypeVar,
    cast,
    overload,
)

from ._abc import MDArg, MultiMapping, MutableMultiMapping, SupportsKeys

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

MAXSIZE = sys.maxsize

# Entry hashes are folded non-negative (& MAXSIZE) so HASH_MARK, the
# hash range's high bit, can mark a hash as temporarily invalid: OR it in,
# AND it out with MAXSIZE. A real folded hash never has that bit set.
HASH_MARK = MAXSIZE + 1
# Same as HT_LOG_RESUME_SLOTS_MINSIZE and HT_RESUME_SLOTS_MIN_STEPS in htkeys.h
_LOG_RESUME_SLOTS_MINSIZE = 10
_RESUME_SLOTS_MIN_STEPS = 32


class istr(str):
    """Case insensitive str."""

    # Implementation note:
    # The class doesn't use __slots__ because slot-based memory model
    # is ~20% slower than classic __dict__-based one.
    # See also: https://github.com/aio-libs/multidict/pull/1386

    __is_istr__ = True
    __istr_identity__: str | None = None


_V = TypeVar("_V")
_T = TypeVar("_T")

_SENTINEL = enum.Enum("_SENTINEL", "sentinel")
sentinel = _SENTINEL.sentinel

_version = array("Q", [0])

_FREE_THREADED = hasattr(sys, "_is_gil_enabled") and not sys._is_gil_enabled()

_VERSION_LOCK = threading.Lock()


def _other_lock(arg: object) -> "threading.RLock | None":
    """Return the RLock backing `arg`, if it's a multidict, one of our
    own views over one, or an iterator over one, that we don't already
    own."""
    if isinstance(arg, MultiDictProxy):
        return arg._md._lock
    if isinstance(arg, MultiDict):
        return arg._lock
    if isinstance(arg, (_ItemsView, _KeysView, _ValuesView)):
        return arg._md._lock
    if isinstance(arg, _Iter):
        return arg._lock
    return None


def _iter_lock(md: "MultiDict[Any]") -> "threading.RLock | None":
    """Return the lock a view iterator over `md` should take per step."""
    return md._lock if _FREE_THREADED else None


class _PairLock:
    """Acquire one or two RLocks in a fixed, deadlock-safe order."""

    __slots__ = ("_a", "_b")

    def __init__(self, a: "threading.RLock", b: "threading.RLock | None") -> None:
        if b is None or b is a:
            self._a: threading.RLock | None = a
            self._b: threading.RLock | None = None
        elif id(a) < id(b):
            self._a, self._b = a, b
        else:
            self._a, self._b = b, a

    def __enter__(self) -> None:
        assert self._a is not None
        self._a.acquire()
        if self._b is not None:
            self._b.acquire()

    def __exit__(self, *exc: object) -> None:
        assert self._a is not None
        if self._b is not None:
            self._b.release()
        self._a.release()


_F = TypeVar("_F", bound=Callable[..., Any])


def _locked_always(fn: _F) -> _F:
    """Wrap `fn` to hold `self._lock` for the call, on every build.

    update()/extend()/merge()/clear()/popitem() have their own race that,
    unlike the rest of this module's locking, is not specific to
    free-threading: pure-Python bytecode is not atomic even under the GIL
    (the GIL can be released between any two bytecodes), so enough
    concurrent contention lets two calls interleave mid hash-table insert
    or deletion and corrupt the shared index table. `_locked`/`_locked_pair`
    skip locking on a GIL-enabled build because the race they guard against
    is free-threading-only; this variant cannot.
    """

    @functools.wraps(fn)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        with self._lock:
            return fn(self, *args, **kwargs)

    return cast(_F, wrapper)


def _locked_pair_always(fn: _F) -> _F:
    """Like `_locked_always`, but also pairs in the first argument's lock,
    for the same reason `_locked_pair` does."""

    @functools.wraps(fn)
    def wrapper(self: Any, arg: Any = None, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        with _PairLock(self._lock, _other_lock(arg)):
            return fn(self, arg, *args, **kwargs)

    return cast(_F, wrapper)


def _locked(fn: _F) -> _F:
    """Wrap `fn` to hold `self._lock` for the call, but only when it
    guards against a free-threading-only race; see `_locked_always` for
    the unconditional version a few methods need."""
    if not _FREE_THREADED:
        return fn
    return _locked_always(fn)


def _locked_md(fn: _F) -> _F:
    """Like `_locked`, but for view objects: holds `self._md._lock`."""
    if not _FREE_THREADED:
        return fn

    @functools.wraps(fn)
    def wrapper(self: Any, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        with self._md._lock:
            return fn(self, *args, **kwargs)

    return cast(_F, wrapper)


def _locked_pair(fn: _F) -> _F:
    """Wrap `fn` to hold `self._lock` and its first argument's lock, but
    only when it guards against a free-threading-only race; see
    `_locked_pair_always`."""
    if not _FREE_THREADED:
        return fn
    return _locked_pair_always(fn)


def _locked_md_pair(fn: _F) -> _F:
    """Like `_locked_md`, but also pairs in its first argument's lock."""
    if not _FREE_THREADED:
        return fn

    @functools.wraps(fn)
    def wrapper(self: Any, other: Any = None, *args: Any, **kwargs: Any) -> Any:  # type: ignore[misc]
        with _PairLock(self._md._lock, _other_lock(other)):
            return fn(self, other, *args, **kwargs)

    return cast(_F, wrapper)


class _Iter(Generic[_T]):
    __slots__ = ("_size", "_iter", "_lock")

    def __init__(
        self,
        size: int,
        iterator: Iterator[_T],
        lock: "threading.RLock | None" = None,
    ):
        self._size = size
        self._iter = iterator
        self._lock = lock

    def __iter__(self) -> Self:
        return self

    @_locked
    def __next__(self) -> _T:
        return next(self._iter)

    def __length_hint__(self) -> int:
        return self._size


class _ViewBase(Generic[_V]):
    def __init__(
        self,
        md: "MultiDict[_V]",
    ):
        self._md = md

    def __len__(self) -> int:
        return len(self._md)


class _ItemsView(_ViewBase[_V], ItemsView[str, _V]):
    @_locked_md
    def __contains__(self, item: object) -> bool:
        if not isinstance(item, (tuple, list)) or len(item) != 2:
            return False
        key, value = item
        try:
            identity = self._md._identity(key)
        except TypeError:
            return False
        hash_ = hash(identity) & MAXSIZE
        for slot, idx, e in self._md._keys.iter_hash(hash_):
            if e.identity == identity and value == e.value:
                return True
        return False

    def __iter__(self) -> _Iter[tuple[str, _V]]:
        return _Iter(len(self), self._iter(self._md._version), _iter_lock(self._md))

    def __reversed__(self) -> _Iter[tuple[str, _V]]:
        return _Iter(
            len(self),
            self._iter(self._md._version, reverse=True),
            _iter_lock(self._md),
        )

    def _iter(self, version: int, reverse: bool = False) -> Iterator[tuple[str, _V]]:
        entries = self._md._keys.iter_entries(reverse)
        while True:
            if version != self._md._version:
                raise RuntimeError("Dictionary changed during iteration")
            try:
                e = next(entries)
            except StopIteration:
                return
            yield self._md._key(e.key), e.value

    @reprlib.recursive_repr()
    @_locked_md
    def __repr__(self) -> str:
        lst = []
        for e in self._md._keys.iter_entries():
            lst.append(f"{e.key!r}: {e.value!r}")
        body = ", ".join(lst)
        return f"<{self.__class__.__name__}({body})>"

    def _parse_item(self, arg: tuple[str, _V] | _T) -> tuple[int, str, str, _V] | None:
        if not isinstance(arg, tuple):
            return None
        if len(arg) != 2:
            return None
        try:
            identity = self._md._identity(arg[0])
            return (hash(identity) & MAXSIZE, identity, arg[0], arg[1])
        except TypeError:
            return None

    def _tmp_set(self, it: Iterable[_T]) -> set[tuple[str, _V]]:
        tmp = set()
        for arg in it:
            item = self._parse_item(arg)
            if item is None:
                continue
            else:
                tmp.add((item[1], item[3]))
        return tmp

    @_locked_md_pair
    def __and__(self, other: Iterable[Any]) -> set[tuple[str, _V]]:  # type: ignore[misc]
        ret = set()
        try:
            it = iter(other)
        except TypeError:
            return NotImplemented
        for arg in it:
            item = self._parse_item(arg)
            if item is None:
                continue
            hash_, identity, key, value = item
            marked = hash_ | HASH_MARK
            matches = []
            for slot, idx, e in self._md._keys.iter_hash(hash_):
                e.hash = marked
                if e.identity == identity:  # pragma: no branch
                    matches.append((e.key, e.value))
            self._md._keys.restore_hash(hash_)
            # Compare values only after restore_hash(): a custom __eq__
            # here could reenter this MultiDict via getall() and must not
            # see entries this walk still has marked.
            for e_key, e_value in matches:
                if e_value == value:
                    ret.add((e_key, e_value))
        return ret

    @_locked_md_pair
    def __rand__(self, other: Iterable[_T]) -> set[_T]:
        ret = set()
        try:
            it = iter(other)
        except TypeError:
            return NotImplemented
        for arg in it:
            item = self._parse_item(arg)
            if item is None:
                continue
            hash_, identity, key, value = item
            for slot, idx, e in self._md._keys.iter_hash(hash_):
                if e.identity == identity and e.value == value:
                    ret.add(arg)
                    break
        return ret

    @_locked_md_pair
    def __or__(self, other: Iterable[_T]) -> set[tuple[str, _V] | _T]:
        ret: set[tuple[str, _V] | _T] = set(self)
        try:
            it = iter(other)
        except TypeError:
            return NotImplemented
        for arg in it:
            item: tuple[int, str, str, _V] | None = self._parse_item(arg)
            if item is None:
                ret.add(arg)
                continue
            hash_, identity, key, value = item
            for slot, idx, e in self._md._keys.iter_hash(hash_):
                if e.identity == identity and e.value == value:  # pragma: no branch
                    break
            else:
                ret.add(arg)
        return ret

    @_locked_md_pair
    def __ror__(self, other: Iterable[_T]) -> set[tuple[str, _V] | _T]:
        try:
            ret: set[tuple[str, _V] | _T] = set(other)
        except TypeError:
            return NotImplemented
        tmp = self._tmp_set(ret)

        for e in self._md._keys.iter_entries():
            if (e.identity, e.value) not in tmp:
                ret.add((e.key, e.value))
        return ret

    @_locked_md_pair
    def __sub__(self, other: Iterable[_T]) -> set[tuple[str, _V] | _T]:
        ret: set[tuple[str, _V] | _T] = set()
        try:
            it = iter(other)
        except TypeError:
            return NotImplemented
        tmp = self._tmp_set(it)

        for e in self._md._keys.iter_entries():
            if (e.identity, e.value) not in tmp:
                ret.add((e.key, e.value))

        return ret

    @_locked_md_pair
    def __rsub__(self, other: Iterable[_T]) -> set[_T]:
        ret: set[_T] = set()
        try:
            it = iter(other)
        except TypeError:
            return NotImplemented
        for arg in it:
            item = self._parse_item(arg)
            if item is None:
                ret.add(arg)
                continue

            hash_, identity, key, value = item
            for slot, idx, e in self._md._keys.iter_hash(hash_):
                if e.identity == identity and e.value == value:  # pragma: no branch
                    break
            else:
                ret.add(arg)
        return ret

    @_locked_md_pair
    def __xor__(self, other: Iterable[_T]) -> set[tuple[str, _V] | _T]:
        try:
            rgt = set(other)
        except TypeError:
            return NotImplemented
        ret: set[tuple[str, _V] | _T] = self - rgt
        ret |= rgt - self
        return ret

    __rxor__ = __xor__

    @_locked_md_pair
    def isdisjoint(self, other: Iterable[tuple[str, _V]]) -> bool:
        for arg in other:
            item = self._parse_item(arg)
            if item is None:
                continue

            hash_, identity, key, value = item
            for slot, idx, e in self._md._keys.iter_hash(hash_):
                if e.identity == identity and e.value == value:  # pragma: no branch
                    return False
        return True


class _ValuesView(_ViewBase[_V], ValuesView[_V]):
    @_locked_md
    def __contains__(self, value: object) -> bool:
        for e in self._md._keys.iter_entries():
            if e.value == value:
                return True
        return False

    def __iter__(self) -> _Iter[_V]:
        return _Iter(len(self), self._iter(self._md._version), _iter_lock(self._md))

    def __reversed__(self) -> _Iter[_V]:
        return _Iter(
            len(self),
            self._iter(self._md._version, reverse=True),
            _iter_lock(self._md),
        )

    def _iter(self, version: int, reverse: bool = False) -> Iterator[_V]:
        entries = self._md._keys.iter_entries(reverse)
        while True:
            if version != self._md._version:
                raise RuntimeError("Dictionary changed during iteration")
            try:
                e = next(entries)
            except StopIteration:
                return
            yield e.value

    @reprlib.recursive_repr()
    @_locked_md
    def __repr__(self) -> str:
        lst = []
        for e in self._md._keys.iter_entries():
            lst.append(repr(e.value))
        body = ", ".join(lst)
        return f"<{self.__class__.__name__}({body})>"


class _KeysView(_ViewBase[_V], KeysView[str]):
    @_locked_md
    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        identity = self._md._identity(key)
        hash_ = hash(identity) & MAXSIZE
        for slot, idx, e in self._md._keys.iter_hash(hash_):
            if e.identity == identity:  # pragma: no branch
                return True
        return False

    def __iter__(self) -> _Iter[str]:
        return _Iter(len(self), self._iter(self._md._version), _iter_lock(self._md))

    def __reversed__(self) -> _Iter[str]:
        return _Iter(
            len(self),
            self._iter(self._md._version, reverse=True),
            _iter_lock(self._md),
        )

    def _iter(self, version: int, reverse: bool = False) -> Iterator[str]:
        entries = self._md._keys.iter_entries(reverse)
        while True:
            if version != self._md._version:
                raise RuntimeError("Dictionary changed during iteration")
            try:
                e = next(entries)
            except StopIteration:
                return
            yield self._md._key(e.key)

    @reprlib.recursive_repr()
    @_locked_md
    def __repr__(self) -> str:
        lst = []
        for e in self._md._keys.iter_entries():
            lst.append(f"{e.key!r}")
        body = ", ".join(lst)
        return f"<{self.__class__.__name__}({body})>"

    @_locked_md_pair
    def __and__(self, other: Iterable[object]) -> set[str]:
        ret = set()
        try:
            it = iter(other)
        except TypeError:
            return NotImplemented
        for key in it:
            if not isinstance(key, str):
                continue
            identity = self._md._identity(key)
            hash_ = hash(identity) & MAXSIZE
            for slot, idx, e in self._md._keys.iter_hash(hash_):
                if e.identity == identity:  # pragma: no branch
                    ret.add(e.key)
                    break
        return ret

    @_locked_md_pair
    def __rand__(self, other: Iterable[_T]) -> set[_T]:
        ret = set()
        try:
            it = iter(other)
        except TypeError:
            return NotImplemented
        for key in it:
            if not isinstance(key, str):
                continue
            if key in self._md:
                ret.add(key)
        return cast(set[_T], ret)

    @_locked_md_pair
    def __or__(self, other: Iterable[_T]) -> set[str | _T]:
        ret: set[str | _T] = set(self)
        try:
            it = iter(other)
        except TypeError:
            return NotImplemented
        for key in it:
            if not isinstance(key, str):
                ret.add(key)
                continue
            if key not in self._md:
                ret.add(key)
        return ret

    @_locked_md_pair
    def __ror__(self, other: Iterable[_T]) -> set[str | _T]:
        try:
            ret: set[str | _T] = set(other)
        except TypeError:
            return NotImplemented

        tmp = set()
        for key in ret:
            if not isinstance(key, str):
                continue
            identity = self._md._identity(key)
            tmp.add(identity)

        for e in self._md._keys.iter_entries():
            if e.identity not in tmp:
                ret.add(e.key)
        return ret

    @_locked_md_pair
    def __sub__(self, other: Iterable[object]) -> set[str]:
        ret = set(self)
        try:
            it = iter(other)
        except TypeError:
            return NotImplemented
        for key in it:
            if not isinstance(key, str):
                continue
            identity = self._md._identity(key)
            hash_ = hash(identity) & MAXSIZE
            for slot, idx, e in self._md._keys.iter_hash(hash_):
                if e.identity == identity:  # pragma: no branch
                    ret.discard(e.key)
                    break
        return ret

    @_locked_md_pair
    def __rsub__(self, other: Iterable[_T]) -> set[_T]:
        try:
            ret: set[_T] = set(other)
        except TypeError:
            return NotImplemented
        for key in other:
            if not isinstance(key, str):
                continue
            if key in self._md:
                ret.discard(key)
        return ret

    @_locked_md_pair
    def __xor__(self, other: Iterable[_T]) -> set[str | _T]:
        try:
            rgt = set(other)
        except TypeError:
            return NotImplemented
        ret: set[str | _T] = self - rgt  # type: ignore[assignment]
        ret |= rgt - self
        return ret

    __rxor__ = __xor__

    @_locked_md_pair
    def isdisjoint(self, other: Iterable[object]) -> bool:
        for key in other:
            if not isinstance(key, str):
                continue
            if key in self._md:
                return False
        return True


class _CSMixin:
    _ci: ClassVar[bool] = False

    def _key(self, key: str) -> str:
        return key

    def _identity(self, key: str) -> str:
        if isinstance(key, str):
            return key
        else:
            raise TypeError("MultiDict keys should be either str or subclasses of str")


class _CIMixin:
    _ci: ClassVar[bool] = True

    def _key(self, key: str) -> str:
        if type(key) is istr:
            return key
        else:
            return istr(key)

    def _identity(self, key: str) -> str:
        if isinstance(key, istr):
            ret = key.__istr_identity__
            if ret is None:
                ret = key.lower()
                key.__istr_identity__ = ret
            return ret
        if isinstance(key, str):
            return key.lower()
        else:
            raise TypeError("MultiDict keys should be either str or subclasses of str")


def estimate_log2_keysize(n: int) -> int:
    # 7 == HT_MINSIZE - 1
    return (((n * 3 + 1) // 2) | 7).bit_length()


@dataclass
class _Entry(Generic[_V]):
    hash: int
    identity: str
    key: str
    value: _V


@dataclass
class _HtKeys(Generic[_V]):
    LOG_MINSIZE: ClassVar[int] = 3
    MINSIZE: ClassVar[int] = 8
    PREALLOCATED_INDICES: ClassVar[dict[int, array]] = {  # type: ignore[type-arg]
        log2_size: array(
            "b" if log2_size < 8 else "h", (-1 for i in range(1 << log2_size))
        )
        for log2_size in range(3, 10)
    }

    log2_size: int
    usable: int

    indices: array  # type: ignore[type-arg] # TODO(PY312): array[int]
    entries: list[_Entry[_V] | None]
    # first slot probed with perturb == 0 -> last slot used after it,
    # created on the first long probe
    resume_slots: dict[int, int] | None = None

    @functools.cached_property
    def nslots(self) -> int:
        return 1 << self.log2_size

    @functools.cached_property
    def mask(self) -> int:
        return self.nslots - 1

    if sys.implementation.name != "pypy":

        def __sizeof__(self) -> int:
            return (
                object.__sizeof__(self)
                + sys.getsizeof(self.indices)
                + sys.getsizeof(self.entries)
                + (0 if self.resume_slots is None else sys.getsizeof(self.resume_slots))
            )

    @classmethod
    def new(cls, log2_size: int, entries: list[_Entry[_V] | None]) -> Self:
        size = 1 << log2_size
        usable = (size << 1) // 3
        if log2_size < 10:
            indices = cls.PREALLOCATED_INDICES[log2_size].__copy__()
        elif log2_size < 16:
            indices = array("h", (-1 for i in range(size)))
        elif log2_size < 32:
            indices = array("l", (-1 for i in range(size)))
        else:  # pragma: no cover  # don't test huge multidicts
            indices = array("q", (-1 for i in range(size)))
        ret = cls(
            log2_size=log2_size,
            usable=usable,
            indices=indices,
            entries=entries,
        )
        return ret

    def clone(self) -> "_HtKeys[_V]":
        entries = [
            _Entry(e.hash, e.identity, e.key, e.value) if e is not None else None
            for e in self.entries
        ]

        return _HtKeys(
            log2_size=self.log2_size,
            usable=self.usable,
            indices=self.indices.__copy__(),
            entries=entries,
        )

    def build_indices(self, update: bool) -> None:
        mask = self.mask
        indices = self.indices
        for idx, e in enumerate(self.entries):
            assert e is not None
            hash_ = e.hash
            if update:
                hash_ &= MAXSIZE
            i = hash_ & mask
            perturb = hash_
            while indices[i] != -1:
                perturb >>= 5
                i = mask & (i * 5 + perturb + 1)
                if not perturb:
                    i = self._find_empty_slot_resume(i)
                    break
            indices[i] = idx

    def find_empty_slot(self, hash_: int) -> int:
        mask = self.mask
        indices = self.indices
        i = hash_ & mask
        perturb = hash_
        ix = indices[i]
        while ix != -1:
            perturb >>= 5
            i = (i * 5 + perturb + 1) & mask
            if not perturb:
                return self._find_empty_slot_resume(i)
            ix = indices[i]
        return i

    def _find_empty_slot_resume(self, start: int) -> int:
        mask = self.mask
        indices = self.indices
        resume_slots = self.resume_slots
        resume = None if resume_slots is None else resume_slots.get(start)
        # the resume slot is used, start after it
        i = start if resume is None else (resume * 5 + 1) & mask
        steps = 0
        while indices[i] != -1:
            i = (i * 5 + 1) & mask
            steps += 1
        if resume_slots is None:
            if (
                steps < _RESUME_SLOTS_MIN_STEPS
                or self.log2_size < _LOG_RESUME_SLOTS_MINSIZE
            ):
                return i
            resume_slots = self.resume_slots = {}
        resume_slots[start] = i
        return i

    def iter_hash(self, hash_: int) -> Iterator[tuple[int, int, _Entry[_V]]]:
        mask = self.mask
        indices = self.indices
        entries = self.entries
        i = hash_ & mask
        perturb = hash_
        ix = indices[i]
        while ix != -1:
            if ix != -2:
                e = entries[ix]
                if e.hash == hash_:
                    yield i, ix, e
            perturb >>= 5
            i = (i * 5 + perturb + 1) & mask
            ix = indices[i]

    def del_idx(self, hash_: int, idx: int) -> None:
        mask = self.mask
        indices = self.indices
        i = hash_ & mask
        perturb = hash_
        ix = indices[i]
        while ix != idx:
            perturb >>= 5
            i = (i * 5 + perturb + 1) & mask
            ix = indices[i]
        indices[i] = -2

    def iter_entries(self, reverse: bool = False) -> Iterator[_Entry[_V]]:
        entries = reversed(self.entries) if reverse else self.entries
        return filter(None, entries)

    def restore_hash(self, hash_: int) -> None:
        mask = self.mask
        indices = self.indices
        entries = self.entries
        i = hash_ & mask
        perturb = hash_
        ix = indices[i]
        while ix != -1:
            if ix != -2:
                entry = entries[ix]
                if entry.hash & HASH_MARK:
                    entry.hash = hash_
            perturb >>= 5
            i = (i * 5 + perturb + 1) & mask
            ix = indices[i]


class MultiDict(_CSMixin, MutableMultiMapping[_V]):
    """Dictionary with the support for duplicate keys."""

    __slots__ = ("_keys", "_used", "_version", "_lock")

    _lock: threading.RLock

    # __new__ runs once per object, never on a later re-init, so the lock
    # identity is stable even across `d.__init__(other)`. The lock itself
    # is always created (not just under free-threading): update()/
    # extend()/merge()/clear()/popitem() need it on every build, since
    # their race isn't free-threading-specific -- see `_locked_always`.
    def __new__(cls, *args: object, **kwargs: object) -> Self:
        self = super().__new__(cls)
        self._lock = threading.RLock()
        return self

    @_locked_pair_always
    def __init__(self, arg: MDArg[_V] = None, /, **kwargs: _V):
        self._used = 0
        self._incr_version()
        if not kwargs:
            md = None
            if isinstance(arg, MultiDictProxy):
                md = arg._md
            elif isinstance(arg, MultiDict):
                md = arg
            if md is not None and md._ci is self._ci:
                self._from_md(md)
                return

        it = self._parse_args(arg, kwargs)
        log2_size = estimate_log2_keysize(cast(int, next(it)))
        if log2_size > 17:  # pragma: no cover
            # Don't overallocate really huge keys space in init
            log2_size = 17
        self._keys: _HtKeys[_V] = _HtKeys.new(log2_size, [])
        self._extend_items(cast(Iterator[_Entry[_V]], it))

    def _from_md(self, md: "MultiDict[_V]") -> None:
        # Copy everything as-is without compacting the new multidict,
        # otherwise it requires reindexing
        self._keys = md._keys.clone()
        self._used = md._used

    @overload
    def getall(self, key: str) -> list[_V]: ...
    @overload
    def getall(self, key: str, default: _T) -> list[_V] | _T: ...
    @_locked
    def getall(self, key: str, default: _T | _SENTINEL = sentinel) -> list[_V] | _T:
        """Return a list of all values matching the key."""
        identity = self._identity(key)
        hash_ = hash(identity) & MAXSIZE
        marked = hash_ | HASH_MARK
        res = []
        restore = []
        for slot, idx, e in self._keys.iter_hash(hash_):
            if e.identity == identity:  # pragma: no branch
                res.append(e.value)
                e.hash = marked
                restore.append(idx)

        if res:
            entries = self._keys.entries
            for idx in restore:
                entries[idx].hash = hash_  # type: ignore[union-attr]
            return res
        if not res and default is not sentinel:
            return default
        raise KeyError(f"Key not found: {key!r}")

    @overload
    def getone(self, key: str) -> _V: ...
    @overload
    def getone(self, key: str, default: _T) -> _V | _T: ...
    @_locked
    def getone(self, key: str, default: _T | _SENTINEL = sentinel) -> _V | _T:
        """Get first value matching the key.

        Raises KeyError if the key is not found and no default is provided.
        """
        identity = self._identity(key)
        hash_ = hash(identity) & MAXSIZE
        for slot, idx, e in self._keys.iter_hash(hash_):
            if e.identity == identity:  # pragma: no branch
                return e.value
        if default is not sentinel:
            return default
        raise KeyError(f"Key not found: {key!r}")

    # Mapping interface #

    def __getitem__(self, key: str) -> _V:
        return self.getone(key)

    @overload
    def get(self, key: str, /) -> _V | None: ...
    @overload
    def get(self, key: str, /, default: _T) -> _V | _T: ...
    def get(self, key: str, default: _T | None = None) -> _V | _T | None:
        """Get first value matching the key.

        If the key is not found, returns the default (or None if no default is provided)
        """
        return self.getone(key, default)

    def __iter__(self) -> Iterator[str]:
        return iter(self.keys())

    def __len__(self) -> int:
        return self._used

    def keys(self) -> KeysView[str]:
        """Return a new view of the dictionary's keys."""
        return _KeysView(self)

    def items(self) -> ItemsView[str, _V]:
        """Return a new view of the dictionary's items as ``(key, value)`` pairs."""
        return _ItemsView(self)

    def values(self) -> _ValuesView[_V]:
        """Return a new view of the dictionary's values."""
        return _ValuesView(self)

    @_locked_pair
    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Mapping):
            return NotImplemented
        if isinstance(other, MultiDictProxy):
            return self == other._md
        if isinstance(other, MultiDict):
            lft = self._keys
            rht = other._keys
            if self._used != other._used:
                return False
            for e1, e2 in zip(lft.iter_entries(), rht.iter_entries()):
                if e1.identity != e2.identity or e1.value != e2.value:
                    return False
            return True
        if self._used != len(other):
            return False
        for k, v in self.items():
            nv = other.get(k, sentinel)
            if v != nv:
                return False
        return True

    @_locked
    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        identity = self._identity(key)
        hash_ = hash(identity) & MAXSIZE
        for slot, idx, e in self._keys.iter_hash(hash_):
            if e.identity == identity:  # pragma: no branch
                return True
        return False

    @reprlib.recursive_repr()
    @_locked
    def __repr__(self) -> str:
        body = ", ".join(f"{e.key!r}: {e.value!r}" for e in self._keys.iter_entries())
        return f"<{self.__class__.__name__}({body})>"

    if sys.implementation.name != "pypy":

        def __sizeof__(self) -> int:
            return object.__sizeof__(self) + sys.getsizeof(self._keys)

    def __reduce__(self) -> tuple[type[Self], tuple[list[tuple[str, _V]]]]:
        return (self.__class__, (list(self.items()),))

    @_locked
    def to_dict(self) -> dict[str, list[_V]]:
        """Return a dict with lists of all values for each key."""
        result: dict[str, list[_V]] = {}
        # Keyed by identity so a case-insensitive multidict groups every
        # spelling of a key together, and holding the list itself rather than
        # the first key so the value append needs no second lookup.
        seen: dict[str, list[_V]] = {}
        version = self._version
        for e in self._keys.iter_entries():
            values = seen.get(e.identity)
            if values is None:
                values = seen[e.identity] = [e.value]
                result[self._key(e.key)] = values
            else:
                values.append(e.value)
            if self._version != version:
                # Building and hashing a key both run a str subclass's own
                # code, which must not mutate what is being converted.
                raise RuntimeError("Dictionary changed during iteration")
        return result

    @_locked_always
    def add(self, key: str, value: _V) -> None:
        identity = self._identity(key)
        hash_ = hash(identity) & MAXSIZE
        self._add_with_hash(_Entry(hash_, identity, key, value))
        self._incr_version()

    def copy(self) -> Self:
        """Return a copy of itself."""
        cls = self.__class__
        return cls(self)

    __copy__ = copy

    @_locked_pair_always
    def extend(self, arg: MDArg[_V] = None, /, **kwargs: _V) -> None:
        """Extend current MultiDict with more values.

        This method must be used instead of update.
        """
        it = self._parse_args(arg, kwargs)
        newsize = self._used + cast(int, next(it))
        self._resize(estimate_log2_keysize(newsize), False)
        self._extend_items(cast(Iterator[_Entry[_V]], it))

    def _parse_args(
        self,
        arg: MDArg[_V],
        kwargs: Mapping[str, _V],
    ) -> Iterator[int | _Entry[_V]]:
        identity_func = self._identity
        if isinstance(arg, MultiDictProxy):
            if arg._md is self:
                arg = [(e.key, e.value) for e in self._keys.iter_entries()]
        elif arg is self:
            arg = [(e.key, e.value) for e in self._keys.iter_entries()]
        if arg:
            if isinstance(arg, MultiDictProxy):
                arg = arg._md
            if isinstance(arg, MultiDict):
                yield len(arg) + len(kwargs)
                if self._ci is not arg._ci:
                    for e in arg._keys.iter_entries():
                        identity = identity_func(e.key)
                        yield _Entry(hash(identity) & MAXSIZE, identity, e.key, e.value)
                else:
                    for e in arg._keys.iter_entries():
                        yield _Entry(e.hash, e.identity, e.key, e.value)
                if kwargs:
                    for key, value in kwargs.items():
                        identity = identity_func(key)
                        yield _Entry(hash(identity) & MAXSIZE, identity, key, value)
            else:
                if hasattr(arg, "keys"):
                    arg = cast(SupportsKeys[_V], arg)
                    arg = [(k, arg[k]) for k in arg.keys()]
                if kwargs:
                    arg = list(arg)
                    arg.extend(list(kwargs.items()))
                try:
                    yield len(arg)  # type: ignore[arg-type]
                except TypeError:
                    yield 0
                for pos, item in enumerate(arg):
                    if not len(item) == 2:
                        raise ValueError(
                            f"multidict update sequence element #{pos} "
                            f"has length {len(item)}; 2 is required"
                        )
                    try:
                        key = item[0]
                    except Exception as exc:
                        raise ValueError(
                            f"multidict update sequence element #{pos}'s "
                            f"key could not be fetched"
                        ) from exc
                    try:
                        value = item[1]
                    except Exception as exc:
                        raise ValueError(
                            f"multidict update sequence element #{pos}'s "
                            f"value could not be fetched"
                        ) from exc
                    identity = identity_func(key)
                    yield _Entry(hash(identity) & MAXSIZE, identity, key, value)
        else:
            yield len(kwargs)
            for key, value in kwargs.items():
                identity = identity_func(key)
                yield _Entry(hash(identity) & MAXSIZE, identity, key, value)

    def _extend_items(self, items: Iterable[_Entry[_V]]) -> None:
        for e in items:
            self._add_with_hash(e)
        self._incr_version()

    @_locked_always
    def clear(self) -> None:
        """Remove all items from MultiDict."""
        self._used = 0
        self._keys = _HtKeys.new(_HtKeys.LOG_MINSIZE, [])
        self._incr_version()

    # Mapping interface #

    @_locked_always
    def __setitem__(self, key: str, value: _V) -> None:
        identity = self._identity(key)
        hash_ = hash(identity) & MAXSIZE
        found = False

        for slot, idx, e in self._keys.iter_hash(hash_):
            if e.identity == identity:  # pragma: no branch
                if not found:
                    e.key = key
                    e.value = value
                    e.hash = hash_ | HASH_MARK
                    found = True
                    self._incr_version()
                elif not (e.hash & HASH_MARK):  # pragma: no branch
                    self._del_at(slot, idx)

        if not found:
            self._add_with_hash(_Entry(hash_, identity, key, value))
        else:
            self._keys.restore_hash(hash_)

    @_locked_always
    def __delitem__(self, key: str) -> None:
        found = False
        identity = self._identity(key)
        hash_ = hash(identity) & MAXSIZE
        for slot, idx, e in self._keys.iter_hash(hash_):
            if e.identity == identity:  # pragma: no branch
                self._del_at(slot, idx)
                found = True
        if not found:
            raise KeyError(key)
        else:
            self._incr_version()

    @overload
    def setdefault(
        self: "MultiDict[_T | None]", key: str, default: None = None
    ) -> _T | None: ...
    @overload
    def setdefault(self, key: str, default: _V) -> _V: ...
    @_locked_always  # type: ignore[misc]
    def setdefault(self, key: str, default: _V | None = None) -> _V | None:
        """Return value for key, set value to default if key is not present."""
        identity = self._identity(key)
        hash_ = hash(identity) & MAXSIZE
        for slot, idx, e in self._keys.iter_hash(hash_):
            if e.identity == identity:  # pragma: no branch
                return e.value
        self.add(key, default)  # type: ignore[arg-type]
        return default

    @overload
    def popone(self, key: str) -> _V: ...
    @overload
    def popone(self, key: str, default: _T) -> _V | _T: ...
    @_locked_always
    def popone(self, key: str, default: _T | _SENTINEL = sentinel) -> _V | _T:
        """Remove specified key and return the corresponding value.

        If key is not found, d is returned if given, otherwise
        KeyError is raised.

        """
        identity = self._identity(key)
        hash_ = hash(identity) & MAXSIZE
        for slot, idx, e in self._keys.iter_hash(hash_):
            if e.identity == identity:  # pragma: no branch
                value = e.value
                self._del_at(slot, idx)
                self._incr_version()
                return value
        if default is sentinel:
            raise KeyError(key)
        else:
            return default

    # Type checking will inherit signature for pop() if we don't confuse it here.
    if not TYPE_CHECKING:
        pop = popone

    @overload
    def popall(self, key: str) -> list[_V]: ...
    @overload
    def popall(self, key: str, default: _T) -> list[_V] | _T: ...
    @_locked_always
    def popall(self, key: str, default: _T | _SENTINEL = sentinel) -> list[_V] | _T:
        """Remove all occurrences of key and return the list of corresponding
        values.

        If key is not found, default is returned if given, otherwise
        KeyError is raised.

        """
        found = False
        identity = self._identity(key)
        hash_ = hash(identity) & MAXSIZE
        ret = []
        for slot, idx, e in self._keys.iter_hash(hash_):
            if e.identity == identity:  # pragma: no branch
                found = True
                ret.append(e.value)
                self._del_at(slot, idx)
                self._incr_version()

        if not found:
            if default is sentinel:
                raise KeyError(key)
            else:
                return default
        else:
            return ret

    @_locked_always
    def popitem(self) -> tuple[str, _V]:
        """Remove and return an arbitrary (key, value) pair."""
        if self._used <= 0:
            raise KeyError("empty multidict")

        entries = self._keys.entries
        pos = len(entries) - 1
        entry = entries[pos]
        while entry is None:
            pos -= 1
            entry = entries[pos]

        # Clear the indices slot before truncating entries, so a
        # concurrent unlocked read never sees an index pointing past
        # the end of the (now shorter) entries list.
        self._keys.del_idx(entry.hash, pos)
        del entries[pos:]

        ret = self._key(entry.key), entry.value
        self._used -= 1
        self._incr_version()
        return ret

    @_locked_pair_always
    def update(self, arg: MDArg[_V] = None, /, **kwargs: _V) -> None:
        """Update the dictionary, overwriting existing keys."""
        it = self._parse_args(arg, kwargs)
        newsize = self._used + cast(int, next(it))
        log2_size = estimate_log2_keysize(newsize)
        if log2_size > 17:  # pragma: no cover
            # Don't overallocate really huge keys space in update,
            # duplicate keys could reduce the resulting amount of entries
            log2_size = 17
        if log2_size > self._keys.log2_size:
            self._resize(log2_size, False)
        try:
            self._update_items(cast(Iterator[_Entry[_V]], it))
        finally:
            self._post_update()

    def _update_items(self, items: Iterator[_Entry[_V]]) -> None:
        for entry in items:
            found = False
            hash_ = entry.hash
            identity = entry.identity
            for slot, idx, e in self._keys.iter_hash(hash_):
                if e.identity == identity:  # pragma: no branch
                    if not found:
                        found = True
                        e.key = entry.key
                        e.value = entry.value
                        e.hash = hash_ | HASH_MARK
                    else:
                        self._del_at_for_upd(e)
            if not found:
                self._add_with_hash_for_upd(entry)

    def _post_update(self) -> None:
        keys = self._keys
        indices = keys.indices
        entries = keys.entries
        for slot in range(keys.nslots):
            idx = indices[slot]
            if idx >= 0:
                e2 = entries[idx]
                assert e2 is not None
                if e2.key is None:
                    entries[idx] = None
                    indices[slot] = -2
                    self._used -= 1
                h = e2.hash
                if h & HASH_MARK:
                    e2.hash = h & MAXSIZE

        self._incr_version()

    @_locked_pair_always
    def merge(self, arg: MDArg[_V] = None, /, **kwargs: _V) -> None:
        """Merge into the dictionary, adding non-existing keys."""
        it = self._parse_args(arg, kwargs)
        newsize = self._used + cast(int, next(it))
        log2_size = estimate_log2_keysize(newsize)
        if log2_size > 17:  # pragma: no cover
            # Don't overallocate really huge keys space in update,
            # duplicate keys could reduce the resulting amount of entries
            log2_size = 17
        if log2_size > self._keys.log2_size:
            self._resize(log2_size, False)
        try:
            self._merge_items(cast(Iterator[_Entry[_V]], it))
        finally:
            self._post_update()

    def _merge_items(self, items: Iterator[_Entry[_V]]) -> None:
        for entry in items:
            hash_ = entry.hash
            identity = entry.identity
            for slot, idx, e in self._keys.iter_hash(hash_):
                if e.identity == identity:  # pragma: no branch
                    break
            else:
                self._add_with_hash_for_upd(entry)

    if _FREE_THREADED:
        # `_version` is shared by every instance, so it needs its own lock.
        def _incr_version(self) -> None:
            with _VERSION_LOCK:
                v = _version[0] + 1
                _version[0] = v
            self._version = v

    else:

        def _incr_version(self) -> None:
            v = _version
            v[0] += 1
            self._version = v[0]

    def _resize(self, log2_newsize: int, update: bool) -> None:
        oldkeys = self._keys
        newentries = self._used

        if len(oldkeys.entries) == newentries:
            entries = oldkeys.entries
        else:
            entries = [e for e in oldkeys.entries if e is not None]
        newkeys: _HtKeys[_V] = _HtKeys.new(log2_newsize, entries)
        newkeys.usable -= newentries
        newkeys.build_indices(update)
        self._keys = newkeys

    def _add_with_hash(self, entry: _Entry[_V]) -> None:
        if self._keys.usable <= 0:
            self._resize((self._used * 3 | _HtKeys.MINSIZE - 1).bit_length(), False)
        keys = self._keys
        slot = keys.find_empty_slot(entry.hash)
        keys.entries.append(entry)
        keys.indices[slot] = len(keys.entries) - 1
        self._incr_version()
        self._used += 1
        keys.usable -= 1

    def _add_with_hash_for_upd(self, entry: _Entry[_V]) -> None:
        if self._keys.usable <= 0:
            self._resize((self._used * 3 | _HtKeys.MINSIZE - 1).bit_length(), True)
        keys = self._keys
        hash_ = entry.hash
        slot = keys.find_empty_slot(hash_)
        entry.hash = hash_ | HASH_MARK
        keys.entries.append(entry)
        keys.indices[slot] = len(keys.entries) - 1
        self._incr_version()
        self._used += 1
        keys.usable -= 1

    def _del_at(self, slot: int, idx: int) -> None:
        self._keys.entries[idx] = None
        self._keys.indices[slot] = -2
        self._used -= 1

    def _del_at_for_upd(self, entry: _Entry[_V]) -> None:
        entry.key = None  # type: ignore[assignment]
        entry.value = None  # type: ignore[assignment]


class CIMultiDict(_CIMixin, MultiDict[_V]):
    """Dictionary with the support for duplicate case-insensitive keys."""


class MultiDictProxy(_CSMixin, MultiMapping[_V]):
    """Read-only proxy for MultiDict instance."""

    __slots__ = ("_md",)

    _md: MultiDict[_V]

    def __init__(self, arg: "MultiDict[_V] | MultiDictProxy[_V]"):
        if not isinstance(arg, (MultiDict, MultiDictProxy)):
            raise TypeError(
                f"ctor requires MultiDict or MultiDictProxy instance, not {type(arg)}"
            )
        if isinstance(arg, MultiDictProxy):
            self._md = arg._md
        else:
            self._md = arg

    def __reduce__(self) -> NoReturn:
        raise TypeError(f"can't pickle {self.__class__.__name__} objects")

    @overload
    def getall(self, key: str) -> list[_V]: ...
    @overload
    def getall(self, key: str, default: _T) -> list[_V] | _T: ...
    def getall(self, key: str, default: _T | _SENTINEL = sentinel) -> list[_V] | _T:
        """Return a list of all values matching the key."""
        if default is not sentinel:
            return self._md.getall(key, default)
        else:
            return self._md.getall(key)

    @overload
    def getone(self, key: str) -> _V: ...
    @overload
    def getone(self, key: str, default: _T) -> _V | _T: ...
    def getone(self, key: str, default: _T | _SENTINEL = sentinel) -> _V | _T:
        """Get first value matching the key.

        Raises KeyError if the key is not found and no default is provided.
        """
        if default is not sentinel:
            return self._md.getone(key, default)
        else:
            return self._md.getone(key)

    # Mapping interface #

    def __getitem__(self, key: str) -> _V:
        return self.getone(key)

    @overload
    def get(self, key: str, /) -> _V | None: ...
    @overload
    def get(self, key: str, /, default: _T) -> _V | _T: ...
    def get(self, key: str, default: _T | None = None) -> _V | _T | None:
        """Get first value matching the key.

        If the key is not found, returns the default (or None if no default is provided)
        """
        return self._md.getone(key, default)

    def __iter__(self) -> Iterator[str]:
        return iter(self._md.keys())

    def __len__(self) -> int:
        return len(self._md)

    def keys(self) -> KeysView[str]:
        """Return a new view of the dictionary's keys."""
        return self._md.keys()

    def items(self) -> ItemsView[str, _V]:
        """Return a new view of the dictionary's items as ``(key, value)`` pairs."""
        return self._md.items()

    def values(self) -> _ValuesView[_V]:
        """Return a new view of the dictionary's values."""
        return self._md.values()

    def __eq__(self, other: object) -> bool:
        return self._md == other

    def __contains__(self, key: object) -> bool:
        return key in self._md

    @reprlib.recursive_repr()
    def __repr__(self) -> str:
        body = ", ".join(f"{k!r}: {v!r}" for k, v in self.items())
        return f"<{self.__class__.__name__}({body})>"

    def to_dict(self) -> dict[str, list[_V]]:
        """Return a dict with lists of all values for each key."""
        return self._md.to_dict()

    def copy(self) -> MultiDict[_V]:
        """Return a copy of itself."""
        return MultiDict(self._md)


class CIMultiDictProxy(_CIMixin, MultiDictProxy[_V]):
    """Read-only proxy for CIMultiDict instance."""

    def __init__(self, arg: MultiDict[_V] | MultiDictProxy[_V]):
        if not isinstance(arg, (CIMultiDict, CIMultiDictProxy)):
            raise TypeError(
                "ctor requires CIMultiDict or CIMultiDictProxy instance"
                f", not {type(arg)}"
            )

        super().__init__(arg)

    def copy(self) -> CIMultiDict[_V]:
        """Return a copy of itself."""
        return CIMultiDict(self._md)


def getversion(md: MultiDict[object] | MultiDictProxy[object]) -> int:
    if isinstance(md, MultiDictProxy):
        md = md._md
    elif not isinstance(md, MultiDict):
        raise TypeError("Parameter should be multidict or proxy")
    return md._version
