#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

import datetime
import logging
import os
import pickle
import platform
import random
import string
import tempfile
from collections.abc import Iterator
from threading import Lock
from typing import Generic, NoReturn, TypeVar

from filelock import FileLock, Timeout
from typing_extensions import NamedTuple, Self

from . import constants

now = datetime.datetime.now
getmtime = os.path.getmtime

T = TypeVar("T")

logger = logging.getLogger(__name__)


class CacheEntry(NamedTuple, Generic[T]):
    expiry: datetime.datetime
    entry: T


K = TypeVar("K")
V = TypeVar("V")


def is_expired(d: datetime.datetime) -> bool:
    return now() >= d


class SFDictCache(Generic[K, V]):
    """A generic in-memory cache that acts somewhat like a dictionary.

    Unlike normal dictionaries keys(), values() and items() return list materialized
    at call time, unlike normal dictionaries that return a view object.
    """

    def __init__(
        self,
        entry_lifetime: int = constants.DAY_IN_SECONDS,
    ) -> None:
        """Inits a SFDictCache with lifetime."""
        self._entry_lifetime = datetime.timedelta(seconds=entry_lifetime)
        self._cache: dict[K, CacheEntry[V]] = {}
        self._lock = Lock()
        self._reset_telemetry()
        # aliasing _getitem to unify the api with SFDictFileCache
        self._getitem_non_locking = self._getitem

    def __len__(self) -> int:
        with self._lock:
            return len(self._cache)

    @classmethod
    def from_dict(
        cls,
        _dict: dict[K, V],
        **kw,
    ) -> Self:
        """Create an dictionary cache from an already existing dictionary.

        Note that the same references will be stored in the cache than in
        the dictionary provided.
        """
        cache = cls(**kw)
        for k, v in _dict.items():
            cache[k] = v
        return cache

    def _getitem(
        self,
        k: K,
        *,
        should_record_hits: bool = True,
    ) -> V:
        """Non-locking version of __getitem__.

        This should only be used by internal functions when already
        holding self._lock.
        """
        try:
            t, v = self._cache[k]
        except KeyError:
            self._miss(k)
            raise
        if is_expired(t):
            self._expire(k)
        if should_record_hits:
            self._hit(k)
        return v

    def _setitem(self, k: K, v: V) -> None:
        """Non-locking version of __setitem__.

        This should only be used by internal functions when already
        holding self._lock.
        """
        self._cache[k] = CacheEntry(
            expiry=now() + self._entry_lifetime,
            entry=v,
        )
        self._add_or_remove()

    def __getitem__(
        self,
        k: K,
    ) -> V:
        """Returns an element if it hasn't expired yet in a thread-safe way."""
        with self._lock:
            return self._getitem(k, should_record_hits=True)

    def __setitem__(
        self,
        k: K,
        v: V,
    ) -> None:
        """Inserts an element in a thread-safe way."""
        with self._lock:
            self._setitem(k, v)

    def __iter__(self) -> Iterator[K]:
        return iter(self.keys())

    def keys(self) -> list[K]:
        return [k for k, _ in self.items()]

    def items(self) -> list[tuple[K, V]]:
        with self._lock:
            values: list[tuple[K, V]] = []
            for k in list(self._cache.keys()):
                try:
                    values.append((k, self._getitem(k, should_record_hits=False)))
                except KeyError:
                    pass
        return values

    def values(self) -> list[V]:
        return [v for _, v in self.items()]

    def get(
        self,
        k: K,
        default: V | None = None,
    ) -> V | None:
        try:
            return self[k]
        except KeyError:
            return default

    def clear(self) -> None:
        with self._lock:
            self._cache.clear()
            self._reset_telemetry()

    def _delitem(
        self,
        key: K,
    ) -> None:
        """Non-locking version of __delitem__.

        This should only be used by internal functions when already
        holding self._lock.
        """
        del self._cache[key]
        self._add_or_remove()

    def __delitem__(
        self,
        key: K,
    ) -> None:
        with self._lock:
            self._delitem(key)

    def __contains__(
        self,
        key: K,
    ) -> bool:
        with self._lock:
            try:
                self._getitem(key, should_record_hits=True)
                return True
            except KeyError:
                # Fall through
                return False

    def _update(
        self,
        other: dict[K, V] | list[tuple[K, V]] | SFDictCache[K, V],
        update_newer_only: bool = False,
    ) -> bool:
        to_insert: dict[K, CacheEntry[V]]
        self._clear_expired_entries()
        if isinstance(other, (list, dict)):
            expiry = now() + self._entry_lifetime
            if isinstance(other, list):
                g = iter(other)
            elif isinstance(other, dict):
                g = iter(other.items())
            to_insert = {k: CacheEntry(expiry=expiry, entry=v) for k, v in g}
        elif isinstance(other, SFDictCache):
            other._clear_expired_entries()
            others_items = list(other._cache.items())
            # Only accept values from another cache if their key is not in self,
            #  or if expiry is later the self known one
            to_insert = {
                k: v
                for k, v in others_items
                if (
                    # self doesn't have this key
                    k not in self._cache
                    # we should update entries, regardless of whether they are newer
                    or (not update_newer_only)
                    # other has newer expiry time we want to update newer values only
                    or self._cache[k].expiry < v.expiry
                )
            }
        else:
            raise TypeError
        self._cache.update(to_insert)
        self._add_or_remove()
        return len(to_insert) > 0

    def update(
        self,
        other: dict[K, V] | list[tuple[K, V]] | SFDictCache[K, V],
    ) -> bool:
        """Insert multiple values at the same time, if self could learn from the other.

        If this function is given a dictionary, or list expiration timestamps
        will be all the same a self._entry_lifetime form now. If it's
        given another SFDictCache then the timestamps will be taken
        from the other cache.

        Returns a boolean. It describes whether self learnt anything from other.

        Note that clear_expired_entries will be called on both caches. To
        prevent deadlocks this is done without acquiring other._lock. The
        intended behavior is to use this function with an unpickled/unused cache.
        If live caches are being merged then use .items() on them first and merge those
        into the other caches.
        """
        with self._lock:
            return self._update(other)

    def update_newer(
        self,
        other: dict[K, V] | list[tuple[K, V]] | SFDictCache[K, V],
    ) -> bool:
        """This function is like update, but it only updates newer elements."""
        with self._lock:
            return self._update(
                other,
                update_newer_only=True,
            )

    def _clear_expired_entries(self) -> None:
        for k in list(self._cache.keys()):
            try:
                self._getitem(k, should_record_hits=False)
            except KeyError:
                continue
        self._add_or_remove()

    def clear_expired_entries(self) -> None:
        """Remove expired entries from the cache."""
        with self._lock:
            self._clear_expired_entries()

    # Telemetry related functions, these can be plugged by child classes
    def _reset_telemetry(self) -> None:
        """(Re)set telemetry fields.

        This function will be called by the initalizer and other functions that should
        reset telemtry entries.
        """
        self.telemetry = {
            "hit": 0,
            "miss": 0,
            "expiration": 0,
            "size": 0,
        }

    def _hit(self, k: K) -> None:
        """This function gets called when a hit occurs.

        Functions that hit every entry (like values) is not going to count.

        Note that while this function does not interact with lock, but it's only
        called from contexts where the lock is already held.
        """
        self.telemetry["hit"] += 1

    def _miss(self, k: K) -> None:
        """This function gets called when a miss occurs.

        Note that while this function does not interact with lock, but it's only
        called from contexts where the lock is already held.
        """
        self.telemetry["miss"] += 1

    def _expiration(self, k: K) -> None:
        """This function gets called when an expiration occurs.

        Note that while this function does not interact with lock, but it's only
        called from contexts where the lock is already held.
        """
        self.telemetry["expiration"] += 1

    def _expire(self, k: K) -> NoReturn:
        """Helper function to call _expiration and delete an item."""
        self._expiration(k)
        self._delitem(k)
        raise KeyError

    def _add_or_remove(self) -> None:
        """This function gets called when an element is added, or removed.

        Note that while this function does not interact with lock, but it's only
        called from contexts where the lock is already held.
        """
        self.telemetry["size"] = len(self._cache)


class SFDictFileCache(SFDictCache):

    # This number decides the chance of saving after writing (probability: 1/n+1)
    MAX_RAND_INT = 9

    def __init__(
        self,
        file_path: str | dict[str, str],
        entry_lifetime: int = constants.DAY_IN_SECONDS,
        file_timeout: int = 0,
    ) -> None:
        """Inits an SFDictFileCache with path, lifetime.

        File path can be a dictionary that contains different paths for different OSes,
        possible keys are: 'darwin', 'linux' and 'windows'. If a current platform
        cannot be determined, or is not in the dictionary we'll use the first value.

        Once we select a location based on file path, we write and read a random
        temporary file to check for read/write permissions. If this fails OSError might
        be thrown.
        """
        super().__init__(
            entry_lifetime=entry_lifetime,
        )
        if isinstance(file_path, str):
            self.file_path = os.path.expanduser(file_path)
        else:
            current_platform = platform.system().lower()
            if current_platform is None or current_platform not in file_path:
                self.file_path = next(iter(file_path.values()))
            else:
                self.file_path = os.path.expanduser(file_path[current_platform])
        # Once we decided on where to put the file cache make sure that this
        #  place is readable/writable by us
        random_string = "".join(random.choice(string.ascii_letters) for _ in range(5))
        cache_folder = os.path.dirname(self.file_path)
        try:
            tmp_file, tmp_file_path = tempfile.mkstemp(
                dir=cache_folder,
            )
        except OSError as o_err:
            raise PermissionError(
                o_err.errno,
                "Cache folder is not writeable",
                cache_folder,
            )
        try:
            with open(tmp_file, "w") as w_file:
                # If mkstemp didn't fail this shouldn't throw an error
                w_file.write(random_string)
            try:
                with open(tmp_file_path) as r_file:
                    if r_file.read() != random_string:
                        Exception("Temporary file just written has wrong content")
            except OSError as o_err:
                raise PermissionError(
                    o_err.errno,
                    "Cache file is not readable",
                    tmp_file_path,
                )
        finally:
            if os.path.exists(tmp_file_path) and os.path.isfile(tmp_file_path):
                os.unlink(tmp_file_path)
        self.file_timeout = file_timeout
        self._file_lock_path = f"{self.file_path}.lock"
        self._file_lock = FileLock(self._file_lock_path, timeout=self.file_timeout)
        self.last_loaded: datetime.datetime | None = None
        if os.path.exists(self.file_path):
            self._load()

    def _getitem_non_locking(
        self,
        k: K,
        *,
        should_record_hits: bool = True,
    ) -> V:
        """Non-locking version of __getitem__ of SFDictFileCache.

        This should only be used by internal functions when already
        holding self._lock.

        Note that we do not overwrite _getitem because _getitem is used by
        self._load to clear in-memory expired caches. Overwriting would cause
        infinite recursive call.
        """
        if k not in self._cache:
            loaded = self._load_if_should()
            if (not loaded) or k not in self._cache:
                self._miss(k)
                raise KeyError
        t, v = self._cache[k]
        if is_expired(t):
            loaded = self._load_if_should()
            expire_item = True
            if loaded:
                t, v = self._cache[k]
                expire_item = is_expired(t)
            if expire_item:
                # Raises KeyError
                self._expire(k)
        self._hit(k)
        return v

    def __getitem__(self, k: K) -> V:
        """Returns an element if it hasn't expired yet in a thread-safe way."""
        self._lock.acquire()
        # TODO: This variable could be replaced by a wrapper class that keeps track
        #  of whether the lock is locked, but unless this function gets extended I
        #  feel like it's an overkill. Make sure to change the bool right after
        #  self._lock.acquire() and self._lock.release().
        currently_holding = True
        try:
            if k not in self._cache:
                self._lock.release()
                currently_holding = False
                loaded = self._load_if_should()
                self._lock.acquire()
                currently_holding = True
                if (not loaded) or k not in self._cache:
                    self._miss(k)
                    raise KeyError
            t, v = self._cache[k]
            if is_expired(t):
                self._lock.release()
                currently_holding = False
                loaded = self._load_if_should()
                self._lock.acquire()
                currently_holding = True
                expire_item = True
                if loaded:
                    t, v = self._cache[k]
                    expire_item = is_expired(t)
                if expire_item:
                    # Raises KeyError
                    self._expire(k)
            self._hit(k)
            return v
        finally:
            if currently_holding:
                self._lock.release()

    def _setitem(self, k: K, v: V) -> None:
        super()._setitem(k, v)
        self._save_if_should()

    def _load(self) -> bool:
        """Load cache from disk if possible, returns whether it was able to load."""
        try:
            with open(self.file_path, "rb") as r_file:
                other = pickle.load(r_file)
            self._update(
                other,
                update_newer_only=True,
            )
            self.last_loaded = now()
            return True
        except OSError:
            return False

    def _save(self, load_first: bool = True) -> bool:
        """Save cache to disk if possible, returns whether it was able to save."""
        self._clear_expired_entries()
        try:
            with self._file_lock:
                if load_first:
                    self._load_if_should()
                _dir, fname = os.path.split(self.file_path)
                try:
                    tmp_file, tmp_file_path = tempfile.mkstemp(
                        prefix=fname,
                        dir=_dir,
                    )
                    with open(tmp_file, "wb") as w_file:
                        pickle.dump(self, w_file)
                    # We write to a tmp file and then move it to have atomic write
                    os.replace(tmp_file_path, self.file_path)
                    self.last_loaded = datetime.datetime.fromtimestamp(
                        getmtime(self.file_path),
                    )
                    return True
                except OSError as o_err:
                    raise PermissionError(
                        o_err.errno,
                        "Cache folder is not writeable",
                        _dir,
                    )
                finally:
                    if os.path.exists(tmp_file_path) and os.path.isfile(tmp_file_path):
                        os.unlink(tmp_file_path)
        except Timeout:
            logger.debug(
                f"acquiring {self._file_lock_path} timed out, skipping saving..."
            )
        return False

    def _save_if_should(self) -> bool:
        """Saves file to disk if necessary and returns whether it saved.

        Uses self._should_save to decide whether to save.
        """
        if self._should_save():
            return self._save()
        return False

    def _load_if_should(self) -> bool:
        """Load file to disk if necessary and returns whether it loaded.

        Uses self._should_load to decide whether to load.
        """
        if self._should_load():
            return self._load()
        return False

    def _should_save(self) -> bool:
        """Decide whether we should save.

        This is a simple random number generator to randomize writes across processes
        that are possibly saving the same values in this cache.
        """
        return random.randint(0, self.MAX_RAND_INT) == 0

    def _should_load(self) -> bool:
        """Decide whether we should load.

        We should load if the file on disk has changed since we have last read it.
        """
        if os.path.exists(self.file_path) and os.path.isfile(self.file_path):
            if self.last_loaded is None:
                return True
            return (
                datetime.datetime.fromtimestamp(
                    getmtime(self.file_path),
                )
                > self.last_loaded
            )
        return False

    def __del__(self) -> None:
        try:
            self._save()
        except Exception:
            # At tear-down time builtins module might be already gone, ignore every error
            pass

    def clear_expired_entries(self) -> None:
        super().clear_expired_entries()
        self._save_if_should()

    def clear(self) -> None:
        super().clear()
        # This unlink prevents us from loading just before saving
        with self._file_lock:
            if os.path.exists(self.file_path) and os.path.isfile(self.file_path):
                os.unlink(self.file_path)
        self._save(load_first=False)

    # Custom pickling implementation

    def __getstate__(self) -> dict:
        state = self.__dict__.copy()
        del state["_lock"]
        del state["_file_lock"]
        return state

    def __setstate__(self, state: dict) -> None:
        self.__dict__.update(state)
        self._lock = Lock()
        self._file_lock = FileLock(self._file_lock_path, timeout=self.file_timeout)
