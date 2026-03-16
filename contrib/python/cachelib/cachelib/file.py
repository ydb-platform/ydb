import errno
import hashlib
import logging
import os
import platform
import stat
import struct
import tempfile
import typing as _t
from contextlib import contextmanager
from pathlib import Path
from time import sleep
from time import time

from cachelib.base import BaseCache
from cachelib.serializers import FileSystemSerializer


def _lazy_md5(string: bytes = b"") -> _t.Any:
    """Don't access ``hashlib.md5`` until runtime. FIPS builds may not include
    md5, in which case the import and use as a default would fail before the
    developer can configure something else.
    """
    return hashlib.md5(string)


class FileSystemCache(BaseCache):
    """A cache that stores the items on the file system.  This cache depends
    on being the only user of the `cache_dir`.  Make absolutely sure that
    nobody but this cache stores files there or otherwise the cache will
    randomly delete files therein.

    :param cache_dir: the directory where cache files are stored.
    :param threshold: the maximum number of items the cache stores before
                      it starts deleting some. A threshold value of 0
                      indicates no threshold.
    :param default_timeout: the default timeout that is used if no timeout is
                            specified on :meth:`~BaseCache.set`. A timeout of
                            0 indicates that the cache never expires.
    :param mode: the file mode wanted for the cache files, default 0600
    :param hash_method: Default hashlib.md5. The hash method used to
                        generate the filename for cached results.
                        Default is lazy loaded and can be overriden by
                        seeting  `_default_hash_method`
    """

    #: used for temporary files by the FileSystemCache
    _fs_transaction_suffix = ".__wz_cache"
    #: keep amount of files in a cache element
    _fs_count_file = "__wz_cache_count"
    #: default file name hashing method
    _default_hash_method = staticmethod(_lazy_md5)

    serializer = FileSystemSerializer()

    def __init__(
        self,
        cache_dir: str,
        threshold: int = 500,
        default_timeout: int = 300,
        mode: _t.Optional[int] = None,
        hash_method: _t.Any = None,
    ):
        BaseCache.__init__(self, default_timeout)
        self._path = cache_dir
        self._threshold = threshold

        self._hash_method = self._default_hash_method
        if hash_method is not None:
            self._hash_method = hash_method

        # Mode set by user takes precedence. If no mode has
        # been given, we need to set the correct default based
        # on user platform.
        self._mode = mode
        if self._mode is None:
            self._mode = self._get_compatible_platform_mode()

        try:
            os.makedirs(self._path)
        except OSError as ex:
            if ex.errno != errno.EEXIST:
                raise

        # If there are many files and a zero threshold,
        # the list_dir can slow initialisation massively
        if self._threshold != 0:
            self._update_count(value=len(list(self._list_dir())))

    def _get_compatible_platform_mode(self) -> int:
        mode = 0o600  # nix systems
        if platform.system() == "Windows":
            mode = stat.S_IWRITE
        return mode

    @property
    def _file_count(self) -> int:
        return self.get(self._fs_count_file) or 0

    def _update_count(
        self, delta: _t.Optional[int] = None, value: _t.Optional[int] = None
    ) -> None:
        # If we have no threshold, don't count files
        if self._threshold == 0:
            return
        if delta:
            new_count = self._file_count + delta
        else:
            new_count = value or 0
        self.set(self._fs_count_file, new_count, mgmt_element=True)

    def _normalize_timeout(self, timeout: _t.Optional[int]) -> int:
        timeout = BaseCache._normalize_timeout(self, timeout)
        if timeout != 0:
            timeout = int(time()) + timeout
        return int(timeout)

    def _is_mgmt(self, name: str) -> bool:
        fshash = self._get_filename(self._fs_count_file).split(os.sep)[-1]
        return name == fshash or name.endswith(self._fs_transaction_suffix)

    def _list_dir(self) -> _t.Generator[str, None, None]:
        """return a list of (fully qualified) cache filenames"""
        return (
            os.path.join(self._path, fn)
            for fn in os.listdir(self._path)
            if not self._is_mgmt(fn)
        )

    def _over_threshold(self) -> bool:
        return self._threshold != 0 and self._file_count > self._threshold

    def _remove_expired(self, now: float) -> None:
        for fname in self._list_dir():
            try:
                with self._safe_stream_open(fname, "rb") as f:
                    expires = struct.unpack("I", f.read(4))[0]
                if expires != 0 and expires < now:
                    os.remove(fname)
                    self._update_count(delta=-1)
            except FileNotFoundError:
                pass
            except (OSError, EOFError, struct.error):
                logging.warning(
                    "Exception raised while handling cache file '%s'",
                    fname,
                    exc_info=True,
                )

    def _remove_older(self) -> bool:
        exp_fname_tuples = []
        for fname in self._list_dir():
            try:
                with self._safe_stream_open(fname, "rb") as f:
                    timestamp = struct.unpack("I", f.read(4))[0]
                    exp_fname_tuples.append((timestamp, fname))
            except FileNotFoundError:
                pass
            except (OSError, EOFError, struct.error):
                logging.warning(
                    "Exception raised while handling cache file '%s'",
                    fname,
                    exc_info=True,
                )
        fname_sorted = (
            fname for _, fname in sorted(exp_fname_tuples, key=lambda item: item[0])
        )
        for fname in fname_sorted:
            try:
                os.remove(fname)
                self._update_count(delta=-1)
            except FileNotFoundError:
                pass
            except OSError:
                logging.warning(
                    "Exception raised while handling cache file '%s'",
                    fname,
                    exc_info=True,
                )
                return False
            if not self._over_threshold():
                break
        return True

    def _prune(self) -> None:
        if self._over_threshold():
            now = time()
            self._remove_expired(now)
        # if still over threshold
        if self._over_threshold():
            self._remove_older()

    def clear(self) -> bool:
        for i, fname in enumerate(self._list_dir()):
            try:
                os.remove(fname)
            except FileNotFoundError:
                pass
            except OSError:
                logging.warning(
                    "Exception raised while handling cache file '%s'",
                    fname,
                    exc_info=True,
                )
                self._update_count(delta=-i)
                return False
        self._update_count(value=0)
        return True

    def _get_filename(self, key: str) -> str:
        if isinstance(key, str):
            bkey = key.encode("utf-8")  # XXX unicode review
            bkey_hash = self._hash_method(bkey).hexdigest()
        else:
            raise TypeError(f"Key must be a string, received type {type(key)}")
        return os.path.join(self._path, bkey_hash)

    def get(self, key: str) -> _t.Any:
        filename = self._get_filename(key)
        try:
            with self._safe_stream_open(filename, "rb") as f:
                pickle_time = struct.unpack("I", f.read(4))[0]
                if pickle_time == 0 or pickle_time >= time():
                    return self.serializer.load(f)
        except FileNotFoundError:
            pass
        except (OSError, EOFError, struct.error):
            logging.warning(
                "Exception raised while handling cache file '%s'",
                filename,
                exc_info=True,
            )
        return None

    def add(self, key: str, value: _t.Any, timeout: _t.Optional[int] = None) -> bool:
        filename = self._get_filename(key)
        if not os.path.exists(filename):
            return self.set(key, value, timeout)
        return False

    def set(
        self,
        key: str,
        value: _t.Any,
        timeout: _t.Optional[int] = None,
        mgmt_element: bool = False,
    ) -> bool:
        # Management elements have no timeout
        if mgmt_element:
            timeout = 0
        # Don't prune on management element update, to avoid loop
        else:
            self._prune()

        timeout = self._normalize_timeout(timeout)
        filename = self._get_filename(key)
        overwrite = os.path.isfile(filename)

        try:
            fd, tmp = tempfile.mkstemp(
                suffix=self._fs_transaction_suffix, dir=self._path
            )
            with os.fdopen(fd, "wb") as f:
                f.write(struct.pack("I", timeout))
                self.serializer.dump(value, f)

            self._run_safely(os.replace, tmp, filename)
            self._run_safely(os.chmod, filename, self._mode)

            fsize = Path(filename).stat().st_size
        except OSError:
            logging.warning(
                "Exception raised while handling cache file '%s'",
                filename,
                exc_info=True,
            )
            return False
        else:
            # Management elements should not count towards threshold
            if not overwrite and not mgmt_element:
                self._update_count(delta=1)
            return fsize > 0  # function should fail if file is empty

    def delete(self, key: str, mgmt_element: bool = False) -> bool:
        try:
            os.remove(self._get_filename(key))
        except FileNotFoundError:  # if file doesn't exist we consider it deleted
            return True
        except OSError:
            logging.warning("Exception raised while handling cache file", exc_info=True)
            return False
        else:
            # Management elements should not count towards threshold
            if not mgmt_element:
                self._update_count(delta=-1)
            return True

    def has(self, key: str) -> bool:
        filename = self._get_filename(key)
        try:
            with self._safe_stream_open(filename, "rb") as f:
                pickle_time = struct.unpack("I", f.read(4))[0]
                if pickle_time == 0 or pickle_time >= time():
                    return True
                else:
                    return False
        except FileNotFoundError:  # if there is no file there is no key
            return False
        except (OSError, EOFError, struct.error):
            logging.warning(
                "Exception raised while handling cache file '%s'",
                filename,
                exc_info=True,
            )
            return False

    def _run_safely(self, fn: _t.Callable, *args: _t.Any, **kwargs: _t.Any) -> _t.Any:
        """On Windows os.replace, os.chmod and open can yield
        permission errors if executed by two different processes."""
        if platform.system() == "Windows":
            output = None
            wait_step = 0.001
            max_sleep_time = 10.0
            total_sleep_time = 0.0

            while total_sleep_time < max_sleep_time:
                try:
                    output = fn(*args, **kwargs)
                except PermissionError:
                    sleep(wait_step)
                    total_sleep_time += wait_step
                    wait_step *= 2
                else:
                    break
        else:
            output = fn(*args, **kwargs)

        return output

    @contextmanager
    def _safe_stream_open(self, path: str, mode: str) -> _t.Generator:
        fs = self._run_safely(open, path, mode)
        if fs is None:
            raise OSError
        try:
            yield fs
        finally:
            fs.close()
