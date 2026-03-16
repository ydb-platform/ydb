#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

import datetime
import logging
import os
import os.path
import pickle
import stat
from unittest import mock

import pytest

from snowflake.connector.compat import IS_WINDOWS

try:
    import snowflake.connector.cache as cache
except ImportError:
    cache = None

# Used to insert entries that expire instantaneously
NO_LIFETIME = datetime.timedelta(seconds=0)


class TestSFDictCache:
    def test_simple_usage(self):
        c = cache.SFDictCache.from_dict({1: "a", 2: "b"})
        assert 1 in c and 2 in c
        assert c[1] == "a"
        assert c[2] == "b"

    def test_miss(self):
        c = cache.SFDictCache.from_dict(
            {"a": 1},
        )
        with pytest.raises(KeyError):
            c["b"]

    def test_expiration(self):
        c = cache.SFDictCache.from_dict(
            {"a": 1},
            entry_lifetime=0,
        )
        with pytest.raises(KeyError):
            c[1]

    def test_access_empty(self):
        c = cache.SFDictCache()
        with pytest.raises(KeyError):
            c[1]

    # The rest of tests test that SFDictCache acts like a regular dictionary.

    def test_cast_list(self):
        c = cache.SFDictCache.from_dict({"a": 1, "b": 2})
        assert list(c) == ["a", "b"]

    def test_access(self):
        c = cache.SFDictCache.from_dict({"a": 1})
        assert c["a"] == 1
        c._entry_lifetime = NO_LIFETIME
        c["b"] = 2
        with pytest.raises(KeyError):
            c["b"]

    def test_delete(self):
        c = cache.SFDictCache.from_dict({"a": 1})
        del c["a"]
        assert len(c.keys()) == 0

    def test_contains(self):
        c = cache.SFDictCache.from_dict({"a": 1})
        assert "a" in c
        c._entry_lifetime = NO_LIFETIME
        c["b"] = 2
        assert "b" not in c
        assert "c" not in c

    def test_iter(self):
        c = cache.SFDictCache.from_dict({1: "a", 2: "b"})
        counter = 1
        # Make sure that this filters out expired entries
        c._entry_lifetime = NO_LIFETIME
        c["a"] = 100
        for e in iter(c):
            assert e == counter
            # Make sure that cache can be modified while iterating
            del c[e]
            counter += 1
        assert len(c._cache) == 0

    def test_clear(self):
        c = cache.SFDictCache.from_dict({1: "a", 2: "b"})
        assert len(c.keys()) == 2
        c.clear()
        assert len(c.keys()) == 0

    def test_get(self):
        c = cache.SFDictCache.from_dict({1: "a", 2: "b"})
        assert c.get("a", -999) == -999
        assert c.get(1) == "a"
        # Make sure that this filters out expired entries
        c._entry_lifetime = NO_LIFETIME
        c["d"] = 4
        assert c.get("d") is None
        assert c._getitem_non_locking(1) == "a"

    def test_items(self):
        c = cache.SFDictCache()
        assert c.items() == []
        c["a"] = 1
        c["b"] = 2
        # Make sure that this filters out expired entries
        c._entry_lifetime = NO_LIFETIME
        c["c"] = 3
        assert c.items() == [("a", 1), ("b", 2)]

    def test_keys(self):
        c = cache.SFDictCache()
        assert list(c.keys()) == []
        c["a"] = 1
        c["b"] = 2
        # Make sure that this filters out expired entries
        c._entry_lifetime = NO_LIFETIME
        c["c"] = 3
        assert c.keys() == ["a", "b"]

    def test_update(self):
        c = cache.SFDictCache()
        c.update({"a": 1})
        other = cache.SFDictCache.from_dict({"b": 2, "c": 3})
        c["b"] = 4
        c.update(other)
        assert c.items() == [("a", 1), ("b", 2), ("c", 3)]
        # Make sure that this filters out expired entries
        c._entry_lifetime = NO_LIFETIME
        c["d"] = 4
        assert c.items() == [("a", 1), ("b", 2), ("c", 3)]

    def test_update_newer(self):
        c = cache.SFDictCache()
        c.update({"a": 1})
        with mock.patch(
            "snowflake.connector.cache.now",
            lambda: datetime.datetime.now() + datetime.timedelta(seconds=1),
        ):
            other = cache.SFDictCache.from_dict({"a": 2, "b": 4, "c": 2})
        with mock.patch(
            "snowflake.connector.cache.now",
            lambda: datetime.datetime.now() + datetime.timedelta(seconds=2),
        ):
            c["b"] = 2
        c.update_newer(other)
        assert c.items() == [("a", 2), ("b", 2), ("c", 2)]
        # Make sure that this filters out expired entries
        c._entry_lifetime = NO_LIFETIME
        c["d"] = 4
        assert c.items() == [("a", 2), ("b", 2), ("c", 2)]

    def test_values(self):
        c = cache.SFDictCache()
        assert c.values() == []
        c["a"] = 1
        c["b"] = 2
        # Make sure that this filters out expired entries
        c._entry_lifetime = NO_LIFETIME
        c["c"] = 3
        assert c.values() == [1, 2]

    def test_telemetry(self):
        c = cache.SFDictCache.from_dict({"a": 1, "b": 2})
        assert c.telemetry == {
            "hit": 0,
            "miss": 0,
            "expiration": 0,
            "size": 2,
        }
        c["a"] = 1
        assert c.telemetry["hit"] == 0
        assert c["a"] == 1
        assert c.telemetry["hit"] == 1
        with pytest.raises(KeyError):
            c["c"]
        assert c.telemetry["miss"] == 1
        # Make sure that this filters out expired entries
        c._entry_lifetime = NO_LIFETIME
        c["c"] = 3
        with pytest.raises(KeyError):
            c["c"]
        assert c.telemetry["expiration"] == 1
        assert c.get("c") is None
        assert c.telemetry["miss"] == 2
        # These functions should not affect any numbers other than expirations
        c["c"] = 3  # expired
        assert c.values() == [1, 2]
        assert c.keys() == ["a", "b"]
        assert c.items() == [("a", 1), ("b", 2)]
        assert c.telemetry == {
            "hit": 1,
            "miss": 2,
            "expiration": 2,
            "size": 2,
        }
        assert "b" in c
        c.clear()
        assert c.telemetry == {
            "hit": 0,
            "miss": 0,
            "expiration": 0,
            "size": 0,
        }
        c.update({"a": 1, "b": 2})
        assert c.telemetry == {
            "hit": 0,
            "miss": 0,
            "expiration": 0,
            "size": 2,
        }


if cache is not None:

    class AlwaysSaveSFDictFileCache(cache.SFDictFileCache):
        def _should_save(self):
            return True

    class NeverSaveSFDictFileCache(cache.SFDictFileCache):
        def _should_save(self):
            return False


class TestSFDictFileCache:
    # Since the __getitem__ is overwritten copied over tests to test it
    def test_simple_usage(self):
        c = cache.SFDictCache.from_dict({1: "a", 2: "b"})
        assert 1 in c and 2 in c
        assert c[1] == "a"
        assert c[2] == "b"
        assert c.telemetry == {
            "hit": 4,
            "miss": 0,
            "expiration": 0,
            "size": 2,
        }

    def test_miss(self):
        c = cache.SFDictCache.from_dict(
            {"a": 1},
        )
        with pytest.raises(KeyError):
            c["b"]
        assert c.telemetry == {
            "hit": 0,
            "miss": 1,
            "expiration": 0,
            "size": 1,
        }

    def test_expiration(self):
        c = cache.SFDictCache.from_dict(
            {"a": 1},
            entry_lifetime=0,
        )
        with pytest.raises(KeyError):
            c["a"]
        assert c.telemetry == {
            "hit": 0,
            "miss": 0,
            "expiration": 1,
            "size": 0,
        }

    # The rest of the tests test the file cache portion

    def test_simple_miss(self, tmpdir):
        """Check whether miss triggers a load."""
        c = AlwaysSaveSFDictFileCache.from_dict(
            {"a": 1, "b": 2}, file_path=os.path.join(tmpdir, "c.txt")
        )
        c2 = pickle.loads(pickle.dumps(c))
        c2["c"] = 3
        with mock.patch(
            "snowflake.connector.cache.getmtime",
            lambda path: os.path.getmtime(path) + 1,
        ):
            assert c["c"] == 3

    def test_simple_expiration(self, tmpdir):
        """Check whether expiration triggers a load."""
        c = AlwaysSaveSFDictFileCache.from_dict(
            {"a": 1, "b": 2}, file_path=os.path.join(tmpdir, "c.txt")
        )
        c2 = pickle.loads(pickle.dumps(c))
        c2._entry_lifetime = NO_LIFETIME
        c2["c"] = 3
        c["c"] = 3
        with mock.patch(
            "snowflake.connector.cache.getmtime",
            lambda path: os.path.getmtime(path) + 1,
        ):
            assert c2["c"] == 3

    @pytest.mark.skip
    def test_filelock_hang(self, tmpdir, caplog):
        """Check whether if file lock is held save doesn't hang."""
        c = AlwaysSaveSFDictFileCache.from_dict(
            {"a": 1, "b": 2}, file_path=os.path.join(tmpdir, "c.txt")
        )
        c2 = pickle.loads(pickle.dumps(c))
        with c2._file_lock:
            with caplog.at_level(logging.DEBUG, logger="snowflake.connector.cache"):
                assert c._save() is False
            assert caplog.record_tuples == [
                (
                    "snowflake.connector.cache",
                    logging.DEBUG,
                    f"acquiring {c._file_lock_path} timed out, skipping saving...",
                ),
            ]

    def test_pickle(self, tmpdir):
        c = AlwaysSaveSFDictFileCache(file_path=os.path.join(tmpdir, "cache.txt"))
        c2 = pickle.loads(pickle.dumps(c))
        assert c is not c2
        assert c._lock is not c2._lock
        assert c._file_lock is not c2._file_lock

    def test_precise_save_load(self, tmpdir):
        c1 = NeverSaveSFDictFileCache(file_path=os.path.join(tmpdir, "cache.txt"))
        c2 = pickle.loads(pickle.dumps(c1))
        c1["a"] = 1
        c1["b"] = 1
        assert c1._save()
        assert c2.telemetry == {
            "hit": 0,
            "miss": 0,
            "expiration": 0,
            "size": 0,
        }
        assert c2._load()
        assert c2.telemetry == {
            "hit": 0,
            "miss": 0,
            "expiration": 0,
            "size": 2,
        }

    def test_clear_expired_entries(self, tmpdir):
        c1 = NeverSaveSFDictFileCache(file_path=os.path.join(tmpdir, "cache.txt"))
        c2 = pickle.loads(pickle.dumps(c1))
        c1["a"] = 1
        c1._entry_lifetime = NO_LIFETIME
        c1["b"] = 1
        c1["c"] = 1
        assert c1._save()  # Save calls self._clear_expired_entries()
        assert c2._load()
        assert c2.keys() == ["a"]

    def test_path_choosing(self, tmpdir):
        tmp_file = os.path.join(tmpdir, "cache.txt")
        non_existent_file = "/nowhere/cache.txt"
        platforms = ("linux", "darwin", "windows")
        wrong_paths = {p: non_existent_file for p in platforms}
        for platform in platforms:
            with mock.patch(
                "snowflake.connector.cache.platform.system", return_value=platform
            ):
                assert (
                    cache.SFDictFileCache(
                        file_path={**wrong_paths, platform: tmp_file},
                    ).file_path
                    == tmp_file
                )
        # Test fallback too
        with mock.patch(
            "snowflake.connector.cache.platform.system",
            return_value="BSD",
        ):
            assert (
                cache.SFDictFileCache(
                    file_path={
                        "linux": tmp_file,
                        "windows": non_existent_file,
                    },
                ).file_path
                == tmp_file
            )

    def test_read_only(self, tmpdir):
        if IS_WINDOWS:
            pytest.skip("chmod does not work on Windows")
        os.chmod(tmpdir, stat.S_IRUSR | stat.S_IXUSR)
        with pytest.raises(
            PermissionError,
            match=f"Cache folder is not writeable: '{tmpdir}'",
        ):
            cache.SFDictFileCache(file_path=os.path.join(tmpdir, "cache.txt"))

    def test_clear(self, tmpdir):
        cache_path = os.path.join(tmpdir, "cache.txt")
        c = AlwaysSaveSFDictFileCache(file_path=cache_path)
        c.clear()
        assert not c._cache
        assert os.path.exists(cache_path)
        # Make sure not existing cache file doesn't raise Exception during cache
        os.unlink(cache_path)
        c["a"] = 1
        assert os.path.exists(cache_path)

    def test_load_with_expired_entries(self, tmpdir):
        # Test: https://snowflakecomputing.atlassian.net/browse/SNOW-698526

        # create cache first
        cache_path = os.path.join(tmpdir, "cache.txt")
        c1 = cache.SFDictFileCache(file_path=cache_path)
        c1["a"] = 1
        c1._save()

        # load cache
        c2 = cache.SFDictFileCache(
            file_path=cache_path, entry_lifetime=int(NO_LIFETIME.total_seconds())
        )
        c2["b"] = 2
        c2["c"] = 3
        # cache will expire immediately due to the NO_LIFETIME setting
        # when loading again, clearing cache logic will be triggered
        # load will trigger clear expired entries, and then further call _getitem
        c2._load()

        assert len(c2) == 1 and c2["a"] == 1 and c2._getitem_non_locking("a") == 1
