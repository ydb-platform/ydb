import json
import logging
import numbers
import re
import time
from collections import defaultdict
from dataclasses import asdict, is_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from typing import Any, Optional
from uuid import UUID
import sys
import platform
import distro  # For Linux OS detection

import six
from dateutil.tz import tzlocal, tzutc

log = logging.getLogger("posthog")


def is_naive(dt):
    """Determines if a given datetime.datetime is naive."""
    return dt.tzinfo is None or dt.tzinfo.utcoffset(dt) is None


def total_seconds(delta):
    """Determines total seconds with python < 2.7 compat."""
    # http://stackoverflow.com/questions/3694835/python-2-6-5-divide-timedelta-with-timedelta
    return (delta.microseconds + (delta.seconds + delta.days * 24 * 3600) * 1e6) / 1e6


def guess_timezone(dt):
    """Attempts to convert a naive datetime to an aware datetime."""
    if is_naive(dt):
        # attempts to guess the datetime.datetime.now() local timezone
        # case, and then defaults to utc
        delta = datetime.now() - dt
        if total_seconds(delta) < 5:
            # this was created using datetime.datetime.now()
            # so we are in the local timezone
            return dt.replace(tzinfo=tzlocal())
        else:
            # at this point, the best we can do is guess UTC
            return dt.replace(tzinfo=tzutc())

    return dt


def remove_trailing_slash(host):
    if host.endswith("/"):
        return host[:-1]
    return host


def clean(item):
    if isinstance(item, Decimal):
        return float(item)
    if isinstance(item, UUID):
        return str(item)
    if isinstance(
        item, (six.string_types, bool, numbers.Number, datetime, date, type(None))
    ):
        return item
    if isinstance(item, (set, list, tuple)):
        return _clean_list(item)
    # Pydantic model
    try:
        # v2+
        if hasattr(item, "model_dump") and callable(item.model_dump):
            item = item.model_dump()
        # v1
        elif hasattr(item, "dict") and callable(item.dict):
            item = item.dict()
    except TypeError as e:
        log.debug(f"Could not serialize Pydantic-like model: {e}")
        pass
    if isinstance(item, dict):
        return _clean_dict(item)
    if is_dataclass(item) and not isinstance(item, type):
        return _clean_dataclass(item)
    return _coerce_unicode(item)


def _clean_list(list_):
    return [clean(item) for item in list_]


def _clean_dict(dict_):
    data = {}
    for k, v in six.iteritems(dict_):
        try:
            data[k] = clean(v)
        except TypeError:
            log.warning(
                'Dictionary values must be serializeable to JSON "%s" value %s of type %s is unsupported.',
                k,
                v,
                type(v),
            )
    return data


def _clean_dataclass(dataclass_):
    data = asdict(dataclass_)
    data = _clean_dict(data)
    return data


def _coerce_unicode(cmplx: Any) -> Optional[str]:
    """
    In theory, this method is only called
    after many isinstance checks are carried out in `utils.clean`.
    When we supported Python 2 it was safe to call `decode` on a `str`
    but in Python 3 that will throw.
    So, we check if the input is bytes and only call `decode` in that case.

    Previously we would always call `decode` on the input
    That would throw an error.
    Then we would call `decode` on the stringified error
    That would throw an error.
    And then we would return `None`

    To avoid a breaking change, we can maintain the behavior
    that anything which did not have `decode` in Python 2
    returns None.
    """
    item = None
    try:
        if isinstance(cmplx, bytes):
            item = cmplx.decode("utf-8", "strict")
        elif isinstance(cmplx, str):
            item = cmplx
    except Exception as exception:
        item = ":".join(map(str, exception.args))
        log.warning("Error decoding: %s", item)
        return None

    return item


def is_valid_regex(value) -> bool:
    try:
        re.compile(value)
        return True
    except re.error:
        return False


class SizeLimitedDict(defaultdict):
    def __init__(self, max_size, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.max_size = max_size

    def __setitem__(self, key, value):
        if len(self) >= self.max_size:
            self.clear()

        super().__setitem__(key, value)


class FlagCacheEntry:
    def __init__(self, flag_result, flag_definition_version, timestamp=None):
        self.flag_result = flag_result
        self.flag_definition_version = flag_definition_version
        self.timestamp = timestamp or time.time()

    def is_valid(self, current_time, ttl, current_flag_version):
        time_valid = (current_time - self.timestamp) < ttl
        version_valid = self.flag_definition_version == current_flag_version
        return time_valid and version_valid

    def is_stale_but_usable(self, current_time, max_stale_age=3600):
        return (current_time - self.timestamp) < max_stale_age


class FlagCache:
    def __init__(self, max_size=10000, default_ttl=300):
        self.cache = {}  # distinct_id -> {flag_key: FlagCacheEntry}
        self.access_times = {}  # distinct_id -> last_access_time
        self.max_size = max_size
        self.default_ttl = default_ttl

    def get_cached_flag(self, distinct_id, flag_key, current_flag_version):
        current_time = time.time()

        if distinct_id not in self.cache:
            return None

        user_flags = self.cache[distinct_id]
        if flag_key not in user_flags:
            return None

        entry = user_flags[flag_key]
        if entry.is_valid(current_time, self.default_ttl, current_flag_version):
            self.access_times[distinct_id] = current_time
            return entry.flag_result

        return None

    def get_stale_cached_flag(self, distinct_id, flag_key, max_stale_age=3600):
        current_time = time.time()

        if distinct_id not in self.cache:
            return None

        user_flags = self.cache[distinct_id]
        if flag_key not in user_flags:
            return None

        entry = user_flags[flag_key]
        if entry.is_stale_but_usable(current_time, max_stale_age):
            return entry.flag_result

        return None

    def set_cached_flag(
        self, distinct_id, flag_key, flag_result, flag_definition_version
    ):
        current_time = time.time()

        # Evict LRU users if we're at capacity
        if distinct_id not in self.cache and len(self.cache) >= self.max_size:
            self._evict_lru()

        # Initialize user cache if needed
        if distinct_id not in self.cache:
            self.cache[distinct_id] = {}

        # Store the flag result
        self.cache[distinct_id][flag_key] = FlagCacheEntry(
            flag_result, flag_definition_version, current_time
        )
        self.access_times[distinct_id] = current_time

    def invalidate_version(self, old_version):
        users_to_remove = []

        for distinct_id, user_flags in self.cache.items():
            flags_to_remove = []
            for flag_key, entry in user_flags.items():
                if entry.flag_definition_version == old_version:
                    flags_to_remove.append(flag_key)

            # Remove invalidated flags
            for flag_key in flags_to_remove:
                del user_flags[flag_key]

            # Remove user entirely if no flags remain
            if not user_flags:
                users_to_remove.append(distinct_id)

        # Clean up empty users
        for distinct_id in users_to_remove:
            del self.cache[distinct_id]
            if distinct_id in self.access_times:
                del self.access_times[distinct_id]

    def _evict_lru(self):
        if not self.access_times:
            return

        # Remove 20% of least recently used entries
        sorted_users = sorted(self.access_times.items(), key=lambda x: x[1])
        to_remove = max(1, len(sorted_users) // 5)

        for distinct_id, _ in sorted_users[:to_remove]:
            if distinct_id in self.cache:
                del self.cache[distinct_id]
            if distinct_id in self.access_times:
                del self.access_times[distinct_id]

    def clear(self):
        self.cache.clear()
        self.access_times.clear()


class RedisFlagCache:
    def __init__(
        self, redis_client, default_ttl=300, stale_ttl=3600, key_prefix="posthog:flags:"
    ):
        self.redis = redis_client
        self.default_ttl = default_ttl
        self.stale_ttl = stale_ttl
        self.key_prefix = key_prefix
        self.version_key = f"{key_prefix}version"

    def _get_cache_key(self, distinct_id, flag_key):
        return f"{self.key_prefix}{distinct_id}:{flag_key}"

    def _serialize_entry(self, flag_result, flag_definition_version, timestamp=None):
        if timestamp is None:
            timestamp = time.time()

        # Use clean to make flag_result JSON-serializable for cross-platform compatibility
        serialized_result = clean(flag_result)

        entry = {
            "flag_result": serialized_result,
            "flag_version": flag_definition_version,
            "timestamp": timestamp,
        }
        return json.dumps(entry)

    def _deserialize_entry(self, data):
        try:
            entry = json.loads(data)
            flag_result = entry["flag_result"]
            return FlagCacheEntry(
                flag_result=flag_result,
                flag_definition_version=entry["flag_version"],
                timestamp=entry["timestamp"],
            )
        except (json.JSONDecodeError, KeyError, ValueError):
            # If deserialization fails, treat as cache miss
            return None

    def get_cached_flag(self, distinct_id, flag_key, current_flag_version):
        try:
            cache_key = self._get_cache_key(distinct_id, flag_key)
            data = self.redis.get(cache_key)

            if data:
                entry = self._deserialize_entry(data)
                if entry and entry.is_valid(
                    time.time(), self.default_ttl, current_flag_version
                ):
                    return entry.flag_result

            return None
        except Exception:
            # Redis error - return None to fall back to normal evaluation
            return None

    def get_stale_cached_flag(self, distinct_id, flag_key, max_stale_age=None):
        try:
            if max_stale_age is None:
                max_stale_age = self.stale_ttl

            cache_key = self._get_cache_key(distinct_id, flag_key)
            data = self.redis.get(cache_key)

            if data:
                entry = self._deserialize_entry(data)
                if entry and entry.is_stale_but_usable(time.time(), max_stale_age):
                    return entry.flag_result

            return None
        except Exception:
            # Redis error - return None
            return None

    def set_cached_flag(
        self, distinct_id, flag_key, flag_result, flag_definition_version
    ):
        try:
            cache_key = self._get_cache_key(distinct_id, flag_key)
            serialized_entry = self._serialize_entry(
                flag_result, flag_definition_version
            )

            # Set with TTL for automatic cleanup (use stale_ttl for total lifetime)
            self.redis.setex(cache_key, self.stale_ttl, serialized_entry)

            # Update the current version
            self.redis.set(self.version_key, flag_definition_version)

        except Exception:
            # Redis error - silently fail, don't break flag evaluation
            pass

    def invalidate_version(self, old_version):
        try:
            # For Redis, we use a simple approach: scan for keys with old version
            # and delete them. This could be expensive with many keys, but it's
            # necessary for correctness.

            cursor = 0
            pattern = f"{self.key_prefix}*"

            while True:
                cursor, keys = self.redis.scan(cursor, match=pattern, count=100)

                for key in keys:
                    if key.decode() == self.version_key:
                        continue

                    try:
                        data = self.redis.get(key)
                        if data:
                            entry_dict = json.loads(data)
                            if entry_dict.get("flag_version") == old_version:
                                self.redis.delete(key)
                    except (json.JSONDecodeError, KeyError):
                        # If we can't parse the entry, delete it to be safe
                        self.redis.delete(key)

                if cursor == 0:
                    break

        except Exception:
            # Redis error - silently fail
            pass

    def clear(self):
        try:
            # Delete all keys matching our pattern
            cursor = 0
            pattern = f"{self.key_prefix}*"

            while True:
                cursor, keys = self.redis.scan(cursor, match=pattern, count=100)
                if keys:
                    self.redis.delete(*keys)
                if cursor == 0:
                    break
        except Exception:
            # Redis error - silently fail
            pass


def convert_to_datetime_aware(date_obj):
    if date_obj.tzinfo is None:
        date_obj = date_obj.replace(tzinfo=timezone.utc)
    return date_obj


def str_icontains(source, search):
    """
    Check if a string contains another string, ignoring case.

    Args:
        source: The string to search within
        search: The substring to search for

    Returns:
        bool: True if search is a substring of source (case-insensitive), False otherwise

    Examples:
        >>> str_icontains("Hello World", "WORLD")
        True
        >>> str_icontains("Hello World", "python")
        False
    """
    return str(search).casefold() in str(source).casefold()


def str_iequals(value, comparand):
    """
    Check if a string equals another string, ignoring case.

    Args:
        value: The string to compare
        comparand: The string to compare with

    Returns:
        bool: True if value and comparand are equal (case-insensitive), False otherwise

    Examples:
        >>> str_iequals("Hello World", "hello world")
        True
        >>> str_iequals("Hello World", "hello")
        False
    """
    return str(value).casefold() == str(comparand).casefold()


def get_os_info():
    """
    Returns standardized OS name and version information.
    Similar to how user agent parsing works in JS.
    """
    os_name = ""
    os_version = ""

    platform_name = sys.platform

    if platform_name.startswith("win"):
        os_name = "Windows"
        if hasattr(platform, "win32_ver"):
            win_version = platform.win32_ver()[0]
            if win_version:
                os_version = win_version

    elif platform_name == "darwin":
        os_name = "Mac OS X"
        if hasattr(platform, "mac_ver"):
            mac_version = platform.mac_ver()[0]
            if mac_version:
                os_version = mac_version

    elif platform_name.startswith("linux"):
        os_name = "Linux"
        linux_info = distro.info()
        if linux_info["version"]:
            os_version = linux_info["version"]

    elif platform_name.startswith("freebsd"):
        os_name = "FreeBSD"
        if hasattr(platform, "release"):
            os_version = platform.release()

    else:
        os_name = platform_name
        if hasattr(platform, "release"):
            os_version = platform.release()

    return os_name, os_version


def system_context() -> dict[str, Any]:
    os_name, os_version = get_os_info()

    return {
        "$python_runtime": platform.python_implementation(),
        "$python_version": "%s.%s.%s" % (sys.version_info[:3]),
        "$os": os_name,
        "$os_version": os_version,
    }
