from django import VERSION, conf

from .base import TimeZoneNotFoundError

USE_PYTZ_DEFAULT = getattr(conf.settings, "USE_DEPRECATED_PYTZ", VERSION < (4, 0))

tz_backend_cache = {}


def get_tz_backend(use_pytz):
    use_pytz = USE_PYTZ_DEFAULT if use_pytz is None else use_pytz
    if use_pytz not in tz_backend_cache:
        if use_pytz:
            from .pytz import PYTZBackend

            klass = PYTZBackend
        else:
            from .zoneinfo import ZoneInfoBackend

            klass = ZoneInfoBackend
        tz_backend_cache[use_pytz] = klass()
    return tz_backend_cache[use_pytz]


__all__ = ["TimeZoneNotFoundError", "get_tz_backend"]
