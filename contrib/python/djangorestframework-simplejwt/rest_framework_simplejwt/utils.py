import hashlib
import logging
from calendar import timegm
from datetime import datetime, timezone
from typing import Callable

from django.conf import settings
from django.utils.functional import lazy


def get_md5_hash_password(password: str) -> str:
    """
    Returns MD5 hash of the given password
    """
    return hashlib.md5(password.encode()).hexdigest().upper()


def make_utc(dt: datetime) -> datetime:
    if settings.USE_TZ and dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)

    return dt


def aware_utcnow() -> datetime:
    dt = datetime.now(tz=timezone.utc)
    if not settings.USE_TZ:
        dt = dt.replace(tzinfo=None)

    return dt


def datetime_to_epoch(dt: datetime) -> int:
    return timegm(dt.utctimetuple())


def datetime_from_epoch(ts: float) -> datetime:
    dt = datetime.fromtimestamp(ts, tz=timezone.utc)
    if not settings.USE_TZ:
        dt = dt.replace(tzinfo=None)

    return dt


def format_lazy(s: str, *args, **kwargs) -> str:
    return s.format(*args, **kwargs)


format_lazy: Callable = lazy(format_lazy, str)

logger = logging.getLogger("rest_framework_simplejwt")
