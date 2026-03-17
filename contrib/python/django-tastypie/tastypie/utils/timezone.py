import datetime
from tastypie.compat import timezone
from django.conf import settings


def make_aware(value):
    if settings.USE_TZ and timezone.is_naive(value):
        default_tz = timezone.get_default_timezone()
        value = timezone.make_aware(value, default_tz)
    return value


def make_naive(value):
    if settings.USE_TZ and timezone.is_aware(value):
        default_tz = timezone.get_default_timezone()
        value = timezone.make_naive(value, default_tz)
    return value


def make_naive_utc(value):
    """
    Translate a datetime to UTC, then strip TZ info; useful as a last step before creating the
    Retry-After header.
    """
    utc_value = timezone.localtime(value, timezone.utc)
    return timezone.make_naive(utc_value)


def now():
    d = timezone.now()

    if d.tzinfo:
        return timezone.localtime(d)

    return d


def aware_date(*args, **kwargs):
    return make_aware(datetime.date(*args, **kwargs))


def aware_datetime(*args, **kwargs):
    return make_aware(datetime.datetime(*args, **kwargs))
