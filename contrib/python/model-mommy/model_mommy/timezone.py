"""
Add support for Django 1.4+ safe datetimes.
https://docs.djangoproject.com/en/1.4/topics/i18n/timezones/
"""
# TODO: the whole file seems to be not needed anymore, since Django has this tooling built-in

from datetime import datetime

try:
    from django.conf import settings
    from django.utils.timezone import now, utc
except ImportError:
    def now():
        return datetime.now()


def smart_datetime(*args):
    value = datetime(*args)
    return tz_aware(value)


def tz_aware(d):
    value = d
    if settings.USE_TZ:
        value = d.replace(tzinfo=utc)

    return value
