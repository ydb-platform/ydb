from clickhouse_driver.util.compat import get_localzone_name_compat
from django.conf import settings
from django.utils import timezone


def get_timezone():
    """Guess what timezone the user is in.

    Just remember, no matter USE_TZ or not,
    ClickHouse always store UNIX timestamp (which is in UTC timezone).

    Results of lookups turning datetime to date are relevant to timezone.
    We should always pass timezone parameter to ClickHouse functions like
    toDate, toStartOfMonth.
    """
    if settings.USE_TZ:
        try:
            return timezone.get_current_timezone_name()
        except TypeError:
            # When TIME_ZONE = None
            pass
    return get_localzone_name_compat()
