from django.utils.timezone import utc
import datetime


def utc_tzinfo_factory(offset):
    if isinstance(offset, int) and offset != 0:
        raise AssertionError("database connection isn't set to UTC")
    if isinstance(offset, datetime.timedelta) and offset != datetime.timedelta(0):
        raise AssertionError("database connection isn't set to UTC")
    return utc
