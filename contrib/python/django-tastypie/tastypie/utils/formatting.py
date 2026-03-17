from django.utils import dateformat

from tastypie.utils.timezone import make_naive, aware_datetime

from dateutil.parser import parse as mk_datetime  # noqa


def format_datetime(dt):
    """
    RFC 2822 datetime formatter
    """
    return dateformat.format(make_naive(dt), 'r')


def format_date(d):
    """
    RFC 2822 date formatter
    """
    # workaround because Django's dateformat utility requires a datetime
    # object (not just date)
    dt = aware_datetime(d.year, d.month, d.day, 0, 0, 0)
    return dateformat.format(dt, 'j M Y')


def format_time(t):
    """
    RFC 2822 time formatter
    """
    # again, workaround dateformat input requirement
    dt = aware_datetime(2000, 1, 1, t.hour, t.minute, t.second)
    return dateformat.format(dt, 'H:i:s O')
