import pytz
from datetime import datetime, timedelta, time


def is_pytz_instance(value):
    return value is pytz.UTC or isinstance(value, pytz.tzinfo.BaseTzInfo)


# DEPRECATED. Use choices.with_gmt_offset instead
def add_gmt_offset_to_choices(timezone_tuple_set):
    """
    Currently timezone choices items show up like this:
    'America/New_York'
    But this function formats the choices to display in this format:
    GMT-05:00 America/New_York
    :return:
    A list of tuples in this format:
    (<pytz.timezone>, <str>)
    """
    gmt_timezone = pytz.timezone('Greenwich')
    time_ref = datetime(2000, 1, 1)
    time_zero = gmt_timezone.localize(time_ref)
    _choices = []
    for tz, tz_str in timezone_tuple_set:
        delta = (time_zero - tz.localize(time_ref)).total_seconds()
        h = (datetime.min + timedelta(seconds=delta.__abs__())).hour
        gmt_diff = time(h).strftime('%H:%M')
        pair_one = tz
        pair_two = "GMT{sign}{gmt_diff} {timezone}".format(
            sign="-" if delta < 0 else "+",
            gmt_diff=gmt_diff,
            timezone=tz_str.replace('_', ' ')
        )
        _choices.append((delta, pair_one, pair_two))

    _choices.sort(key=lambda x: x[0])
    choices = [(one, two) for zero, one, two in _choices]
    return choices
