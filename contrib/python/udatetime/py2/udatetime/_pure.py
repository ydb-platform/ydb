from datetime import tzinfo, timedelta, datetime as dt_datetime
from time import time, gmtime
from math import floor, ceil

DATE_TIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'


class TZFixedOffset(tzinfo):

    def __init__(self, offset):
        self.offset = offset

    def utcoffset(self, dt=None):
        return timedelta(seconds=self.offset * 60)

    def dst(self, dt=None):
        return timedelta(0)

    def tzname(self, dt=None):
        sign = '+'
        if self.offset < 0:
            sign = '-'

        return "%s%d:%d" % (sign, self.offset / 60, self.offset % 60)

    def __repr__(self):
        return self.tzname()


def _timestamp_to_date_time(timestamp, tzinfo):
    t_full = timestamp + (tzinfo.offset * 60)
    timestamp = int(floor(t_full))
    frac = (t_full - timestamp) * 1e6
    us = int(floor(frac + 0.5) if frac >= 0.0 else ceil(frac - 0.5))

    if us == 1e6:
        timestamp += 1
        us = 0

    y, m, d, hh, mm, ss, weekday, jday, dst = gmtime(timestamp)
    ss = min(ss, 59)  # if sec > 59, set 59 (platform leap support)
    return dt_datetime(y, m, d, hh, mm, ss, us, tzinfo)


def _format_date_time(date_time):
    tm = date_time.timetuple()
    offset = 0
    sign = '+'

    if date_time.tzinfo is not None:
        if date_time.tzinfo.__class__ is not TZFixedOffset:
            # TODO: Support all tzinfo subclasses by calling utcoffset()
            raise ValueError('Only TZFixedOffset supported.')
        offset = date_time.tzinfo.offset

    if offset < 0:
        offset = offset * -1
        sign = '-'

    return '%04d-%02d-%02dT%02d:%02d:%02d.%06d%c%02d:%02d' % (
        tm.tm_year, tm.tm_mon, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec,
        date_time.microsecond, sign, offset / 60, offset % 60
    )


def _get_local_utc_offset():
    ts = time()
    return (
        dt_datetime.fromtimestamp(ts) - dt_datetime.utcfromtimestamp(ts)
    ).total_seconds() / 60

local_utc_offset = _get_local_utc_offset()
local_timezone = TZFixedOffset(local_utc_offset)
utc_timezone = TZFixedOffset(0)


def utcnow():
    '''datetime aware object in UTC with current date and time.'''
    return _timestamp_to_date_time(time(), utc_timezone)


def now():
    '''datetime aware object in local timezone with current date and time.'''
    return _timestamp_to_date_time(time(), local_timezone)


def from_rfc3339_string(rfc3339_string):
    '''Parse RFC3339 compliant date-time string.'''

    rfc3339_string = rfc3339_string.replace(' ', '').lower()

    if 't' not in rfc3339_string:
        raise ValueError(
            'Invalid RFC3339 string. Missing \'T\' date/time separator.'
        )

    (date, _, _time) = rfc3339_string.partition('t')

    if not date or not _time:
        raise ValueError('Invalid RFC3339 string.')

    try:
        (year, month, day) = date.split('-')
        year = int(year)
        month = int(month)
        day = int(day)
    except ValueError:
        raise ValueError('Invalid RFC3339 string. Invalid date.')

    try:
        (hour, minute, second) = _time[:8].split(':')
        hour = int(hour)
        minute = int(minute)
        second = int(second)
    except ValueError:
        raise ValueError('Invalid RFC3339 string. Invalid time.')

    usec = 0
    offset = None

    if len(_time) > 8:
        if _time[8] == '.':
            usec_buf = ''

            for c in _time[9:]:
                if c in '0123456789':
                    usec_buf += c
                else:
                    break

            if len(usec_buf) > 6:
                raise ValueError('Invalid RFC3339 string. Invalid fractions.')

            usec = int(usec_buf)

            if len(usec_buf) > 0 and len(usec_buf) < 6:
                # ugly as shit, but good damn multiplication precision makes
                # it a mess
                usec = usec * int('1' + '0' * (6 - len(usec_buf)))

            _time = _time[9 + len(usec_buf):]
        elif _time[8] == 'z':
            offset = 0

            if len(_time[9:]):
                raise ValueError(
                    'Invalid RFC3339 string. Remaining data after time zone.'
                )
        else:
            _time = _time[8:]
    else:
        offset = 0

    if offset is None and (len(_time) == 0 or _time[0] == 'z'):
        offset = 0

        if len(_time[1:]):
            raise ValueError(
                'Invalid RFC3339 string. Remaining data after time zone.'
            )
    elif offset is None:
        if _time[0] not in '+-':
            raise ValueError('Invalid RFC3339 string. Expected timezone.')

        negative = True if _time[0] == '-' else False

        try:
            (off_hour, off_minute) = _time[1:].split(':')

            off_hour = int(off_hour)
            off_minute = int(off_minute)
        except ValueError:
            raise ValueError('Invalid RFC3339 string. Invalid timezone.')

        offset = (off_hour * 60) + off_minute

        if negative:
            offset = offset * -1

    return dt_datetime(
        year, month, day, hour, minute, second, usec, TZFixedOffset(offset)
    )


def to_rfc3339_string(date_time):
    '''Serialize date_time to RFC3339 compliant date-time string.'''

    if date_time and date_time.__class__ is not dt_datetime:
        raise ValueError("Expected a datetime object.")

    return _format_date_time(date_time)


def from_timestamp(timestamp, tz=None):
    '''timestamp[, tz] -> tz's local time from POSIX timestamp.'''
    if tz is None:
        tz = local_timezone
    elif tz.__class__ is not TZFixedOffset:
        # TODO: Support all tzinfo subclasses by calling utcoffset()
        raise ValueError('Only TZFixedOffset supported.')

    return _timestamp_to_date_time(timestamp, tz)


def from_utctimestamp(timestamp):
    '''timestamp -> UTC datetime from a POSIX timestamp (like time.time()).'''
    return _timestamp_to_date_time(timestamp, utc_timezone)


def utcnow_to_string():
    '''Current UTC date and time RFC3339 compliant date-time string.'''
    return _format_date_time(utcnow())


def now_to_string():
    '''Local date and time RFC3339 compliant date-time string.'''
    return _format_date_time(now())
