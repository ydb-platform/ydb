
'''
crontab.py

Originally written July 15, 2011 by Josiah Carlson <josiah.carlson@gmail.com>
Copyright 2011-2025 Josiah Carlson
Released under the GNU LGPL v2.1 and v3
available:
http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html
http://www.gnu.org/licenses/lgpl.html

Other licenses may be available upon request.

'''

from collections import namedtuple
from datetime import datetime, timedelta
import random
import sys
import warnings

_ranges = [
    (0, 59),
    (0, 59),
    (0, 23),
    (1, 31),
    (1, 12),
    (0, 6),
    (1970, 2099),
]

ENTRIES = len(_ranges)
SECOND_OFFSET, MINUTE_OFFSET, HOUR_OFFSET, DAY_OFFSET, MONTH_OFFSET, WEEK_OFFSET, YEAR_OFFSET = range(ENTRIES)

_attribute = [
    'second',
    'minute',
    'hour',
    'day',
    'month',
    'isoweekday',
    'year'
]
_alternate = {
    MONTH_OFFSET: {'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov':11, 'dec':12},
    WEEK_OFFSET: {'sun': 0, 'mon': 1, 'tue': 2, 'wed': 3, 'thu': 4, 'fri': 5,
        'sat': 6},
}
_aliases = {
    '@yearly':  '0 0 1 1 *',
    '@annually':  '0 0 1 1 *',
    '@monthly': '0 0 1 * *',
    '@weekly':  '0 0 * * 0',
    '@daily':   '0 0 * * *',
    '@hourly':  '0 * * * *',
}

WARNING_CHANGE_MESSAGE = '''\
Version 0.22.0+ of crontab will use datetime.utcnow() and
datetime.utcfromtimestamp() instead of datetime.now() and
datetime.fromtimestamp() as was previous. This had been a bug, which will be
remedied. If you would like to keep the *old* behavior:
`ct.next(..., default_utc=False)` . If you want to use the new behavior *now*:
`ct.next(..., default_utc=True)`. If you pass a datetime object with a tzinfo
attribute that is not None, timezones will *just work* to the best of their
ability. There are tests...'''


if sys.version_info >= (3, 0):
    _number_types = (int, float)
    xrange = range
else:
    _number_types = (int, long, float)

SECOND = timedelta(seconds=1)
MINUTE = timedelta(minutes=1)
HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
WEEK = timedelta(days=7)
MONTH = timedelta(days=28)
YEAR = timedelta(days=365)

WARN_CHANGE = object()

# find the next scheduled time
def _end_of_month(dt):
    ndt = dt + DAY
    while dt.month == ndt.month:
        ndt += DAY
    return ndt.replace(day=1) - DAY

def _month_incr(dt, m):
    odt = dt
    dt += MONTH
    while dt.month == odt.month:
        dt += DAY
    # get to the first of next month, let the backtracking handle it
    dt = dt.replace(day=1)
    return dt - odt

def _year_incr(dt, m):
    # simple leapyear stuff works for 1970-2099 :)
    mod = dt.year % 4
    if mod == 0 and (dt.month, dt.day) < (2, 29):
        return YEAR + DAY
    if mod == 3 and (dt.month, dt.day) > (2, 29):
        return YEAR + DAY
    return YEAR

_increments = [
    lambda *a: SECOND,
    lambda *a: MINUTE,
    lambda *a: HOUR,
    lambda *a: DAY,
    _month_incr,
    lambda *a: DAY,
    _year_incr,
    lambda dt,x: dt.replace(second=0),
    lambda dt,x: dt.replace(minute=0),
    lambda dt,x: dt.replace(hour=0),
    lambda dt,x: dt.replace(day=1) if x > DAY else dt,
    lambda dt,x: dt.replace(month=1) if x > DAY else dt,
    lambda dt,x: dt,
]

# find the previously scheduled time
def _day_decr(dt, m):
    if m.day.input != 'l':
        return -DAY
    odt = dt
    ndt = dt = dt - DAY
    while dt.month == ndt.month:
        dt -= DAY
    return dt - odt

def _month_decr(dt, m):
    odt = dt
    # get to the last day of last month, let the backtracking handle it
    dt = dt.replace(day=1) - DAY
    return dt - odt

def _year_decr(dt, m):
    # simple leapyear stuff works for 1970-2099 :)
    mod = dt.year % 4
    if mod == 0 and (dt.month, dt.day) > (2, 29):
        return -(YEAR + DAY)
    if mod == 1 and (dt.month, dt.day) < (2, 29):
        return -(YEAR + DAY)
    return -YEAR

def _day_decr_reset(dt, x):
    if x >= -DAY:
        return dt
    cur = dt.month
    while dt.month == cur:
        dt += DAY
    return dt - DAY

_decrements = [
    lambda *a: -SECOND,
    lambda *a: -MINUTE,
    lambda *a: -HOUR,
    _day_decr,
    _month_decr,
    lambda *a: -DAY,
    _year_decr,
    lambda dt,x: dt.replace(second=59),
    lambda dt,x: dt.replace(minute=59),
    lambda dt,x: dt.replace(hour=23),
    _day_decr_reset,
    lambda dt,x: dt.replace(month=12) if x < -DAY else dt,
    lambda dt,x: dt,
    _year_decr,
]

Matcher = namedtuple('Matcher', 'second, minute, hour, day, month, weekday, year')

def _assert(condition, message, *args):
    if not condition:
        raise ValueError(message%args)

class _Matcher(object):
    __slots__ = 'allowed', 'end', 'any', 'input', 'which', 'split', 'loop'
    def __init__(self, which, entry, loop=False):
        """
        input:
            `which` - index into the increment / validation lookup tables
            `entry` - the value of the column
            `loop` - do we loop when we validate / construct counts
                     (turning 55-5,1 -> 0,1,2,3,4,5,55,56,57,58,59 in a "minutes" column)
        """
        _assert(0 <= which <= YEAR_OFFSET,
            "improper number of cron entries specified")
        self.input = entry.lower()
        self.split = self.input.split(',')
        self.which = which
        self.allowed = set()
        self.end = None
        self.any = '*' in self.split or '?' in self.split
        self.loop = loop

        for it in self.split:
            al, en = self._parse_crontab(which, it)
            if al is not None:
                self.allowed.update(al)
            self.end = en
        _assert(self.end is not None,
            "improper item specification: %r", entry.lower()
        )
        self.allowed = frozenset(self.allowed)

    def __call__(self, v, dt):
        for i, x in enumerate(self.split):
            if x == 'l':
                if v == _end_of_month(dt).day:
                    return True

            elif x.startswith('l'):
                # We have to do this in here, otherwise we can end up, for
                # example, accepting *any* Friday instead of the *last* Friday.
                if dt.month == (dt + WEEK).month:
                    continue

                x = x[1:]
                if x.isdigit():
                    x = int(x, 10) if x != '7' else 0
                    if v == x:
                        return True
                    continue

                start, end = (int(i, 10) for i in x.partition('-')[::2])
                allowed = set(range(start, end+1))
                if 7 in allowed:
                    allowed.add(0)
                if v in allowed:
                    return True

            elif x.startswith('z'):
                x = x[1:]
                eom = _end_of_month(dt).day
                if x.isdigit():
                    x = int(x, 10)
                    return (eom - x) == v

                start, end = (int(i, 10) for i in x.partition('-')[::2])
                if v in set(eom - i for i in range(start, end+1)):
                    return True

        return self.any or v in self.allowed

    def __lt__(self, other):
        if self.any:
            return self.end < other
        return all(item < other for item in self.allowed)

    def __gt__(self, other):
        if self.any:
            return _ranges[self.which][0] > other
        return all(item > other for item in self.allowed)

    def __eq__(self, other):
        if self.any:
            return other.any
        return self.allowed == other.allowed

    def __hash__(self):
        return hash((self.any, self.allowed))

    def _parse_crontab(self, which, entry):
        '''
        This parses a single crontab field and returns the data necessary for
        this matcher to accept the proper values.

        See the README for information about what is accepted.
        '''

        # this handles day of week/month abbreviations
        def _fix(it):
            if which in _alternate and not it.isdigit():
                if it in _alternate[which]:
                    return _alternate[which][it]
            _assert(it.isdigit(),
                "invalid range specifier: %r (%r)", it, entry)
            it = int(it, 10)
            _assert(_start <= it <= _end_limit,
                "item value %r out of range [%r, %r]",
                it, _start, _end_limit)
            return it

        # this handles individual items/ranges
        def _parse_piece(it):
            if '-' in it:
                start, end = map(_fix, it.split('-'))
                # Allow "sat-sun"
                if which in (DAY_OFFSET, WEEK_OFFSET) and end == 0:
                    end = 7
            elif it == '*':
                start = _start
                end = _end
            else:
                start = _fix(it)
                end = _end
                if increment is None:
                    return set([start])

            _assert(_start <= start <= _end_limit,
                "%s range start value %r out of range [%r, %r]",
                _attribute[which], start, _start, _end_limit)
            _assert(_start <= end <= _end_limit,
                "%s range end value %r out of range [%r, %r]",
                _attribute[which], end, _start, _end_limit)
            if not self.loop:
                _assert(start <= end,
                    "%s range start value %r > end value %r",
                    _attribute[which], start, end)

            if increment and not self.loop:
                next_value = start + increment
                _assert(next_value <= _end_limit,
                        "first next value %r is out of range [%r, %r]",
                        next_value, start, _end_limit)

            if start <= end:
                return set(range(start, end+1, increment or 1))

            right = set(range(end, _end_limit + 1, increment or 1))
            first = max(right, default=end + (increment or 1)) % _end_limit
            return set(range(first, start+1, increment or 1)) | right

        _start, _end = _ranges[which]
        _end_limit = _end
        # wildcards
        if entry in ('*', '?'):
            if entry == '?':
                _assert(which in (DAY_OFFSET, WEEK_OFFSET),
                    "cannot use '?' in the %r field", _attribute[which])
            return None, _end

        # last day of the month
        if entry == 'l':
            _assert(which == DAY_OFFSET,
                "you can only specify a bare 'L' in the 'day' field")
            return None, _end

        # for the days before the last day of the month
        elif entry.startswith('z'):
            _assert(which == DAY_OFFSET,
                "you can only specify a leading 'Z' in the 'day' field")
            es, _, ee = entry[1:].partition('-')
            _assert((entry[1:].isdigit() and 0 <= int(es, 10) <= 7) or
                    (_ and es.isdigit() and ee.isdigit() and 0 <= int(es, 10) <= 7 and 1 <= int(ee, 10) <= 7 and es <= ee),
                "<day> specifier must include a day number or range 0..7 in the 'day' field, you entered %r", entry)
            return None, _end

        # for the last 'friday' of the month, for example
        elif entry.startswith('l'):
            _assert(which == WEEK_OFFSET,
                "you can only specify a leading 'L' in the 'weekday' field")
            es, _, ee = entry[1:].partition('-')
            _assert((entry[1:].isdigit() and 0 <= int(es, 10) <= 7) or
                    (_ and es.isdigit() and ee.isdigit() and 0 <= int(es, 10) <= 7 and 0 <= int(ee, 10) <= 7),
                "last <day> specifier must include a day number or range 0..7 in the 'weekday' field, you entered %r", entry)
            return None, _end

        # allow Sunday to be specified as weekday 7
        if which == WEEK_OFFSET:
            _end_limit = 7

        increment = None
        # increments
        if '/' in entry:
            entry, increment = entry.split('/')
            increment = int(increment, 10)
            _assert(increment > 0,
                "you can only use positive increment values, you provided %r",
                increment)
            _assert(increment <= _end_limit,
                    "increment value must be less than %r, you provided %r",
                    _end_limit, increment)

        # handle singles and ranges
        good = _parse_piece(entry)

        # change Sunday to weekday 0
        if which == WEEK_OFFSET and 7 in good:
            good.discard(7)
            good.add(0)

        return good, _end


_gv = lambda: str(random.randrange(60))


class CronTab(object):
    __slots__ = 'matchers', 'rs'
    def __init__(self, crontab, loop=False, random_seconds=False):
        """
        inputs:
            `crontab` - crontab specification of "[S=0] Mi H D Mo DOW [Y=*]"
            `loop` - do we loop when we validate / construct counts
                     (turning 55-5,1 -> 0,1,2,3,4,5,55,56,57,58,59 in a "minutes" column)
            `random_seconds` - randomly select starting second for tasks
        """
        self.rs = random_seconds
        self.matchers = self._make_matchers(crontab, loop, random_seconds)

    def __eq__(self, other):
        if not isinstance(other, CronTab):
            return False
        match_last = self.matchers[1:] == other.matchers[1:]
        return match_last and ((self.rs and other.rs) or (not self.rs and
            not other.rs and self.matchers[0] == other.matchers[0]))

    def _make_matchers(self, crontab, loop, random_seconds):
        '''
        This constructs the full matcher struct.
        '''
        crontab = _aliases.get(crontab, crontab)
        ct = crontab.split()

        if len(ct) == 5:
            ct.insert(0, _gv() if random_seconds else '0')
            ct.append('*')
        elif len(ct) == 6:
            ct.insert(0, _gv() if random_seconds else '0')
        _assert(len(ct) == 7,
            "improper number of cron entries specified; got %i need 5 to 7"%(len(ct,)))

        matchers = [_Matcher(which, entry, loop) for which, entry in enumerate(ct)]

        return Matcher(*matchers)

    def _test_match(self, index, dt):
        '''
        This tests the given field for whether it matches with the current
        datetime object passed.
        '''
        at = _attribute[index]
        attr = getattr(dt, at)
        if index == WEEK_OFFSET:
            attr = attr() % 7
        return self.matchers[index](attr, dt)

    def next(self, now=None, increments=_increments, delta=True, default_utc=WARN_CHANGE, return_datetime=False):
        '''
        How long to wait in seconds before this crontab entry can next be
        executed.
        '''
        if default_utc is WARN_CHANGE and (isinstance(now, _number_types) or (now and not now.tzinfo) or now is None):
            warnings.warn(WARNING_CHANGE_MESSAGE, FutureWarning, 2)
            default_utc = False

        now = now or (datetime.utcnow() if default_utc and default_utc is not WARN_CHANGE else datetime.now())
        if isinstance(now, _number_types):
            now = datetime.utcfromtimestamp(now) if default_utc else datetime.fromtimestamp(now)

        # handle timezones if the datetime object has a timezone and get a
        # reasonable future/past start time
        onow, now = now, now.replace(tzinfo=None)
        tz = onow.tzinfo
        future = now.replace(microsecond=0) + increments[0]()
        if future < now:
            # we are going backwards...
            _test = lambda: future.year < self.matchers.year
            if now.microsecond:
                future = now.replace(microsecond=0)
        else:
            # we are going forwards
            _test = lambda: self.matchers.year < future.year

        # Start from the year and work our way down. Any time we increment a
        # higher-magnitude value, we reset all lower-magnitude values. This
        # gets us performance without sacrificing correctness. Still more
        # complicated than a brute-force approach, but also orders of
        # magnitude faster in basically all cases.
        to_test = ENTRIES - 1
        while to_test >= 0:
            if not self._test_match(to_test, future):
                inc = increments[to_test](future, self.matchers)
                future += inc
                for i in xrange(0, to_test):
                    future = increments[ENTRIES+i](future, inc)
                try:
                    if _test():
                        return None
                except:
                    print(future, type(future), type(inc))
                    raise
                to_test = ENTRIES-1
                continue
            to_test -= 1

        # verify the match
        match = [self._test_match(i, future) for i in xrange(ENTRIES)]
        _assert(all(match),
            "\nYou have discovered a bug with crontab, please notify the\n" \
            "author with the following information:\n" \
            "crontab: %r\n" \
            "now: %r", ' '.join(m.input for m in self.matchers), now)

        if return_datetime:
            return future.replace(tzinfo=tz)

        if not delta:
            onow = now = datetime(1970, 1, 1)

        delay = future - now
        if tz:
            delay += _fix_none(onow.utcoffset())
            if hasattr(tz, 'localize'):
                delay -= _fix_none(tz.localize(future).utcoffset())
            else:
                delay -= _fix_none(future.replace(tzinfo=tz).utcoffset())

        return delay.days * 86400 + delay.seconds + delay.microseconds / 1000000.

    def previous(self, now=None, delta=True, default_utc=WARN_CHANGE, return_datetime=False):
        return self.next(now, _decrements, delta, default_utc, return_datetime)

    def test(self, entry):
        if isinstance(entry, _number_types):
            entry = datetime.utcfromtimestamp(entry)
        for index in xrange(ENTRIES):
            if not self._test_match(index, entry):
                return False
        return True

def _fix_none(d, _=timedelta(0)):
    if d is None:
        return _
    return d
