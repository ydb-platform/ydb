"""
Preaggregated reporting

"""

import calendar
import datetime
import random
from collections import defaultdict

import pytool
from pytool.lang import classproperty

import humbledb
from humbledb import _version
from humbledb.document import Document, Embed
from humbledb.index import Index

# Interval and Period constants
YEAR = 5
MONTH = 4
DAY = 3
HOUR = 2
MINUTE = 1

# Constants used for informative string messages
_PERIOD_NAMES = {
    YEAR: "YEAR",
    MONTH: "MONTH",
    DAY: "DAY",
    HOUR: "HOUR",
    MINUTE: "MINUTE",
}


class Report(Document):
    """
    A report document.

    """

    config_period = MONTH
    """ The period for which this report stores data. There will be one
    document created per period for each event. For example, if period is
    ``DAY``, then there will be one document per day for each event. """

    config_intervals = [MONTH, DAY]
    """ The intervals at which event counts are recorded. The intervals listed
    here must be less than or equal to the period for this report. """

    config_id_format = (
        "%(event)s@%(year)04d%(month)02d%(day)02d-%(hour)02d:%(minute)02d"
    )
    """ The format for the ``_id`` for a report document. If the event is
    placed first in the string, then the documents will be spread more evenly
    on sharding, but an index on ``meta.period`` is needed for range queries.
    If the date information is placed first, it's possible to use a regex query
    on the date portion of the ``_id`` to query for ranges and then the index
    on ``meta.period`` can be dropped. """

    config_preallocation = 0
    """ The chance each write that preallocation for the future will be
    attempted, from 0.0 to 1.0. Set this to 0 to disable future preallocation.
    """

    config_indexes = [
        Index([("meta.period", humbledb.ASC), ("meta.event", humbledb.ASC)])
    ]
    """ Default indexes. """

    meta = Embed("u")
    meta.period = "p"
    meta.event = "e"

    year = "Y"
    month = "M"
    day = "d"
    hour = "h"
    minute = "m"

    # Mapping of intervals to their document keys. This is used by
    # :meth:`_map_interval`. This has to be created after class declaration so
    # if a subclass changes a key, it is picked up correctly.
    _intervals = {}

    # Mapping of datetimes to already preallocated document strings
    _preallocated = defaultdict(set)

    @classmethod
    def record_id(cls, event, stamp):
        """
        Return a string suitable for use as the document id.

        :param event: A event identifier
        :param stamp: Datetime for this event
        :type event: str
        :type stamp: datetime.datetime

        """
        period = cls._period(stamp)
        info = {
            "event": event,
            "year": period.year,
            "month": period.month,
            "day": period.day,
            "hour": period.hour,
            "minute": period.minute,
        }
        return cls.config_id_format % info

    @classmethod
    def record(cls, event, stamp=None, safe=False, count=1):
        """
        Record an instance of `event` that happened at `stamp`.

        If `safe` is ``True``, then this method will wait for write
        acknowledgement from the server. The `safe` keyword has no effect for
        `pymongo >= 3.0.0`.

        :param event: Event identifier string
        :param stamp: Datetime stamp for this event (default: now)
        :param safe: Safe write option passed to pymongo
        :param count: Number to increment
        :type event: str
        :type stamp: datetime.datetime
        :type safe: bool
        :type count: int

        """
        if not isinstance(count, int):
            raise ValueError("'count' must be int, got %r instead" % type(count))

        if stamp and not isinstance(stamp, (datetime.datetime, datetime.date)):
            raise ValueError(
                "'stamp' must be datetime or date, got %r instead" % type(stamp)
            )

        # Get our stamp as UTC time or use the current time
        stamp = pytool.time.as_utc(stamp) if stamp else pytool.time.utcnow()
        # Do preallocation
        cls._attempt_preallocation(event, stamp)
        # Get the update query
        update = cls._update_query(stamp, count)
        # Get our query doc
        doc = {"_id": cls.record_id(event, stamp)}
        _opts = {}
        if _version._lt("3.0.0"):
            _opts["safe"] = safe
        # Update/upsert the document, hooray
        cls.update(doc, update, upsert=True, **_opts)

    @classproperty
    def yearly(cls):
        return ReportQuery(cls, YEAR)

    @classproperty
    def monthly(cls):
        return ReportQuery(cls, MONTH)

    @classproperty
    def daily(cls):
        return ReportQuery(cls, DAY)

    @classproperty
    def hourly(cls):
        return ReportQuery(cls, HOUR)

    @classproperty
    def per_minute(cls):
        return ReportQuery(cls, MINUTE)

    @classmethod
    def _update_query(cls, stamp, count=1):
        """
        Return the update query for the datetime `stamp`.

        :param stamp: A UTC datetime
        :param count: Number to increment
        :type stamp: datetime.datetime
        :type count: int

        """
        # Compose an update clause for all intervals
        update = {}
        for interval in cls.config_intervals:
            update.update(cls._update_clause(interval, stamp, count))

        return {"$inc": update}

    @classmethod
    def _update_clause(cls, interval, stamp, count=1):
        """
        Return an update dictionary suitable for merging into a ``$inc`` update
        clause.

        :param interval: Time interval being recorded
        :param stamp: A UTC datetime being recorded
        :param count: Number to increment
        :type interval: int
        :type stamp: datetime.datetime
        :type count: int

        """
        period = cls.config_period
        # Zero index these values
        month = stamp.month - 1
        day = stamp.day - 1
        # These are already zero-indexed
        hour = stamp.hour
        minute = stamp.minute

        # Build a dotted key name like 'y.12.25'
        intervals = [0, minute, hour, day, month]
        key = intervals[interval:period] + [cls._map_interval(interval)]
        key = ".".join(str(s) for s in reversed(key))

        return {key: count}

    @classmethod
    def _map_interval(cls, interval):
        """
        Return the document key for `interval`.

        :param interval: Interval
        :type interval: int

        """
        # Memoize the intervals to the class so we don't rebuild this dict
        # with every lookup
        if not cls._intervals:
            cls._intervals.update(
                {
                    YEAR: cls.year,
                    MONTH: cls.month,
                    DAY: cls.day,
                    HOUR: cls.hour,
                    MINUTE: cls.minute,
                }
            )
        return cls._intervals[interval]

    @classmethod
    def _attempt_preallocation(cls, event, stamp):
        """
        Determine if the current document or the future document needs to be
        preallocated and do so.

        :param event: Event identifier string
        :param stamp: A UTC datetime indicating the document period
        :type event: str
        :type stamp: datetime.datetime

        """
        # We always attempt to preallocate the current stamp, which should be
        # very fast once it is confirmed as preallocated by the client
        cls._preallocate(event, stamp)

        # We sometimes attempt to preallocate for the next period... this needs
        # to be tuned according to how many documents and writes are generated
        # per period
        if random.random() < cls.config_preallocation:
            # Get the next period
            future_stamp = _relative_period(cls.config_period, stamp, 1)
            cls._preallocate(event, future_stamp)

    @classmethod
    def _preallocate(cls, event, stamp):
        """
        Preallocate a new document for `event` during the period containing
        `stamp`.

        :param event: Event identifier string
        :param stamp: A UTC datetime indicating the document period
        :type event: str
        :type stamp: datetime.datetime

        """
        # Get the time period for this report
        period = cls._period(stamp)
        # If we already have preallocated for this time period, get out of here
        if event in cls._preallocated[period]:
            return

        # Do a fast check if the document exists
        # if cls.find({cls._id: cls.record_id(event, stamp)}).limit(1).count():
        if cls.find_one({cls._id: cls.record_id(event, stamp)}):
            return

        # Get our query and update clauses
        query, update = cls._preallocate_query(event, stamp)
        try:
            _opts = {}
            if _version._lt("3.0.0"):
                _opts["safe"] = True
            # We always want preallocation to be "safe" in order to avoid race
            # conditions with the subsequent update
            cls.update(query, update, upsert=True, **_opts)
        except humbledb.errors.DuplicateKeyError:  # pragma: no cover
            # Get out of here, we're done
            return

        # XXX: This will not scale well! However it should be good up to 100k
        # or so event identifiers. Maybe a bloom filter would work better?

        # Add the event identifier to the list of already preallocated
        # documents for this period
        cls._preallocated[period].add(event)

        # Clean up the old preallocation records
        if len(cls._preallocated) > 2:
            previous = _relative_period(cls.config_period, period, -2)
            cls._preallocated.pop(previous, None)

    @classmethod
    def _preallocate_query(cls, event, stamp):
        """
        Return the query and update for preallocating a document.

        :param event: Event identifier string
        :param stamp: A UTC datetime indicating the document period
        :type event: str
        :type stamp: datetime.datetime

        """
        period = cls.config_period

        # Build the base query, which is just a lookup against the id
        query = {"_id": cls.record_id(event, stamp)}

        # Start with an empty update clause
        update = {}
        for interval in cls.config_intervals:
            key = cls._map_interval(interval)
            # Update the query to exclude documents which already have a value
            # for each interval key
            query[key] = {"$exists": 0}
            # Update the update clause with the preallocated structures
            update[key] = cls._preallocate_interval(period, interval, stamp)

        update[cls.meta.event] = event
        update[cls.meta.period] = cls._period(stamp)

        # Make the update clause a $set
        update = {"$set": update}

        return query, update

    @classmethod
    def _preallocate_interval(cls, period, interval, stamp, hint=None):
        """
        Return a value suitable for preallocating `interval` for `stamp`.

        Period and interval should be one of :data:`YEAR`, :data:`MONTH`,
        :data:`DAY`, :data:`HOUR` or :data:`MINUTE`.

        :param period: The containing period
        :param interval: A time interval
        :param stamp: A datetime UTC within the preallocated period
        :param hint: Used for hinting the month
        :type period: int
        :type interval: int
        :type stamp: datetime.datetime
        :type hint: int

        """
        # XXX: This method does recursive allocation of the sub-keys values,
        # which when done many, many times, may be slow. This would be easily
        # optimized by memoizing these at module load time.
        start = 1
        if period == interval:
            return 0
        if interval <= MINUTE and period == HOUR:
            start, count = 0, 60
        elif interval <= HOUR and period == DAY:
            start, count = 0, 24
        elif interval <= DAY and period == MONTH:
            if not hint:
                count = 31 + 1
            else:
                count = calendar.monthrange(stamp.year, hint)[1] + 1
        elif interval <= MONTH and period == YEAR:
            count = 12 + 1

        return [
            cls._preallocate_interval(period - 1, interval, stamp, r)
            for r in range(start, count)
        ]

    @classmethod
    def _period(cls, stamp):
        """
        Convenience wrapper around :func:`_period`.

        :param stamp: A timestamp
        :type stamp: datetime.datetime

        """
        return _period(cls.config_period, stamp)


class ReportQuery(object):
    """
    Class used to slice :class:`Report`: objects and get data back in a
    convenient form.

    This enables syntactic sugar of the form:

        class PageViews(Report):
            config_database = 'humble'
            config_collection = 'views.page'

        # Datetimes always work for indices and slices
        start = datetime.datetime(2013, 1, 1)
        end = datetime.datetime(2013, 8, 1)
        day = datetime.datetime(2013, 7, 5)

        views = PageViews.monthly[start:end]

        home_views = PageViews.monthly('home')[start:]

        content_views = PageViews.daily('content/.*')[day:]

        # What makes sense for yearly?
        per_year = PageViews.yearly[-3:]  # Last three years (including this year)

        # What makes sense for monthly?
        per_month = PageViews.monthly[2:7]  # February through June (July excluded)
        per_month = PageViews.monthly[1:]  # This year

        # What makes sense for daily?
        per_day = PageViews.daily[-7:]  # Last 7 days
        per_day = PageViews.daily[1:]  # This month

        # What makes sense for hourly?
        per_hour = PageViews.hourly[8:13]  # 8am - 12pm
        per_hour = PageViews.hourly[1:]  # Today

        # What makes sense for minutes
        per_minute = PageViews.per_minute[-5:]  # Last 5 minutes

        # Get all views for all URLs over the last 7 days
        views = PageViews.daily[-7:]
        # Without an event restriction, all matching events are returned as a
        # dictionary mapping event identifier strings to lists of integers
        for url in views:
            for count in url:
                print url, count.timestamp, count  # datetime(2013, 7, 5), 3

        # With one, it just returns a list of integers
        views = PageViews.daily('home')[-1:]
        for count in views:
            print 'home', count.timestamp, count

    """

    def __init__(self, cls, interval):
        self.cls = cls
        self.interval = interval
        self.event = None
        self.regex = False
        self.anywhere = False

        # We need to get a document key that works best for the interval we're
        # looking for
        self.query_interval = max(
            [0] + [k for k in self.cls.config_intervals if k <= self.interval]
        )

        # If the query_interval is equal to 0, it means we can't satisfy the
        # required precision for this query type
        if not self.query_interval:
            raise ValueError(
                "Unable to satisfy precision: %r" % (_PERIOD_NAMES[self.interval])
            )
        self.query_key = self.cls._map_interval(self.query_interval)

    def __call__(self, event, regex=False, anywhere=False):
        self.event = event
        self.regex = regex
        self.anywhere = anywhere
        return self

    def __getitem__(self, index):
        if isinstance(index, slice):
            if index.step:
                raise TypeError("Reports do not allow extended slices")
            return self._get_range(index.start, index.stop)

        raise TypeError("Reports must use slice syntax")

    def _get_range(self, start, stop):
        """
        Return counts for the range `start` to `stop` as a list.

        :param start: Start time (inclusive)
        :param stop: Stop time (excluded)
        :type start: datetime.datetime
        :type stop: datetime.datetime

        """
        # We use the same value for `now` for both `start` and `stop` to cover
        # the *very* rare edge case where the two method calls would cover an
        # interval boundary
        now = pytool.time.utcnow()

        # Coerce whatever values the user gave us into datetimes or throw an
        # exception
        if start is None:
            # XXX: This has a limit against Unix 0 (1970-01-01), but if you
            # need data with dates before then, this probably isn't for you
            # This means get all records up until stop
            start = pytool.time.fromutctimestamp(0)

        if stop is None:
            # This means get all records between start and now
            stop = now

        start = self._coerce_index(start, now)
        stop = self._coerce_index(stop, now, stop=True)

        # Get the query dictionary
        query = self._range_query(start, stop)

        query_interval = self.query_interval
        query_key = self.query_key

        # Get our results
        results = self.cls.find(
            query,
            {query_key: 1, self.cls.meta.event: 1, self.cls.meta.period: 1},
            sort=[(self.cls.meta.period, 1)],
        )

        # Now we have to parse the results for the maximum ease of consumption
        results = self._parse_results(results, start, stop, query_key, query_interval)

        # We need to coerce the results according to whether or not we have
        # a distinct event, or we're using a regex query
        results = self._coerce_results(results)

        return results

    def _range_query(self, start, stop):
        """
        Return the query dict for getting docs between `start` and `stop`,
        inclusive.

        :param start: Start time (inclusive)
        :param stop: Stop time (excluded)
        :type start: datetime.datetime
        :type stop: datetime.datetime

        """
        # We need to get the periods that cover the start and end of the range,
        # so we can query against the document periods
        period = self.cls.config_period
        period_key = self.cls.meta.period
        starting_period = _period(period, start)
        ending_period = _period(period, stop)

        # The base query looks for any report documents matching the period
        query = {
            period_key: {
                "$gte": starting_period,
                "$lte": ending_period,
            },
        }

        # If the event is a regex, we make it a regex query against _id
        event = self.event
        if event and self.regex:
            if not self.anywhere and not event.startswith("^"):
                event = "^" + event
            query["_id"] = {"$regex": event}

        # Otherwise we just query against the indexed event field
        elif event and not self.regex:
            query[self.cls.meta.event] = event

        return query

    def _parse_results(self, results, start, stop, query_key, query_interval):
        """
        Return a dictionary mapping event names to dicts of event counts.

        The event counts are returned as :class:`ReportCount` which holds the
        timestamp for the counts as well as the count itself.

        :param results: Raw result list
        :param start: Starting timestamp (inclusive)
        :param end: Ending timestamp (excluded)
        :param query_key: Document key which we queried against
        :param query_interval: The interval for `query_key`
        :type results: list
        :type start: datetime.datetime
        :type send: datetime.datetime
        :type query_key: str
        :type query_interval: int

        """
        # This is the period for this query
        period = self.interval

        parsed = {}  # Stores the stamp-count dicts per event
        key_interval = self.cls.config_period - 1  # Top level interval

        # Create a preallocated dict for the range (start, stop]. This ensures
        # we always return the correct number of counts, even if we're going
        # backwards or forwards to when we don't have any docs
        periods = []  # All the periods in this query
        current = _period(period, start)
        end = _relative_period(period, current, 1)
        while current < stop:
            periods.append(current)
            current = _relative_period(period, current, 1)
        empty_counts = {p: ReportCount(0, p) for p in periods}

        # Iterate over the docs, which we got back in sorted order
        for doc in results:
            if period > query_interval:
                # Set up the current and ending timeframe, based on the query
                # starting period, which is only relavant if we have to sum the
                # parsed counts
                current = _period(period, start)
                end = _relative_period(period, current, 1)
            # Get the values for the key we used
            values = doc[query_key]
            # Get the doc's event and period
            event = doc.meta.event
            doc_period = doc.meta.period
            # Ensure we have the empty counts for this event
            if event not in parsed:
                parsed[event] = empty_counts.copy()

            # Iterate over the parsed counts and timestamps, which will come
            # according to the doc's interval
            for stamp, count in _parse_section(values, key_interval, doc_period):
                # Ensure we only take values from within the query frame
                if stamp < start:
                    # If we're before the start, we skip
                    continue
                if stamp >= stop:
                    # If we're after the stop we break because the parsed
                    # counts will be ordered
                    break

                # If we have a greater period than what we queried for, we have
                # to sum those values
                if period > query_interval:
                    # If the current stamp is past the end of this period
                    # create new current and end timestamps
                    if stamp >= end:
                        current = _relative_period(period, stamp, 0)
                        end = _relative_period(period, current, 1)

                    # Otherwise we just sum it up
                    parsed[event][current] += count

                # It's impossible to have a smaller period than what's
                # available in the doc, so this means we matched what we
                # queried for, and we just add it
                else:
                    parsed[event][stamp] += count

        # Convert the count dictionaries to lists
        for event in parsed:
            counts = parsed[event]
            parsed[event] = [counts[p] for p in periods]

        return parsed

    def _coerce_results(self, results):
        """
        Return results coerced appropriately. If this query has an event
        specified and not a regex, it will just return the list.

        """
        # XXX: This may be prohibitively slow
        # Coerce the results from a defaultdict to a dict
        results = dict(results)

        # If it's not a regex and the event is specified, we should expect
        # only a single event, and thus return just the list of counts
        if self.event and not self.regex:
            if len(results) < 1:
                return []
            assert len(results) == 1, "%r != 1" % len(results)
            event, results = results.popitem()
            return results

        return results

    def _coerce_index(self, index, now, stop=False):
        """
        Return `index` as a datetime UTC object or raise an :exc:`IndexError`.

        :param index: Object to be coerced
        :param now: Datetime to be used as current time
        :param stop: Whether or not this is an ending index
        :type now: datetime.datetime
        :type stop: bool

        """
        # If it's already a datetime, make sure it's UTC
        if isinstance(index, datetime.datetime):
            return pytool.time.as_utc(index)

        # If it's a date, make a datetime out of it
        if isinstance(index, datetime.date):
            return datetime.datetime(*index.timetuple()[:6], tzinfo=pytool.time.UTC())

        # If it's an integer, we have to handle it depending on the interval
        if isinstance(index, int):
            now = now or pytool.time.utcnow()
            return self._coerce_int(index, now, stop)

        # XXX: Handle strings? Probably not.
        raise TypeError("Report indices must be datetime.datetime or integers")

    def _coerce_int(self, index, now, stop=False):
        """
        Return `index` converted to a datetime depending on :attr:`interval`.

        :param index: An index value to coerce
        :param now: Datetime to use as current time
        :param stop: Whether or not this is an ending index
        :type index: int
        :type now: datetime.datetime
        :type stop: bool

        """
        interval = self.interval
        # TODO: Make this handle positive integers which are out of range in a
        # sensible way

        # Indices < 0 represent time from the present on back
        if index < 0:
            # We add 1 here because -1 represents the current timeframe (it is
            # the last in the list)
            index += 1
            return _relative_period(interval, now, index)

        # Positive indices correspond to exact years, months in the year, days
        # in the month, hours in the day, or minutes in the hour
        if interval == YEAR:
            self._check_range(index, 1970, 2037)
            return datetime.datetime(index, 1, 1, tzinfo=now.tzinfo)

        if interval == MONTH:
            if stop and index == 13:
                # If the ending index is greater than 12, we return the first
                # of the next year
                return _relative_period(YEAR, now, 1)

            self._check_range(index, 1, 13)
            return datetime.datetime(now.year, index, 1, tzinfo=now.tzinfo)

        if interval == DAY:
            _, end_of_month = calendar.monthrange(now.year, now.month)
            if stop and index == end_of_month + 1:
                # If the ending index is greater than the end of the month, we
                # return the start of the next month
                return _relative_period(MONTH, now, 1)

            self._check_range(index, 1, end_of_month + 1)
            return datetime.datetime(now.year, now.month, index, tzinfo=now.tzinfo)

        if interval == HOUR:
            if stop and index == 24:
                # If the ending index is greater than the end of the day, we
                # return the start of the next day
                return _relative_period(DAY, now, 1)

            self._check_range(index, 0, 24)
            return datetime.datetime(
                now.year, now.month, now.day, index, tzinfo=now.tzinfo
            )

        if interval == MINUTE:
            if stop and index == 60:
                # If the ending index is greater than the last minute, we
                # return the start of the next hour
                return _relative_period(HOUR, now, 1)
            self._check_range(index, 0, 60)
            return datetime.datetime(
                now.year, now.month, now.day, now.hour, index, tzinfo=now.tzinfo
            )

    @staticmethod
    def _check_range(value, minimum, maximum):
        """
        Raise an IndexError if `value` is out of the given range.

        :param value: Value to check
        :param minimum: Minimum value (inclusive)
        :param maximum: Maximum value (inclusive)
        :type value: int
        :type minimum: int
        :type maximum: int

        """
        if value < minimum or value > maximum:
            raise IndexError(
                "Value %r out of range [%r, %r]" % (value, minimum, maximum)
            )


class ReportCount(int):
    """
    A helper class which allows an integer count to be assigned a timestamp
    representing its timeframe.

    :param value: Integer value
    :param timestamp: Datetime for this count
    :type value: int
    :type timestamp: datetime.datetime

    """

    def __new__(cls, value, timestamp):
        instance = super(ReportCount, cls).__new__(cls, value)
        instance.timestamp = timestamp
        return instance

    def __add__(self, value):
        if isinstance(value, ReportCount):
            timestamp = min(self.timestamp, value.timestamp)
        else:
            timestamp = self.timestamp
        value = super(type(self), self).__add__(value)
        return ReportCount(value, timestamp)

    __radd__ = __add__
    __iadd__ = __add__

    @property
    def year(self):
        return self.timestamp.year

    @property
    def month(self):
        return self.timestamp.month

    @property
    def day(self):
        return self.timestamp.day

    @property
    def hour(self):
        return self.timestamp.hour

    @property
    def minute(self):
        return self.timestamp.minute


def _relative_period(period, stamp, diff):
    """
    Return `stamp` offset by `diff` periods.

    For example, if `period` is :data:`YEAR` and `diff` is -2, this method will
    return the start of the period 2 years before. If `period` is :data:`MONTH`
    and `diff` is 1, this method will return the start of the next month.

    :param period: Report period
    :param stamp: A datetime
    :param diff: Relative number of periods to adjust
    :type period: int
    :type stamp: datetime.datetime
    :type diff: int

    """
    # Trivial case, where diff is 0
    if not diff:
        return _period(period, stamp)

    # This one is easy, just replace the year and go
    if period == YEAR:
        return _period(period, stamp.replace(year=stamp.year + diff))

    # This one is also easy, we let timedelta do the work for us
    if period == DAY:
        return _period(period, stamp + datetime.timedelta(days=diff))

    # Hours is just seconds, right?
    if period == HOUR:
        return _period(period, stamp + datetime.timedelta(seconds=diff * 60 * 60))

    # Minutes are also seconds
    if period == MINUTE:
        return _period(period, stamp + datetime.timedelta(seconds=diff * 60))

    # This algorithm for calculating a relative date is borrowed in part
    # from relativedelta in the python-dateutil library.
    # https://github.com/paxan/python-dateutil
    if period == MONTH:
        year = stamp.year
        month = stamp.month
        month += diff
        if month > 12:
            year += 1
            month -= 12
        elif month < 1:
            year -= 1
            month += 12

        day = min(calendar.monthrange(year, month)[1], stamp.day)
        return _period(period, stamp.replace(year=year, month=month, day=day))


def _period(period, stamp):
    """
    Return `stamp` floored to the start of `period`. For example, if the
    `period` is a ``YEAR``, then January 1 of the year containing `stamp` will
    be returned.

    :param stamp: Datetime to be floored
    :type stamp: datetime.datetime

    """
    if period == YEAR:
        return datetime.datetime(stamp.year, 1, 1, tzinfo=stamp.tzinfo)
    if period == MONTH:
        return pytool.time.floor_month(stamp)
    if period == DAY:
        return pytool.time.floor_day(stamp)
    if period == HOUR:
        seconds = stamp.minute * 60 + stamp.second
        return stamp - datetime.timedelta(
            seconds=seconds, microseconds=stamp.microsecond
        )
    if period == MINUTE:
        return stamp - datetime.timedelta(
            seconds=stamp.second, microseconds=stamp.microsecond
        )


def _parse_section(values, interval, stamp):
    """
    A generator which yields 2-tuples of the timestamp and value for
    each value in a section. This will recursively crawl the `values`
    structure, which should be nested lists, to find integer values to
    yield.

    """
    # If it's a number, we yield it
    if isinstance(values, int):
        yield stamp, values
    else:
        # If it's a list, we iterate over it
        for i in range(len(values)):
            if interval == MINUTE:
                stamp = stamp.replace(minute=i)
            elif interval == HOUR:
                stamp = stamp.replace(hour=i)
            elif interval == DAY:
                try:
                    stamp = stamp.replace(day=i + 1)
                except ValueError:
                    # Due to lazy preallocation, we may actually end up with
                    # illegal lengths of values (for instance 31 days in
                    # September) so we just ignore this and move on
                    continue
            elif interval == MONTH:
                stamp = stamp.replace(month=i + 1)
            # Get the value we're working with
            value = values[i]
            # If it's a number, yield it
            if isinstance(value, int):
                yield stamp, value
                continue
            # If it's a list, recursively process it
            for vals in _parse_section(value, interval - 1, stamp):
                yield vals
