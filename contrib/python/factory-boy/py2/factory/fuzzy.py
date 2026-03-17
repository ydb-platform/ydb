# -*- coding: utf-8 -*-
# Copyright: See the LICENSE file.


"""Additional declarations for "fuzzy" attribute definitions."""

from __future__ import unicode_literals

import datetime
import decimal
import string
import warnings

from . import compat
from . import declarations
from . import random

random_seed_warning = (
    "Setting a specific random seed for {} can still have varying results "
    "unless you also set a specific end date. For details and potential solutions "
    "see https://github.com/FactoryBoy/factory_boy/issues/331"
)


def get_random_state():
    warnings.warn(
        "`factory.fuzzy.get_random_state` is deprecated. "
        "You should use `factory.random.get_random_state` instead",
        DeprecationWarning,
        stacklevel=2
    )
    return random.get_random_state()


def set_random_state(state):
    warnings.warn(
        "`factory.fuzzy.set_random_state` is deprecated. "
        "You should use `factory.random.set_random_state` instead",
        DeprecationWarning,
        stacklevel=2
    )
    return random.set_random_state(state)


def reseed_random(seed):
    warnings.warn(
        "`factory.fuzzy.set_random_state` is deprecated. "
        "You should use `factory.random.reseed_random` instead",
        DeprecationWarning,
        stacklevel=2
    )
    random.reseed_random(seed)


class BaseFuzzyAttribute(declarations.BaseDeclaration):
    """Base class for fuzzy attributes.

    Custom fuzzers should override the `fuzz()` method.
    """

    def fuzz(self):  # pragma: no cover
        raise NotImplementedError()

    def evaluate(self, instance, step, extra):
        return self.fuzz()


class FuzzyAttribute(BaseFuzzyAttribute):
    """Similar to LazyAttribute, but yields random values.

    Attributes:
        function (callable): function taking no parameters and returning a
            random value.
    """

    def __init__(self, fuzzer, **kwargs):
        super(FuzzyAttribute, self).__init__(**kwargs)
        self.fuzzer = fuzzer

    def fuzz(self):
        return self.fuzzer()


class FuzzyText(BaseFuzzyAttribute):
    """Random string with a given prefix.

    Generates a random string of the given length from chosen chars.
    If a prefix or a suffix are supplied, they will be prepended / appended
    to the generated string.

    Args:
        prefix (text): An optional prefix to prepend to the random string
        length (int): the length of the random part
        suffix (text): An optional suffix to append to the random string
        chars (str list): the chars to choose from

    Useful for generating unique attributes where the exact value is
    not important.
    """

    def __init__(self, prefix='', length=12, suffix='', chars=string.ascii_letters, **kwargs):
        super(FuzzyText, self).__init__(**kwargs)
        self.prefix = prefix
        self.suffix = suffix
        self.length = length
        self.chars = tuple(chars)  # Unroll iterators

    def fuzz(self):
        chars = [random.randgen.choice(self.chars) for _i in range(self.length)]
        return self.prefix + ''.join(chars) + self.suffix


class FuzzyChoice(BaseFuzzyAttribute):
    """Handles fuzzy choice of an attribute.

    Args:
        choices (iterable): An iterable yielding options; will only be unrolled
            on the first call.
        getter (callable or None): a function to parse returned values
    """

    def __init__(self, choices, getter=None, **kwargs):
        self.choices = None
        self.choices_generator = choices
        self.getter = getter
        super(FuzzyChoice, self).__init__(**kwargs)

    def fuzz(self):
        if self.choices is None:
            self.choices = list(self.choices_generator)
        value = random.randgen.choice(self.choices)
        if self.getter is None:
            return value
        return self.getter(value)


class FuzzyInteger(BaseFuzzyAttribute):
    """Random integer within a given range."""

    def __init__(self, low, high=None, step=1, **kwargs):
        if high is None:
            high = low
            low = 0

        self.low = low
        self.high = high
        self.step = step

        super(FuzzyInteger, self).__init__(**kwargs)

    def fuzz(self):
        return random.randgen.randrange(self.low, self.high + 1, self.step)


class FuzzyDecimal(BaseFuzzyAttribute):
    """Random decimal within a given range."""

    def __init__(self, low, high=None, precision=2, **kwargs):
        if high is None:
            high = low
            low = 0.0

        self.low = low
        self.high = high
        self.precision = precision

        super(FuzzyDecimal, self).__init__(**kwargs)

    def fuzz(self):
        base = decimal.Decimal(str(random.randgen.uniform(self.low, self.high)))
        return base.quantize(decimal.Decimal(10) ** -self.precision)


class FuzzyFloat(BaseFuzzyAttribute):
    """Random float within a given range."""

    def __init__(self, low, high=None, precision=15, **kwargs):
        if high is None:
            high = low
            low = 0

        self.low = low
        self.high = high
        self.precision = precision

        super(FuzzyFloat, self).__init__(**kwargs)

    def fuzz(self):
        base = random.randgen.uniform(self.low, self.high)
        return float(format(base, '.%dg' % self.precision))


class FuzzyDate(BaseFuzzyAttribute):
    """Random date within a given date range."""

    def __init__(self, start_date, end_date=None, **kwargs):
        super(FuzzyDate, self).__init__(**kwargs)
        if end_date is None:
            if random.randgen.state_set:
                cls_name = self.__class__.__name__
                warnings.warn(random_seed_warning.format(cls_name), stacklevel=2)
            end_date = datetime.date.today()

        if start_date > end_date:
            raise ValueError(
                "FuzzyDate boundaries should have start <= end; got %r > %r."
                % (start_date, end_date))

        self.start_date = start_date.toordinal()
        self.end_date = end_date.toordinal()

    def fuzz(self):
        return datetime.date.fromordinal(random.randgen.randint(self.start_date, self.end_date))


class BaseFuzzyDateTime(BaseFuzzyAttribute):
    """Base class for fuzzy datetime-related attributes.

    Provides fuzz() computation, forcing year/month/day/hour/...
    """

    def _check_bounds(self, start_dt, end_dt):
        if start_dt > end_dt:
            raise ValueError(
                """%s boundaries should have start <= end, got %r > %r""" % (
                    self.__class__.__name__, start_dt, end_dt))

    def _now(self):
        raise NotImplementedError()

    def __init__(self, start_dt, end_dt=None,
                 force_year=None, force_month=None, force_day=None,
                 force_hour=None, force_minute=None, force_second=None,
                 force_microsecond=None, **kwargs):
        super(BaseFuzzyDateTime, self).__init__(**kwargs)

        if end_dt is None:
            if random.randgen.state_set:
                cls_name = self.__class__.__name__
                warnings.warn(random_seed_warning.format(cls_name), stacklevel=2)
            end_dt = self._now()

        self._check_bounds(start_dt, end_dt)

        self.start_dt = start_dt
        self.end_dt = end_dt
        self.force_year = force_year
        self.force_month = force_month
        self.force_day = force_day
        self.force_hour = force_hour
        self.force_minute = force_minute
        self.force_second = force_second
        self.force_microsecond = force_microsecond

    def fuzz(self):
        delta = self.end_dt - self.start_dt
        microseconds = delta.microseconds + 1000000 * (delta.seconds + (delta.days * 86400))

        offset = random.randgen.randint(0, microseconds)
        result = self.start_dt + datetime.timedelta(microseconds=offset)

        if self.force_year is not None:
            result = result.replace(year=self.force_year)
        if self.force_month is not None:
            result = result.replace(month=self.force_month)
        if self.force_day is not None:
            result = result.replace(day=self.force_day)
        if self.force_hour is not None:
            result = result.replace(hour=self.force_hour)
        if self.force_minute is not None:
            result = result.replace(minute=self.force_minute)
        if self.force_second is not None:
            result = result.replace(second=self.force_second)
        if self.force_microsecond is not None:
            result = result.replace(microsecond=self.force_microsecond)

        return result


class FuzzyNaiveDateTime(BaseFuzzyDateTime):
    """Random naive datetime within a given range.

    If no upper bound is given, will default to datetime.datetime.utcnow().
    """

    def _now(self):
        return datetime.datetime.now()

    def _check_bounds(self, start_dt, end_dt):
        if start_dt.tzinfo is not None:
            raise ValueError(
                "FuzzyNaiveDateTime only handles naive datetimes, got start=%r"
                % start_dt)
        if end_dt.tzinfo is not None:
            raise ValueError(
                "FuzzyNaiveDateTime only handles naive datetimes, got end=%r"
                % end_dt)
        super(FuzzyNaiveDateTime, self)._check_bounds(start_dt, end_dt)


class FuzzyDateTime(BaseFuzzyDateTime):
    """Random timezone-aware datetime within a given range.

    If no upper bound is given, will default to datetime.datetime.now()
    If no timezone is given, will default to utc.
    """

    def _now(self):
        return datetime.datetime.now(tz=compat.UTC)

    def _check_bounds(self, start_dt, end_dt):
        if start_dt.tzinfo is None:
            raise ValueError(
                "FuzzyDateTime requires timezone-aware datetimes, got start=%r"
                % start_dt)
        if end_dt.tzinfo is None:
            raise ValueError(
                "FuzzyDateTime requires timezone-aware datetimes, got end=%r"
                % end_dt)
        super(FuzzyDateTime, self)._check_bounds(start_dt, end_dt)
