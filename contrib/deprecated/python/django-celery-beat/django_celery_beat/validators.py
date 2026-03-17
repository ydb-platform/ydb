"""Validators."""
from __future__ import absolute_import, unicode_literals

import crontab
from django.core.exceptions import ValidationError


class _CronSlices(crontab.CronSlices):
    """Cron slices with customized validation."""

    def __init__(self, *args):
        super(crontab.CronSlices, self).__init__(
            [_CronSlice(info) for info in crontab.S_INFO]
        )
        self.special = None
        self.setall(*args)
        self.is_valid = self.is_self_valid

    @classmethod
    def validate(cls, *args):
        try:
            cls(*args)
        except Exception as e:
            raise ValueError(e)


class _CronSlice(crontab.CronSlice):
    """Cron slice with custom range parser."""

    def get_range(self, *vrange):
        ret = _CronRange(self, *vrange)
        if ret.dangling is not None:
            return [ret.dangling, ret]
        return [ret]


class _CronRange(crontab.CronRange):
    """Cron range parser class."""

    # rewrite whole method to raise error on bad range
    def parse(self, value):
        if value.count('/') == 1:
            value, seq = value.split('/')
            try:
                self.seq = self.slice.parse_value(seq)
            except crontab.SundayError:
                self.seq = 1
                value = "0-0"
            if self.seq < 1 or self.seq > self.slice.max:
                raise ValueError("Sequence can not be divided by zero or max")
        if value.count('-') == 1:
            vfrom, vto = value.split('-')
            self.vfrom = self.slice.parse_value(vfrom, sunday=0)
            try:
                self.vto = self.slice.parse_value(vto)
            except crontab.SundayError:
                if self.vfrom == 1:
                    self.vfrom = 0
                else:
                    self.dangling = 0
                self.vto = self.slice.parse_value(vto, sunday=6)
            if self.vto < self.vfrom:
                raise ValueError("Bad range '{0.vfrom}-{0.vto}'".format(self))
        elif value == '*':
            self.all()
        else:
            raise ValueError('Unknown cron range value "%s"' % value)


def crontab_validator(value):
    """Validate crontab."""
    try:
        _CronSlices.validate(value)
    except ValueError as e:
        raise ValidationError(e)


def minute_validator(value):
    """Validate minutes crontab value."""
    _validate_crontab(value, 0)


def hour_validator(value):
    """Validate hours crontab value."""
    _validate_crontab(value, 1)


def day_of_month_validator(value):
    """Validate day of month crontab value."""
    _validate_crontab(value, 2)


def month_of_year_validator(value):
    """Validate month crontab value."""
    _validate_crontab(value, 3)


def day_of_week_validator(value):
    """Validate day of week crontab value."""
    _validate_crontab(value, 4)


def _validate_crontab(value, index):
    tab = ['*'] * 5
    tab[index] = value
    tab = ' '.join(tab)
    crontab_validator(tab)
