# -*- coding: utf-8 -*-

# This file is part of convertdate.
# http://github.com/fitnr/convertdate

# Licensed under the MIT license:
# http://opensource.org/licenses/MIT
# Copyright (c) 2016, fitnr <fitnr@fakeisthenewreal>
from . import gregorian
from . import julianday
from . import julian


class DayCount(object):

    '''A day count converter for the given epoch (in terms of Julian Day Count)'''

    def __init__(self, epoch):
        self.epoch = epoch

    def to_gregorian(self, dc):
        return gregorian.from_jd(self.to_jd(dc))

    def from_gregorian(self, year, month, day):
        return self.from_jd(gregorian.to_jd(year, month, day))

    def to_jd(self, dc):
        return dc + self.epoch

    def from_jd(self, jdc):
        return jdc - self.epoch

    def from_julian(self, year, month, day):
        return self.from_jd(julian.to_jd(year, month, day))

    def to_julian(self, dc):
        return julian.from_jd(self.to_jd(dc))

    def to_datetime(self, dc):
        return julianday.to_datetime(self.to_jd(dc))

    def from_datetime(self, dt):
        return self.from_jd(julianday.from_datetime(dt))
