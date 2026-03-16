# -*- coding: utf-8 -*-


# PyMeeus: Python module implementing astronomical algorithms.
# Copyright (C) 2018  Dagoberto Salazar
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.


import calendar
import datetime
from math import radians, cos, sin, asin, sqrt, acos, degrees

from pymeeus.base import TOL, get_ordinal_suffix, iint
from pymeeus.Angle import Angle


"""
.. module:: Epoch
   :synopsis: Class to handle time
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Dagoberto Salazar
"""


DAY2SEC = 86400.0
"""Number of seconds per day"""

DAY2MIN = 1440.0
"""Number of minutes per day"""

DAY2HOURS = 24.0
"""Number of hours per day"""

LEAP_TABLE = {
    1972.5: 1,
    1973.0: 2,
    1974.0: 3,
    1975.0: 4,
    1976.0: 5,
    1977.0: 6,
    1978.0: 7,
    1979.0: 8,
    1980.0: 9,
    1981.5: 10,
    1982.5: 11,
    1983.5: 12,
    1985.5: 13,
    1988.0: 14,
    1990.0: 15,
    1991.0: 16,
    1992.5: 17,
    1993.5: 18,
    1994.5: 19,
    1996.0: 20,
    1997.5: 21,
    1999.0: 22,
    2006.0: 23,
    2009.0: 24,
    2012.5: 25,
    2015.5: 26,
    2017.0: 27,
}
"""This table represents the point in time FROM WHERE the given number of leap
seconds is valid. Given that leap seconds are (so far) always added at
June 30th or December 31st, a leap second added in 1997/06/30 is represented
here as '1997.5', while a leap second added in 2005/12/31 appears here as
'2006.0'."""


class Epoch(object):
    """
    Class Epoch deals with the tasks related to time handling.

    The constructor takes either a single JDE value, another Epoch object, or a
    series of values representing year, month, day, hours, minutes, seconds.
    This series of values are by default supposed to be in **Terrestial Time**
    (TT).

    This is not necesarily the truth, though. For instance, the time of a
    current observation is tipically in UTC time (civil time), not in TT, and
    there is some offset between those two time references.

    When a UTC time is provided, the parameter **utc=True** must be given.
    Then, the input is converted to International Atomic Time (TAI) using an
    internal table of leap seconds, and from there, it is converted to (and
    stored as) Terrestrial Time (TT).

    Given that leap seconds are added or subtracted in a rather irregular
    basis, it is not possible to predict them in advance, and the internal leap
    seconds table will become outdated at some point in time. To counter this,
    you have two options:

    - Download an updated version of this Pymeeus package.
    - Use the argument **leap_seconds** in the constructor or :meth:`set`
      method to provide the correct number of leap seconds (w.r.t. TAI) to be
      applied.

    .. note:: Providing the **leap_seconds** argument will automatically set
       the argument **utc** to True.

    For instance, if at some time in the future the TAI-UTC difference is 43
    seconds, you should set **leap_seconds=43** if you don't have an updated
    version of this class.

    In order to know which is the most updated leap second value stored in this
    class, you may use the :meth:`get_last_leap_second()` method.

    .. note:: The current version of UTC was implemented in January 1st, 1972.
       Therefore, for dates before that date the correction is **NOT** carried
       out, even if the **utc** argument is set to True, and it is supposed
       that the input data is already in TT scale.

    .. note:: For conversions between TT and Universal Time (UT), please use
       the method :meth:`tt2ut`.

    .. note:: Internally, time values are stored as a Julian Ephemeris Day
       (JDE), based on the uniform scale of Dynamical Time, or more
       specifically, Terrestial Time (TT) (itself the redefinition of
       Terrestrial Dynamical Time, TDT).

    .. note:: The UTC-TT conversion is composed of three corrections:

       a. TT-TAI, comprising 32.184 s,
       b. TAI-UTC(1972), 10 s, and
       c. UTC(1972)-UTC(target)

       item c. is the corresponding amount of leap seconds to the target Epoch.
       When you do, for instance, **leap_seconds=43**, you modify the c. part.

    .. note:: Given that this class stores the epoch as JDE, if the JDE value
       is in the order of millions of days then, for a computer with 15-digit
       accuracy, the final time resolution is about 10 milliseconds. That is
       considered enough for most applications of this class.
    """

    def __init__(self, *args, **kwargs):
        """Epoch constructor.

        This constructor takes either a single JDE value, another Epoch object,
        or a series of values representing year, month, day, hours, minutes,
        seconds. This series of values are by default supposed to be in
        **Terrestial Time** (TT).

        It is also possible that the year, month, etc. arguments be provided in
        a tuple or list. Moreover, it is also possible provide :class:`date` or
        :class:`datetime` objects for initialization.

        The **month** value can be provided as an integer (1 = January, 2 =
        February, etc), or it can be provided with short (Jan, Feb,...) or long
        (January, February,...) names. Also, hours, minutes, seconds can be
        provided separately, or as decimals of the day value.

        When a UTC time is provided, the parameter **utc=True** must be given.
        Then, the input is converted to International Atomic Time (TAI) using
        an internal table of leap seconds, and from there, it is converted to
        (and stored as) Terrestrial Time (TT). If **utc** is not provided, it
        is supposed that the input data is already in TT scale.

        If a value is provided with the **leap_seconds** argument, then that
        value will be used for the UTC->TAI conversion, and the internal leap
        seconds table will be bypassed.

        :param args: Either JDE, Epoch, date, datetime or year, month, day,
           hours, minutes, seconds values, by themselves or inside a tuple or
           list
        :type args: int, float, :py:class:`Epoch`, tuple, list, date,
           datetime
        :param utc: Whether the provided epoch is a civil time (UTC)
        :type utc: bool
        :param leap_seconds: This is the value to be used in the UTC->TAI
            conversion, instead of taking it from internal leap seconds table.
        :type leap_seconds: int, float

        :returns: Epoch object.
        :rtype: :py:class:`Epoch`
        :raises: ValueError if input values are in the wrong range.
        :raises: TypeError if input values are of wrong type.

        >>> e = Epoch(1987, 6, 19.5)
        >>> print(e)
        2446966.0
        """

        # Initialize field
        self._jde = 0.0
        self.set(*args, **kwargs)  # Use 'set()' method to handle the setup

    def __hash__(self):
        """Method used to create  hash from an Epoch object.

        This method allows Epoch objects to be used as the key in a dictionary.
        """
        return float(self).__hash__()

    def set(self, *args, **kwargs):
        """Method used to set the value of this object.

        This method takes either a single JDE value, or a series of values
        representing year, month, day, hours, minutes, seconds. This series of
        values are by default supposed to be in **Terrestial Time** (TT).

        It is also possible to provide another Epoch object as input for the
        :meth:`set` method, or the year, month, etc arguments can be provided
        in a tuple or list. Moreover, it is also possible provide :class:`date`
        or :class:`datetime` objects for initialization.

        The **month** value can be provided as an integer (1 = January, 2 =
        February, etc), or it can be provided as short (Jan, Feb, ...) or long
        (January, February, ...) names. Also, hours, minutes, seconds can be
        provided separately, or as decimals of the day value.

        When a UTC time is provided, the parameter **utc=True** must be given.
        Then, the input is converted to International Atomic Time (TAI) using
        an internal table of leap seconds, and from there, it is converted to
        (and stored as) Terrestrial Time (TT). If **utc** is not provided, it
        is supposed that the input data is already in TT scale.

        If a value is provided with the **leap_seconds** argument, then that
        value will be used for the UTC->TAI conversion, and the internal leap
        seconds table will be bypassed.

        It is also possible to provide a local time with the parameter
        **local=True**. In such case, the method :meth:`utc2local()` is called
        to compute the LocalTime-UTC difference. This implies that the
        parameter **utc=True** is automatically set.

        .. note:: The UTC to TT correction is only carried out for dates after
           January 1st, 1972.

        .. note:: Please bear in mind that, in order for the method
           :meth:`utc2local()` to work, your operative system must be correctly
           configured, with the right time and corresponding time zone.

        :param args: Either JDE, Epoch, date, datetime or year, month, day,
           hours, minutes, seconds values, by themselves or inside a tuple or
           list.
        :type args: int, float, :py:class:`Epoch`, tuple, list, date,
           datetime
        :param utc: Whether the provided epoch is a civil time (UTC).
        :type utc: bool
        :param leap_seconds: This is the value to be used in the UTC->TAI
            conversion, instead of taking it from internal leap seconds table.
        :type leap_seconds: int, float
        :param local: Whether the provided epoch is a local time.
        :type utc: bool

        :returns: None.
        :rtype: None
        :raises: ValueError if input values are in the wrong range.
        :raises: TypeError if input values are of wrong type.

        >>> e = Epoch()
        >>> e.set(1987, 6, 19.5)
        >>> print(e)
        2446966.0
        >>> e.set(1977, 'Apr', 26.4)
        >>> print(e)
        2443259.9
        >>> e.set(1957, 'October', 4.81)
        >>> print(e)
        2436116.31
        >>> e.set(333, 'Jan', 27, 12)
        >>> print(e)
        1842713.0
        >>> e.set(1900, 'Jan', 1)
        >>> print(e)
        2415020.5
        >>> e.set(-1001, 'august', 17.9)
        >>> print(e)
        1355671.4
        >>> e.set(-4712, 1, 1.5)
        >>> print(e)
        0.0
        >>> e.set((1600, 12, 31))
        >>> print(e)
        2305812.5
        >>> e.set([1988, 'JUN', 19, 12])
        >>> print(e)
        2447332.0
        >>> d = datetime.date(2000, 1, 1)
        >>> e.set(d)
        >>> print(e)
        2451544.5
        >>> e.set(837, 'Apr', 10, 7, 12)
        >>> print(e)
        2026871.8
        >>> d = datetime.datetime(837, 4, 10, 7, 12, 0, 0)
        >>> e.set(d)
        >>> print(e)
        2026871.8
        >>> e = Epoch(JDE2000, utc=True)
        >>> print(round((e - JDE2000) * DAY2SEC, 3))
        64.184
        >>> e = Epoch(2451545.0, utc=True)
        >>> print(round((e - JDE2000) * DAY2SEC, 3))
        64.184
        >>> e = Epoch(JDE2000, local=True)
        >>> print(round((e - JDE2000) * DAY2SEC - Epoch.utc2local(), 3))
        64.184
        >>> e = Epoch(JDE2000, local=True, leap_seconds=35.0)
        >>> print(round((e - JDE2000) * DAY2SEC - Epoch.utc2local(), 3))
        77.184
        """

        # Clean up the internal parameters
        self._jde = 0.0
        # If no arguments are given, return. Internal values are 0.0
        if len(args) == 0:
            return
        # If we have only one argument, it can be a JDE, another Epoch object,
        # a tuple with year, month, day, etc or a datetime object
        elif len(args) == 1:
            if isinstance(args[0], Epoch):
                self._jde = args[0]._jde
                year, month, day, hours, minutes, sec = self.get_full_date()
            elif isinstance(args[0], (int, float)):
                self._jde = args[0]
                year, month, day, hours, minutes, sec = self.get_full_date()
            elif isinstance(args[0], (tuple, list)):
                year, month, day, hours, minutes, sec = \
                    self._check_values(*args[0])
            elif isinstance(args[0], datetime.datetime):
                d = args[0]
                year, month, day, hours, minutes, sec = self._check_values(
                    d.year,
                    d.month,
                    d.day,
                    d.hour,
                    d.minute,
                    d.second + d.microsecond / 1e6,
                )
            elif isinstance(args[0], datetime.date):
                d = args[0]
                year, month, day, hours, minutes, sec = self._check_values(
                    d.year, d.month, d.day
                )
            else:
                raise TypeError("Invalid input type")
        elif len(args) == 2:
            # Insuficient data to set the Epoch
            raise ValueError("Invalid number of input values")
        elif len(args) >= 3:  # Year, month, day
            year, month, day, hours, minutes, sec = self._check_values(*args)
        day += hours / DAY2HOURS + minutes / DAY2MIN + sec / DAY2SEC
        # Handle the 'leap_seconds' argument, if pressent
        if "leap_seconds" in kwargs:
            if "local" in kwargs:
                self._jde = self._compute_jde(
                    year, month, day, utc2tt=False,
                    leap_seconds=kwargs["leap_seconds"],
                    local=kwargs["local"])
            else:
                self._jde = self._compute_jde(
                    year, month, day, utc2tt=False,
                    leap_seconds=kwargs["leap_seconds"])
        elif "utc" in kwargs:
            self._jde = self._compute_jde(year, month, day,
                                          utc2tt=kwargs["utc"])
        elif "local" in kwargs:
            self._jde = self._compute_jde(year, month, day,
                                          local=kwargs["local"])
        else:
            self._jde = self._compute_jde(year, month, day, utc2tt=False)

    def _compute_jde(self, y, m, d, utc2tt=False, leap_seconds=0.0,
                     local=False):
        """Method to compute the Julian Ephemeris Day (JDE).

        .. note:: The UTC to TT correction is only carried out for dates after
           January 1st, 1972.

        :param y: Year
        :type y: int
        :param m: Month
        :type m: int
        :param d: Day
        :type d: float
        :param utc2tt: Whether correction UTC to TT is done automatically.
        :type utc2tt: bool
        :param leap_seconds: Number of leap seconds to apply.
        :type leap_seconds: float
        :param local: Whether a local time has been provided.
        :type utc2tt: bool

        :returns: Julian Ephemeris Day (JDE)
        :rtype: float
        """

        # The best approach here is first convert to JDE, and then adjust secs
        if m <= 2:
            y -= 1
            m += 12
        a = iint(y / 100.0)
        b = 0.0
        if not Epoch.is_julian(y, m, iint(d)):
            b = 2.0 - a + iint(a / 4.0)
        jde = (iint(365.25 * (y + 4716.0))
               + iint(30.6001 * (m + 1.0)) + d + b - 1524.5)
        # If enabled, let's convert from UTC to TT, adding the needed seconds
        deltasec = 0.0
        if local:
            deltasec = Epoch.utc2local()
            if not utc2tt and leap_seconds == 0.0:
                utc2tt = True
        # In this case, UTC to TT correction is applied automatically
        if utc2tt:
            if y >= 1972:
                deltasec += 32.184  # Difference between TT and TAI
                deltasec += 10.0  # Difference between UTC and TAI in 1972
                deltasec += Epoch.leap_seconds(y, m)
        else:  # Correction is NOT automatic
            if leap_seconds != 0.0:  # We apply provided leap seconds
                if y >= 1972:
                    deltasec += 32.184  # Difference between TT and TAI
                    deltasec += 10.0  # Difference between UTC-TAI in 1972
                    deltasec += leap_seconds
        return jde + deltasec / DAY2SEC

    def _check_values(self, *args):
        """This method takes the input arguments to 'set()' method (year,
        month, day, etc) and carries out some sanity checks on them.

        It returns a tuple containing those values separately, assigning zeros
        to those arguments which were not provided.

        :param args: Year, month, day, hours, minutes, seconds values.
        :type args: int, float

        :returns: Tuple with year, month, day, hours, minutes, seconds values.
        :rtype: tuple
        :raises: ValueError if input values are in the wrong range, or too few
        arguments given as input.
        """

        # This list holds the maximum amount of days a given month can have
        maxdays = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

        # Initialize some variables
        year = -9999
        month = -9999
        day = -9999
        hours = 0.0
        minutes = 0.0
        sec = 0.0

        # Carry out some basic checks
        if len(args) < 3:
            raise ValueError("Invalid number of input values")
        elif len(args) >= 3:  # Year, month, day
            year = args[0]
            month = args[1]
            day = args[2]
        if len(args) >= 4:  # Year, month, day, hour
            hours = args[3]
        if len(args) >= 5:  # Year, month, day, hour, minutes
            minutes = args[4]
        if len(args) >= 6:  # Year, month, day, hour, minutes, seconds
            sec = args[5]
        if year < -4712:  # No negative JDE will be allowed
            raise ValueError("Invalid value for the input year")
        if day < 1 or day >= 32:
            raise ValueError("Invalid value for the input day")
        if hours < 0 or hours >= 24:
            raise ValueError("Invalid value for the input hours")
        if minutes < 0 or minutes >= 60:
            raise ValueError("Invalid value for the input minutes")
        if sec < 0 or sec >= 60:
            raise ValueError("Invalid value for the input seconds")

        # Test the days according to the month
        month = Epoch.get_month(month)
        limit_day = maxdays[month - 1]
        # We need extra tests if month is '2' (February)
        if month == 2:
            if Epoch.is_leap(year):
                limit_day = 29
        # Add '1' to 'limit_day' in order to allow for fractional days
        if day >= limit_day + 1:
            raise ValueError("Invalid value for the input day")

        # We are ready to return the parameters
        return year, month, day, hours, minutes, sec

    @staticmethod
    def check_input_date(*args, **kwargs):
        """Method to check that the input is a proper date.

        This method returns an Epoch object, and the **leap_seconds** argument
        then controls the way the UTC->TT conversion is handled for that new
        object. If **leap_seconds** argument is set to a value different than
        zero, then that value will be used for the UTC->TAI conversion, and the
        internal leap seconds table will be bypassed. On the other hand, if it
        is set to zero, then the UTC to TT correction is disabled, and it is
        supposed that the input data is already in TT scale.

        :param args: Either Epoch, date, datetime or year, month, day values,
            by themselves or inside a tuple or list
        :type args: int, float, :py:class:`Epoch`, datetime, date, tuple,
            list
        :param leap_seconds: If different from zero, this is the value to be
           used in the UTC->TAI conversion. If equals to zero, conversion is
           disabled. If not given, UTC to TT conversion is carried out
           (default).
        :type leap_seconds: int, float

        :returns: Epoch object corresponding to the input date
        :rtype: :py:class:`Epoch`
        :raises: ValueError if input values are in the wrong range.
        :raises: TypeError if input values are of wrong type.
        """

        t = Epoch()
        if len(args) == 0:
            raise ValueError("Invalid input: No date given")
        # If we have only one argument, it can be an Epoch, a date, a datetime
        # or a tuple/list
        elif len(args) == 1:
            if isinstance(args[0], Epoch):
                t = args[0]
            elif isinstance(args[0], (tuple, list)):
                if len(args[0]) >= 3:
                    t = Epoch(args[0][0], args[0][1], args[0][2], **kwargs)
                else:
                    raise ValueError("Invalid input")
            elif isinstance(args[0], datetime.datetime) or isinstance(
                args[0], datetime.date
            ):
                t = Epoch(args[0].year, args[0].month, args[0].day, **kwargs)
            else:
                raise TypeError("Invalid input type")
        elif len(args) == 2:
            raise ValueError("Invalid input: Date given is not valid")
        elif len(args) >= 3:
            # We will rely on Epoch capacity to handle improper input
            t = Epoch(args[0], args[1], args[2], **kwargs)
        return t

    @staticmethod
    def is_julian(year, month, day):
        """This method returns True if given date is in the Julian calendar.

        :param year: Year
        :type y: int
        :param month: Month
        :type m: int
        :param day: Day
        :type day: int

        :returns: Whether the provided date belongs to Julian calendar or not.
        :rtype: bool

        >>> Epoch.is_julian(1997, 5, 27.1)
        False
        >>> Epoch.is_julian(1397, 7, 7.0)
        True
        """

        if (
            (year < 1582)
            or (year == 1582 and month < 10)
            or (year == 1582 and month == 10 and day < 5.0)
        ):
            return True
        else:
            return False

    def julian(self):
        """This method returns True if this Epoch object holds a date in the
        Julian calendar.

        :returns: Whether this Epoch object holds a date belonging to Julian
            calendar or not.
        :rtype: bool

        >>> e = Epoch(1997, 5, 27.1)
        >>> e.julian()
        False
        >>> e = Epoch(1397, 7, 7.0)
        >>> e.julian()
        True
        """

        y, m, d = self.get_date()
        return Epoch.is_julian(y, m, d)

    @staticmethod
    def get_month(month, as_string=False):
        """Method to get the month as a integer in the [1, 12] range, or as a
        full name.

        :param month: Month, in numeric, short name or long name format
        :type month: int, float, str
        :param as_string: Whether the output will be numeric, or a long name.
        :type as_string: bool

        :returns: Month as integer in the [1, 12] range, or as a long name.
        :rtype: int, str
        :raises: ValueError if input month value is invalid.

        >>> Epoch.get_month(4.0)
        4
        >>> Epoch.get_month('Oct')
        10
        >>> Epoch.get_month('FEB')
        2
        >>> Epoch.get_month('August')
        8
        >>> Epoch.get_month('august')
        8
        >>> Epoch.get_month('NOVEMBER')
        11
        >>> Epoch.get_month(9.0, as_string=True)
        'September'
        >>> Epoch.get_month('Feb', as_string=True)
        'February'
        >>> Epoch.get_month('March', as_string=True)
        'March'
        """

        months_mmm = [
            "Jan",
            "Feb",
            "Mar",
            "Apr",
            "May",
            "Jun",
            "Jul",
            "Aug",
            "Sep",
            "Oct",
            "Nov",
            "Dec",
        ]

        months_full = [
            "January",
            "February",
            "March",
            "April",
            "May",
            "June",
            "July",
            "August",
            "September",
            "October",
            "November",
            "December",
        ]

        if isinstance(month, (int, float)):
            month = int(month)  # Truncate if it has decimals
            if month >= 1 and month <= 12:
                if not as_string:
                    return month
                else:
                    return months_full[month - 1]
            else:
                raise ValueError("Invalid value for the input month")
        elif isinstance(month, str):
            month = month.strip().capitalize()
            if len(month) == 3:
                if month in months_mmm:
                    if not as_string:
                        return months_mmm.index(month) + 1
                    else:
                        return months_full[months_mmm.index(month)]
                else:
                    raise ValueError("Invalid value for the input month")
            else:
                if month in months_full:
                    if not as_string:
                        return months_full.index(month) + 1
                    else:
                        return month
                else:
                    raise ValueError("Invalid value for the input month")

    @staticmethod
    def is_leap(year):
        """Method to check if a given year is a leap year.

        :param year: Year to be checked.
        :type year: int, float

        :returns: Whether or not year is a leap year.
        :rtype: bool
        :raises: ValueError if input year value is invalid.

        >>> Epoch.is_leap(2003)
        False
        >>> Epoch.is_leap(2012)
        True
        >>> Epoch.is_leap(1900)
        False
        >>> Epoch.is_leap(-1000)
        True
        >>> Epoch.is_leap(1000)
        True
        """

        if isinstance(year, (int, float)):
            # Mind the difference between Julian and Gregorian calendars
            if year >= 1582:
                year = iint(year)
                return calendar.isleap(year)
            else:
                return (abs(year) % 4) == 0
        else:
            raise ValueError("Invalid value for the input year")

    def leap(self):
        """This method checks if the current Epoch object holds a leap year.

        :returns: Whether or the year in this Epoch object is a leap year.
        :rtype: bool

        >>> e = Epoch(2003, 1, 1)
        >>> e.leap()
        False
        >>> e = Epoch(2012, 1, 1)
        >>> e.leap()
        True
        >>> e = Epoch(1900, 1, 1)
        >>> e.leap()
        False
        >>> e = Epoch(-1000, 1, 1)
        >>> e.leap()
        True
        >>> e = Epoch(1000, 1, 1)
        >>> e.leap()
        True
        """

        y, m, d = self.get_date()
        return Epoch.is_leap(y)

    @staticmethod
    def get_doy(yyyy, mm, dd):
        """This method returns the Day Of Year (DOY) for the given date.

        :param yyyy: Year, in four digits format
        :type yyyy: int, float
        :param mm: Month, in numeric format (1 = January, 2 = February, etc)
        :type mm: int, float
        :param dd: Day, in numeric format
        :type dd: int, float

        :returns: Day Of Year (DOY).
        :rtype: float
        :raises: ValueError if input values correspond to a wrong date.

        >>> Epoch.get_doy(1999, 1, 29)
        29.0
        >>> Epoch.get_doy(1978, 11, 14)
        318.0
        >>> Epoch.get_doy(2017, 12, 31.7)
        365.7
        >>> Epoch.get_doy(2012, 3, 3.1)
        63.1
        >>> Epoch.get_doy(-400, 2, 29.9)
        60.9
        """

        # Let's carry out first some basic checks
        if dd < 1 or dd >= 32 or mm < 1 or mm > 12:
            raise ValueError("Invalid input data")
        day = int(dd)
        frac = dd % 1
        if yyyy >= 1:  # datetime's minimum year is 1
            try:
                d = datetime.date(yyyy, mm, day)
            except ValueError:
                raise ValueError("Invalid input date")
            doy = d.timetuple().tm_yday
        else:
            k = 2 if Epoch.is_leap(yyyy) else 1
            doy = (iint((275.0 * mm) / 9.0)
                   - k * iint((mm + 9.0) / 12.0) + day - 30.0)
        return float(doy + frac)

    def doy(self):
        """This method returns the Day Of Year (DOY) for the current Epoch
        object.

        :returns: Day Of Year (DOY).
        :rtype: float

        >>> e = Epoch(1999, 1, 29)
        >>> round(e.doy(), 1)
        29.0
        >>> e = Epoch(2017, 12, 31.7)
        >>> round(e.doy(), 1)
        365.7
        >>> e = Epoch(2012, 3, 3.1)
        >>> round(e.doy(), 1)
        63.1
        >>> e = Epoch(-400, 2, 29.9)
        >>> round(e.doy(), 1)
        60.9
        """

        y, m, d = self.get_date()
        return Epoch.get_doy(y, m, d)

    @staticmethod
    def doy2date(year, doy):
        """This method takes a year and a Day Of Year values, and returns the
        corresponding date.

        :param year: Year, in four digits format
        :type year: int, float
        :param doy: Day of Year number
        :type doy: int, float

        :returns: Year, month, day.
        :rtype: tuple
        :raises: ValueError if either input year or doy values are invalid.

        >>> t = Epoch.doy2date(1999, 29)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        1999/1/29.0
        >>> t = Epoch.doy2date(2017, 365.7)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        2017/12/31.7
        >>> t = Epoch.doy2date(2012, 63.1)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        2012/3/3.1
        >>> t = Epoch.doy2date(-1004, 60)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        -1004/2/29.0
        >>> t = Epoch.doy2date(0, 60)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        0/2/29.0
        >>> t = Epoch.doy2date(1, 60)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        1/3/1.0
        >>> t = Epoch.doy2date(-1, 60)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        -1/3/1.0
        >>> t = Epoch.doy2date(-2, 60)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        -2/3/1.0
        >>> t = Epoch.doy2date(-3, 60)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        -3/3/1.0
        >>> t = Epoch.doy2date(-4, 60)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        -4/2/29.0
        >>> t = Epoch.doy2date(-5, 60)
        >>> print("{}/{}/{}".format(t[0], t[1], round(t[2], 1)))
        -5/3/1.0
        """

        if isinstance(year, (int, float)) and isinstance(doy, (int, float)):
            frac = float(doy % 1)
            doy = int(doy)
            if year >= 1:  # datetime's minimum year is 1
                ref = datetime.date(year, 1, 1)
                mydate = datetime.date.fromordinal(ref.toordinal() + doy - 1)
                return year, mydate.month, mydate.day + frac
            else:
                # The algorithm provided by Meeus doesn't work for years below
                # +1. This little hack solves that problem (the 'if' result is
                # inverted here).
                k = 1 if Epoch.is_leap(year) else 2
                if doy < 32:
                    m = 1
                else:
                    m = iint((9.0 * (k + doy)) / 275.0 + 0.98)
                d = (doy - iint((275.0 * m) / 9.0)
                     + k * iint((m + 9.0) / 12.0) + 30)
                return year, int(m), d + frac
        else:
            raise ValueError("Invalid input values")

    @staticmethod
    def leap_seconds(year, month):
        """Returns the leap seconds accumulated for the given year and month.

        :param year: Year
        :type year: int
        :param month: Month, in numeric format ([1:12] range)
        :type month: int

        :returns: Leap seconds accumulated for given year and month.
        :rtype: int

        >>> Epoch.leap_seconds(1972, 4)
        0
        >>> Epoch.leap_seconds(1972, 6)
        0
        >>> Epoch.leap_seconds(1972, 7)
        1
        >>> Epoch.leap_seconds(1983, 6)
        11
        >>> Epoch.leap_seconds(1983, 7)
        12
        >>> Epoch.leap_seconds(1985, 8)
        13
        >>> Epoch.leap_seconds(2016, 11)
        26
        >>> Epoch.leap_seconds(2017, 1)
        27
        >>> Epoch.leap_seconds(2018, 7)
        27
        """

        list_years = sorted(LEAP_TABLE.keys())
        # First test the extremes of the table
        if (year + month / 12.0) <= list_years[0]:
            return 0
        if (year + month / 12.0) >= list_years[-1]:
            return LEAP_TABLE[list_years[-1]]
        lyear = (year + 0.25) if month <= 6 else (year + 0.75)
        idx = 0
        while lyear > list_years[idx]:
            idx += 1
        return LEAP_TABLE[list_years[idx - 1]]

    @staticmethod
    def get_last_leap_second():
        """Method to get the date and value of the last leap second added to
        the table

        :returns: Tuple with year, month, day, leap second value.
        :rtype: tuple
        """

        list_years = sorted(LEAP_TABLE.keys())
        lyear = list_years[-1]
        lseconds = LEAP_TABLE[lyear]
        year = iint(lyear)
        # So far, leap seconds are added either on June 30th or December 31th
        if lyear % 1 == 0.0:
            year -= 1
            month = 12
            day = 31.0
        else:
            month = 6
            day = 30.0
        return year, month, day, lseconds

    @staticmethod
    def utc2local():
        """Method to return the difference between UTC and local time.

        This method provides you the seconds that you have to add or subtract
        to UTC time to convert to your local time. The correct way to apply
        this method is according to the expression:

        utc2local() = LocalTime - UTC

        Therefore, if for example you are located in a place whose
        corresponding time zone is UTC+2, then this method will yield 7200
        (seconds in two hours) and you must then apply this expression:

        LocalTime = UTC + utclocal() = UTC + 7200

        .. note:: Please bear in mind that, in order for this method to work,
           your operative system must be correctly configured, with the right
           time and corresponding time zone.

        .. note:: Remember that class :py:class:`Epoch` internally stores time
           as Terrestrial Time (TT), not UTC. In order to get the UTC time you
           must use a method like :meth:`get_date()` or :meth:`get_full_date()`
           and pass the parameter **utc=True**.

        :returns: Difference in seconds between local and UTC time, in that
            order.
        :rtype: float
        """

        localhour = datetime.datetime.now().hour
        utchour = datetime.datetime.utcnow().hour
        localminute = datetime.datetime.now().minute
        utcminute = datetime.datetime.utcnow().minute
        return ((localhour - utchour) * 3600.0
                + (localminute - utcminute) * 60.0)

    @staticmethod
    def easter(year):
        """Method to return the Easter day for given year.

        .. note:: This method is valid for both Gregorian and Julian years.

        :param year: Year
        :type year: int

        :returns: Easter month and day, as a tuple
        :rtype: tuple
        :raises: TypeError if input values are of wrong type.

        >>> Epoch.easter(1991)
        (3, 31)
        >>> Epoch.easter(1818)
        (3, 22)
        >>> Epoch.easter(1943)
        (4, 25)
        >>> Epoch.easter(2000)
        (4, 23)
        >>> Epoch.easter(1954)
        (4, 18)
        >>> Epoch.easter(179)
        (4, 12)
        >>> Epoch.easter(1243)
        (4, 12)
        """

        # This algorithm is describes in pages 67-69 of Meeus book
        if not isinstance(year, (int, float)):
            raise TypeError("Invalid input type")
        year = int(year)
        if year >= 1583:
            # In this case, we are using the Gregorian calendar
            a = year % 19
            b = iint(year / 100.0)
            c = year % 100
            d = iint(b / 4.0)
            e = b % 4
            f = iint((b + 8.0) / 25.0)
            g = iint((b - f + 1.0) / 3.0)
            h = (19 * a + b - d - g + 15) % 30
            i = iint(c / 4.0)
            k = c % 4
            ll = (32 + 2 * (e + i) - h - k) % 7
            m = iint((a + 11 * h + 22 * ll) / 451.0)
            n = iint((h + ll - 7 * m + 114) / 31.0)
            p = (h + ll - 7 * m + 114) % 31
            return (n, p + 1)
        else:
            # The Julian calendar is used here
            a = year % 4
            b = year % 7
            c = year % 19
            d = (19 * c + 15) % 30
            e = (2 * a + 4 * b - d + 34) % 7
            f = iint((d + e + 114) / 31.0)
            g = (d + e + 114) % 31
            return (f, g + 1)

    @staticmethod
    def jewish_pesach(year):
        """Method to return the Jewish Easter (Pesach) day for given year.

        .. note:: This method is valid for both Gregorian and Julian years.

        :param year: Year
        :type year: int

        :returns: Jewish Easter (Pesach) month and day, as a tuple
        :rtype: tuple
        :raises: TypeError if input values are of wrong type.

        >>> Epoch.jewish_pesach(1990)
        (4, 10)
        """

        # This algorithm is described in pages 71-73 of Meeus book
        if not isinstance(year, (int, float)):
            raise TypeError("Invalid input type")
        year = iint(year)
        c = iint(year / 100.0)
        s = 0 if year < 1583 else iint((3.0 * c - 5.0) / 4.0)
        a = (12 * (year + 1)) % 19
        b = year % 4
        q = (-1.904412361576 + 1.554241796621 * a
             + 0.25 * b - 0.003177794022 * year + s)
        j = (iint(q) + 3 * year + 5 * b + 2 + s) % 7
        r = q - iint(q)
        if j == 2 or j == 4 or j == 6:
            d = iint(q) + 23
        elif j == 1 and a > 6 and r > 0.632870370:
            d = iint(q) + 24
        elif j == 0 and a > 11 and r > 0.897723765:
            d = iint(q) + 23
        else:
            d = iint(q) + 22
        if d > 31:
            return (4, d - 31)
        else:
            return (3, d)

    @staticmethod
    def moslem2gregorian(year, month, day):
        """Method to convert a date in the Moslen calendar to the Gregorian
        (or Julian) calendar.

        .. note:: This method is valid for both Gregorian and Julian years.

        :param year: Year
        :type year: int
        :param month: Month
        :type month: int
        :param day: Day
        :type day: int

        :returns: Date in Gregorian (Julian) calendar: year, month and day, as
           a tuple
        :rtype: tuple
        :raises: TypeError if input values are of wrong type.

        >>> Epoch.moslem2gregorian(1421, 1, 1)
        (2000, 4, 6)
        """

        # First, check that input types are correct
        if (
            not isinstance(year, (int, float))
            or not isinstance(month, (int, float))
            or not isinstance(day, (int, float))
        ):
            raise TypeError("Invalid input type")
        if day < 1 or day > 30 or month < 1 or month > 12 or year < 1:
            raise ValueError("Invalid input data")
        # This algorithm is described in pages 73-75 of Meeus book
        # Note: Ramadan is month Nr. 9
        h = iint(year)
        m = iint(month)
        d = iint(day)
        n = d + iint(29.5001 * (m - 1) + 0.99)
        q = iint(h / 30.0)
        r = h % 30
        a = iint((11.0 * r + 3.0) / 30.0)
        w = 404 * q + 354 * r + 208 + a
        q1 = iint(w / 1461.0)
        q2 = w % 1461
        g = 621 + 4 * iint(7.0 * q + q1)
        k = iint(q2 / 365.2422)
        e = iint(365.2422 * k)
        j = q2 - e + n - 1
        x = g + k
        if j > 366 and x % 4 == 0:
            j -= 366
            x += 1
        elif j > 365 and x % 4 > 0:
            j -= 365
            x += 1

        # Check if date is in Gregorian calendar. '277' is DOY of October 4th
        if (x > 1583) or (x == 1582 and j > 277):
            jd = iint(365.25 * (x - 1.0)) + 1721423 + j
            alpha = iint((jd - 1867216.25) / 36524.25)
            beta = jd if jd < 2299161 else (jd + 1 + alpha - iint(alpha / 4.0))
            b = beta + 1524
            c = iint((b - 122.1) / 365.25)
            d = iint(365.25 * c)
            e = iint((b - d) / 30.6001)
            day = b - d - iint(30.6001 * e)
            month = (e - 1) if e < 14 else (e - 13)
            year = (c - 4716) if month > 2 else (c - 4715)
            return year, month, day
        else:
            # It is a Julian date. We have year and DOY
            return Epoch.doy2date(x, j)

    @staticmethod
    def gregorian2moslem(year, month, day):
        """Method to convert a date in the Gregorian (or Julian) calendar to
        the Moslen calendar.

        :param year: Year
        :type year: int
        :param month: Month
        :type month: int
        :param day: Day
        :type day: int

        :returns: Date in Moslem calendar: year, month and day, as a tuple
        :rtype: tuple
        :raises: TypeError if input values are of wrong type.

        >>> Epoch.gregorian2moslem(1991, 8, 13)
        (1412, 2, 2)
        """

        # First, check that input types are correct
        if (
            not isinstance(year, (int, float))
            or not isinstance(month, (int, float))
            or not isinstance(day, (int, float))
        ):
            raise TypeError("Invalid input type")
        if day < 1 or day > 31 or month < 1 or month > 12 or year < -4712:
            raise ValueError("Invalid input data")
        # This algorithm is described in pages 75-76 of Meeus book
        x = iint(year)
        m = iint(month)
        d = iint(day)
        if m < 3:
            x -= 1
            m += 12
        alpha = iint(x / 100.0)
        beta = 2 - alpha + iint(alpha / 4.0)
        b = iint(365.25 * x) + iint(30.6001 * (m + 1.0)) + d + 1722519 + beta
        c = iint((b - 122.1) / 365.25)
        d = iint(365.25 * c)
        e = iint((b - d) / 30.6001)
        d = b - d - iint(30.6001 * e)
        m = (e - 1) if e < 14 else (e - 13)
        x = (c - 4716) if month > 2 else (c - 4715)
        w = 1 if x % 4 == 0 else 2
        n = iint((275.0 * m) / 9.0) - w * iint((m + 9.0) / 12.0) + d - 30
        a = x - 623
        b = iint(a / 4.0)
        c = a % 4
        c1 = 365.2501 * c
        c2 = iint(c1)
        if c1 - c2 > 0.5:
            c2 += 1
        dp = 1461 * b + 170 + c2
        q = iint(dp / 10631.0)
        r = dp % 10631
        j = iint(r / 354.0)
        k = r % 354
        o = iint((11.0 * j + 14.0) / 30.0)
        h = 30 * q + j + 1
        jj = k - o + n - 1
        # jj is the number of the day in the moslem year h. If jj > 354 we need
        # to know if h is a leap year
        if jj > 354:
            cl = h % 30
            dl = (11 * cl + 3) % 30
            if dl < 19:
                jj -= 354
                h += 1
            else:
                jj -= 355
                h += 1
            if jj == 0:
                jj = 355
                h -= 1
        # Now, let's convert DOY jj to month and day
        if jj == 355:
            m = 12
            d = 30
        else:
            s = iint((jj - 1.0) / 29.5)
            m = 1 + s
            d = iint(jj - 29.5 * s)
        return h, m, d

    def __str__(self):
        """Method used when trying to print the object.

        :returns: Internal JDE value as a string.
        :rtype: string

        >>> e = Epoch(1987, 6, 19.5)
        >>> print(e)
        2446966.0
        """

        return str(self._jde)

    def __repr__(self):
        """Method providing the 'official' string representation of the object.
        It provides a valid expression that could be used to recreate the
        object.

        :returns: As string with a valid expression to recreate the object
        :rtype: string

        >>> e = Epoch(1987, 6, 19.5)
        >>> repr(e)
        'Epoch(2446966.0)'
        """

        return "{}({})".format(self.__class__.__name__, self._jde)

    def get_date(self, **kwargs):
        """This method converts the internal JDE value back to a date.

        Use **utc=True** to enable the TT to UTC conversion mechanism, or
        provide a non zero value to **leap_seconds** to apply a specific leap
        seconds value.

        It is also possible to retrieve a local time with the parameter
        **local=True**. In such case, the method :meth:`utc2local()` is called
        to compute the LocalTime-UTC difference. This implies that the
        parameter **utc=True** is automatically set.

        .. note:: Please bear in mind that, in order for the method
           :meth:`utc2local()` to work, your operative system must be correctly
           configured, with the right time and corresponding time zone.

        :param utc: Whether the TT to UTC conversion mechanism will be enabled
        :type utc: bool
        :param leap_seconds: Optional value for leap seconds.
        :type leap_seconds: int, float
        :param local: Whether the retrieved epoch is converted to local time.
        :type utc: bool

        :returns: Year, month, day in a tuple
        :rtype: tuple

        >>> e = Epoch(2436116.31)
        >>> y, m, d = e.get_date()
        >>> print("{}/{}/{}".format(y, m, round(d, 2)))
        1957/10/4.81
        >>> e = Epoch(1988, 1, 27)
        >>> y, m, d = e.get_date()
        >>> print("{}/{}/{}".format(y, m, round(d, 2)))
        1988/1/27.0
        >>> e = Epoch(1842713.0)
        >>> y, m, d = e.get_date()
        >>> print("{}/{}/{}".format(y, m, round(d, 2)))
        333/1/27.5
        >>> e = Epoch(1507900.13)
        >>> y, m, d = e.get_date()
        >>> print("{}/{}/{}".format(y, m, round(d, 2)))
        -584/5/28.63
        """

        jd = self._jde + 0.5
        z = iint(jd)
        f = jd % 1
        if z < 2299161:
            a = z
        else:
            alpha = iint((z - 1867216.25) / 36524.25)
            a = z + 1 + alpha - iint(alpha / 4.0)
        b = a + 1524
        c = iint((b - 122.1) / 365.25)
        d = iint(365.25 * c)
        e = iint((b - d) / 30.6001)
        day = b - d - iint(30.6001 * e) + f
        if e < 14:
            month = e - 1
        elif e == 14 or e == 15:
            month = e - 13
        if month > 2:
            year = c - 4716
        elif month == 1 or month == 2:
            year = c - 4715
        year = int(year)
        month = int(month)

        # If enabled, let's convert from TT to UTC, subtracting needed seconds
        deltasec = 0.0
        tt2utc = False
        if "utc" in kwargs:
            tt2utc = kwargs["utc"]
        if "leap_seconds" in kwargs:
            tt2utc = False
            leap_seconds = kwargs["leap_seconds"]
        else:
            leap_seconds = 0.0
        if "local" in kwargs:
            deltasec = Epoch.utc2local()
            if not tt2utc and leap_seconds == 0.0:
                tt2utc = True
        # In this case, TT to UTC correction is applied automatically, but only
        # for dates after July 1st, 1972
        if tt2utc:
            if year > 1972 or (year == 1972 and month >= 7):
                deltasec += 32.184  # Difference between TT and TAI
                deltasec += 10.0  # Difference between UTC and TAI in 1972
                deltasec += Epoch.leap_seconds(year, month)
        else:  # Correction is NOT automatic
            if leap_seconds != 0.0:  # We apply provided leap seconds
                if year > 1972 or (year == 1972 and month >= 7):
                    deltasec += 32.184  # Difference between TT and TAI
                    deltasec += 10.0  # Difference between UTC-TAI in 1972
                    deltasec += leap_seconds
        # Apply the correction if needed
        if deltasec != 0.0:
            doy = Epoch.get_doy(year, month, day)
            doy -= deltasec / DAY2SEC
            # Check that we didn't change year
            if doy < 1.0:
                year -= 1
                doy = 366.0 + doy if Epoch.is_leap(year) else 365.0 + doy
            year, month, day = Epoch.doy2date(year, doy)
        return year, month, day

    def get_full_date(self, **kwargs):
        """This method converts the internal JDE value back to a full date.

        Use **utc=True** to enable the TT to UTC conversion mechanism, or
        provide a non zero value to **leap_seconds** to apply a specific leap
        seconds value.

        It is also possible to retrieve a local time with the parameter
        **local=True**. In such case, the method :meth:`utc2local()` is called
        to compute the LocalTime-UTC difference. This implies that the
        parameter **utc=True** is automatically set.

        .. note:: Please bear in mind that, in order for the method
           :meth:`utc2local()` to work, your operative system must be correctly
           configured, with the right time and corresponding time zone.

        :param utc: Whether the TT to UTC conversion mechanism will be enabled
        :type utc: bool
        :param leap_seconds: Optional value for leap seconds.
        :type leap_seconds: int, float
        :param local: Whether the retrieved epoch is converted to local time.
        :type utc: bool

        :returns: Year, month, day, hours, minutes, seconds in a tuple
        :rtype: tuple

        >>> e = Epoch(2436116.31)
        >>> y, m, d, h, mi, s = e.get_full_date()
        >>> print("{}/{}/{} {}:{}:{}".format(y, m, d, h, mi, round(s, 1)))
        1957/10/4 19:26:24.0
        >>> e = Epoch(1988, 1, 27)
        >>> y, m, d, h, mi, s = e.get_full_date()
        >>> print("{}/{}/{} {}:{}:{}".format(y, m, d, h, mi, round(s, 1)))
        1988/1/27 0:0:0.0
        >>> e = Epoch(1842713.0)
        >>> y, m, d, h, mi, s = e.get_full_date()
        >>> print("{}/{}/{} {}:{}:{}".format(y, m, d, h, mi, round(s, 1)))
        333/1/27 12:0:0.0
        >>> e = Epoch(1507900.13)
        >>> y, m, d, h, mi, s = e.get_full_date()
        >>> print("{}/{}/{} {}:{}:{}".format(y, m, d, h, mi, round(s, 1)))
        -584/5/28 15:7:12.0
        """

        y, m, d = self.get_date(**kwargs)
        r = d % 1
        d = int(d)
        h = int(r * 24.0)
        r = r * 24 - h
        mi = int(r * 60.0)
        s = 60.0 * (r * 60.0 - mi)
        return y, m, d, h, mi, s

    @staticmethod
    def tt2ut(year, month):
        """This method provides an approximation of the difference, in seconds,
        between Terrestrial Time and Universal Time, denoted **DeltaT**, where:
        DeltaT = TT - UT.

        Here we depart from Meeus book and use the polynomial expressions from:

        https://eclipse.gsfc.nasa.gov/LEcat5/deltatpoly.html

        Which are regarded as more elaborate and precise than Meeus'.

        Please note that, by definition, the UTC time used internally in this
        Epoch class by default is kept within 0.9 seconds from UT. Therefore,
        UTC is in itself a quite good approximation to UT, arguably better than
        some of the results provided by this method.

        :param year: Year we want to compute DeltaT for.
        :type year: int, float
        :param month: Month we want to compute DeltaT for.
        :type month: int, float

        :returns: DeltaT, in seconds
        :rtype: float

        >>> round(Epoch.tt2ut(1642, 1), 1)
        62.1
        >>> round(Epoch.tt2ut(1680, 1), 1)
        15.3
        >>> round(Epoch.tt2ut(1700, 1), 1)
        8.8
        >>> round(Epoch.tt2ut(1726, 1), 1)
        10.9
        >>> round(Epoch.tt2ut(1750, 1), 1)
        13.4
        >>> round(Epoch.tt2ut(1774, 1), 1)
        16.7
        >>> round(Epoch.tt2ut(1800, 1), 1)
        13.7
        >>> round(Epoch.tt2ut(1820, 1), 1)
        11.9
        >>> round(Epoch.tt2ut(1890, 1), 1)
        -6.1
        >>> round(Epoch.tt2ut(1928, 2), 1)
        24.2
        >>> round(Epoch.tt2ut(1977, 2), 1)
        47.7
        >>> round(Epoch.tt2ut(1998, 1), 1)
        63.0
        >>> round(Epoch.tt2ut(2015, 7), 1)
        69.3
        """

        y = year + (month - 0.5) / 12.0
        if year < -500:
            u = (year - 1820.0) / 100.0
            dt = -20.0 + 32.0 * u * u
        elif year >= -500 and year < 500:
            u = y / 100.0
            dt = 10583.6 + u * (
                -1014.41
                + u
                * (
                    33.78311
                    + u
                    * (
                        -5.952053
                        + (u * (-0.1798452
                                + u * (0.022174192 + 0.0090316521 * u)))
                    )
                )
            )
        elif year >= 500 and year < 1600:
            u = (year - 1000) / 100.0
            dt = 1574.2 + u * (
                -556.01
                + u
                * (
                    71.23472
                    + u
                    * (
                        0.319781
                        + (u * (-0.8503463
                                + u * (-0.005050998 + 0.0083572073 * u)))
                    )
                )
            )
        elif year >= 1600 and year < 1700:
            t = y - 1600.0
            dt = 120.0 + t * (-0.9808 + t * (-0.01532 + t / 7129.0))
        elif year >= 1700 and year < 1800:
            t = y - 1700.0
            dt = 8.83 + t * (
                0.1603 + t * (-0.0059285 + t * (0.00013336 - t / 1174000.0))
            )
        elif year >= 1800 and year < 1860:
            t = y - 1800.0
            dt = 13.72 + t * (
                -0.332447
                + t
                * (
                    0.0068612
                    + t
                    * (
                        0.0041116
                        + t
                        * (
                            -0.00037436
                            + t
                            * (0.0000121272 + t * (-0.0000001699
                                                   + 0.000000000875 * t))
                        )
                    )
                )
            )
        elif year >= 1860 and year < 1900:
            t = y - 1860.0
            dt = 7.62 + t * (
                0.5737
                + t
                * (-0.251754 + t * (0.01680668
                                    + t * (-0.0004473624 + t / 233174.0)))
            )
        elif year >= 1900 and year < 1920:
            t = y - 1900.0
            dt = -2.79 + t * (
                1.494119 + t * (-0.0598939 + t * (0.0061966 - 0.000197 * t))
            )
        elif year >= 1920 and year < 1941:
            t = y - 1920.0
            dt = 21.20 + t * (0.84493 + t * (-0.076100 + 0.0020936 * t))
        elif year >= 1941 and year < 1961:
            t = y - 1950.0
            dt = 29.07 + t * (0.407 + t * (-1.0 / 233.0 + t / 2547.0))
        elif year >= 1961 and year < 1986:
            t = y - 1975.0
            dt = 45.45 + t * (1.067 + t * (-1.0 / 260.0 - t / 718.0))
        elif year >= 1986 and year < 2005:
            t = y - 2000.0
            dt = 63.86 + t * (
                0.3345
                + t
                * (-0.060374 + t * (0.0017275
                                    + t * (0.000651814 + 0.00002373599 * t)))
            )
        elif year >= 2005 and year < 2050:
            t = y - 2000.0
            dt = 62.92 + t * (0.32217 + 0.005589 * t)
        elif year >= 2050 and year < 2150:
            dt = (-20.0 + 32.0 * ((y - 1820.0) / 100.0) ** 2
                  - 0.5628 * (2150.0 - y))
        else:
            u = (year - 1820.0) / 100.0
            dt = -20.0 + 32.0 * u * u
        return dt

    def dow(self, as_string=False):
        """Method to return the day of week corresponding to this Epoch.

        By default, this method returns an integer value: 0 for Sunday, 1 for
        Monday, etc. However, when **as_string=True** is passed, the names of
        the days are returned.

        :param as_string: Whether result will be given as a integer or as a
           string. False by default.
        :type as_string: bool

        :returns: Day of the week, as a integer or as a string.
        :rtype: int, str

        >>> e = Epoch(1954, 'June', 30)
        >>> e.dow()
        3
        >>> e = Epoch(2018, 'Feb', 14.9)
        >>> e.dow(as_string=True)
        'Wednesday'
        >>> e = Epoch(2018, 'Feb', 15)
        >>> e.dow(as_string=True)
        'Thursday'
        >>> e = Epoch(2018, 'Feb', 15.99)
        >>> e.dow(as_string=True)
        'Thursday'
        >>> e.set(2018, 'Jul', 15.4)
        >>> e.dow(as_string=True)
        'Sunday'
        >>> e.set(2018, 'Jul', 15.9)
        >>> e.dow(as_string=True)
        'Sunday'
        """

        jd = iint(self._jde - 0.5) + 2.0
        doy = iint(jd % 7)
        if not as_string:
            return doy
        else:
            day_names = [
                "Sunday",
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
            ]
            return day_names[doy]

    def mean_sidereal_time(self):
        """Method to compute the _mean_ sidereal time at Greenwich for the
        epoch stored in this object. It represents the Greenwich hour angle of
        the mean vernal equinox.

        .. note:: If you require the result as an angle, you should convert the
           result from this method to hours with decimals (with
           :const:`DAY2HOURS`), and then multiply by 15 deg/hr. Alternatively,
           you can convert the result to hours with decimals, and feed this
           value to an :class:`Angle` object, setting **ra=True**, and making
           use of :class:`Angle` facilities for further handling.

        :returns: Mean sidereal time, in days
        :rtype: float

        >>> e = Epoch(1987, 4, 10)
        >>> round(e.mean_sidereal_time(), 9)
        0.549147764
        >>> e = Epoch(1987, 4, 10, 19, 21, 0.0)
        >>> round(e.mean_sidereal_time(), 9)
        0.357605204
        """

        jd0 = iint(self()) + 0.5 if self() % 1 >= 0.5 else iint(self()) - 0.5
        t = (jd0 - 2451545.0) / 36525.0
        theta0 = 6.0 / DAY2HOURS + 41.0 / DAY2MIN + 50.54841 / DAY2SEC
        s = t * (8640184.812866 + t * (0.093104 - 0.0000062 * t))
        theta0 += (s % DAY2SEC) / DAY2SEC
        deltajd = self() - jd0
        if abs(deltajd) < TOL:  # In this case, we are done
            return theta0 % 1
        else:
            deltajd *= 1.00273790935
            return (theta0 + deltajd) % 1

    def apparent_sidereal_time(self, true_obliquity, nutation_longitude):
        """Method to compute the _apparent_ sidereal time at Greenwich for the
        epoch stored in this object. It represents the Greenwich hour angle of
        the true vernal equinox.

        .. note:: If you require the result as an angle, you should convert the
           result from this method to hours with decimals (with
           :const:`DAY2HOURS`), and then multiply by 15 deg/hr. Alternatively,
           you can convert the result to hours with decimals, and feed this
           value to an :class:`Angle` object, setting **ra=True**, and making
           use of :class:`Angle` facilities for further handling.

        :param true_obliquity: The true obliquity of the ecliptic as an int,
            float or :class:`Angle`, in degrees. You can use the method
            `Earth.true_obliquity()` to find it.
        :type true_obliquity: int, float, :class:`Angle`
        :param nutation_longitude: The nutation in longitude as an int, float
            or :class:`Angle`, in degrees. You can use method
            `Earth.nutation_longitude()` to find it.
        :type nutation_longitude: int, float, :class:`Angle`

        :returns: Apparent sidereal time, in days
        :rtype: float
        :raises: TypeError if input value is of wrong type.

        >>> e = Epoch(1987, 4, 10)
        >>> round(e.apparent_sidereal_time(23.44357, (-3.788)/3600.0), 8)
        0.54914508
        """

        if not (
            isinstance(true_obliquity, (int, float, Angle))
            and isinstance(nutation_longitude, (int, float, Angle))
        ):
            raise TypeError("Invalid input value")
        if isinstance(true_obliquity, Angle):
            true_obliquity = float(true_obliquity)  # Convert to a float
        if isinstance(nutation_longitude, Angle):
            nutation_longitude = float(nutation_longitude)
        mean_stime = self.mean_sidereal_time()
        epsilon = radians(true_obliquity)  # Convert to radians
        delta_psi = nutation_longitude * 3600.0  # From degrees to seconds
        # Correction is in seconds of arc: It must be converted to seconds of
        # time, and then to days (sidereal time is given here in days)
        return mean_stime + ((delta_psi * cos(epsilon)) / 15.0) / DAY2SEC

    def mjd(self):
        """This method returns the Modified Julian Day (MJD).

        :returns: Modified Julian Day (MJD).
        :rtype: float

        >>> e = Epoch(1858, 'NOVEMBER', 17)
        >>> e.mjd()
        0.0
        """

        return self._jde - 2400000.5

    def jde(self):
        """Method to return the internal value of the Julian Ephemeris Day.

        :returns: The internal value of the Julian Ephemeris Day.
        :rtype: float

        >>> a = Epoch(-1000, 2, 29.0)
        >>> print(a.jde())
        1355866.5
        """

        return self._jde

    def year(self):
        """This method returns the contents of this object as a year with
        decimals.

        :returns: Year with decimals.
        :rtype: float

        >>> e = Epoch(1993, 'October', 1)
        >>> print(round(e.year(), 4))
        1993.7479
        """

        y, m, d = self.get_date()
        doy = Epoch.get_doy(y, m, d)
        # We must substract 1 from doy in order to compute correctly
        doy -= 1
        days_of_year = 365.0
        if self.leap():
            days_of_year = 366.0
        return y + doy / days_of_year

    def rise_set(self, latitude, longitude, altitude=0.0):
        """This method computes the times of rising and setting of the Sun.

        .. note:: The algorithm used is the one explained in the article
            "Sunrise equation" of the Wikipedia at:
            https://en.wikipedia.org/wiki/Sunrise_equation

        .. note:: This algorithm is only valid outside the artic and antartic
            circles (+/- 66d 33'). For latitudes higher than +66d 33' and
            smaller than -66d 33' this method returns a ValueError exception

        .. note:: The results are given in UTC time.

        :param latitude: Latitude of the observer, as an Angle object. Positive
            to the North
        :type latitude: :py:class:`Angle`
        :param longitude: Longitude of the observer, as an Angle object.
            Positive to the East
        :type longitude: :py:class:`Angle`
        :param altitude: Altitude of the observer, as meters above sea level
        :type altitude: int, float

        :returns: Two :py:class:`Epoch` objects representing rising time and
            setting time, in a tuple
        :rtype: tuple
        :raises: TypeError if input values are of wrong type.
        :raises: ValueError if latitude outside the +/- 66d 33' range.

        >>> e = Epoch(2019, 4, 2)
        >>> latitude = Angle(48, 8, 0)
        >>> longitude = Angle(11, 34, 0)
        >>> altitude = 520.0
        >>> rising, setting = e.rise_set(latitude, longitude, altitude)
        >>> y, m, d, h, mi, s = rising.get_full_date()
        >>> print("{}:{}".format(h, mi))
        4:48
        >>> y, m, d, h, mi, s = setting.get_full_date()
        >>> print("{}:{}".format(h, mi))
        17:48
        """

        if not (isinstance(latitude, Angle) and isinstance(longitude, Angle)
                and isinstance(altitude, (int, float))):
            raise TypeError("Invalid input types")
        # Check that latitude is within valid range
        limit = Angle(66, 33, 0)
        if latitude > limit or latitude < -limit:
            raise ValueError("Latitude outside the +/- 66d 33' range")
        # Let's start computing the number of days since 2000/1/1 12:00 (cjd)
        # Compute fractional Julian Day for leap seconds and terrestrial time
        # We need current epoch without hours, minutes and seconds
        year, month, day = self.get_date()
        e = Epoch(year, month, day)
        frac = (10.0 + 32.184 + Epoch.leap_seconds(year, month)) / 86400.0
        cjd = e.jde() - 2451545.0 + frac
        # Compute mean solar noon
        jstar = cjd - (float(longitude) / 360.0)
        # Solar mean anomaly
        m = (357.5291 + 0.98560028 * jstar) % 360
        mr = radians(m)
        # Equation of the center
        c = 1.9148 * sin(mr) + 0.02 * sin(2.0 * mr) + 0.0003 * sin(3.0 * mr)
        # Ecliptic longitude
        lambd = (m + c + 180.0 + 102.9372) % 360
        lr = radians(lambd)
        # Solar transit
        jtran = 2451545.5 + jstar + 0.0053 * sin(mr) - 0.0069 * sin(2.0 * lr)
        # NOTE: The original algorithm indicates a value of 2451545.0, but that
        # leads to transit times around midnight, which is an error
        # Declination of the Sun
        sin_delta = sin(lr) * sin(radians(23.44))
        delta = asin(sin_delta)
        cos_delta = cos(delta)
        # Hour angle
        # First, correct by elevation
        corr = -0.83 - 2.076 * sqrt(altitude) / 60.0
        cos_om = ((sin(radians(corr)) - sin(latitude.rad()) * sin_delta)
                  / (cos(latitude.rad()) * cos_delta))
        # Finally, compute rising and setting times
        omega = degrees(acos(cos_om))
        jrise = Epoch(jtran - (omega / 360.0))
        jsett = Epoch(jtran + (omega / 360.0))
        return jrise, jsett

    def __call__(self):
        """Method used when Epoch is called only with parenthesis.

        :returns: The internal value of the Julian Ephemeris Day.
        :rtype: float

        >>> a = Epoch(-122, 1, 1.0)
        >>> print(a())
        1676497.5
        """

        return self._jde

    def __add__(self, b):
        """This method defines the addition between an Epoch and some days.

        :param b: Value to be added, in days.
        :type b: int, float

        :returns: A new Epoch object.
        :rtype: :py:class:`Epoch`
        :raises: TypeError if operand is of wrong type.

        >>> a = Epoch(1991, 7, 11)
        >>> b = a + 10000
        >>> y, m, d = b.get_date()
        >>> print("{}/{}/{}".format(y, m, round(d, 2)))
        2018/11/26.0
        """

        if isinstance(b, (int, float)):
            return Epoch(self._jde + float(b))
        else:
            raise TypeError("Wrong operand type")

    def __sub__(self, b):
        """This method defines the subtraction between Epochs or between an
        Epoch and a given number of days.

        :param b: Value to be subtracted, either an Epoch or days.
        :type b: py:class:`Epoch`, int, float

        :returns: A new Epoch object if parameter 'b' is in days, or the
           difference between provided Epochs, in days.
        :rtype: :py:class:`Epoch`, float
        :raises: TypeError if operand is of wrong type.

        >>> a = Epoch(1986, 2, 9.0)
        >>> print(round(a(), 2))
        2446470.5
        >>> b = Epoch(1910, 4, 20.0)
        >>> print(round(b(), 2))
        2418781.5
        >>> c = a - b
        >>> print(round(c, 2))
        27689.0
        >>> a = Epoch(2003, 12, 31.0)
        >>> b = a - 365.5
        >>> y, m, d = b.get_date()
        >>> print("{}/{}/{}".format(y, m, round(d, 2)))
        2002/12/30.5
        """

        if isinstance(b, (int, float)):
            return Epoch(self._jde - b)
        elif isinstance(b, Epoch):
            return float(self._jde - b._jde)
        else:
            raise TypeError("Invalid operand type")

    def __iadd__(self, b):
        """This method defines the accumulative addition to this Epoch.

        :param b: Value to be added, in days.
        :type b: int, float

        :returns: This Epoch.
        :rtype: :py:class:`Epoch`
        :raises: TypeError if operand is of wrong type.

        >>> a = Epoch(2003, 12, 31.0)
        >>> a += 32.5
        >>> y, m, d = a.get_date()
        >>> print("{}/{}/{}".format(y, m, round(d, 2)))
        2004/2/1.5
        """

        if isinstance(b, (int, float)):
            self = self + b
            return self
        else:
            raise TypeError("Wrong operand type")

    def __isub__(self, b):
        """This method defines the accumulative subtraction to this Epoch.

        :param b: Value to be subtracted, in days.
        :type b: int, float

        :returns: This Epoch.
        :rtype: :py:class:`Epoch`
        :raises: TypeError if operand is of wrong type.

        >>> a = Epoch(2001, 12, 31.0)
        >>> a -= 2*365
        >>> y, m, d = a.get_date()
        >>> print("{}/{}/{}".format(y, m, round(d, 2)))
        2000/1/1.0
        """

        if isinstance(b, (int, float)):
            self = self - b
            return self
        else:
            raise TypeError("Wrong operand type")

    def __radd__(self, b):
        """This method defines the addition to a Epoch by the right

        :param b: Value to be added, in days.
        :type b: int, float

        :returns: A new Epoch object.
        :rtype: :py:class:`Epoch`
        :raises: TypeError if operand is of wrong type.

        >>> a = Epoch(2004, 2, 27.8)
        >>> b = 2.2 + a
        >>> y, m, d = b.get_date()
        >>> print("{}/{}/{}".format(y, m, round(d, 2)))
        2004/3/1.0
        """

        if isinstance(b, (int, float)):
            return self.__add__(b)  # It is the same as by the left
        else:
            raise TypeError("Wrong operand type")

    def __int__(self):
        """This method returns the internal JDE value as an int.

        :returns: Internal JDE value as an int.
        :rtype: int

        >>> a = Epoch(2434923.85)
        >>> int(a)
        2434923
        """

        return int(self._jde)

    def __float__(self):
        """This method returns the internal JDE value as a float.

        :returns: Internal JDE value as a float.
        :rtype: float

        >>> a = Epoch(2434923.85)
        >>> float(a)
        2434923.85
        """

        return float(self._jde)

    def __eq__(self, b):
        """This method defines the 'is equal' operator between Epochs.

        .. note:: For the comparison, the **base.TOL** value is used.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Epoch(2007, 5, 20.0)
        >>> b = Epoch(2007, 5, 20.000001)
        >>> a == b
        False
        >>> a = Epoch(2004, 10, 15.7)
        >>> b = Epoch(a)
        >>> a == b
        True
        >>> a = Epoch(2434923.85)
        >>> a == 2434923.85
        True
        """

        if isinstance(b, (int, float)):
            return abs(self._jde - float(b)) < TOL
        elif isinstance(b, Epoch):
            return abs(self._jde - b._jde) < TOL
        else:
            raise TypeError("Wrong operand type")

    def __ne__(self, b):
        """This method defines the 'is not equal' operator between Epochs.

        .. note:: For the comparison, the **base.TOL** value is used.

        :returns: A boolean.
        :rtype: bool

        >>> a = Epoch(2007, 5, 20.0)
        >>> b = Epoch(2007, 5, 20.000001)
        >>> a != b
        True
        >>> a = Epoch(2004, 10, 15.7)
        >>> b = Epoch(a)
        >>> a != b
        False
        >>> a = Epoch(2434923.85)
        >>> a != 2434923.85
        False
        """

        return not self.__eq__(b)  # '!=' == 'not(==)'

    def __lt__(self, b):
        """This method defines the 'is less than' operator between Epochs.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Epoch(2004, 10, 15.7)
        >>> b = Epoch(2004, 10, 15.7)
        >>> a < b
        False
        """

        if isinstance(b, (int, float)):
            return self._jde < float(b)
        elif isinstance(b, Epoch):
            return self._jde < b._jde
        else:
            raise TypeError("Wrong operand type")

    def __ge__(self, b):
        """This method defines 'is equal or greater' operator between Epochs.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Epoch(2004, 10, 15.71)
        >>> b = Epoch(2004, 10, 15.7)
        >>> a >= b
        True
        """

        return not self.__lt__(b)  # '>=' == 'not(<)'

    def __gt__(self, b):
        """This method defines the 'is greater than' operator between Epochs.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Epoch(2004, 10, 15.71)
        >>> b = Epoch(2004, 10, 15.7)
        >>> a > b
        True
        >>> a = Epoch(-207, 11, 5.2)
        >>> b = Epoch(-207, 11, 5.2)
        >>> a > b
        False
        """

        if isinstance(b, (int, float)):
            return self._jde > float(b)
        elif isinstance(b, Epoch):
            return self._jde > b._jde
        else:
            raise TypeError("Wrong operand type")

    def __le__(self, b):
        """This method defines 'is equal or less' operator between Epochs.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Epoch(2004, 10, 15.71)
        >>> b = Epoch(2004, 10, 15.7)
        >>> a <= b
        False
        >>> a = Epoch(-207, 11, 5.2)
        >>> b = Epoch(-207, 11, 5.2)
        >>> a <= b
        True
        """

        return not self.__gt__(b)  # '<=' == 'not(>)'


JDE2000 = Epoch(2000, 1, 1.5)
"""Standard epoch for January 1st, 2000 at 12h corresponding to JDE2451545.0"""


def main():

    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Let's do some work with the Epoch class
    print("\n" + 35 * "*")
    print("*** Use of Epoch class")
    print(35 * "*" + "\n")

    e = Epoch(1987, 6, 19.5)
    print_me("JDE for 1987/6/19.5", e)

    # Redefine the Epoch object
    e.set(333, "Jan", 27, 12)
    print_me("JDE for 333/1/27.5", e)

    # We can create an Epoch from a 'date' or 'datetime' object
    d = datetime.datetime(837, 4, 10, 7, 12, 0, 0)
    f = Epoch(d)
    print_me("JDE for 837/4/10.3", f)

    print("")

    # Check if a given date belong to the Julian or Gregorian calendar
    print_me("Is 1590/4/21.4 a Julian date?", Epoch.is_julian(1590, 4, 21.4))

    print("")

    # We can also check if a given year is leap or not
    print_me("Is -1000 a leap year?", Epoch.is_leap(-1000))
    print_me("Is 1800 a leap year?", Epoch.is_leap(1800))
    print_me("Is 2012 a leap year?", Epoch.is_leap(2012))

    print("")

    # Get the Day Of Year corresponding to a given date
    print_me("Day Of Year (DOY) of 1978/11/14", Epoch.get_doy(1978, 11, 14))
    print_me("Day Of Year (DOY) of -400/2/29.9", Epoch.get_doy(-400, 2, 29.9))
    # As before, but using the contents of the Epoch object
    e = Epoch(2017, 12, 31.7)
    print_me("Day Of Year (DOY) of 2017/12/31.7", round(e.doy(), 1))

    print("")

    # Now the opposite: Get a date from a DOY
    t = Epoch.doy2date(2017, 365.7)
    s = str(t[0]) + "/" + str(t[1]) + "/" + str(round(t[2], 2))
    print_me("Date from DOY 2017:365.7", s)

    t = Epoch.doy2date(-4, 60)
    s = str(t[0]) + "/" + str(t[1]) + "/" + str(round(t[2], 2))
    print_me("Date from DOY -4:60", s)

    print("")

    # There is an internal table which we can use to get the leap seconds
    print_me("Number of leap seconds applied up to July 1983",
             Epoch.leap_seconds(1983, 7))

    print("")

    # We can convert the internal JDE value back to a date
    e = Epoch(2436116.31)
    y, m, d = e.get_date()
    s = str(y) + "/" + str(m) + "/" + str(round(d, 2))
    print_me("Date from JDE 2436116.31", s)

    print("")

    # It is possible to get the day of the week corresponding to a given date
    e = Epoch(2018, "Feb", 15)
    print_me("The day of week of 2018/2/15 is", e.dow(as_string=True))

    print("")

    # In some cases it is useful to get the Modified Julian Day (MJD)
    e = Epoch(1923, "August", 23)
    print_me("Modified Julian Day for 1923/8/23", round(e.mjd(), 2))

    print("")

    # If your system is appropriately configured, you can get the difference in
    # seconds between your local time and UTC
    print_me(
        "To convert from local system time to UTC you must add/subtract"
        + " this amount of seconds",
        Epoch.utc2local(),
    )

    print("")

    # Compute DeltaT = TT - UT differences for various dates
    print_me("DeltaT (TT - UT) for Feb/333", round(Epoch.tt2ut(333, 2), 1))
    print_me("DeltaT (TT - UT) for Jan/1642", round(Epoch.tt2ut(1642, 1), 1))
    print_me("DeltaT (TT - UT) for Feb/1928", round(Epoch.tt2ut(1928, 1), 1))
    print_me("DeltaT (TT - UT) for Feb/1977", round(Epoch.tt2ut(1977, 2), 1))
    print_me("DeltaT (TT - UT) for Jan/1998", round(Epoch.tt2ut(1998, 1), 1))

    print("")

    # The difference between civil day and sidereal day is almost 4 minutes
    e = Epoch(1987, 4, 10)
    st1 = round(e.mean_sidereal_time(), 9)
    e = Epoch(1987, 4, 11)
    st2 = round(e.mean_sidereal_time(), 9)
    ds = (st2 - st1) * DAY2MIN
    msg = "{}m {}s".format(iint(ds), (ds % 1) * 60.0)
    print_me("Difference between sidereal time 1987/4/11 and 1987/4/10", msg)

    print("")

    print(
        "When correcting for nutation-related effects, we get the "
        + "'apparent' sidereal time:"
    )
    e = Epoch(1987, 4, 10)
    print("e = Epoch(1987, 4, 10)")
    print_me(
        "e.apparent_sidereal_time(23.44357, (-3.788)/3600.0)",
        e.apparent_sidereal_time(23.44357, (-3.788) / 3600.0),
    )
    #    0.549145082637

    print("")

    # Epoch class can also provide the date of Easter for a given year
    # Let's spice up the output a little bit, calling dow() and get_month()
    month, day = Epoch.easter(2019)
    e = Epoch(2019, month, day)
    s = (
        e.dow(as_string=True)
        + ", "
        + str(day)
        + get_ordinal_suffix(day)
        + " of "
        + Epoch.get_month(month, as_string=True)
    )
    print_me("Easter day for 2019", s)
    # I know Easter is always on Sunday, by the way... ;-)

    print("")

    # Compute the date of the Jewish Easter (Pesach) for a given year
    month, day = Epoch.jewish_pesach(1990)
    s = (
        str(day)
        + get_ordinal_suffix(day)
        + " of "
        + Epoch.get_month(month, as_string=True)
    )
    print_me("Jewish Pesach day for 1990", s)

    print("")

    # Now, convert a date in the Moslem calendar to the Gregorian calendar
    y, m, d = Epoch.moslem2gregorian(1421, 1, 1)
    print_me("The date 1421/1/1 in the Moslem calendar is, in Gregorian "
             + "calendar", "{}/{}/{}".format(y, m, d))
    y, m, d = Epoch.moslem2gregorian(1439, 9, 1)
    print_me(
        "The start of Ramadan month (9/1) for Gregorian year 2018 is",
        "{}/{}/{}".format(y, m, d),
    )
    # We can go from the Gregorian calendar back to the Moslem calendar too
    print_me(
        "Date 1991/8/13 in Gregorian calendar is, in Moslem calendar",
        "{}/{}/{}".format(*Epoch.gregorian2moslem(1991, 8, 13)),
    )
    # Note: The '*' before 'Epoch' will _unpack_ the tuple into components

    print("")

    # It is possible to carry out some algebraic operations with Epochs

    # Add 10000 days to a given date
    a = Epoch(1991, 7, 11)
    b = a + 10000
    y, m, d = b.get_date()
    s = str(y) + "/" + str(m) + "/" + str(round(d, 2))
    print_me("1991/7/11 plus 10000 days is", s)

    # Subtract two Epochs to find the number of days between them
    a = Epoch(1986, 2, 9.0)
    b = Epoch(1910, 4, 20.0)
    print_me("The number of days between 1986/2/9 and 1910/4/20 is",
             round(a - b, 2))

    # We can also subtract a given amount of days from an Epoch
    a = Epoch(2003, 12, 31.0)
    b = a - 365.5
    y, m, d = b.get_date()
    s = str(y) + "/" + str(m) + "/" + str(round(d, 2))
    print_me("2003/12/31 minus 365.5 days is", s)

    # Accumulative addition and subtraction of days is also allowed
    a = Epoch(2003, 12, 31.0)
    a += 32.5
    y, m, d = a.get_date()
    s = str(y) + "/" + str(m) + "/" + str(round(d, 2))
    print_me("2003/12/31 plus 32.5 days is", s)

    a = Epoch(2001, 12, 31.0)
    a -= 2 * 365
    y, m, d = a.get_date()
    s = str(y) + "/" + str(m) + "/" + str(round(d, 2))
    print_me("2001/12/31 minus 2*365 days is", s)

    # It is also possible to add days from the right
    a = Epoch(2004, 2, 27.8)
    b = 2.2 + a
    y, m, d = b.get_date()
    s = str(y) + "/" + str(m) + "/" + str(round(d, 2))
    print_me("2004/2/27.8 plus 2.2 days is", s)

    print("")

    # Comparison operadors between epochs are also defined
    a = Epoch(2007, 5, 20.0)
    b = Epoch(2007, 5, 20.000001)
    print_me("2007/5/20.0 == 2007/5/20.000001", a == b)
    print_me("2007/5/20.0 != 2007/5/20.000001", a != b)
    print_me("2007/5/20.0 > 2007/5/20.000001", a > b)
    print_me("2007/5/20.0 <= 2007/5/20.000001", a <= b)

    print("")

    # Compute the time of rise and setting of the Sun in a given day
    e = Epoch(2018, 5, 2)
    print("On May 2nd, 2018, Sun rising/setting times in Munich were (UTC):")
    latitude = Angle(48, 8, 0)
    longitude = Angle(11, 34, 0)
    altitude = 520.0
    rising, setting = e.rise_set(latitude, longitude, altitude)
    y, m, d, h, mi, s = rising.get_full_date()
    print("Rising time: {}:{}".format(h, mi))                           # 3:50
    y, m, d, h, mi, s = setting.get_full_date()
    print("Setting time: {}:{}".format(h, mi))                          # 18:33

    print("")

    # Compute the hash of a given Epoch
    h = e.__hash__()
    print("Hash of Epoch({}): {}".format(e, h))


if __name__ == "__main__":

    main()
