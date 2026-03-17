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


from pymeeus.base import TOL
from pymeeus.Epoch import Epoch, JDE2000, DAY2SEC
from pymeeus.Angle import Angle


# Epoch class

def test_epoch_constructor():
    """Tests the constructor of Epoch class"""

    a = Epoch(1987, 6, 19.5)
    assert abs(a._jde - 2446966.0) < TOL, \
        "ERROR: 1st constructor test, JDE value doesn't match"

    a = Epoch(1988, 6, 19.5)
    assert abs(a._jde - 2447332.0) < TOL, \
        "ERROR: 2nd constructor test, JDE value doesn't match"

    a = Epoch(1600, 'jan', 1.0)
    assert abs(a._jde - 2305447.5) < TOL, \
        "ERROR: 3rd constructor test, JDE value doesn't match"

    a = Epoch(-123, 'DECEMBER', 31.0)
    assert abs(a._jde - 1676496.5) < TOL, \
        "ERROR: 4th constructor test, JDE value doesn't match"

    a = Epoch(-4712, 1, 1.5)
    assert abs(a._jde - 0.0) < TOL, \
        "ERROR: 5th constructor test, JDE value doesn't match"

    e = Epoch(JDE2000, utc=True)
    a = round((e - JDE2000) * DAY2SEC, 3)
    assert abs(a - 64.184) < TOL, \
        "ERROR: 6th constructor test, difference value doesn't match"

    e = Epoch(2451545.0, utc=True)
    a = round((e - JDE2000) * DAY2SEC, 3)
    assert abs(a - 64.184) < TOL, \
        "ERROR: 7th constructor test, difference value doesn't match"

    e = Epoch(JDE2000, local=True)
    a = round((e - JDE2000) * DAY2SEC - Epoch.utc2local(), 3)
    assert abs(a - 64.184) < TOL, \
        "ERROR: 8th constructor test, difference value doesn't match"

    e = Epoch(JDE2000, local=True, leap_seconds=35.0)
    a = round((e - JDE2000) * DAY2SEC - Epoch.utc2local(), 3)
    assert abs(a - 77.184) < TOL, \
        "ERROR: 9th constructor test, difference value doesn't match"


def test_epoch_hashable():
    a = Epoch(1987, 6, 19.5)
    # can use as a key to a dict
    _ = {a: 1}


def test_epoch_is_julian():
    """Tests the is_julian() static method of Epoch class"""

    assert Epoch.is_julian(1582, 10, 4.99), \
        "ERROR: 1st is_julian() test, output doesn't match"

    assert not Epoch.is_julian(1582, 10, 5.0), \
        "ERROR: 2nd is_julian() test, output doesn't match"

    assert not Epoch.is_julian(2018, 11, 6.0), \
        "ERROR: 3rd is_julian() test, output doesn't match"

    assert Epoch.is_julian(-2000, 3, 16.0), \
        "ERROR: 4th is_julian() test, output doesn't match"


def test_epoch_get_month():
    """Test the get_month() static method of Epoch class"""

    assert Epoch.get_month(10) == 10, \
        "ERROR: 1st get_month() test, output doesn't match"

    assert Epoch.get_month('Feb') == 2, \
        "ERROR: 2nd get_month() test, output doesn't match"

    assert Epoch.get_month('MAR') == 3, \
        "ERROR: 3rd get_month() test, output doesn't match"

    assert Epoch.get_month('January') == 1, \
        "ERROR: 4th get_month() test, output doesn't match"

    assert Epoch.get_month('DECEMBER') == 12, \
        "ERROR: 5th get_month() test, output doesn't match"

    assert Epoch.get_month('september') == 9, \
        "ERROR: 6th get_month() test, output doesn't match"

    assert Epoch.get_month('may') == 5, \
        "ERROR: 7th get_month() test, output doesn't match"

    assert Epoch.get_month(6, as_string=True) == 'June', \
        "ERROR: 8th get_month() test, output doesn't match"


def test_epoch_is_leap():
    """Tests the is_leap() static method of Epoch class"""

    assert not Epoch.is_leap(1582), \
        "ERROR: 1st is_leap() test, output doesn't match"

    assert Epoch.is_leap(1580), \
        "ERROR: 2nd is_leap() test, output doesn't match"

    assert not Epoch.is_leap(2018), \
        "ERROR: 3rd is_leap() test, output doesn't match"

    assert Epoch.is_leap(2000), \
        "ERROR: 4th is_leap() test, output doesn't match"

    assert Epoch.is_leap(-2000), \
        "ERROR: 5th is_leap() test, output doesn't match"


def test_epoch_get_doy():
    """Tests the get_doy() static method of Epoch class"""

    assert Epoch.get_doy(1978, 11, 14) == 318, \
        "ERROR: 1st get_doy() test, output doesn't match"

    assert Epoch.get_doy(2012, 3, 3.1) == 63.1, \
        "ERROR: 2nd get_doy() test, output doesn't match"

    assert Epoch.get_doy(-400, 2, 29.9) == 60.9, \
        "ERROR: 3rd get_doy() test, output doesn't match"


def test_epoch_doy():
    """Tests the doy() method of Epoch class"""

    e = Epoch(2017, 12, 31.7)

    assert round(e.doy(), 1) == 365.7, \
        "ERROR: 1st doy() test, output doesn't match"


def test_epoch_doy2date():
    """Tests the doy2date() static method of Epoch class"""

    assert Epoch.doy2date(2017, 32) == (2017, 2, 1), \
        "ERROR: 1st doy2date() test, output doesn't match"

    assert Epoch.doy2date(-3, 60) == (-3, 3, 1.0), \
        "ERROR: 2nd doy2date() test, output doesn't match"

    assert Epoch.doy2date(-4, 60) == (-4, 2, 29.0), \
        "ERROR: 3rd doy2date() test, output doesn't match"

    t = Epoch.doy2date(2017, 365.7)
    assert t[0] == 2017 and t[1] == 12 and abs(t[2] - 31.7) < TOL, \
        "ERROR: 4th doy2date() test, output doesn't match"

    t = Epoch.doy2date(2012, 63.1)
    assert t[0] == 2012 and t[1] == 3 and abs(t[2] - 3.1) < TOL, \
        "ERROR: 5th doy2date() test, output doesn't match"

    t = Epoch.doy2date(-4, 60)
    assert t[0] == -4 and t[1] == 2 and abs(t[2] - 29) < TOL, \
        "ERROR: 6th doy2date() test, output doesn't match"


def test_epoch_leap_seconds():
    """Tests the leap_seconds() static method of Epoch class"""

    assert Epoch.leap_seconds(1983, 6) == 11, \
        "ERROR: 1st leap_seconds() test, output doesn't match"

    assert Epoch.leap_seconds(1983, 7) == 12, \
        "ERROR: 2nd leap_seconds() test, output doesn't match"

    assert Epoch.leap_seconds(2016, 11) == 26, \
        "ERROR: 3rd leap_seconds() test, output doesn't match"

    assert Epoch.leap_seconds(2017, 1) == 27, \
        "ERROR: 4th leap_seconds() test, output doesn't match"

    assert Epoch.leap_seconds(1972, 6) == 0, \
        "ERROR: 5th leap_seconds() test, output doesn't match"

    assert Epoch.leap_seconds(1972, 7) == 1, \
        "ERROR: 6th leap_seconds() test, output doesn't match"

    assert Epoch.leap_seconds(2018, 7) == 27, \
        "ERROR: 7th leap_seconds() test, output doesn't match"


def test_epoch_easter():
    """Tests the easter() method of Epoch class"""

    t = Epoch.easter(2000)
    assert t[0] == 4 and t[1] == 23, \
        "ERROR: 1st easter() test, output doesn't match"

    t = Epoch.easter(1954)
    assert t[0] == 4 and t[1] == 18, \
        "ERROR: 2nd easter() test, output doesn't match"

    t = Epoch.easter(179)
    assert t[0] == 4 and t[1] == 12, \
        "ERROR: 3rd easter() test, output doesn't match"

    t = Epoch.easter(1243)
    assert t[0] == 4 and t[1] == 12, \
        "ERROR: 4th easter() test, output doesn't match"

    t = Epoch.easter(1991)
    assert t[0] == 3 and t[1] == 31, \
        "ERROR: 5th easter() test, output doesn't match"

    t = Epoch.easter(1993)
    assert t[0] == 4 and t[1] == 11, \
        "ERROR: 6th easter() test, output doesn't match"


def test_epoch_jewish_pesach():
    """Tests the jewish_pesach() method of Epoch class"""

    t = Epoch.jewish_pesach(1990)
    assert t[0] == 4 and t[1] == 10, \
        "ERROR: 1st jewish_pesach() test, output doesn't match"


def test_epoch_moslem2gregorian():
    """Tests the moslem2gregorian() method of Epoch class"""

    t = Epoch.moslem2gregorian(1421, 1, 1)
    assert t[0] == 2000 and t[1] == 4 and t[2] == 6, \
        "ERROR: 1st moslem2gregorian() test, output doesn't match"


def test_epoch_gregorian2moslem():
    """Tests the gregorian2moslem() method of Epoch class"""

    t = Epoch.gregorian2moslem(1991, 8, 13)
    assert t[0] == 1412 and t[1] == 2 and t[2] == 2, \
        "ERROR: 1st gregorian2moslem() test, output doesn't match"


def test_epoch_get_date():
    """Tests the get_date() method of Epoch class"""

    e = Epoch(2436116.31)
    t = e.get_date()
    assert t[0] == 1957 and t[1] == 10 and abs(t[2] - 4.81) < TOL, \
        "ERROR: 1st get_date() test, output doesn't match"

    e = Epoch(1842713.0)
    t = e.get_date()
    assert t[0] == 333 and t[1] == 1 and abs(t[2] - 27.5) < TOL, \
        "ERROR: 2nd get_date() test, output doesn't match"

    e = Epoch(1507900.13)
    t = e.get_date()
    assert t[0] == -584 and t[1] == 5 and abs(round(t[2], 2) - 28.63) < TOL, \
        "ERROR: 3rd get_date() test, output doesn't match"


def test_epoch_tt2ut():
    """Tests the tt2ut() method of Epoch class"""

    assert abs(round(Epoch.tt2ut(1642, 1), 1) - 62.1) < TOL, \
        "ERROR: 1st tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(1680, 1), 1) - 15.3) < TOL, \
        "ERROR: 2nd tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(1774, 1), 1) - 16.7) < TOL, \
        "ERROR: 3rd tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(1890, 1), 1) - (-6.1)) < TOL, \
        "ERROR: 4th tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(1928, 2), 1) - 24.2) < TOL, \
        "ERROR: 5th tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(2015, 7), 1) - 69.3) < TOL, \
        "ERROR: 6th tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(1000, 1), 1) - 1574.2) < TOL, \
        "ERROR: 7th tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(-501, 1), 1) - 17218.5) < TOL, \
        "ERROR: 8th tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(1801, 1), 1) - 13.4) < TOL, \
        "ERROR: 9th tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(1930, 1), 1) - 24.1) < TOL, \
        "ERROR: 10th tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(1945, 1), 1) - 26.9) < TOL, \
        "ERROR: 11th tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(1970, 1), 1) - 40.2) < TOL, \
        "ERROR: 12th tt2ut() test, output doesn't match"

    assert abs(round(Epoch.tt2ut(2000, 1), 1) - 63.9) < TOL, \
        "ERROR: 13th tt2ut() test, output doesn't match"


def test_epoch_dow():
    """Tests the dow() method of Epoch class"""

    e = Epoch(1954, 'June', 30)
    assert e.dow() == 3, "ERROR: 1st dow() test, output doesn't match"

    e = Epoch(2018, 'Feb', 14.9)
    assert e.dow(as_string=True) == 'Wednesday', \
        "ERROR: 2nd dow() test, output doesn't match"

    e = Epoch(2018, 'Feb', 15)
    assert e.dow(as_string=True) == 'Thursday', \
        "ERROR: 3rd dow() test, output doesn't match"

    e = Epoch(2018, 'Jul', 15.9)
    assert e.dow(as_string=True) == 'Sunday', \
        "ERROR: 4th dow() test, output doesn't match"


def test_epoch_mean_sidereal_time():
    """Tests the mean_sidereal_time() method of Epoch class"""

    e = Epoch(1987, 4, 10)
    assert abs(round(e.mean_sidereal_time(), 9) - 0.549147764) < TOL, \
        "ERROR: 1st mean_sidereal_time() test, output doesn't match"

    e = Epoch(1987, 4, 10, 19, 21, 0.0)
    assert abs(round(e.mean_sidereal_time(), 9) - 0.357605204) < TOL, \
        "ERROR: 2nd mean_sidereal_time() test, output doesn't match"


def test_epoch_apparent_sidereal_time():
    """Tests the apparent_sidereal_time() method of Epoch class"""

    e = Epoch(1987, 4, 10)
    assert abs(round(e.apparent_sidereal_time(23.44357, (-3.788)/3600.0), 8)
               - 0.54914508) < TOL, \
        "ERROR: 1st apparent_sidereal_time() test, output doesn't match"


def test_epoch_mjd():
    """Tests the mjd() method of Epoch class"""

    e = Epoch(1858, 'NOVEMBER', 17)
    assert e.mjd() == 0.0, "ERROR: 1st mjd() test, output doesn't match"


def test_epoch_jde():
    """Tests the jde() method of Epoch class"""

    e = Epoch(-1000, 2, 29.0)
    assert e.jde() == 1355866.5, "ERROR: 1st jde() test, output doesn't match"


def test_epoch_call():
    """Tests the __call__() method of Epoch class"""

    e = Epoch(-122, 1, 1.0)
    assert e() == 1676497.5, "ERROR: 1st __call__() test, output doesn't match"


def test_epoch_add():
    """Tests the addition between Epochs"""

    a = Epoch(1991, 7, 11)
    b = a + 10000
    y, m, d = b.get_date()
    assert y == 2018 and m == 11 and abs(round(d, 2) - 26.0) < TOL, \
        "ERROR: 1st __add__() test, output doesn't match"


def test_epoch_sub():
    """Tests the subtraction between Epochs"""

    a = Epoch(1986, 2, 9.0)
    b = Epoch(1910, 4, 20.0)
    c = a - b
    assert abs(round(c, 1) - 27689.0) < TOL, \
        "ERROR: 1st __sub__() test, output doesn't match"

    a = Epoch(2003, 12, 31.0)
    b = a - 365.5
    y, m, d = b.get_date()
    assert y == 2002 and m == 12 and abs(round(d, 1) - 30.5) < TOL, \
        "ERROR: 2nd __sub__() test, output doesn't match"


def test_epoch_iadd():
    """Tests the accumulative addition in Epochs"""

    a = Epoch(2003, 12, 31.0)
    a += 32.5
    y, m, d = a.get_date()
    assert y == 2004 and m == 2 and abs(round(d, 1) - 1.5) < TOL, \
        "ERROR: 1st __iadd__() test, output doesn't match"


def test_epoch_isub():
    """Tests the accumulative subtraction in Epochs"""

    a = Epoch(2001, 12, 31.0)
    a -= 2*365
    y, m, d = a.get_date()
    assert y == 2000 and m == 1 and abs(round(d, 1) - 1.0) < TOL, \
        "ERROR: 1st __isub__() test, output doesn't match"


def test_epoch_radd():
    """Tests the addition with Epochs by the right"""

    a = Epoch(2004, 2, 27.8)
    b = 2.2 + a
    y, m, d = b.get_date()
    assert y == 2004 and m == 3 and abs(round(d, 1) - 1.0) < TOL, \
        "ERROR: 1st __radd__() test, output doesn't match"


def test_epoch_int():
    """Tests the 'int()' operation on Epochs"""

    a = Epoch(2434923.85)
    assert abs(int(a) - 2434923) < TOL, \
        "ERROR: 1st __int__() test, output doesn't match"


def test_epoch_float():
    """Tests the 'float()' operation on Epochs"""

    a = Epoch(2434923.85)
    assert abs(float(a) - 2434923.85) < TOL, \
        "ERROR: 1st __float__() test, output doesn't match"


def test_epoch_ne():
    """Tests the 'is not equal' operator of Epochs"""
    # NOTE: Test 'is not equal' also tests 'is equal' operator

    a = Epoch(2007, 5, 20.0)
    b = Epoch(2007, 5, 20.000001)
    assert a != b, "ERROR: 1st __ne__() test, output doesn't match"

    a = Epoch(2004, 10, 15.7)
    b = Epoch(a)
    assert not a != b, "ERROR: 2nd __ne__() test, output doesn't match"

    a = Epoch(2434923.85)
    assert not a != 2434923.85, \
        "ERROR: 3rd __ne__() test, output doesn't match"


def test_epoch_ge():
    """Tests the 'is greater or equal' operator of Epochs"""
    # NOTE: Test of 'is greater or equal' also tests 'is less than' operator

    a = Epoch(2004, 10, 15.71)
    b = Epoch(2004, 10, 15.7)
    assert a >= b, "ERROR: 1st __ge__() test, output doesn't match"


def test_epoch_le():
    """Tests the 'is less or equal' operator of Epochs"""
    # NOTE: Test of 'is less or equal' also tests 'is greater than' operator

    a = Epoch(2004, 10, 15.71)
    b = Epoch(2004, 10, 15.7)
    assert not a <= b, "ERROR: 1st __le__() test, output doesn't match"

    a = Epoch(-207, 11, 5.2)
    b = Epoch(-207, 11, 5.2)
    assert a <= b, "ERROR: 2nd __le__() test, output doesn't match"


def test_epoch_rise_set():
    """Tests the 'rise_set()' method of Epochs"""

    e = Epoch(2019, 4, 2)
    latitude = Angle(48, 8, 0)
    longitude = Angle(11, 34, 0)
    altitude = 520.0
    rising, setting = e.rise_set(latitude, longitude, altitude)
    y, m, d, h, mi, s = rising.get_full_date()

    assert h == 4, "ERROR: 1st rise_set() test, output doesn't match"
    assert mi == 48, "ERROR: 2nd rise_set() test, output doesn't match"

    y, m, d, h, mi, s = setting.get_full_date()

    assert h == 17, "ERROR: 3rd rise_set() test, output doesn't match"
    assert mi == 48, "ERROR: 3rd rise_set() test, output doesn't match"


def test_epoch_utc_constructor():
    """Tests that the Epoch constructor handles UTC conversion well"""

    e = Epoch(JDE2000, utc=True)
    diff = round((e - JDE2000) * DAY2SEC, 3)

    assert diff == 64.184, "ERROR: 1st UTC test, output doesn't match"

    e = Epoch(2451545.0, utc=True)
    diff = round((e - JDE2000) * DAY2SEC, 3)

    assert diff == 64.184, "ERROR: 2nd UTC test, output doesn't match"
