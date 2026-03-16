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


from math import sqrt, degrees

from pymeeus.base import TOL
from pymeeus.Angle import Angle
from pymeeus.Interpolation import Interpolation


# Declare some objects to be used later
i_ra = Interpolation()
i_angles1 = Interpolation()
i_angles2 = Interpolation()
i_sine = Interpolation()


def setup():
    """This function is used to set up the environment for the tests"""
    # Set up a interpolation object which uses Right Ascension
    y0 = Angle(10, 18, 48.732, ra=True)
    y1 = Angle(10, 23, 22.835, ra=True)
    y2 = Angle(10, 27, 57.247, ra=True)
    y3 = Angle(10, 32, 31.983, ra=True)

    i_ra.set([8.0, 10.0, 12.0, 14.0], [y0, y1, y2, y3])

    # Set up a couple interpolation objects with Angles
    y0 = Angle(0, -28, 13.4)
    y1 = Angle(0, 6, 46.3)
    y2 = Angle(0, 38, 23.2)

    i_angles1.set([26.0, 27.0, 28.0], [y0, y1, y2])

    y0 = Angle(-1, 11, 21.23)
    y1 = Angle(0, -28, 12.31)
    y2 = Angle(0, 16, 7.02)
    y3 = Angle(1, 1, 0.13)
    y4 = Angle(1, 45, 46.33)

    i_angles2.set([25.0, 26.0, 27.0, 28.0, 29.0], [y0, y1, y2, y3, y4])

    # Set up an interpolation object with 6 interpolation table entries, based
    # on sine function
    i_sine.set([29.43, 30.97, 27.69, 28.11, 31.58, 33.05],
               [0.4913598528, 0.5145891926, 0.4646875083, 0.4711658342,
                0.5236885653, 0.5453707057])


def teardown():
    pass


# Interpolation class

def test_interpolation_constructor():
    """Tests the constructor of Interpolation class"""

    i = Interpolation([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
    assert i._x == [1, 2, 3, 4, 5, 6], \
        "ERROR: 1st constructor test, 'x' values don't match"

    assert i._y == [2, 4, 6, 8, 10, 12], \
        "ERROR: 2nd constructor test, 'y' values don't match"

    j = Interpolation([3, -8, 1, 12, 2, 5, 8])
    assert j._x == [0, 1, 2, 3, 4, 5, 6], \
        "ERROR: 3rd constructor test, 'x' values don't match"

    assert j._y == [3, -8, 1, 12, 2, 5, 8], \
        "ERROR: 4th constructor test, 'y' values don't match"

    k = Interpolation(3, -8, 1, 12, 2, 5, 8)
    assert k._x == [1, 2, 3], \
        "ERROR: 5th constructor test, 'x' values don't match"

    assert k._y == [12, 5, -8], \
        "ERROR: 6th constructor test, 'y' values don't match"


def test_interpolation_call():
    """Tests the __call__() method of Interpolation class"""

    m = Interpolation([-1.0, 0.0, 1.0], [-2.0, 3.0, 2.0])

    assert abs(m(-0.8) - (-0.52)) < TOL, \
        "ERROR: In 1st __call__() test, output value doesn't match"

    assert abs(m(0.7) - 2.93) < TOL, \
        "ERROR: In 2nd __call__() test, output value doesn't match"

    assert abs(m(-1.0) - (-2.0)) < TOL, \
        "ERROR: In 3rd __call__() test, output value doesn't match"

    m = Interpolation([-3.0, 0.0, 2.5], [12.0, -3.0, -1.75])

    assert abs(m(-2.0) - 5.0) < TOL, \
        "ERROR: In 4th __call__() test, output value doesn't match"

    assert abs(m(2.5) - (-1.75)) < TOL, \
        "ERROR: In 5th __call__() test, output value doesn't match"

    # This interpolation test uses Right Ascension
    a = Angle(i_ra(11.0))
    h, m, s, sign = a.ra_tuple()
    assert abs(h - 10.0) < TOL and \
        abs(m - 25.0) < TOL and \
        abs(s - 40.0014375) < TOL and \
        abs(sign == 1.0), \
        "ERROR: In 6th __call__() test, output value doesn't match"

    # Test with 6 interpolation table entries, based on sine function
    assert abs(i_sine(30.0) - 0.5) < TOL, \
        "ERROR: In 7th __call__() test, output value doesn't match"


def test_interpolation_derivative():
    """Tests the derivative() method of Interpolation class"""

    m = Interpolation([-1.0, 0.0, 1.0], [-2.0, 3.0, 2.0])

    assert abs(m.derivative(-1.0) - 8.0) < TOL, \
        "ERROR: In 1st derivative() test, output value doesn't match"

    assert abs(m.derivative(0.0) - 2.0) < TOL, \
        "ERROR: In 2nd derivative() test, output value doesn't match"

    assert abs(m.derivative(0.5) - (-1.0)) < TOL, \
        "ERROR: In 3rd derivative() test, output value doesn't match"

    m = Interpolation([-3.0, 0.0, 2.5], [12.0, -3.0, -1.75])

    assert abs(m.derivative(-3.0) - (-8.0)) < TOL, \
        "ERROR: In 4th derivative() test, output value doesn't match"

    assert abs(m.derivative(0.0) - (-2.0)) < TOL, \
        "ERROR: In 5th derivative() test, output value doesn't match"

    assert abs(m.derivative(2.5) - 3.0) < TOL, \
        "ERROR: In 6th derivative() test, output value doesn't match"

    # Do test with an interpolation object with 6 table entries, based on sine
    # We need to adjust the result because degrees were used instead of radians
    res = degrees(i_sine.derivative(30.0))
    assert abs(res - sqrt(3.0)/2.0) < TOL, \
        "ERROR: In 7th derivative() test, output value doesn't match"


def test_interpolation_root():
    """Tests the root() method of Interpolation class"""

    m = Interpolation([-1.0, 0.0, 1.0], [-2.0, 3.0, 2.0])

    assert abs(m.root() - (-0.7207592200561265)) < TOL, \
        "ERROR: In 1st root() test, output value doesn't match"

    m = Interpolation([-3.0, 0.0, 2.5], [12.0, -3.0, -1.75])

    assert abs(m.root(-2.0, 0.0) - (-1.0)) < TOL, \
        "ERROR: In 2nd root() test, output value doesn't match"

    assert abs(m.root() - (-1.0)) < TOL, \
        "ERROR: In 3rd root() test, output value doesn't match"

    m = Interpolation([-3.0, 0.0, 2.5, 3.5], [12.0, -3.0, -1.75, 2.25])

    assert abs(m.root(0.0, 3.15) - 3.0) < TOL, \
        "ERROR: In 4th root() test, output value doesn't match"

    # Let's do some tests with Angles
    assert abs(i_angles1.root() - 26.798732705) < TOL, \
        "ERROR: In 5th root() test, output value doesn't match"

    assert abs(i_angles2.root() - 26.6385869469) < TOL, \
        "ERROR: In 6th root() test, output value doesn't match"


def test_interpolation_minmax():
    """Tests the minmax() method of Interpolation class"""

    m = Interpolation([-1.0, 0.0, 1.0], [-2.0, 3.0, 2.0])

    assert abs(m.minmax() - 0.3333333333) < TOL, \
        "ERROR: In 1st minmax() test, output value doesn't match"

    m = Interpolation([-3.0, 0.0, 2.5], [12.0, -3.0, -1.75])

    assert abs(m.minmax() - 1.0) < TOL, \
        "ERROR: In 2nd minmax() test, output value doesn't match"

    m = Interpolation([12.0, 16.0, 20.0], [1.3814294, 1.3812213, 1.3812453])

    assert abs(m.minmax() - 17.5863851788) < TOL, \
        "ERROR: In 3rd minmax() test, output value doesn't match"

    assert abs(m(m.minmax()) - 1.38120304666) < TOL, \
        "ERROR: In 4th minmax() test, output value doesn't match"
