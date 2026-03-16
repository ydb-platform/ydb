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


from math import sqrt, radians, sin

from pymeeus.base import TOL
from pymeeus.CurveFitting import CurveFitting


# Declare some objects to be used later
cf1 = CurveFitting()
cf2 = CurveFitting()
cf3 = CurveFitting()
cf4 = CurveFitting()


def setup():
    """This function is used to set up the environment for the tests"""

    # Set up a few CurveFitting objects
    cf1.set([73.0, 38.0, 35.0, 42.0, 78.0, 68.0, 74.0, 42.0, 52.0, 54.0, 39.0,
             61.0, 42.0, 49.0, 50.0, 62.0, 44.0, 39.0, 43.0, 54.0, 44.0, 37.0],
            [90.4, 125.3, 161.8, 143.4, 52.5, 50.8, 71.5, 152.8, 131.3, 98.5,
             144.8, 78.1, 89.5, 63.9, 112.1, 82.0, 119.8, 161.2, 208.4, 111.6,
             167.1, 162.1])

    cf2.set([0.2982, 0.2969, 0.2918, 0.2905, 0.2707, 0.2574, 0.2485, 0.2287,
             0.2238, 0.2156, 0.1992, 0.1948, 0.1931, 0.1889, 0.1781, 0.1772,
             0.1770, 0.1755, 0.1746],
            [10.92, 11.01, 10.99, 10.78, 10.87, 10.80, 10.75, 10.14, 10.21,
             9.97, 9.69, 9.57, 9.66, 9.63, 9.65, 9.44, 9.44, 9.32, 9.20])

    cf3.set([-2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0],
            [-9.372, -3.821, 0.291, 3.730, 5.822, 8.324, 9.083, 6.957, 7.006,
             0.365, -1.722])

    cf4.set([3, 20, 34, 50, 75, 88, 111, 129, 143, 160, 183, 200, 218, 230,
             248, 269, 290, 303, 320, 344],
            [0.0433, 0.2532, 0.3386, 0.3560, 0.4983, 0.7577, 1.4585, 1.8628,
             1.8264, 1.2431, -0.2043, -1.2431, -1.8422, -1.8726, -1.4889,
             -0.8372, -0.4377, -0.3640, -0.3508, -0.2126])


def teardown():
    pass


# CurveFitting class

def test_curvefitting_constructor():
    """Tests the constructor of CurveFitting class"""

    i = CurveFitting([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
    assert i._x == [5, 3, 6, 1, 2, 4], \
        "ERROR: 1st constructor test, 'x' values don't match"

    assert i._y == [10, 6, 12, 2, 4, 8], \
        "ERROR: 2nd constructor test, 'y' values don't match"

    j = CurveFitting([3, -8, 1, 12, 2, 5, 8])
    assert j._x == [0, 1, 2, 3, 4, 5, 6], \
        "ERROR: 3rd constructor test, 'x' values don't match"

    assert j._y == [3, -8, 1, 12, 2, 5, 8], \
        "ERROR: 4th constructor test, 'y' values don't match"

    k = CurveFitting(3, -8, 1, 12, 2, 5, 8)
    assert k._x == [3, 1, 2], \
        "ERROR: 5th constructor test, 'x' values don't match"

    assert k._y == [-8, 12, 5], \
        "ERROR: 6th constructor test, 'y' values don't match"


def test_curvefitting_correlation_coeff():
    """Tests the correlation_coeff() method of CurveFitting class"""

    r = cf1.correlation_coeff()
    assert abs(round(r, 3) - (-0.767)) < TOL, \
        "ERROR: 1st correlation_coeff() test, 'r' value doesn't match"


def test_curvefitting_linear_fitting():
    """Tests the linear_fitting() method of CurveFitting class"""

    a, b = cf1.linear_fitting()
    assert abs(round(a, 2) - (-2.49)) < TOL, \
        "ERROR: In 1st linear_fitting() test, 'a' value doesn't match"

    assert abs(round(b, 2) - 244.18) < TOL, \
        "ERROR: In 2nd linear_fitting() test, 'b' value doesn't match"

    a, b = cf2.linear_fitting()
    assert abs(round(a, 2) - 13.67) < TOL, \
        "ERROR: In 3rd linear_fitting() test, 'a' value doesn't match"

    assert abs(round(b, 2) - 7.03) < TOL, \
        "ERROR: In 4th linear_fitting() test, 'b' value doesn't match"


def test_curvefitting_quadratic_fitting():
    """Tests the quadratic_fitting() method of CurveFitting class"""

    a, b, c = cf3.quadratic_fitting()
    assert abs(round(a, 2) - (-2.22)) < TOL, \
        "ERROR: In 1st quadratic_fitting() test, 'a' value doesn't match"

    assert abs(round(b, 2) - 3.76) < TOL, \
        "ERROR: In 2nd quadratic_fitting() test, 'b' value doesn't match"

    assert abs(round(c, 2) - 6.64) < TOL, \
        "ERROR: In 3rd quadratic_fitting() test, 'c' value doesn't match"


def test_curvefitting_general_fitting():
    """Tests the general_fitting() method of CurveFitting class"""

    # Let's define the three functions to be used for fitting
    def sin1(x):
        return sin(radians(x))

    def sin2(x):
        return sin(radians(2.0*x))

    def sin3(x):
        return sin(radians(3.0*x))

    a, b, c = cf4.general_fitting(sin1, sin2, sin3)
    assert abs(round(a, 2) - 1.2) < TOL, \
        "ERROR: In 1st general_fitting() test, 'a' value doesn't match"

    assert abs(round(b, 2) - (-0.77)) < TOL, \
        "ERROR: In 2nd general_fitting() test, 'b' value doesn't match"

    assert abs(round(c, 2) - 0.39) < TOL, \
        "ERROR: In 3rd general_fitting() test, 'c' value doesn't match"

    cf5 = CurveFitting([0, 1.2, 1.4, 1.7, 2.1, 2.2])

    a, b, c = cf5.general_fitting(sqrt)
    assert abs(round(a, 3) - 1.016) < TOL, \
        "ERROR: In 4th general_fitting() test, 'a' value doesn't match"

    assert abs(round(b, 3) - 0.0) < TOL, \
        "ERROR: In 5th general_fitting() test, 'b' value doesn't match"

    assert abs(round(c, 3) - 0.0) < TOL, \
        "ERROR: In 6th general_fitting() test, 'c' value doesn't match"
