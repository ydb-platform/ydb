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


from math import sqrt, radians, fsum, sin

from pymeeus.base import TOL
from pymeeus.Angle import Angle


"""
.. module:: CurveFitting
   :synopsis: Class to get the best fit of a curve to a set of (x, y) points
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Dagoberto Salazar
"""


class CurveFitting(object):
    """
    Class CurveFitting deals with finding the function (linear, cuadratic, etc)
    that best fit a given set of points.

    The constructor takes pairs of (x, y) values from the table of interest.
    These pairs of values can be given as a sequence of int/floats, tuples,
    lists or Angles. It is also possible to provide a CurveFitting object to
    the constructor in order to get a copy.

    .. note:: When using Angles, be careful with the 360-to-0 discontinuity.

    If a sequence of int, floats or Angles is given, the values in the odd
    positions are considered to belong to the 'x' set, while the values in the
    even positions belong to the 'y' set. If only one tuple or list is
    provided, it is assumed that it is the 'y' set, and the 'x' set is build
    from 0 onwards with steps of length 1.

    Please keep in mind that a minimum of two data pairs are needed in order to
    carry out any fitting. If only one value pair is provided, a ValueError
    exception will be raised.
    """

    def __init__(self, *args):
        """CurveFitting constructor.

        This takes pairs of (x, y) values from the table of interest. These
        pairs of values can be given as a sequence of int/floats, tuples, lists
        or Angles. It is also possible to provide a CurveFitting object to the
        constructor in order to get a copy.

        .. note:: When using Angles, be careful with the 360-to-0 discontinuity

        If a sequence of int, floats or Angles is given, the values in the odd
        positions are considered to belong to the 'x' set, while the values in
        the even positions belong to the 'y' set. If only one tuple or list is
        provided, it is assumed that it is the 'y' set, and the 'x' set is
        build from 0 onwards with steps of length 1.

        Please keep in mind that a minimum of two data pairs are needed in
        order to carry out any interpolation. If only one value pair is
        provided, a ValueError exception will be raised.

        :param args: Input tabular values, or another CurveFitting object.
        :type args: int, float, list, tuple, :py:class:`Angle`,
           :py:class:`CurveFitting`

        :returns: CurveFitting object.
        :rtype: :py:class:`CurveFitting`
        :raises: ValueError if not enough input data pairs are provided.
        :raises: TypeError if input values are of wrong type.

        >>> i = CurveFitting([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> print(i._x)
        [5, 3, 6, 1, 2, 4]
        >>> print(i._y)
        [10, 6, 12, 2, 4, 8]
        >>> j = CurveFitting([3, -8, 1, 12, 2, 5, 8])
        >>> print(j._x)
        [0, 1, 2, 3, 4, 5, 6]
        >>> print(j._y)
        [3, -8, 1, 12, 2, 5, 8]
        >>> k = CurveFitting(3, -8, 1, 12, 2, 5, 8)
        >>> print(k._x)
        [3, 1, 2]
        >>> print(k._y)
        [-8, 12, 5]
        >>> m = CurveFitting(k)
        >>> print(m._x)
        [3, 1, 2]
        >>> print(m._y)
        [-8, 12, 5]
        """

        # Initialize data table
        self._x = []
        self._y = []
        self.set(*args)  # Let's use 'set()' method to handle the setup

    def set(self, *args):
        """Method used to define the value pairs of CurveFitting object.

        This takes pairs of (x, y) values from the table of interest. These
        pairs of values can be given as a sequence of int/floats, tuples,
        lists, or Angles. It is also possible to provide a CurveFitting object
        to this method in order to get a copy.

        .. note:: When using Angles, be careful with the 360-to-0 discontinuity

        If a sequence of int, floats or Angles is given, the values in the odd
        positions are considered to belong to the 'x' set, while the values in
        the even positions belong to the 'y' set. If only one tuple or list is
        provided, it is assumed that it is the 'y' set, and the 'x' set is
        build from 0 onwards with steps of length 1.

        Please keep in mind that a minimum of two data pairs are needed in
        order to carry out any interpolation. If only one value is provided, a
        ValueError exception will be raised.

        :param args: Input tabular values, or another CurveFitting object.
        :type args: int, float, list, tuple, :py:class:`Angle`

        :returns: None.
        :rtype: None
        :raises: ValueError if not enough input data pairs are provided.
        :raises: TypeError if input values are of wrong type.

        >>> i = CurveFitting()
        >>> i.set([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> print(i._x)
        [5, 3, 6, 1, 2, 4]
        >>> print(i._y)
        [10, 6, 12, 2, 4, 8]
        >>> j = CurveFitting()
        >>> j.set([3, -8, 1, 12, 2, 5, 8])
        >>> print(j._x)
        [0, 1, 2, 3, 4, 5, 6]
        >>> print(j._y)
        [3, -8, 1, 12, 2, 5, 8]
        >>> k = CurveFitting(3, -8, 1, 12, 2, 5, 8)
        >>> print(k._x)
        [3, 1, 2]
        >>> print(k._y)
        [-8, 12, 5]
        """

        # Clean up the internal data tables and parameters
        self._x = []
        self._y = []
        # If no arguments are given, return. Internal data tables are empty
        if len(args) == 0:
            return
        # If we have only one argument, it can be a single value or tuple/list
        elif len(args) == 1:
            if isinstance(args[0], CurveFitting):
                self._x = args[0]._x
                self._y = args[0]._y
            elif isinstance(args[0], (int, float, Angle)):
                # Insuficient data for curve fitting. Raise ValueError
                raise ValueError("Invalid number of input values")
            elif isinstance(args[0], (list, tuple)):
                seq = args[0]
                if len(seq) < 2:
                    raise ValueError("Invalid number of input values")
                else:
                    # Read input values into 'y', and create 'x'
                    i = 0
                    for value in seq:
                        self._x.append(i)
                        self._y.append(value)
                        i += 1
            else:
                raise TypeError("Invalid input value")
        elif len(args) == 2:
            if isinstance(args[0], (int, float, Angle)) or isinstance(
                args[1], (int, float, Angle)
            ):
                # Insuficient data for curve fitting. Raise ValueError
                raise ValueError("Invalid number of input values")
            elif isinstance(args[0], (list, tuple)) and isinstance(
                args[1], (list, tuple)
            ):
                x = args[0]
                y = args[1]
                # Check if they have the same length. If not, make them equal
                length_min = min(len(x), len(y))
                x = x[:length_min]
                y = y[:length_min]
                if len(x) < 2 or len(y) < 2:
                    raise ValueError("Invalid number of input values")
                else:
                    # Read input values into 'x' and 'y'
                    for xval, yval in zip(x, y):
                        self._x.append(xval)
                        self._y.append(yval)
            else:
                raise TypeError("Invalid input value")
        elif len(args) == 3:
            # In this case, no combination of input values is valid
            raise ValueError("Invalid number of input values")
        else:
            # If there is an odd number of arguments, drop the last one
            if len(args) % 2 != 0:
                args = args[:-1]
            # Check that all the arguments are ints, floats or Angles
            all_numbers = True
            for arg in args:
                all_numbers = (all_numbers
                               and isinstance(arg, (int, float, Angle)))
            # If any of the values failed the test, raise an exception
            if not all_numbers:
                raise TypeError("Invalid input value")
            # Now, extract the data: Odds are x's, evens are y's
            for i in range(int(len(args) / 2.0)):
                self._x.append(args[2 * i])
                self._y.append(args[2 * i + 1])
        # Compute parameters
        if len(self._x) > 0:
            self._compute_parameters()

    def _compute_parameters(self):
        """Method to compute the intermediate parameters using for fitting."""

        self._P = 0.0
        self._Q = 0.0
        self._R = 0.0
        self._S = 0.0
        self._T = 0.0
        self._U = 0.0
        self._V = 0.0
        self._W = 0.0
        self._N = len(self._x)
        self._P = fsum(self._x)
        self._T = fsum(self._y)
        for i in range(self._N):
            x2 = self._x[i] * self._x[i]
            xy = self._x[i] * self._y[i]
            self._Q += x2
            self._R += x2 * self._x[i]
            self._S += x2 * x2
            self._U += xy
            self._V += xy * self._x[i]
            self._W += self._y[i] * self._y[i]
        return

    def __str__(self):
        """Method used when trying to print the object.

        :returns: Internal tabular values as strings.
        :rtype: string

        >>> i = CurveFitting([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> print(i)
        X: [5, 3, 6, 1, 2, 4]
        Y: [10, 6, 12, 2, 4, 8]
        """

        xstr = "X: " + str(self._x) + "\n"
        ystr = "Y: " + str(self._y)
        return xstr + ystr

    def __repr__(self):
        """Method providing the 'official' string representation of the object.
        It provides a valid expression that could be used to recreate the
        object.

        :returns: As string with a valid expression to recreate the object
        :rtype: string

        >>> i = CurveFitting([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> repr(i)
        'CurveFitting([5, 3, 6, 1, 2, 4], [10, 6, 12, 2, 4, 8])'
        """

        return "{}({}, {})".format(self.__class__.__name__, self._x, self._y)

    def __len__(self):
        """This method returns the number of value pairs internally stored in
        this object.

        :returns: Number of value pairs internally stored
        :rtype: int

        >>> i = CurveFitting([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> len(i)
        6
        """

        return len(self._x)

    def correlation_coeff(self):
        """This method returns the coefficient of correlation, as a float.

        :returns: Coefficient of correlation.
        :rtype: float

        >>> cf = CurveFitting([73.0, 38.0, 35.0, 42.0, 78.0, 68.0, 74.0, 42.0,
        ...                    52.0, 54.0, 39.0, 61.0, 42.0, 49.0, 50.0, 62.0,
        ...                    44.0, 39.0, 43.0, 54.0, 44.0, 37.0],
        ...                   [90.4, 125.3, 161.8, 143.4, 52.5, 50.8, 71.5,
        ...                    152.8, 131.3, 98.5, 144.8, 78.1, 89.5, 63.9,
        ...                    112.1, 82.0, 119.8, 161.2, 208.4, 111.6, 167.1,
        ...                    162.1])
        >>> r = cf.correlation_coeff()
        >>> print(round(r, 3))
        -0.767
        """

        n = self._N
        sxy = self._U
        sx = self._P
        sy = self._T
        sx2 = self._Q
        sy2 = self._W
        return ((n * sxy - sx * sy) / (sqrt(n * sx2 - sx * sx)
                                       * sqrt(n * sy2 - sy * sy)))

    def linear_fitting(self):
        """This method returns a tuple with the 'a', 'b' coefficients of the
        linear equation *'y = a*x + b'* that best fits the table data, using
        the least squares approach.

        :returns: 'a', 'b' coefficients of best linear equation fit.
        :rtype: tuple
        :raises: ZeroDivisionError if input data leads to a division by zero

        >>> cf = CurveFitting([73.0, 38.0, 35.0, 42.0, 78.0, 68.0, 74.0, 42.0,
        ...                    52.0, 54.0, 39.0, 61.0, 42.0, 49.0, 50.0, 62.0,
        ...                    44.0, 39.0, 43.0, 54.0, 44.0, 37.0],
        ...                   [90.4, 125.3, 161.8, 143.4, 52.5, 50.8, 71.5,
        ...                    152.8, 131.3, 98.5, 144.8, 78.1, 89.5, 63.9,
        ...                    112.1, 82.0, 119.8, 161.2, 208.4, 111.6, 167.1,
        ...                    162.1])
        >>> a, b = cf.linear_fitting()
        >>> print("a = {}\tb = {}".format(round(a, 2), round(b, 2)))
        a = -2.49	b = 244.18
        """

        n = self._N
        sxy = self._U
        sx = self._P
        sy = self._T
        sx2 = self._Q
        d = n * sx2 - sx * sx

        if abs(d) < TOL:
            raise ZeroDivisionError("Input data leads to a division by zero")

        a = (n * sxy - sx * sy) / d
        b = (sy * sx2 - sx * sxy) / d
        return (a, b)

    def quadratic_fitting(self):
        """This method returns a tuple with the 'a', 'b', 'c' coefficients of
        the quadratic equation *'y = a*x*x + b*x + c'* that best fits the table
        data, using the least squares approach.

        :returns: 'a', 'b', 'c' coefficients of best quadratic equation fit.
        :rtype: tuple
        :raises: ZeroDivisionError if input data leads to a division by zero

        >>> cf2 = CurveFitting([-2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5,
        ...                     2.0, 2.5,3.0],
        ...                    [-9.372, -3.821, 0.291, 3.730, 5.822, 8.324,
        ...                     9.083, 6.957, 7.006, 0.365, -1.722])
        >>> a, b, c = cf2.quadratic_fitting()
        >>> print("a = {}; b = {}; c = {}".format(round(a, 2), round(b, 2),
        ...                                       round(c, 2)))
        a = -2.22; b = 3.76; c = 6.64
        """

        n = self._N
        p = self._P
        q = self._Q
        r = self._R
        s = self._S
        t = self._T
        u = self._U
        v = self._V
        q2 = q * q
        d = n * q * s + 2.0 * p * q * r - q2 * q - p * p * s - n * r * r

        if abs(d) < TOL:
            raise ZeroDivisionError("Input data leads to a division by zero")

        a = (n * q * v + p * r * t + p * q * u
             - q2 * t - p * p * v - n * r * u) / d
        b = (n * s * u + p * q * v + q * r * t
             - q2 * u - p * s * t - n * r * v) / d
        c = (q * s * t + q * r * u + p * r * v
             - q2 * v - p * s * u - r * r * t) / d
        return (a, b, c)

    def general_fitting(self, f0, f1=lambda *args: 0.0, f2=lambda *args: 0.0):
        """This method returns a tuple with the 'a', 'b', 'c' coefficients of
        the general equation *'y = a*f0(x) + b*f1(x) + c*f2(x)'* that best fits
        the table data, using the least squares approach.

        :param f0, f1, f2: Functions used to build the general equation.
        :type f0, f1, f2: function
        :returns: 'a', 'b', 'c' coefficients of best general equation fit.
        :rtype: tuple
        :raises: ZeroDivisionError if input functions are null or input data
           leads to a division by zero

        >>> cf4 = CurveFitting([3, 20, 34, 50, 75, 88, 111, 129, 143, 160, 183,
        ...                     200, 218, 230, 248, 269, 290, 303, 320, 344],
        ...                    [0.0433, 0.2532, 0.3386, 0.3560, 0.4983, 0.7577,
        ...                     1.4585, 1.8628, 1.8264, 1.2431, -0.2043,
        ...                     -1.2431, -1.8422, -1.8726, -1.4889, -0.8372,
        ...                     -0.4377, -0.3640, -0.3508, -0.2126])
        >>> def sin1(x): return sin(radians(x))
        >>> def sin2(x): return sin(radians(2.0*x))
        >>> def sin3(x): return sin(radians(3.0*x))
        >>> a, b, c = cf4.general_fitting(sin1, sin2, sin3)
        >>> print("a = {}; b = {}; c = {}".format(round(a, 2), round(b, 2),
        ...                                       round(c, 2)))
        a = 1.2; b = -0.77; c = 0.39

        >>> cf5 = CurveFitting([0, 1.2, 1.4, 1.7, 2.1, 2.2])
        >>> a, b, c = cf5.general_fitting(sqrt)
        >>> print("a = {}; b = {}; c = {}".format(round(a, 3), round(b, 3),
        ...                                       round(c, 3)))
        a = 1.016; b = 0.0; c = 0.0
        """

        m = 0
        p = 0
        q = 0
        r = 0
        s = 0
        t = 0
        u = 0
        v = 0
        w = 0
        xl = list(self._x)
        yl = list(self._y)
        for i, value in enumerate(xl):
            x = value
            y = yl[i]
            m += f0(x) * f0(x)
            p += f0(x) * f1(x)
            q += f0(x) * f2(x)
            r += f1(x) * f1(x)
            s += f1(x) * f2(x)
            t += f2(x) * f2(x)
            u += y * f0(x)
            v += y * f1(x)
            w += y * f2(x)

        if abs(r) < TOL and abs(t) < TOL and abs(m) >= TOL:
            return (u / m, 0.0, 0.0)

        if abs(m * r * t) < TOL:
            raise ZeroDivisionError("Invalid input functions: They are null")

        d = m * r * t + 2.0 * p * q * s - m * s * s - r * q * q - t * p * p

        if abs(d) < TOL:
            raise ZeroDivisionError("Input data leads to a division by zero")

        a = (u * (r * t - s * s) + v * (q * s - p * t)
             + w * (p * s - q * r)) / d
        b = (u * (s * q - p * t) + v * (m * t - q * q)
             + w * (p * q - m * s)) / d
        c = (u * (p * s - r * q) + v * (p * q - m * s)
             + w * (m * r - p * p)) / d
        return (a, b, c)


def main():

    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Now let's work with the CurveFitting class
    print("\n" + 35 * "*")
    print("*** Use of CurveFitting class")
    print(35 * "*" + "\n")

    # Create a CurveFitting object
    cf1 = CurveFitting(
        [
            73.0,
            38.0,
            35.0,
            42.0,
            78.0,
            68.0,
            74.0,
            42.0,
            52.0,
            54.0,
            39.0,
            61.0,
            42.0,
            49.0,
            50.0,
            62.0,
            44.0,
            39.0,
            43.0,
            54.0,
            44.0,
            37.0,
        ],
        [
            90.4,
            125.3,
            161.8,
            143.4,
            52.5,
            50.8,
            71.5,
            152.8,
            131.3,
            98.5,
            144.8,
            78.1,
            89.5,
            63.9,
            112.1,
            82.0,
            119.8,
            161.2,
            208.4,
            111.6,
            167.1,
            162.1,
        ],
    )

    # Let's use 'linear_fitting()'
    a, b = cf1.linear_fitting()
    print("Linear fitting for cf1:")
    print("   a = {}\tb = {}".format(round(a, 2), round(b, 2)))

    print("")

    # Use the copy constructor
    print("Let's make a copy:")
    cf2 = CurveFitting(cf1)
    print("   cf2 = CurveFitting(cf1)")
    a, b = cf2.linear_fitting()
    print("Linear fitting for cf2:")
    print("   a = {}\tb = {}".format(round(a, 2), round(b, 2)))

    print("")

    # Get the number of value pairs internally stored
    print_me("Number of value pairs inside 'cf2'", len(cf2))  # 22

    print("")

    # Compute the correlation coefficient
    r = cf1.correlation_coeff()
    print("Correlation coefficient:")
    print_me("   r", round(r, 3))

    cf2 = CurveFitting(
        [-2.0, -1.5, -1.0, -0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0],
        [
            -9.372,
            -3.821,
            0.291,
            3.730,
            5.822,
            8.324,
            9.083,
            6.957,
            7.006,
            0.365,
            -1.722,
        ],
    )

    print("")

    # Now use 'quadratic_fitting()'
    a, b, c = cf2.quadratic_fitting()
    # Original curve: y = -2.0*x*x + 3.5*x + 7.0 + noise
    print("Quadratic fitting:")
    print("   a = {}\tb = {}\tc = {}".format(round(a, 2), round(b, 2),
                                             round(c, 2)))

    print("")

    cf4 = CurveFitting(
        [
            3,
            20,
            34,
            50,
            75,
            88,
            111,
            129,
            143,
            160,
            183,
            200,
            218,
            230,
            248,
            269,
            290,
            303,
            320,
            344,
        ],
        [
            0.0433,
            0.2532,
            0.3386,
            0.3560,
            0.4983,
            0.7577,
            1.4585,
            1.8628,
            1.8264,
            1.2431,
            -0.2043,
            -1.2431,
            -1.8422,
            -1.8726,
            -1.4889,
            -0.8372,
            -0.4377,
            -0.3640,
            -0.3508,
            -0.2126,
        ],
    )

    # Let's define the three functions to be used for fitting
    def sin1(x):
        return sin(radians(x))

    def sin2(x):
        return sin(radians(2.0 * x))

    def sin3(x):
        return sin(radians(3.0 * x))

    # Use 'general_fitting()' here
    a, b, c = cf4.general_fitting(sin1, sin2, sin3)
    print("General fitting with f0 = sin(x), f1 = sin(2*x), f2 = sin(3*x):")
    print("   a = {}\tb = {}\tc = {}".format(round(a, 2), round(b, 2),
                                             round(c, 2)))

    print("")

    cf5 = CurveFitting([0, 1.2, 1.4, 1.7, 2.1, 2.2])

    a, b, c = cf5.general_fitting(sqrt)
    print("General fitting with f0 = sqrt(x), f1 = 0.0 and f2 = 0.0:")
    print("   a = {}\tb = {}\t\tc = {}".format(round(a, 3), round(b, 3),
                                               round(c, 3)))


if __name__ == "__main__":

    main()
