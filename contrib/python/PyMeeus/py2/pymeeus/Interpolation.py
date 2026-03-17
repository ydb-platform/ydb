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


"""
.. module:: Interpolation
   :synopsis: Basic interpolation routines
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Dagoberto Salazar
"""


class Interpolation(object):
    """
    Class Interpolation deals with finding intermediate values to those given
    in a table.

    Besides the basic interpolation, this class also provides methods to find
    the extrema (maximum or minimum value) in a given interval (if present),
    and it also has methods to find roots (the values of the argument 'x' for
    which the function 'y' becomes zero).

    Please note that it seems that Meeus uses the Bessel interpolation method
    (Chapter 3). However, this class uses the Newton interpolation method
    because it is (arguably) more flexible regarding the number of entries
    provided in the interpolation table.

    The constructor takes pairs of (x, y) values from the table of interest.
    These pairs of values can be given as a sequence of int/floats, tuples,
    lists or Angles. It is also possible to provide an Interpolation object to
    the constructor in order to get a copy.

    .. note:: When using Angles, be careful with the 360-to-0 discontinuity.

    If a sequence of int, floats or Angles is given, the values in the odd
    positions are considered to belong to the 'x' set, while the values in the
    even positions belong to the 'y' set. If only one tuple or list is
    provided, it is assumed that it is the 'y' set, and the 'x' set is build
    from 0 onwards with steps of length 1.

    Please keep in mind that a minimum of two data pairs are needed in order to
    carry out any interpolation. If only one value pair is provided, a
    ValueError exception will be raised.
    """

    def __init__(self, *args):
        """Interpolation constructor.

        This takes pairs of (x, y) values from the table of interest. These
        pairs of values can be given as a sequence of int/floats, tuples, lists
        or Angles. It is also possible to provide an Interpolation object to
        the constructor in order to get a copy.

        .. note:: When using Angles, be careful with the 360-to-0 discontinuity

        If a sequence of int, floats or Angles is given, the values in the odd
        positions are considered to belong to the 'x' set, while the values in
        the even positions belong to the 'y' set. If only one tuple or list is
        provided, it is assumed that it is the 'y' set, and the 'x' set is
        build from 0 onwards with steps of length 1.

        Please keep in mind that a minimum of two data pairs are needed in
        order to carry out any interpolation. If only one value pair is
        provided, a ValueError exception will be raised.

        :param args: Input tabular values, or another Interpolation object.
        :type args: int, float, list, tuple, :py:class:`Angle`,
           :py:class:`Interpolation`

        :returns: Interpolation object.
        :rtype: :py:class:`Interpolation`
        :raises: ValueError if not enough input data pairs are provided.
        :raises: TypeError if input values are of wrong type.

        >>> i = Interpolation([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> print(i._x)
        [1, 2, 3, 4, 5, 6]
        >>> print(i._y)
        [2, 4, 6, 8, 10, 12]
        >>> j = Interpolation([3, -8, 1, 12, 2, 5, 8])
        >>> print(j._x)
        [0, 1, 2, 3, 4, 5, 6]
        >>> print(j._y)
        [3, -8, 1, 12, 2, 5, 8]
        >>> k = Interpolation(3, -8, 1, 12, 2, 5, 8)
        >>> print(k._x)
        [1, 2, 3]
        >>> print(k._y)
        [12, 5, -8]
        """

        self._x = []
        self._y = []
        self._table = []
        self._tol = TOL
        self.set(*args)  # Let's use 'set()' method to handle the setup

    def _order_points(self):
        """Method to put the data points in ascending order w.r.t. 'x'."""

        # Let's work with copies of the original lists
        x = list(self._x)
        y = list(self._y)

        xnew = []
        ynew = []
        xmax = max(x) + 1.0
        for _ in range(len(x)):
            # Get the index of the minimum value
            imin = x.index(min(x))
            # Append the current minimum value to the new 'x' list
            xnew.append(x[imin])
            # Store the *position* of the current minimum value to new 'y' list
            ynew.append(imin)
            # The current minimum value will no longer be the minimum
            x[imin] = xmax

        # In the new 'y' list, substitute the positions by the real values
        for i in range(len(x)):
            ynew[i] = y[ynew[i]]

        # Store the results in the corresponding fields
        self._x = xnew
        self._y = ynew

    def set(self, *args):
        """Method used to define the value pairs of Interpolation object.

        This takes pairs of (x, y) values from the table of interest. These
        pairs of values can be given as a sequence of int/floats, tuples,
        lists, or Angles. It is also possible to provide an Interpolation
        object to this method in order to get a copy.

        .. note:: When using Angles, be careful with the 360-to-0 discontinuity

        If a sequence of int, floats or Angles is given, the values in the odd
        positions are considered to belong to the 'x' set, while the values in
        the even positions belong to the 'y' set. If only one tuple or list is
        provided, it is assumed that it is the 'y' set, and the 'x' set is
        build from 0 onwards with steps of length 1.

        Please keep in mind that a minimum of two data pairs are needed in
        order to carry out any interpolation. If only one value pair is
        provided, a ValueError exception will be raised.

        :param args: Input tabular values, or another Interpolation object.
        :type args: int, float, list, tuple, :py:class:`Angle`

        :returns: None.
        :rtype: None
        :raises: ValueError if not enough input data pairs are provided.
        :raises: TypeError if input values are of wrong type.

        >>> i = Interpolation()
        >>> i.set([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> print(i._x)
        [1, 2, 3, 4, 5, 6]
        >>> print(i._y)
        [2, 4, 6, 8, 10, 12]
        >>> j = Interpolation()
        >>> j.set([3, -8, 1, 12, 2, 5, 8])
        >>> print(j._x)
        [0, 1, 2, 3, 4, 5, 6]
        >>> print(j._y)
        [3, -8, 1, 12, 2, 5, 8]
        >>> k = Interpolation(3, -8, 1, 12, 2, 5, 8)
        >>> print(k._x)
        [1, 2, 3]
        >>> print(k._y)
        [12, 5, -8]
        >>> m = Interpolation(k)
        >>> print(m._x)
        [1, 2, 3]
        >>> print(m._y)
        [12, 5, -8]
        """

        # Clean up the internal data tables
        self._x = []
        self._y = []
        self._table = []
        # If no arguments are given, return. Internal data tables are empty
        if len(args) == 0:
            return
        # If we have only one argument, it can be a single value or tuple/list
        elif len(args) == 1:
            if isinstance(args[0], Interpolation):  # Copy constructor
                self._x = args[0]._x
                self._y = args[0]._y
                self._table = args[0]._table
                self._tol = args[0]._tol
                return
            elif isinstance(args[0], (int, float, Angle)):
                # Insuficient data to interpolate. Raise ValueError exception
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
                # Insuficient data to interpolate. Raise ValueError exception
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
        # After self._x is found, confirm that x's are different to each other
        for i in range(len(self._x) - 1):
            for k in range(i + 1, len(self._x)):
                if abs(self._x[i] - self._x[k]) < self._tol:
                    raise ValueError("Invalid input: Values in 'x' are equal")
        # Order the data points if needed
        self._order_points()
        # Create table containing Newton coefficientes, only if values given
        if len(self._x) > 0:
            self._compute_table()

    def __str__(self):
        """Method used when trying to print the object.

        :returns: Internal tabular values as strings.
        :rtype: string

        >>> i = Interpolation([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> print(i)
        X: [1, 2, 3, 4, 5, 6]
        Y: [2, 4, 6, 8, 10, 12]
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

        >>> i = Interpolation([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> repr(i)
        'Interpolation([1, 2, 3, 4, 5, 6], [2, 4, 6, 8, 10, 12])'
        """

        return "{}({}, {})".format(self.__class__.__name__, self._x, self._y)

    def __len__(self):
        """This method returns the number of interpolation points (x, y, pairs)
        stored in this :class:`Interpolation` object.

        :returns: Number of interpolation points internally stored
        :rtype: int

        >>> i = Interpolation([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
        >>> len(i)
        6
        """

        return len(self._x)

    def get_tolerance(self):
        """Gets the internal tolerance value used for comparisons.

        .. note:: The default tolerance value is TOL.

        :returns: Internal tolerance.
        :rtype: float
        """

        return self._tol

    def set_tolerance(self, tol):
        """Changes the internal tolerance value used for comparisons.

        :param tol: New tolerance value.
        :type tol: int, float

        :returns: None
        :rtype: None
        """

        self._tol = tol
        return

    def _compute_table(self):
        """Method to compute coefficients of Newton interpolation method."""

        for i in range(len(self._x)):
            self._table.append(self._newton_diff(0, i))

    def _newton_diff(self, start, end):
        """Auxiliary method to compute the elements of the Newton table.

        :param start: Starting index
        :type start: int
        :param end: Ending index
        :type end: int

        :returns: Resulting value of the element of the Newton table.
        :rtype: float
        """

        if abs(end - start) < self._tol:
            val = self._y[start]
        else:
            x = list(self._x)  # Let's make a copy, just in case
            val = (self._newton_diff(start, end - 1)
                   - self._newton_diff(start + 1, end)) / (x[start] - x[end])

        return val

    def __call__(self, x):
        """Method to interpolate the function at a given 'x'.

        :param x: Point where the interpolation will be carried out.
        :type x: int, float, :py:class:`Angle`

        :returns: Resulting value of the interpolation.
        :rtype: float
        :raises: ValueError if input value is outside of interpolation range.
        :raises: TypeError if input value is of wrong type.

        >>> i = Interpolation([7, 8, 9], [0.884226, 0.877366, 0.870531])
        >>> y = round(i(8.18125), 6)
        >>> print(y)
        0.876125
        """

        # Check if input value is of correct type
        if isinstance(x, (int, float, Angle)):
            # Check if 'x' already belongs to the data table
            for i in range(len(self._x)):
                if abs(x - self._x[i]) < self._tol:
                    return self._y[i]  # We don't need to look further
            # Check if Newton coefficients table is not empty
            if len(self._table) == 0:
                raise RuntimeError("Internal table is empty. Use set().")
            # Check that x is within interpolation table values
            if x < self._x[0] or x > self._x[-1]:
                raise ValueError("Input value outside of interpolation range.")
            # Horner's method is used to efficiently compute the result
            val = self._table[-1]
            for i in range(len(self._table) - 1, 0, -1):
                val = self._table[i - 1] + (x - self._x[i - 1]) * val

            return val
        else:
            raise TypeError("Invalid input value")

    def derivative(self, x):
        """Method to compute the derivative from interpolation polynomial.

        :param x: Point where the interpolation derivative will be carried out.
        :type x: int, float, :py:class:`Angle`

        :returns: Resulting value of the interpolation derivative.
        :rtype: float
        :raises: ValueError if input value is outside of interpolation range.
        :raises: TypeError if input value is of wrong type.

        >>> m = Interpolation([-1.0, 0.0, 1.0], [-2.0, 3.0, 2.0])
        >>> m.derivative(-1.0)
        8.0
        >>> m.derivative(0.5)
        -1.0
        """

        # Check if input value is of correct type
        if isinstance(x, (int, float, Angle)):
            # Check that x is within interpolation table values
            if x < self._x[0] or x > self._x[-1]:
                raise ValueError("Input value outside of interpolation range.")
            # If we only have two interpolation points, derivative is simple
            if len(self._x) == 2:
                return (self._y[1] - self._y[0]) / (self._x[1] - self._x[0])
            else:
                res = self._table[1]
                for k in range(len(self._table) - 1, 1, -1):
                    val = 0.0
                    for j in range(k):
                        s = 1.0
                        for i in range(k):
                            if i != j:
                                s *= x - self._x[i]
                        val += s
                    res += val * self._table[k]
                return res
        else:
            raise TypeError("Invalid input value")

    def root(self, xl=0, xh=0, max_iter=1000):
        """Method to find the root inside the [xl, xh] range.

        This method applies, in principle, the Newton method to find the root;
        however, if conditions are such that Newton method may not bei properly
        behaving or converging, then it switches to the linear Interpolation
        method.

        If values xl, xh are not given, the limits of the interpolation table
        values will be used.

        .. note:: This method returns a ValueError exception if the
           corresponding yl = f(xl) and yh = f(xh) values have the same sign.
           In that case, the method assumes there is no root in the [xl, xh]
           interval.

        .. note:: If any of the xl, xh values is beyond the limits given by the
           interpolation values, its value will be set to the corresponding
           limit.

        .. note:: If xl == xh (and not zero), a ValueError exception is raised.

        .. note:: If the method doesn't converge within max_iter ierations,
           then a ValueError exception is raised.

        :param xl: Lower limit of interval where the root will be looked for.
        :type xl: int, float, :py:class:`Angle`
        :param xh: Higher limit of interval where the root will be looked for.
        :type xh: int, float, :py:class:`Angle`
        :param max_iter: Maximum number of iterations allowed.
        :type max_iter: int

        :returns: Root of the interpolated function within [xl, xh] interval.
        :rtype: int, float, :py:class:`Angle`
        :raises: ValueError if yl = f(xl), yh = f(xh) have same sign.
        :raises: ValueError if xl == xh.
        :raises: ValueError if maximum number of iterations is exceeded.
        :raises: TypeError if input value is of wrong type.

        >>> m = Interpolation([-1.0, 0.0, 1.0], [-2.0, 3.0, 2.0])
        >>> round(m.root(), 8)
        -0.72075922
        """

        # Get the limits of the interpolation table
        xmin = self._x[0]
        xmax = self._x[-1]
        # Check if input value is of correct type
        if (isinstance(xl, (int, float, Angle))
                and isinstance(xh, (int, float, Angle))):
            # Check if BOTH values are zero
            if xl == 0 and xh == 0:
                xl = xmin
                xh = xmax
            # Check if limits are equal
            if abs(xl - xh) < self._tol:
                raise ValueError("Invalid limits: xl and xh values are equal")
            # Put limits in order. Swap them if necessary
            if xl > xh:
                xl, xh = xh, xl
            # Check limits against interpolation table. Reset if necessary
            if xl < self._x[0]:
                xl = xmin
            if xh < self._x[-1]:
                xh = xmax
            yl = self.__call__(xl)
            yh = self.__call__(xh)
            # Check for a couple special cases
            if abs(yl) < self._tol:
                return xl  # xl is a root
            if abs(yh) < self._tol:
                return xh  # xh is a root
            # Check if signs of ordinates are the same
            if (yl * yh) > 0.0:
                raise ValueError("Invalid interval: Probably no root exists")
            # We are good to go. First option: Newton's root-finding method
            x = (xl + xh) / 2.0  # Start in the middle of interval
            y = self.__call__(x)
            num_iter = 0  # Count the number of iterations
            while abs(y) > self._tol:
                if num_iter >= max_iter:
                    raise ValueError(
                        "Too many iterations: Probably no root\
                                     exists"
                    )
                num_iter += 1
                yp = self.derivative(x)
                # If derivative is too small, switch to linear interpolation
                if abs(yp) < 1e-3:
                    x = (xl * yh - xh * yl) / (yh - yl)
                    y = self.__call__(x)
                else:
                    x = x - y / yp
                    # Check if x is within limits
                    if x < xmin or x > xmax:
                        # Switch to linear interpolation
                        x = (xl * yh - xh * yl) / (yh - yl)
                        y = self.__call__(x)
                    else:
                        y = self.__call__(x)
                if (y * yl) >= 0.0:
                    xl = x
                    yl = y
                else:
                    xh = x
                    yh = y
            return x
        else:
            raise TypeError("Invalid input value")

    def minmax(self, xl=0, xh=0, max_iter=1000):
        """Method to find the minimum or maximum inside the [xl, xh] range.

        Finding the minimum or maximum (extremum) of a function within a given
        interval is akin to find the root of its derivative. Therefore, this
        method creates an interpolation object for the derivative function, and
        calls the root method of that object. See :meth:`root` method for more
        details.

        If values xl, xh are not given, the limits of the interpolation table
        values will be used.

        .. note::

           This method returns a ValueError exception if the corresponding
           derivatives yl' = f'(xl) and yh' = f'(xh) values have the same sign.
           In that case, the method assumes there is no extremum in the
           [xl, xh] interval.

        .. note::

           If any of the xl, xh values is beyond the limits given by the
           interpolation values, its value will be set to the corresponding
           limit.

        .. note:: If xl == xh (and not zero), a ValueError exception is raised.

        .. note:: If the method doesn't converge within max_iter ierations,
           then a ValueError exception is raised.

        :param xl: Lower limit of interval where a extremum will be looked for.
        :type xl: int, float, :py:class:`Angle`
        :param xh: Higher limit of interval where extremum will be looked for.
        :type xh: int, float, :py:class:`Angle`
        :param max_iter: Maximum number of iterations allowed.
        :type max_iter: int

        :returns: Extremum of interpolated function within [xl, xh] interval.
        :rtype: int, float, :py:class:`Angle`
        :raises: ValueError if yl = f(xl), yh = f(xh) have same sign.
        :raises: ValueError if xl == xh.
        :raises: ValueError if maximum number of iterations is exceeded.
        :raises: TypeError if input value is of wrong type.

        >>> m = Interpolation([-1.0, 0.0, 1.0], [-2.0, 3.0, 2.0])
        >>> round(m.minmax(), 8)
        0.33333333
        """

        # Compute the derivatives for the current data
        x = list(self._x)
        y = []
        for xi in x:
            y.append(self.derivative(xi))
        # Create a new Interpolation object
        prime = Interpolation(x, y)
        # Find the root within that object, and return it
        return prime.root(xl, xh, max_iter)


def main():

    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Let's now work with the Interpolation class
    print("\n" + 35 * "*")
    print("*** Use of Interpolation class")
    print(35 * "*" + "\n")

    i = Interpolation([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])
    print("i = Interpolation([5, 3, 6, 1, 2, 4, 9], [10, 6, 12, 2, 4, 8])")
    print(i)
    print("NOTE:")
    print("   a. They are ordered in 'x'")
    print("   b. The extra value in 'x' was dropped")

    print("")

    # Use the copy constructor
    print("We can easily make a copy of an Interpolation object")
    j = Interpolation(i)
    print("j = Interpolation(i)")
    print(j)

    print("")

    # Get the number of interpolation points stored
    print_me("Number or interpolation points in 'j'", len(j))  # 6

    print("")

    j = Interpolation([0.0, 1.0, 3.0], [-1.0, -2.0, 2.0])
    print("j = Interpolation([0.0, 1.0, 3.0], [-1.0, -2.0, 2.0])")
    print(j)
    print_me("j(2)", j(2))
    print_me("j(0.5)", j(0.5))
    # Test with a value already in the data table
    print_me("j(1)", j(1))

    print("")

    # We can interpolate Angles too
    k = Interpolation(
        [27.0, 27.5, 28.0, 28.5, 29.0],
        [
            Angle(0, 54, 36.125),
            Angle(0, 54, 24.606),
            Angle(0, 54, 15.486),
            Angle(0, 54, 8.694),
            Angle(0, 54, 4.133),
        ],
    )
    print(
        "k = Interpolation([27.0, 27.5, 28.0, 28.5, 29.0],\n\
                      [Angle(0, 54, 36.125), Angle(0, 54, 24.606),\n\
                       Angle(0, 54, 15.486), Angle(0, 54, 8.694),\n\
                       Angle(0, 54, 4.133)])"
    )

    print_me("k(28.278)", Angle(k(28.278)).dms_str())

    print("")

    m = Interpolation([-1.0, 0.0, 1.0], [-2.0, 3.0, 2.0])
    print("m = Interpolation([-1.0, 0.0, 1.0], [-2.0, 3.0, 2.0])")
    print(m)
    # Get interpolated values
    print_me("m(-0.5)", m(-0.5))
    print_me("m(0.5)", m(0.5))
    # Get derivatives
    print_me("m'(-1.0)", m.derivative(-1.0))
    print_me("m'(-0.5)", m.derivative(-0.5))
    print_me("m'(0.0)", m.derivative(0.0))
    print_me("m'(0.5)", m.derivative(0.5))
    print_me("m'(1.0)", m.derivative(1.0))
    # Get the root within the interval
    print_me("m.root()", m.root())
    # Get the extremum within the interval
    print_me("m.minmax()", m.minmax())

    m = Interpolation(
        [29.43, 30.97, 27.69, 28.11, 31.58, 33.05],
        [
            0.4913598528,
            0.5145891926,
            0.4646875083,
            0.4711658342,
            0.5236885653,
            0.5453707057,
        ],
    )

    print_me("sin(29.5)\t", m(29.5))
    print_me("sin(30.0)\t", m(30.0))
    print_me("sin(30.5)\t", m(30.5))
    # Derivative must be adjusted because degrees were used instead of radians
    print_me("sin'(29.5)\t", degrees(m.derivative(29.5)))
    print_me("sin'(30.0)\t", degrees(m.derivative(30.0)))
    print_me("sqrt(3.0)/2.0\t", sqrt(3.0) / 2.0)
    print_me("sin'(30.5)\t", degrees(m.derivative(30.5)))


if __name__ == "__main__":

    main()
