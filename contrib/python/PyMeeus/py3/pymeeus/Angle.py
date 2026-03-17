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


from math import pi, degrees, radians

from pymeeus.base import TOL


"""
.. module:: Angle
   :synopsis: Class to handle angles
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Dagoberto Salazar
"""


class Angle(object):
    """
    Class Angle deals with angles in either decimal format (d.dd) or in
    sexagesimal format (d m' s'').

    It provides methods to handle an Angle object like it were a simple float,
    but adding the functionality associated with an angle.

    The constructor takes decimals and sexagesimal input. The sexagesimal
    angles can be given as separate degree, minutes, seconds values, or as
    tuples or lists. It is also possible to provide another Angle object as
    input.

    Also, if **radians=True** is passed to the constructor, then the input
    value is considered as in radians, and converted to degrees.
    """

    def __init__(self, *args, **kwargs):
        """Angle constructor.

        It takes decimals and sexagesimal input. The sexagesimal angles can be
        given as separate degree, minutes, seconds values, or as tuples or
        lists. It is also possible to provide another Angle object as input.

        If **radians=True** is passed, then the input value is converted from
        radians to degrees.

        If **ra=True** is passed, then the input value is converted from Right
        Ascension to degrees

        :param args: Input angle, in decimal or sexagesimal format, or Angle
        :type args: int, float, list, tuple, :py:class:`Angle`
        :param radians: If True, input angle is in radians. False by default.
        :type radians: bool
        :param ra: If True, input angle is in Right Ascension. False by default
        :type ra: bool

        :returns: Angle object.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(-13, 30, 0.0)
        >>> print(a)
        -13.5
        >>> b = Angle(a)
        >>> print(b)
        -13.5
        """

        self._deg = 0.0  # Angle value is stored here in decimal format
        self._tol = TOL
        self.set(*args, **kwargs)  # Let's use 'set()' method to set angle

    @staticmethod
    def reduce_deg(deg):
        """Takes a degree value in decimal format and converts it to a float
        value in the +/-[0:360) range.

        :param deg: Input degree angle in decimal format.
        :type deg: int, float, :py:class:`Angle`

        :returns: Float value of the angle in the +/-[0:360) range.
        :rtype: float

        >>> a = 386.3
        >>> b = Angle.reduce_deg(a)
        >>> print(round(b, 1))
        26.3
        """

        if abs(deg) >= 360.0:
            # Extract the sign
            sign = 1.0 if deg >= 0 else -1.0
            frac = abs(deg) % 1  # Separate the fractional part
            deg = int(abs(deg)) % 360  # Reduce to [0:360) range
            deg = sign * (deg + frac)  # Rebuild the value
        return float(deg)

    @staticmethod
    def reduce_dms(degrees, minutes, seconds=0.0):
        """Takes a degree value in sexagesimal format and converts it to a
        value in the +/-[0:360) range (degrees) and [0:60) range (minutes and
        seconds). It also takes care of fractional degrees and minutes.

        :param degrees: Degrees.
        :type degrees: int, float
        :param minutes: Minutes.
        :type minutes: int, float
        :param seconds: Seconds. 0.0 by default.
        :type seconds: int, float

        :returns: Angle in sexagesimal format, with ranges properly adjusted.
        :rtype: tuple

        >>> print(Angle.reduce_dms(-743.0, 26.0, 49.6))
        (23, 26, 49.6, -1.0)
        """

        # If any of the input values is negative, the sign is negative
        sign = -1.0 if (degrees < 0) or (minutes < 0) or (seconds < 0) else 1.0
        degrees = abs(degrees)
        minutes = abs(minutes)
        seconds = abs(seconds)
        # We need to work first from degrees to seconds
        if degrees % 1 > 0.0:
            # The degrees value has decimals, push them to minutes
            minutes += (degrees % 1) * 60.0
        degrees = int(degrees)  # Keep the integer part
        if minutes % 1 > 0.0:
            # The minutes value has decimals, push them to seconds
            seconds += (minutes % 1) * 60.0
        minutes = int(minutes)
        # We now need to work from seconds to degrees, because of overflow
        if seconds >= 60.0:
            minutes += int(seconds / 60.0)  # Push the excess to minutes
            seconds = seconds % 60  # Keep the rest
        if minutes >= 60.0:
            degrees += int(minutes / 60.0)  # Push the excess to degrees
            minutes = minutes % 60  # Keep the rest
        degrees = degrees % 360  # Keep degrees in [0:360) range
        return (degrees, minutes, seconds, sign)

    @staticmethod
    def deg2dms(deg):
        """Converts input from decimal to sexagesimal angle format.

        :param deg: Degrees decimal format.
        :type deg: int, float

        :returns: Angle in sexagesimal format, with ranges adjusted.
        :rtype: tuple

        .. note:: The output format is (Degrees, Minutes, Seconds, sign)

        >>> print(Angle.deg2dms(23.44694444))
        (23, 26, 48.999983999997596, 1.0)
        """

        deg = Angle.reduce_deg(deg)  # Reduce the degrees to the [0:360) range
        # Extract the sign
        sign = 1.0 if deg >= 0 else -1.0
        # We have the sign, now let's work with positive numbers
        deg = abs(deg)
        mi = (deg % 1) * 60.0  # Get the minutes, with decimals
        de = int(deg)  # Get the integer part of the degrees
        se = (mi % 1) * 60.0  # Get the seconds
        mi = int(mi)
        return (de, mi, se, sign)

    @staticmethod
    def dms2deg(degrees, minutes, seconds=0.0):
        """Converts an angle from sexagesimal to decimal format.

        :param degrees: Degrees.
        :type degrees: int, float
        :param minutes: Minutes.
        :type minutes: int, float
        :param seconds: Seconds. 0.0 by default.
        :type seconds: int, float

        :returns: Angle in decimal format, within +/-[0:360) range.
        :rtype: float

        >>> print(Angle.dms2deg(-23, 26, 48.999983999997596))
        -23.44694444
        """

        (de, mi, se, sign) = Angle.reduce_dms(degrees, minutes, seconds)
        deg = sign * (de + mi / 60.0 + se / 3600.0)
        return float(deg)

    def get_tolerance(self):
        """Gets the internal tolerance value used to compare Angles.

        .. note:: The default tolerance value is **base.TOL**.

        :returns: Internal tolerance.
        :rtype: float
        """

        return self._tol

    def set_tolerance(self, tol):
        """Changes the internal tolerance value used to compare Angles.

        :param tol: New tolerance value.
        :type tol: int, float

        :returns: None
        :rtype: None
        """

        self._tol = tol
        return

    def __call__(self):
        """Method used when object is called only with parenthesis.

        :returns: The internal value of the Angle object.
        :rtype: int, float

        >>> a = Angle(54.6)
        >>> print(a())
        54.6
        """

        return self._deg

    def __str__(self):
        """Method used when trying to print the object.

        :returns: Angle as string.
        :rtype: string

        >>> a = Angle(12.5)
        >>> print(a)
        12.5
        """

        return str(self._deg)

    def __repr__(self):
        """Method providing the 'official' string representation of the object.
        It provides a valid expression that could be used to recreate the
        object.

        :returns: As string with a valid expression to recreate the object
        :rtype: string

        >>> a = Angle(12.5)
        >>> repr(a)
        'Angle(12.5)'
        """

        return "{}({})".format(self.__class__.__name__, self._deg)

    def set(self, *args, **kwargs):
        """Method used to define the value of the Angle object.

        It takes decimals and sexagesimal input. The sexagesimal angles can be
        given as separate degree, minutes, seconds values, or as tuples or
        lists. It is also possible to provide another Angle object as input.

        If **radians=True** is passed, then the input value is converted from
        radians to degrees

        If **ra=True** is passed, then the input value is converted from Right
        Ascension to degrees

        :param args: Input angle, in decimal or sexagesimal format, or Angle
        :type args: int, float, list, tuple, :py:class:`Angle`
        :param radians: If True, input angle is in radians. False by default.
        :type radians: bool
        :param ra: If True, input angle is in Right Ascension. False by default
        :type ra: bool

        :returns: None.
        :rtype: None
        :raises: TypeError if input values are of wrong type.
        """

        if "ra" in kwargs:
            if kwargs["ra"]:
                # Input values are a Right Ascension
                self.set_ra(*args)
                return
        # If no arguments are given, internal angle is set to zero
        if len(args) == 0:
            self._deg = 0.0
            return
        # If we have only one argument, it can be a single value, a tuple/list
        # or an Angle
        elif len(args) == 1:
            deg = args[0]
            if isinstance(deg, Angle):  # Copy constructor
                self._deg = deg._deg
                self._tol = deg._tol
                return
            if isinstance(deg, (int, float)):
                if "radians" in kwargs:
                    if kwargs["radians"]:
                        # Input value is in radians. Convert to degrees
                        deg = degrees(deg)
                # This works for ints, floats and Angles
                self._deg = Angle.reduce_deg(deg)
                return
            elif isinstance(deg, (list, tuple)):
                if len(deg) == 0:
                    raise TypeError("Invalid input value")
                elif len(deg) == 1:
                    # This is a single value
                    if "radians" in kwargs:
                        if kwargs["radians"]:
                            # Input value is in radians. Convert to degrees
                            deg[0] = degrees(deg[0])
                    self._deg = Angle.reduce_deg(deg[0])
                    return
                elif len(deg) == 2:
                    # Seconds value is set to zero
                    self._deg = Angle.dms2deg(deg[0], deg[1])
                    return
                elif len(deg) == 3:
                    # The first three values are taken into account
                    self._deg = Angle.dms2deg(deg[0], deg[1], deg[2])
                    return
                else:
                    # Only the first four values are taken into account
                    sign = (
                        -1.0
                        if deg[0] < 0 or deg[1] < 0 or deg[2] < 0 or deg[3] < 0
                        else 1.0
                    )
                    # If sign < 0, make all values negative, to be sure
                    deg0 = sign * abs(deg[0])
                    deg1 = sign * abs(deg[1])
                    deg2 = sign * abs(deg[2])
                    self._deg = Angle.dms2deg(deg0, deg1, deg2)
                    return
            else:
                raise TypeError("Invalid input value")
        elif len(args) == 2:
            # Seconds value is set to zero
            self._deg = Angle.dms2deg(args[0], args[1])
            return
        elif len(args) == 3:
            # The first three values are taken into account
            self._deg = Angle.dms2deg(args[0], args[1], args[2])
            return
        else:
            # Only the first four values are taken into account
            sign = (
                -1.0
                if args[0] < 0 or args[1] < 0 or args[2] < 0 or args[3] < 0
                else 1.0
            )
            # If sign < 0, make all values negative, to be sure
            args0 = sign * abs(args[0])
            args1 = sign * abs(args[1])
            args2 = sign * abs(args[2])
            self._deg = Angle.dms2deg(args0, args1, args2)
            return

    def set_radians(self, rads):
        """Method to define the value of the Angle object from radians.

        :param rads: Input angle, in radians.
        :type rads: int, float

        :returns: None.
        :rtype: None
        :raises: TypeError if input value is of wrong type.

        >>> a = Angle()
        >>> a.set_radians(pi)
        >>> print(a)
        180.0
        """

        self.set(rads, radians=True)
        return

    def set_ra(self, *args):
        """Define the value of the Angle object from a Right Ascension.

        It takes decimals and sexagesimal input. The sexagesimal Right
        Ascensions can be given as separate hours, minutes, seconds values, or
        as tuples or lists.

        :param args: Input Right Ascension, in decimal or sexagesimal format.
        :type args: int, float, list, tuple

        :returns: None.
        :rtype: None
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle()
        >>> a.set_ra(9, 14, 55.8)
        >>> print(a)
        138.7325
        """

        self.set(*args)  # Carry out a standard set(), without *kwargs
        self._deg *= 15.0  # Multipy Right Ascension by 15.0 to get degrees
        return

    def dms_str(self, fancy=True, n_dec=-1):
        """Returns the Angle value as a sexagesimal string.

        The parameter **fancy** allows to print in "Dd M' S''" format if True,
        and in "D:M:S" (easier to parse) if False. On the other hand, the
        **n_dec** parameter sets the number of decimals used to print the
        seconds. Set to a negative integer to disable (default).

        :param fancy: Format of output string. True by default.
        :type fancy: bool
        :param n_dec: Number of decimals used to print the seconds
        :type fancy: int

        :returns: Angle value as string in sexagesimal format.
        :rtype: string
        :raises: TypeError if input value is of wrong type.

        >>> a = Angle(42.75)
        >>> print(a.dms_str())
        42d 45' 0.0''
        >>> print(a.dms_str(fancy=False))
        42:45:0.0
        >>> a = Angle(49, 13, 42.4817)
        >>> print(a.dms_str(n_dec=2))
        49d 13' 42.48''
        """

        if not isinstance(n_dec, int):
            raise TypeError("Invalid input value")
        d, m, s, sign = Angle.deg2dms(self._deg)
        if n_dec >= 0:
            s = round(s, n_dec)
            if abs(s - 60.0) < TOL:
                s = 0.0
                m += 1
            if abs(m - 60.0) < TOL:
                m = 0
                d += 1.0
            if d >= 360.0:
                d -= 360.0
        if fancy:
            if d != 0:
                return "{}d {}' {}''".format(int(sign * d), m, s)
            elif m != 0:
                return "{}' {}''".format(int(sign * m), s)
            elif s != 0.0:
                return "{}''".format(sign * s)
            else:
                return "0d 0' 0.0''"
        else:
            if d != 0:
                return "{}:{}:{}".format(int(sign * d), m, s)
            elif m != 0:
                return "0:{}:{}".format(int(sign * m), s)
            elif s != 0.0:
                return "0:0:{}".format(sign * s)
            else:
                return "0:0:0.0"

    def get_ra(self):
        """Returns the Angle value as a Right Ascension in float format

        :returns: The internal value of the Angle object as Right Ascension.
        :rtype: int, float

        >>> a = Angle(138.75)
        >>> print(a.get_ra())
        9.25
        """

        return self._deg / 15.0

    def ra_str(self, fancy=True, n_dec=-1):
        """Returns the Angle value as a sexagesimal string in Right Ascension.

        The parameter **fancy** allows to print in "Hh M' S''" format if True,
        and in "H:M:S" (easier to parse) if False. On the other hand, the
        **n_dec** parameter sets the number of decimals used to print the
        seconds. Set to a negative integer to disable (default).

        :param fancy: Format of output string. True by default.
        :type fancy: bool
        :param n_dec: Number of decimals used to print the seconds
        :type fancy: int

        :returns: Angle value as Right Ascension in sexagesimal format.
        :rtype: string
        :raises: TypeError if input value is of wrong type.

        >>> a = Angle(138.75)
        >>> print(a.ra_str())
        9h 15' 0.0''
        >>> print(a.ra_str(fancy=False))
        9:15:0.0
        >>> a = Angle(2, 44, 11.98581, ra=True)
        >>> print(a.ra_str(n_dec=3))
        2h 44' 11.986''
        """

        a = Angle(self()) / 15.0
        s = a.dms_str(fancy, n_dec)
        if fancy:
            s = s.replace("d", "h")
        return s

    def rad(self):
        """Returns the Angle value in radians.

        :returns: Angle value in radians.
        :rtype: float

        >>> a = Angle(47.762)
        >>> print(round(a.rad(), 8))
        0.83360416
        """

        return radians(self._deg)

    def dms_tuple(self):
        """Returns the Angle as a tuple containing (degrees, minutes, seconds,
        sign).

        :returns: Angle value as (degrees, minutes, seconds, sign).
        :rtype: tuple
        """

        return Angle.deg2dms(self())

    def ra_tuple(self):
        """Returns the Angle in Right Ascension format as a tuple containing
        (hours, minutes, seconds, sign).

        :returns: Angle value as RA in (hours, minutes, seconds, sign) format.
        :rtype: tuple
        """

        return Angle.deg2dms(self() / 15.0)

    def to_positive(self):
        """Converts the internal angle value from negative to positive.

        :returns: This angle object.
        :rtype: :py:class:`Angle`

        >>> a = Angle(-87.32)
        >>> print(a.to_positive())
        272.68
        """

        if self._deg < 0:
            self._deg = 360.0 - abs(self._deg)
        return self

    def __eq__(self, b):
        """This method defines the 'is equal' operator between Angles.

        .. note:: For the comparison, the internal tolerance value is used.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(172.01)
        >>> b = Angle(172.009)
        >>> a == b
        False
        """

        if isinstance(b, (int, float)):
            return abs(self._deg - float(b)) < self._tol
        elif isinstance(b, Angle):
            return abs(self._deg - b._deg) < self._tol
        else:
            raise TypeError("Wrong operand type")

    def __ne__(self, b):
        """This method defines the 'is not equal' operator between Angles.

        .. note:: For the comparison, the internal tolerance value is used.

        :returns: A boolean.
        :rtype: bool

        >>> a = Angle(11.200001)
        >>> b = Angle(11.200000)
        >>> a != b
        True
        """

        return not self.__eq__(b)  # '!=' == 'not(==)'

    def __lt__(self, b):
        """This method defines the 'is less than' operator between Angles.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(72.0)
        >>> b = Angle(72.0)
        >>> a < b
        False
        """

        if isinstance(b, (int, float)):
            return self._deg < float(b)
        elif isinstance(b, Angle):
            return self._deg < b._deg
        else:
            raise TypeError("Wrong operand type")

    def __ge__(self, b):
        """This method defines 'is equal or greater' operator between Angles.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(172.01)
        >>> b = Angle(172.009)
        >>> a >= b
        True
        """

        return not self.__lt__(b)  # '>=' == 'not(<)'

    def __gt__(self, b):
        """This method defines the 'is greater than' operator between Angles.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(172.01)
        >>> b = Angle(172.009)
        >>> a > b
        True
        """

        if isinstance(b, (int, float)):
            return self._deg > float(b)
        elif isinstance(b, Angle):
            return self._deg > b._deg
        else:
            raise TypeError("Wrong operand type")

    def __le__(self, b):
        """This method defines 'is equal or less' operator between Angles.

        :returns: A boolean.
        :rtype: bool
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(72.0)
        >>> b = Angle(72.0)
        >>> a <= b
        True
        """

        return not self.__gt__(b)  # '<=' == 'not(>)'

    def __neg__(self):
        """This method is used to obtain the negative version of this Angle.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`

        >>> a = Angle(-11.2)
        >>> print(-a)
        11.2
        """

        return Angle(-self._deg)

    def __abs__(self):
        """This method is used to obtain the absolute value of this Angle.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`

        >>> a = Angle(-303.67)
        >>> print(abs(a))
        303.67
        """

        return Angle(abs(self._deg))

    def __mod__(self, b):
        """This method is used to obtain the module b of this Angle.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(333.0)
        >>> b = Angle(72.0)
        >>> print(a % b)
        45.0
        """

        # Negative values will be treated as if they were positive
        sign = 1.0 if self._deg >= 0.0 else -1.0
        if isinstance(b, (int, float)):
            return Angle(sign * (abs(self._deg) % b))
        elif isinstance(b, Angle):
            return Angle(sign * (abs(self._deg) % b._deg))
        else:
            raise TypeError("Wrong operand type")

    def __add__(self, b):
        """This method defines the addition between Angles.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(83.1)
        >>> b = Angle(18.4)
        >>> print(a + b)
        101.5
        """

        if isinstance(b, (int, float)):
            return Angle(self._deg + float(b))
        elif isinstance(b, Angle):
            return Angle(self._deg + b._deg)
        else:
            raise TypeError("Wrong operand type")

    def __sub__(self, b):
        """This method defines the subtraction between Angles.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(25.4)
        >>> b = Angle(10.2)
        >>> print(a - b)
        15.2
        """

        return self.__add__(-b)

    def __mul__(self, b):
        """This method defines the multiplication between Angles.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(33.0)
        >>> b = Angle(72.0)
        >>> print(a * b)
        216.0
        """

        if isinstance(b, (int, float)):
            return Angle(self._deg * float(b))
        elif isinstance(b, Angle):
            return Angle(self._deg * b._deg)
        else:
            raise TypeError("Wrong operand type")

    def __div__(self, b):
        """This method defines the division between Angles.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: ZeroDivisionError if divisor is zero.
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(172.0)
        >>> b = Angle(86.0)
        >>> print(a/b)
        2.0
        """

        if b == 0.0:
            raise ZeroDivisionError("Division by zero is not allowed")
        if isinstance(b, (int, float)):
            return Angle(self._deg / float(b))
        elif isinstance(b, Angle):
            return Angle(self._deg / b._deg)
        else:
            raise TypeError("Wrong operand type")

    def __truediv__(self, b):
        """This method defines the division between Angles (Python 3).

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: ZeroDivisionError if divisor is zero.
        :raises: TypeError if input values are of wrong type.
        :see: __div__
        """

        return self.__div__(b)

    def __pow__(self, b):
        """This method defines the power operation for Angles.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(12.5)
        >>> b = Angle(4.0)
        >>> print(a ** b)
        294.0625
        """

        if isinstance(b, (int, float)):
            return Angle(self._deg ** b)
        elif isinstance(b, Angle):
            return Angle(self._deg ** b._deg)
        else:
            raise TypeError("Wrong operand type")

    def __imod__(self, b):
        """This method defines the accumulative module b of this Angle.

        :returns: This Angle.
        :rtype: :py:class:`Angle`

        >>> a = Angle(330.0)
        >>> b = Angle(45.0)
        >>> a %= b
        >>> print(a)
        15.0
        """

        # Negative values will be treated as if they were positive
        self = self % b
        return self

    def __iadd__(self, b):
        """This method defines the accumulative addition to this Angle.

        :returns: This Angle.
        :rtype: :py:class:`Angle`

        >>> a = Angle(172.1)
        >>> b = Angle(54.6)
        >>> a += b
        >>> print(a)
        226.7
        """

        self = self + b
        return self

    def __isub__(self, b):
        """This method defines the accumulative subtraction to this Angle.

        :returns: This Angle.
        :rtype: :py:class:`Angle`

        >>> a = Angle(97.0)
        >>> b = Angle(39.0)
        >>> a -= b
        >>> print(a)
        58.0
        """

        self = self - b
        return self

    def __imul__(self, b):
        """This method defines the accumulative multiplication to this Angle.

        :returns: This Angle.
        :rtype: :py:class:`Angle`

        >>> a = Angle(30.0)
        >>> b = Angle(55.0)
        >>> a *= b
        >>> print(a)
        210.0
        """

        self = self * b
        return self

    def __idiv__(self, b):
        """This method defines the accumulative division to this Angle.

        :returns: This Angle.
        :rtype: :py:class:`Angle`
        :raises: ZeroDivisionError if divisor is zero.
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(330.0)
        >>> b = Angle(30.0)
        >>> a /= b
        >>> print(a)
        11.0
        """

        if b == 0.0:
            raise ZeroDivisionError("Division by zero is not allowed")
        if not isinstance(b, (int, float, Angle)):
            raise TypeError("Wrong operand type")
        self = self / b
        return self

    def __itruediv__(self, b):
        """This method defines accumulative division to this Angle (Python3).

        :returns: This Angle.
        :rtype: :py:class:`Angle`
        :raises: ZeroDivisionError if divisor is zero.
        :raises: TypeError if input values are of wrong type.
        :see: __idiv__
        """

        return self.__idiv__(b)

    def __ipow__(self, b):
        """This method defines the accumulative power to this Angle.

        :returns: This Angle.
        :rtype: :py:class:`Angle`

        >>> a = Angle(37.0)
        >>> b = Angle(3.0)
        >>> a **= b
        >>> print(a)
        253.0
        """

        self = self ** b
        return self

    def __rmod__(self, b):
        """This method defines module operation between Angles by the right.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`

        >>> a = Angle(80.0)
        >>> print(350 % a)
        30.0
        """

        if isinstance(b, (int, float)):
            b = Angle(b)
        # Negative values will be treated as if they were positive
        sign = 1.0 if b._deg >= 0.0 else -1.0
        return Angle(sign * (abs(b._deg) % self._deg))

    def __radd__(self, b):
        """This method defines the addition between Angles by the right

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`

        >>> a = Angle(83.1)
        >>> print(8.5 + a)
        91.6
        """

        return self.__add__(b)  # In this case, it is the same as by the left

    def __rsub__(self, b):
        """This method defines the subtraction between Angles by the right.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(25.0)
        >>> print(24.0 - a)
        -1.0
        """

        return -self.__sub__(b)  # b - a = -(a - b)

    def __rmul__(self, b):
        """This method defines multiplication between Angles by the right.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(11.0)
        >>> print(250.0 * a)
        230.0
        """

        return self.__mul__(b)  # In this case, it is the same as by the left

    def __rdiv__(self, b):
        """This method defines division between Angles by the right.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: ZeroDivisionError if divisor is zero.
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(80.0)
        >>> print(350 / a)
        4.375
        """

        if self == 0.0:
            raise ZeroDivisionError("Division by zero is not allowed")
        if isinstance(b, (int, float)):
            return Angle(float(b) / self._deg)
        elif isinstance(b, Angle):
            return Angle(b._deg / self._deg)
        else:
            raise TypeError("Wrong operand type")

    def __rtruediv__(self, b):
        """This method defines division between Angle by the right (Python3).

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: ZeroDivisionError if divisor is zero.
        :raises: TypeError if input values are of wrong type.
        :see: __rdiv__
        """

        return self.__rdiv__(b)

    def __rpow__(self, b):
        """This method defines the power operation for Angles by the right.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input values are of wrong type.

        >>> a = Angle(5.0)
        >>> print(24.0 ** a)
        144.0
        """

        if isinstance(b, (int, float)):
            return Angle(b ** self._deg)
        elif isinstance(b, Angle):
            return Angle(b._deg ** self._deg)
        else:
            raise TypeError("Wrong operand type")

    def __float__(self):
        """This method returns Angle value as a float.

        :returns: Internal angle value as a float.
        :rtype: float

        >>> a = Angle(213.8)
        >>> float(a)
        213.8
        """

        return float(self._deg)

    def __int__(self):
        """This method returns Angle value as an int.

        :returns: Internal angle value as an int.
        :rtype: int

        >>> a = Angle(213.8)
        >>> int(a)
        213
        """

        return int(self._deg)

    def __round__(self, n=0):
        """This method returns an Angle with content rounded to 'n' decimal.

        :returns: A new Angle object.
        :rtype: :py:class:`Angle`

        >>> a = Angle(11.4361)
        >>> print(round(a, 2))
        11.44
        """

        # NOTE: This method is only called in Python 3
        return Angle(round(self._deg, n))


def main():

    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Let's show some uses of Angle class
    print("\n" + 35 * "*")
    print("*** Use of Angle class")
    print(35 * "*" + "\n")

    # Create an Angle object, providing degrees, minutes and seconds
    a = Angle(-23.0, 26.0, 48.999983999)
    print("a = Angle(-23.0, 26.0, 48.999983999)")

    # First we print using the __call__ method (note the extra parentheses)
    print_me("The angle 'a()' is", a())  # -23.44694444

    # Second we print using the __str__ method (no extra parentheses needed)
    print_me("The angle 'a' is", a)  # -23.44694444

    print("")

    # Use the copy constructor
    b = Angle(a)
    print_me("Angle 'b', which is a copy of 'a', is", b)

    print("")

    # Use the static 'deg2dms()' method to carry out conversions
    d, m, s, sign = Angle.deg2dms(23.44694444)
    val = "{}d {}' {}''".format(int(sign * d), m, s)
    print_me("{Deg}d {Min}' {Sec}''", val)  # 23d 26' 48.999984''

    # We can print Angle 'a' directly in sexagesimal format
    # In 'fancy' format:                                # -23d 26' 48.999984''
    print_me("{Deg}d {Min}' {Sec}''", a.dms_str(n_dec=6))
    # In plain format:
    print_me("{Deg}:{Min}:{Sec}", a.dms_str(False, 6))  # -23:26:48.999983999

    print("")

    # Print directly as a tuple
    a = Angle(23.44694444)
    print_me("a.dms_tuple()", a.dms_tuple())
    print_me("a.ra_tuple()", a.ra_tuple())

    print("")

    # Redefine Angle 'a' several times
    a.set(-0.44694444)
    print("a.set(-0.44694444)")
    print_me("   a.dms_str()", a.dms_str())  # -26' 48.999984''
    a.set(0, 0, -46.31)
    print("a.set(0, 0, -46.31)")
    print_me("   a.dms_str(False)", a.dms_str(False))  # 0:0:-46.31

    print("")

    # We can use decimals in degrees/minutes. They are converted automatically
    a.set(0, -46.25, 0.0)
    print("a.set(0, -46.25, 0.0)")
    print_me("   a.dms_str()", a.dms_str())  # -46' 15.0''
    a.set(0, 0, 0.0)
    print("a.set(0, 0, 0.0)")
    print_me("   a.dms_str()", a.dms_str())  # 0d 0' 0.0''

    print("")

    # We can define the angle as in radians. It will be converted to degrees
    b = Angle(pi, radians=True)
    print_me("b = Angle(pi, radians=True); print(b)", b)  # 180.0

    # And we can easily carry out the 'degrees to radians' conversion
    print_me("print(b.rad())", b.rad())  # 3.14159265359

    print("")

    # We can also specify the angle as a Right Ascension
    print("Angle can be given as a Right Ascension: Hours, Minutes, Seconds")
    a.set_ra(9, 14, 55.8)
    print("a.set_ra(9, 14, 55.8)")
    print_me("   print(a)", a)
    b = Angle(9, 14, 55.8, ra=True)
    print("b = Angle(9, 14, 55.8, ra=True)")
    print_me("   print(b)", b)

    print("")

    # We can print the Angle as Right Ascension, as a float and as string
    a = Angle(138.75)
    print("a = Angle(138.75)")
    print_me("   print(a.get_ra())", a.get_ra())
    print_me("   print(a.ra_str())", a.ra_str())
    print_me("   print(a.ra_str(False))", a.ra_str(False))

    print("")

    # Use the 'to_positive()' method to get the positive version of an angle
    a = Angle(-87.32)  # 272.68
    print("a = Angle(-87.32)")
    print_me("   print(a.to_positive())", a.to_positive())

    print("")

    # Call the __repr__() method to get a string defining the current object
    # This string can then be fed to 'eval()' function to generate the object
    print_me("print(b.__repr__())", b.__repr__())  # Angle(138.7325)

    c = b

    print("")

    print_me("c", c)  # 138.7325

    # Negate an angle
    d = Angle(13, 30)
    print_me("d", d)  # 13.5
    e = -d
    print_me("   e = -d", e)  # -13.5

    # Get the absolute value of an angle
    e = abs(e)
    print_me("   e = abs(e)", e)  # 13.5

    # Module operation on an angle
    d = Angle(17.0)
    print_me("d", d)  # 17.0
    e = c % d
    print_me("   e = c % d", e)  # 10.0

    print("")

    # Convert the angle to an integer
    d = Angle(13.95)
    print_me("d", d)  # 13.95
    print_me("   int(d)", int(d))  # 13.0
    d = Angle(-4.95)
    print_me("d", d)  # -4.95
    print_me("   int(d)", int(d))  # -4.0

    # Convert the angle to a float
    print_me("   float(d)", float(d))  # -4.95

    # Round the angle to a float
    e = Angle(-4.951648)
    print_me("e", e)  # -4.951648
    print_me("   round(e)", round(e))  # -5.0
    print_me("   round(e, 2)", round(e, 2))  # -4.95
    print_me("   round(e, 3)", round(e, 3))  # -4.952
    print_me("   round(e, 4)", round(e, 4))  # -4.9516

    print("")

    # Comparison operators
    print_me("   d == e", d == e)  # False
    print_me("   d != e", d != e)  # True
    print_me("   d > e", d > e)  # True
    print_me("   c >= 180.0", c >= 180.0)  # False
    print_me("   c < 180.0", c < 180.0)  # True
    print_me("   c <= 180.0", c <= 180.0)  # True

    print("")

    # It is very easy to add Angles to obtain a new Angle
    e = c + d
    print_me("   c + d", e)  # 133.7825

    # We can also directly add a decimal angle
    e = c + 11.5
    print_me("   c + 11.5", e)  # 150.2325

    print("")

    # Types allowed are int, float and Angle
    print('e = c + "32.5"')
    try:
        e = c + "32.5"
    except TypeError:
        print("TypeError!: Valid types are int, float, and Angle, not string!")

    print("")

    # Subtraction
    e = c - d
    print_me("   c - d", e)  # 143.6825

    # Multiplication
    c.set(150.0)
    d.set(5.0)
    print_me("c", c)  # 150.0
    print_me("d", d)  # 5.0
    e = c * d
    print_me("   c * d", e)  # 30.0

    # Division
    c.set(150.0)
    d.set(6.0)
    print_me("d", d)  # 6.0
    e = c / d
    print_me("   c / d", e)  # 25.0

    print("")

    # Division by zero is not allowed
    d.set(0.0)
    print_me("d", d)  # 0.0
    print("e = c / d")
    try:
        e = c / d
    except ZeroDivisionError:
        print("ZeroDivisionError!: Division by zero is not allowed!")

    print("")

    # Power
    d.set(2.2)
    print_me("d", d)  # 2.2
    e = c ** d
    print_me("   c ** d", e)  # 91.57336709992524

    print("")

    # Accumulative module operation
    d.set(17.0)
    print_me("d", d)  # 17.0
    e %= d
    print_me("   e %= d", e)  # 6.573367099925235

    # Accumulative addition
    c += d
    print_me("   c += d", c)  # 167.0

    # Accumulative subtraction
    print_me("b", b)  # 138.7325
    c -= b
    print_me("   c -= b", c)  # 28.2675

    # Accumulative multiplication
    print_me("b", b)  # 138.7325
    c *= b
    print_me("   c *= b", c)  # 321.62094375

    # Accumulative division
    print_me("b", b)  # 138.7325
    d.set(6.0)
    print_me("d", d)  # 6.0
    b /= d
    print_me("   b /= d", b)  # 23.1220833333

    # Accumulative power
    d.set(2.2)
    print_me("d", d)  # 2.2
    c = abs(c)
    print_me("   c = abs(c)", c)  # 321.62094375
    c **= d
    print_me("   c **= d", c)  # 254.307104203

    print("")

    # The same operation, but by the right side
    e = 3.5 + b
    print_me("   e = 3.5 + b", e)  # 26.6220833333
    e = 3.5 - b
    print_me("   e = 3.5 - b", e)  # -19.6220833333
    e = 3.5 * b
    print_me("   e = 3.5 * b", e)  # 80.9272916667
    e = 3.5 / b
    print_me("   e = 3.5 / b", e)  # 0.151370443119
    e = 3.5 ** b
    print_me("   e = 3.5 ** b", e)  # 260.783691406


if __name__ == "__main__":

    main()
