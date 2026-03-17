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


from math import floor


"""
.. module:: base
   :synopsis: Basic routines and constants used by the pymeeus module
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Dagoberto Salazar
"""


TOL = 1e-10
"""Internal tolerance being used by default"""


def machine_accuracy():
    """This function computes the accuracy of the computer being used.

    This function returns a tuple containing the number of significant bits in
    the mantissa of a floating number, and the number of significant digits in
    a decimal number.

    :returns: Number of significant bits, and of significant digits
    :rtype: tuple
    """

    j = 0.0
    x = 2.0
    while x + 1.0 != x:
        j += 1.0
        x *= 2.0
    return (j, int(j * 0.30103))


def get_ordinal_suffix(ordinal):
    """Method to get the suffix of a given ordinal number, like 1'st',
    2'nd', 15'th', etc.

    :param ordinal: Ordinal number
    :type ordinal: int

    :returns: Suffix corresponding to input ordinal number
    :rtype: str
    :raises: TypeError if input type is invalid.

    >>> get_ordinal_suffix(40)
    'th'
    >>> get_ordinal_suffix(101)
    'st'
    >>> get_ordinal_suffix(2)
    'nd'
    >>> get_ordinal_suffix(19)
    'th'
    >>> get_ordinal_suffix(23)
    'rd'
    """

    if not isinstance(ordinal, (int, float)):
        raise TypeError("Invalid input type")
    else:
        ordinal = int(floor(ordinal))
        unit = ordinal % 10
        if unit == 1 and ordinal != 11:
            return "st"
        elif unit == 2 and ordinal != 12:
            return "nd"
        elif unit == 3 and ordinal != 13:
            return "rd"
        else:
            return "th"


def iint(number):
    """This method behaves in the same way as the **INT()** function described
    by Meeus in his book: Greatest integer which is not greater than number.

    :param number: Number or expresion
    :type number: int, float

    :returns: Greatest integer which is not greater than number
    :rtype: int
    :raises: TypeError if input type is invalid.

    >>> iint(19)
    19
    >>> iint(19.95)
    19
    >>> iint(-2.4)
    -3
    """

    if not isinstance(number, (int, float)):
        raise TypeError("Invalid input type")
    else:
        return int(floor(number))


def main():

    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Let's print the tolerance
    print_me("The default value for the tolerance is", TOL)

    # Find the accuracy of this computer
    j, d = machine_accuracy()
    print_me("Number of significant BITS in the mantissa\t", j)
    print_me("Number of significant DIGITS in a decimal number", d)

    print("")

    print_me("The suffix for ordinal 2 is", get_ordinal_suffix(2))
    print_me("The suffix for ordinal 11 is", get_ordinal_suffix(11))
    print_me("The suffix for ordinal 12 is", get_ordinal_suffix(12))
    print_me("The suffix for ordinal 13 is", get_ordinal_suffix(13))
    print_me("The suffix for ordinal 14 is", get_ordinal_suffix(14))
    print_me("The suffix for ordinal 16 is", get_ordinal_suffix(16))
    print_me("The suffix for ordinal 23 is", get_ordinal_suffix(23))


if __name__ == "__main__":

    main()
