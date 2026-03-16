
# -*- coding: utf-8 -*-


# PyMeeus: Python module implementing astronomical algorithms.
# Copyright (C) 2021  Dagoberto Salazar
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


from math import sin, cos, asin, atan2, tan, sqrt
from pymeeus.Angle import Angle
from pymeeus.Epoch import Epoch, JDE2000
from pymeeus.Sun import Sun
from pymeeus.Coordinates import (
    nutation_longitude, true_obliquity, ecliptical2equatorial
)

"""
.. module:: Moon
   :synopsis: Class to model the Moon
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Dagoberto Salazar
"""


PERIODIC_TERMS_LR_TABLE = [
    [0,  0,  1,  0, 6288774.0, -20905355.0],
    [2,  0, -1,  0, 1274027.0,  -3699111.0],
    [2,  0,  0,  0,  658314.0,  -2955968.0],
    [0,  0,  2,  0,  213618.0,   -569925.0],
    [0,  1,  0,  0, -185116.0,     48888.0],
    [0,  0,  0,  2, -114332.0,     -3149.0],
    [2,  0, -2,  0,   58793.0,    246158.0],
    [2, -1, -1,  0,   57066.0,   -152138.0],
    [2,  0,  1,  0,   53322.0,   -170733.0],
    [2, -1,  0,  0,   45758.0,   -204586.0],
    [0,  1, -1,  0,  -40923.0,   -129620.0],
    [1,  0,  0,  0,  -34720.0,    108743.0],
    [0,  1,  1,  0,  -30383.0,    104755.0],
    [2,  0,  0, -2,   15327.0,     10321.0],
    [0,  0,  1,  2,  -12528.0,         0.0],
    [0,  0,  1, -2,   10980.0,     79661.0],
    [4,  0, -1,  0,   10675.0,    -34782.0],
    [0,  0,  3,  0,   10034.0,    -23210.0],
    [4,  0, -2,  0,    8548.0,    -21636.0],
    [2,  1, -1,  0,   -7888.0,     24208.0],
    [2,  1,  0,  0,   -6766.0,     30824.0],
    [1,  0, -1,  0,   -5163.0,     -8379.0],
    [1,  1,  0,  0,    4987.0,    -16675.0],
    [2, -1,  1,  0,    4036.0,    -12831.0],
    [2,  0,  2,  0,    3994.0,    -10445.0],
    [4,  0,  0,  0,    3861.0,    -11650.0],
    [2,  0, -3,  0,    3665.0,     14403.0],
    [0,  1, -2,  0,   -2689.0,     -7003.0],
    [2,  0, -1,  2,   -2602.0,         0.0],
    [2, -1, -2,  0,    2390.0,     10056.0],
    [1,  0,  1,  0,   -2348.0,      6322.0],
    [2, -2,  0,  0,    2236.0,     -9884.0],
    [0,  1,  2,  0,   -2120.0,      5751.0],
    [0,  2,  0,  0,   -2069.0,         0.0],
    [2, -2, -1,  0,    2048.0,     -4950.0],
    [2,  0,  1, -2,   -1773.0,      4130.0],
    [2,  0,  0,  2,   -1595.0,         0.0],
    [4, -1, -1,  0,    1215.0,     -3958.0],
    [0,  0,  2,  2,   -1110.0,         0.0],
    [3,  0, -1,  0,    -892.0,      3258.0],
    [2,  1,  1,  0,    -810.0,      2616.0],
    [4, -1, -2,  0,     759.0,     -1897.0],
    [0,  2, -1,  0,    -713.0,     -2117.0],
    [2,  2, -1,  0,    -700.0,      2354.0],
    [2,  1, -2,  0,     691.0,         0.0],
    [2, -1,  0, -2,     596.0,         0.0],
    [4,  0,  1,  0,     549.0,     -1423.0],
    [0,  0,  4,  0,     537.0,     -1117.0],
    [4, -1,  0,  0,     520.0,     -1571.0],
    [1,  0, -2,  0,    -487.0,     -1739.0],
    [2,  1,  0, -2,    -399.0,         0.0],
    [0,  0,  2, -2,    -381.0,     -4421.0],
    [1,  1,  1,  0,     351.0,         0.0],
    [3,  0, -2,  0,    -340.0,         0.0],
    [4,  0, -3,  0,     330.0,         0.0],
    [2, -1,  2,  0,     327.0,         0.0],
    [0,  2,  1,  0,    -323.0,      1165.0],
    [1,  1, -1,  0,     299.0,         0.0],
    [2,  0,  3,  0,     294.0,         0.0],
    [2,  0, -1, -2,       0.0,      8752.0]
]
"""This table contains the periodic terms for the longitude (Sigmal) and
distance (Sigmar) of the Moon. Units are 0.000001 degree for Sigmal, and 0.001
kilometer for Sigmar. In Meeus' book this is Table 47.A and can be found in
pages 339-340."""

PERIODIC_TERMS_B_TABLE = [
    [0,  0,  0,  1, 5128122.0],
    [0,  0,  1,  1,  280602.0],
    [0,  0,  1, -1,  277693.0],
    [2,  0,  0, -1,  173237.0],
    [2,  0, -1,  1,   55413.0],
    [2,  0, -1, -1,   46271.0],
    [2,  0,  0,  1,   32573.0],
    [0,  0,  2,  1,   17198.0],
    [2,  0,  1, -1,    9266.0],
    [0,  0,  2, -1,    8822.0],
    [2, -1,  0, -1,    8216.0],
    [2,  0, -2, -1,    4324.0],
    [2,  0,  1,  1,    4200.0],
    [2,  1,  0, -1,   -3359.0],
    [2, -1, -1,  1,    2463.0],
    [2, -1,  0,  1,    2211.0],
    [2, -1, -1, -1,    2065.0],
    [0,  1, -1, -1,   -1870.0],
    [4,  0, -1, -1,    1828.0],
    [0,  1,  0,  1,   -1794.0],
    [0,  0,  0,  3,   -1749.0],
    [0,  1, -1,  1,   -1565.0],
    [1,  0,  0,  1,   -1491.0],
    [0,  1,  1,  1,   -1475.0],
    [0,  1,  1, -1,   -1410.0],
    [0,  1,  0, -1,   -1344.0],
    [1,  0,  0, -1,   -1335.0],
    [0,  0,  3,  1,    1107.0],
    [4,  0,  0, -1,    1021.0],
    [4,  0, -1,  1,     833.0],
    [0,  0,  1, -3,     777.0],
    [4,  0, -2,  1,     671.0],
    [2,  0,  0, -3,     607.0],
    [2,  0,  2, -1,     596.0],
    [2, -1,  1, -1,     491.0],
    [2,  0, -2,  1,    -451.0],
    [0,  0,  3, -1,     439.0],
    [2,  0,  2,  1,     422.0],
    [2,  0, -3, -1,     421.0],
    [2,  1, -1,  1,    -366.0],
    [2,  1,  0,  1,    -351.0],
    [4,  0,  0,  1,     331.0],
    [2, -1,  1,  1,     315.0],
    [2, -2,  0, -1,     302.0],
    [0,  0,  1,  3,    -283.0],
    [2,  1,  1, -1,    -229.0],
    [1,  1,  0, -1,     223.0],
    [1,  1,  0,  1,     223.0],
    [0,  1, -2, -1,    -220.0],
    [2,  1, -1, -1,    -220.0],
    [1,  0,  1,  1,    -185.0],
    [2, -1, -2, -1,     181.0],
    [0,  1,  2,  1,    -177.0],
    [4,  0, -2, -1,     176.0],
    [4, -1, -1, -1,     166.0],
    [1,  0,  1, -1,    -164.0],
    [4,  0,  1, -1,     132.0],
    [1,  0, -1, -1,    -119.0],
    [4, -1,  0, -1,     115.0],
    [2, -2,  0,  1,     107.0],
]
"""This table contains the periodic terms for the latitude of the Moon Sigmab.
Units are 0.000001 degree. In Meeus' book this is Table 47.B and can be found
in page 341."""


class Moon(object):
    """
    Class Moon models Earth's satellite.
    """

    @staticmethod
    def geocentric_ecliptical_pos(epoch):
        """This method computes the geocentric ecliptical position (longitude,
        latitude) of the Moon for a given instant, referred to the mean equinox
        of the date, as well as the Moon-Earth distance in kilometers and the
        equatorial horizontal parallax.

        :param epoch: Instant to compute the Moon's position, as an
            py:class:`Epoch` object.
        :type epoch: :py:class:`Epoch`

        :returns: Tuple containing:

            * Geocentric longitude of the center of the Moon, as an
              py:class:`Epoch` object.
            * Geocentric latitude of the center of the Moon, as an
              py:class:`Epoch` object.
            * Distance in kilometers between the centers of Earth and Moon, in
              kilometers (float)
            * Equatorial horizontal parallax of the Moon, as an
              py:class:`Epoch` object.
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1992, 4, 12.0)
        >>> Lambda, Beta, Delta, ppi = Moon.geocentric_ecliptical_pos(epoch)
        >>> print(round(Lambda, 6))
        133.162655
        >>> print(round(Beta, 6))
        -3.229126
        >>> print(round(Delta, 1))
        368409.7
        >>> print(round(ppi, 5))
        0.99199
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch)):
            raise TypeError("Invalid input type")
        # Get the time from J2000.0 in Julian centuries
        t = (epoch - JDE2000) / 36525.0
        # Compute Moon's mean longitude, referred to mean equinox of date
        Lprime = 218.3164477 + (481267.88123421
                                + (-0.0015786
                                   + (1.0/538841.0
                                       - t/65194000.0) * t) * t) * t
        # Mean elongation of the Moon
        D = 297.8501921 + (445267.1114034
                           + (-0.0018819
                              + (1.0/545868.0 - t/113065000.0) * t) * t) * t
        # Sun's mean anomaly
        M = 357.5291092 + (35999.0502909 + (-0.0001536 + t/24490000.0) * t) * t
        # Moon's mean anomaly
        Mprime = 134.9633964 + (477198.8675055
                                + (0.0087414
                                   + (1.0/69699.9
                                      + t/14712000.0) * t) * t) * t
        # Moon's argument of latitude
        F = 93.2720950 + (483202.0175233
                          + (-0.0036539
                             + (-1.0/3526000.0 + t/863310000.0) * t) * t) * t
        # Let's compute some additional arguments
        A1 = 119.75 + 131.849 * t
        A2 = 53.09 + 479264.290 * t
        A3 = 313.45 + 481266.484 * t
        # Eccentricity of Earth's orbit around the Sun
        E = 1.0 + (-0.002516 - 0.0000074 * t) * t
        E2 = E * E
        # Reduce the angles to a [0 360] range
        Lprime = Angle(Angle.reduce_deg(Lprime)).to_positive()
        Lprimer = Lprime.rad()
        D = Angle(Angle.reduce_deg(D)).to_positive()
        Dr = D.rad()
        M = Angle(Angle.reduce_deg(M)).to_positive()
        Mr = M.rad()
        Mprime = Angle(Angle.reduce_deg(Mprime)).to_positive()
        Mprimer = Mprime.rad()
        F = Angle(Angle.reduce_deg(F)).to_positive()
        Fr = F.rad()
        A1 = Angle(Angle.reduce_deg(A1)).to_positive()
        A1r = A1.rad()
        A2 = Angle(Angle.reduce_deg(A2)).to_positive()
        A2r = A2.rad()
        A3 = Angle(Angle.reduce_deg(A3)).to_positive()
        A3r = A3.rad()
        # Let's store this results in a list, in preparation for using tables
        arguments = [Dr, Mr, Mprimer, Fr]
        # Now we use the tables of periodic terms. First for sigmal and sigmar
        sigmal = 0.0
        sigmar = 0.0
        for i, value in enumerate(PERIODIC_TERMS_LR_TABLE):
            argument = 0.0
            for j in range(4):
                if PERIODIC_TERMS_LR_TABLE[i][j]:  # Avoid multiply by zero
                    argument += PERIODIC_TERMS_LR_TABLE[i][j] * arguments[j]
            coeffl = value[4]
            coeffr = value[5]
            if abs(value[1]) == 1:
                coeffl = coeffl * E
                coeffr = coeffr * E
            elif abs(value[1]) == 2:
                coeffl = coeffl * E2
                coeffr = coeffr * E2
            sigmal += coeffl * sin(argument)
            sigmar += coeffr * cos(argument)
        # Add the additive terms to sigmal
        sigmal += (3958.0 * sin(A1r) + 1962.0 * sin(Lprimer - Fr)
                   + 318.0 * sin(A2r))
        # Now use the tabla for sigmab
        sigmab = 0.0
        for i, value in enumerate(PERIODIC_TERMS_B_TABLE):
            argument = 0.0
            for j in range(4):
                if PERIODIC_TERMS_B_TABLE[i][j]:  # Avoid multiply by zero
                    argument += PERIODIC_TERMS_B_TABLE[i][j] * arguments[j]
            coeffb = value[4]
            if abs(value[1]) == 1:
                coeffb = coeffb * E
            elif abs(value[1]) == 2:
                coeffb = coeffb * E2
            sigmab += coeffb * sin(argument)
        # Add the additive terms to sigmab
        sigmab += (-2235.0 * sin(Lprimer) + 382.0 * sin(A3r)
                   + 175.0 * sin(A1r - Fr) + 175.0 * sin(A1r + Fr)
                   + 127.0 * sin(Lprimer - Mprimer)
                   - 115.0 * sin(Lprimer + Mprimer))
        Lambda = Lprime + (sigmal / 1000000.0)
        Beta = Angle(sigmab / 1000000.0)
        Delta = 385000.56 + (sigmar / 1000.0)
        ppii = asin(6378.14 / Delta)
        ppi = Angle(ppii, radians=True)
        return Lambda, Beta, Delta, ppi

    @staticmethod
    def apparent_ecliptical_pos(epoch):
        """This method computes the apparent geocentric ecliptical position
        (longitude, latitude) of the Moon for a given instant, referred to the
        mean equinox of the date, as well as the Moon-Earth distance in
        kilometers and the equatorial horizontal parallax.

        :param epoch: Instant to compute the Moon's position, as an
            py:class:`Epoch` object.
        :type epoch: :py:class:`Epoch`

        :returns: Tuple containing:

            * Apparent geocentric longitude of the center of the Moon, as an
              py:class:`Epoch` object.
            * Apparent geocentric latitude of the center of the Moon, as an
              py:class:`Epoch` object.
            * Distance in kilometers between the centers of Earth and Moon, in
              kilometers (float)
            * Equatorial horizontal parallax of the Moon, as an
              py:class:`Epoch` object.
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1992, 4, 12.0)
        >>> Lambda, Beta, Delta, ppi = Moon.apparent_ecliptical_pos(epoch)
        >>> print(round(Lambda, 5))
        133.16726
        >>> print(round(Beta, 6))
        -3.229126
        >>> print(round(Delta, 1))
        368409.7
        >>> print(round(ppi, 5))
        0.99199
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch)):
            raise TypeError("Invalid input type")
        # Now, let's call the method geocentric_ecliptical_pos()
        Lambda, Beta, Delta, ppi = Moon.geocentric_ecliptical_pos(epoch)
        # Compute the nutation in longitude (deltaPsi)
        deltaPsi = nutation_longitude(epoch)
        # Correct the longitude to obtain the apparent longitude
        aLambda = Lambda + deltaPsi
        return aLambda, Beta, Delta, ppi

    @staticmethod
    def apparent_equatorial_pos(epoch):
        """This method computes the apparent equatorial position (right
        ascension, declination) of the Moon for a given instant, referred to
        the mean equinox of the date, as well as the Moon-Earth distance in
        kilometers and the equatorial horizontal parallax.

        :param epoch: Instant to compute the Moon's position, as an
            py:class:`Epoch` object.
        :type epoch: :py:class:`Epoch`

        :returns: Tuple containing:

            * Apparent right ascension of the center of the Moon, as an
              py:class:`Epoch` object.
            * Apparent declination of the center of the Moon, as an
              py:class:`Epoch` object.
            * Distance in kilometers between the centers of Earth and Moon, in
              kilometers (float)
            * Equatorial horizontal parallax of the Moon, as an
              py:class:`Epoch` object.
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1992, 4, 12.0)
        >>> ra, dec, Delta, ppi = Moon.apparent_equatorial_pos(epoch)
        >>> print(round(ra, 6))
        134.688469
        >>> print(round(dec, 6))
        13.768367
        >>> print(round(Delta, 1))
        368409.7
        >>> print(round(ppi, 5))
        0.99199
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch)):
            raise TypeError("Invalid input type")
        # Let's start calling the method 'apparent_ecliptical_pos()'
        Lambda, Beta, Delta, ppi = Moon.apparent_ecliptical_pos(epoch)
        # Now we need the obliquity of the ecliptic
        epsilon = true_obliquity(epoch)
        # And now let's carry out the transformation ecliptical->equatorial
        ra, dec = ecliptical2equatorial(Lambda, Beta, epsilon)
        return ra, dec, Delta, ppi

    @staticmethod
    def longitude_mean_ascending_node(epoch):
        """This method computes the longitude of the mean ascending node of the
        Moon in degrees, for a given instant, measured from the mean equinox of
        the date.

        :param epoch: Instant to compute the Moon's mean ascending node, as an
            py:class:`Epoch` object.
        :type epoch: :py:class:`Epoch`

        :returns: The longitude of the mean ascending node.
        :rtype: py:class:`Angle`
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1913, 5, 27.0)
        >>> Omega = Moon.longitude_mean_ascending_node(epoch)
        >>> print(round(Omega, 1))
        0.0
        >>> epoch = Epoch(2043, 9, 10.0)
        >>> Omega = Moon.longitude_mean_ascending_node(epoch)
        >>> print(round(Omega, 1))
        0.0
        >>> epoch = Epoch(1959, 12, 7.0)
        >>> Omega = Moon.longitude_mean_ascending_node(epoch)
        >>> print(round(Omega, 1))
        180.0
        >>> epoch = Epoch(2108, 11, 3.0)
        >>> Omega = Moon.longitude_mean_ascending_node(epoch)
        >>> print(round(Omega, 1))
        180.0
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch)):
            raise TypeError("Invalid input type")
        # Get the time from J2000.0 in Julian centuries
        t = (epoch - JDE2000) / 36525.0
        # Compute Moon's longitude of the mean ascending node
        Omega = 125.0445479 + (-1934.1362891
                               + (0.0020754
                                  + (1.0/476441.0
                                     - t/60616000.0) * t) * t) * t
        Omega = Angle(Omega).to_positive()
        return Omega

    @staticmethod
    def longitude_true_ascending_node(epoch):
        """This method computes the longitude of the true ascending node of the
        Moon in degrees, for a given instant, measured from the mean equinox of
        the date.

        :param epoch: Instant to compute the Moon's true ascending node, as an
            py:class:`Epoch` object.
        :type epoch: :py:class:`Epoch`

        :returns: The longitude of the true ascending node.
        :rtype: py:class:`Angle`
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1913, 5, 27.0)
        >>> Omega = Moon.longitude_true_ascending_node(epoch)
        >>> print(round(Omega, 4))
        0.8763
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch)):
            raise TypeError("Invalid input type")
        # Let's start computing the longitude of the MEAN ascending node
        Omega = Moon.longitude_mean_ascending_node(epoch)
        # Get the time from J2000.0 in Julian centuries
        t = (epoch - JDE2000) / 36525.0
        # Mean elongation of the Moon
        D = 297.8501921 + (445267.1114034
                           + (-0.0018819
                              + (1.0/545868.0 - t/113065000.0) * t) * t) * t
        # Sun's mean anomaly
        M = 357.5291092 + (35999.0502909 + (-0.0001536 + t/24490000.0) * t) * t
        # Moon's mean anomaly
        Mprime = 134.9633964 + (477198.8675055
                                + (0.0087414
                                   + (1.0/69699.9
                                      + t/14712000.0) * t) * t) * t
        # Moon's argument of latitude
        F = 93.2720950 + (483202.0175233
                          + (-0.0036539
                             + (-1.0/3526000.0 + t/863310000.0) * t) * t) * t
        # Reduce the angles to a [0 360] range
        D = Angle(Angle.reduce_deg(D)).to_positive()
        Dr = D.rad()
        M = Angle(Angle.reduce_deg(M)).to_positive()
        Mr = M.rad()
        Mprime = Angle(Angle.reduce_deg(Mprime)).to_positive()
        Mprimer = Mprime.rad()
        F = Angle(Angle.reduce_deg(F)).to_positive()
        Fr = F.rad()
        # Compute the periodic terms
        corr = (-1.4979 * sin(2.0 * (Dr - Fr)) - 0.15 * sin(Mr)
                - 0.1226 * sin(2.0 * Dr) + 0.1176 * sin(2.0 * Fr)
                - 0.0801 * sin(2.0 * (Mprimer - Fr)))
        Omega += Angle(corr)
        return Omega

    @staticmethod
    def longitude_mean_perigee(epoch):
        """This method computes the longitude of the mean perigee of the lunar
        orbitn in degrees, for a given instant, measured from the mean equinox
        of the date.

        :param epoch: Instant to compute the Moon's mean perigee, as an
            py:class:`Epoch` object.
        :type epoch: :py:class:`Epoch`

        :returns: The longitude of the mean perigee.
        :rtype: py:class:`Angle`
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(2021, 3, 5.0)
        >>> Pi = Moon.longitude_mean_perigee(epoch)
        >>> print(round(Pi, 5))
        224.89194
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch)):
            raise TypeError("Invalid input type")
        # Get the time from J2000.0 in Julian centuries
        t = (epoch - JDE2000) / 36525.0
        # Compute Moon's longitude of the mean perigee
        ppii = 83.3532465 + (4069.0137287
                             + (-0.01032
                                + (-1.0/80053.0 + t/18999000.0) * t) * t) * t
        ppii = Angle(ppii)
        return ppii

    @staticmethod
    def illuminated_fraction_disk(epoch):
        """This method computes the approximate illuminated fraction 'k' of the
        disk of the Moon. The method used has a relatively low accuracy, but it
        is enough to the 2nd decimal place.

        :param epoch: Instant to compute the Moon's illuminated fraction of the
            disk, as a py:class:`Epoch` object.
        :type epoch: :py:class:`Epoch`

        :returns: The approximate illuminated fraction of the Moon's disk.
        :rtype: float
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1992, 4, 12.0)
        >>> k = Moon.illuminated_fraction_disk(epoch)
        >>> print(round(k, 2))
        0.68
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch)):
            raise TypeError("Invalid input type")
        # Get the time from J2000.0 in Julian centuries
        t = (epoch - JDE2000) / 36525.0
        # Mean elongation of the Moon
        D = 297.8501921 + (445267.1114034
                           + (-0.0018819
                              + (1.0/545868.0 - t/113065000.0) * t) * t) * t
        # Sun's mean anomaly
        M = 357.5291092 + (35999.0502909 + (-0.0001536 + t/24490000.0) * t) * t
        # Moon's mean anomaly
        Mprime = 134.9633964 + (477198.8675055
                                + (0.0087414
                                   + (1.0/69699.9
                                      + t/14712000.0) * t) * t) * t
        # Reduce the angles to a [0 360] range
        D = Angle(Angle.reduce_deg(D)).to_positive()
        Dr = D.rad()
        M = Angle(Angle.reduce_deg(M)).to_positive()
        Mr = M.rad()
        Mprime = Angle(Angle.reduce_deg(Mprime)).to_positive()
        Mprimer = Mprime.rad()
        # Compute the 'i' angle
        i = Angle(180.0 - D - 6.289 * sin(Mprimer) + 2.1 * sin(Mr)
                  - 1.274 * sin(2.0 * Dr - Mprimer) - 0.658 * sin(2.0 * Dr)
                  - 0.214 * sin(2.0 * Mprimer) - 0.11 * sin(Dr))
        k = (1.0 + cos(i.rad())) / 2.0
        return k

    @staticmethod
    def position_bright_limb(epoch):
        """This method computes the position angle of the Moon's bright limb,
        i.e., the position angle of the midpoint of the illuminated limb,
        reckoned eastward from the North Point of the disk (not from the axis
        of rotation of the lunar globe).

        :param epoch: Instant to compute the position angle of the  Moon's
            bright limb, as a py:class:`Epoch` object.
        :type epoch: :py:class:`Epoch`

        :returns: The position angle of the Moon's bright limb.
        :rtype: :py:class:`Angle`
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1992, 4, 12.0)
        >>> xi = Moon.position_bright_limb(epoch)
        >>> print(round(xi, 1))
        285.0
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch)):
            raise TypeError("Invalid input type")
        # Compute the right ascension and declination of the Sun
        a0, d0, r0 = Sun.apparent_rightascension_declination_coarse(epoch)
        # Now compute the right ascension and declination of the Moon
        a, d, r, ppi = Moon.apparent_equatorial_pos(epoch)
        a0r = a0.rad()
        d0r = d0.rad()
        ar = a.rad()
        dr = d.rad()
        # Compute the numerator of the tan(xi) formula
        numerator = cos(d0r) * sin(a0r - ar)
        # Now the denominator
        denominator = sin(d0r) * cos(dr) - cos(d0r) * sin(dr) * cos(a0r - ar)
        # Now let's compute xi
        xi = atan2(numerator, denominator)
        xi = Angle(xi, radians=True).to_positive()
        return xi

    @staticmethod
    def moon_phase(epoch, target="new"):
        """This method computes the time of the phase of the moon closest to
        the provided epoch. The resulting time is expressed in the uniform time
        scale of Dynamical Time (TT).

        :param epoch: Approximate epoch we want to compute the Moon phase for.
        :type year: :py:class:`Epoch`
        :param target: Corresponding phase. It can be "new" (New Moon), "first"
            (First Quarter), "full" (Full Moon) and "last" (Last Quarter). It
            is 'new' by default.
        :type target: str

        :returns: The instant of time when the provided phase happens.
        :rtype: :py:class:`Epoch`
        :raises: TypeError if input values are of wrong type.
        :raises: ValueError if 'target' value is invalid.

        >>> epoch = Epoch(1977, 2, 15.0)
        >>> new_moon = Moon.moon_phase(epoch, target="new")
        >>> y, m, d, h, mi, s = new_moon.get_full_date()
        >>> print("{}/{}/{} {}:{}:{}".format(y, m, d, h, mi, round(s, 0)))
        1977/2/18 3:37:42.0
        >>> epoch = Epoch(2044, 1, 1.0)
        >>> new_moon = Moon.moon_phase(epoch, target="last")
        >>> y, m, d, h, mi, s = new_moon.get_full_date()
        >>> print("{}/{}/{} {}:{}:{}".format(y, m, d, h, mi, round(s)))
        2044/1/21 23:48:17
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch) and isinstance(target, str)):
            raise TypeError("Invalid input types")
        # Second, check that the target is correct
        if (
            (target != "new")
            and (target != "first")
            and (target != "full")
            and (target != "last")
        ):
            raise ValueError("'target' value is invalid")
        # Let's start computing the year with decimals
        y, m, d = epoch.get_date()
        num_days_year = 365.0
        if Epoch.is_leap(y):
            num_days_year = 366.0
        doy = Epoch.get_doy(y, m, d)
        year = y + doy / num_days_year
        # We compute the 'k' parameter
        k = round((year - 2000.0) * 12.3685, 0)
        if target == "first":
            k += 0.25
        elif target == "full":
            k += 0.5
        elif target == "last":
            k += 0.75
        t = k / 1236.85
        # Compute the time of the 'mean' phase of the Moon
        jde = (2451550.09766 + 29.530588861 * k
               + (0.00015437 + (-0.00000015 + 0.00000000073 * t) * t) * t * t)
        # Eccentricity of Earth's orbit around the Sun
        E = 1.0 + (-0.002516 - 0.0000074 * t) * t
        # Sun's mean anomaly
        M = 2.5534 + 29.1053567 * k + (-0.0000014 - 0.00000011 * t) * t * t
        # Moon's mean anomaly
        Mprime = (201.5643 + 385.81693528 * k
                  + (0.0107582 + (0.00001238 - 0.000000058 * t) * t) * t * t)
        # Moon's argument of latitude
        F = (160.7108 + 390.67050284 * k
             + (-0.0016118 + (-0.00000227 + 0.000000011 * t) * t) * t * t)
        # Longitude of the ascending node of the lunar orbit
        Omega = (124.7746 - 1.56375588 * k
                 + (0.0020672 + 0.00000215 * t) * t * t)
        M = Angle(Angle.reduce_deg(M)).to_positive()
        Mr = M.rad()
        Mprime = Angle(Angle.reduce_deg(Mprime)).to_positive()
        Mprimer = Mprime.rad()
        F = Angle(Angle.reduce_deg(F)).to_positive()
        Fr = F.rad()
        Omega = Angle(Angle.reduce_deg(Omega)).to_positive()
        Omegar = Omega.rad()
        # Planetary arguments
        a1 = 299.77 + 0.107408 * k - 0.009173 * t * t
        a2 = 251.88 + 0.016321 * k
        a3 = 251.83 + 26.651886 * k
        a4 = 349.42 + 36.412478 * k
        a5 = 84.66 + 18.206239 * k
        a6 = 141.74 + 53.303771 * k
        a7 = 207.14 + 2.453732 * k
        a8 = 154.84 + 7.30686 * k
        a9 = 34.52 + 27.261239 * k
        a10 = 207.19 + 0.121824 * k
        a11 = 291.34 + 1.844379 * k
        a12 = 161.72 + 24.198154 * k
        a13 = 239.56 + 25.513099 * k
        a14 = 331.55 + 3.592518 * k
        a1 = Angle(Angle.reduce_deg(a1)).to_positive()
        a1r = a1.rad()
        a2 = Angle(Angle.reduce_deg(a2)).to_positive()
        a2r = a2.rad()
        a3 = Angle(Angle.reduce_deg(a3)).to_positive()
        a3r = a3.rad()
        a4 = Angle(Angle.reduce_deg(a4)).to_positive()
        a4r = a4.rad()
        a5 = Angle(Angle.reduce_deg(a5)).to_positive()
        a5r = a5.rad()
        a6 = Angle(Angle.reduce_deg(a6)).to_positive()
        a6r = a6.rad()
        a7 = Angle(Angle.reduce_deg(a7)).to_positive()
        a7r = a7.rad()
        a8 = Angle(Angle.reduce_deg(a8)).to_positive()
        a8r = a8.rad()
        a9 = Angle(Angle.reduce_deg(a9)).to_positive()
        a9r = a9.rad()
        a10 = Angle(Angle.reduce_deg(a10)).to_positive()
        a10r = a10.rad()
        a11 = Angle(Angle.reduce_deg(a11)).to_positive()
        a11r = a11.rad()
        a12 = Angle(Angle.reduce_deg(a12)).to_positive()
        a12r = a12.rad()
        a13 = Angle(Angle.reduce_deg(a13)).to_positive()
        a13r = a13.rad()
        a14 = Angle(Angle.reduce_deg(a14)).to_positive()
        a14r = a14.rad()
        # Now let's compute the corrections
        corr = 0.0
        w = 0.0
        if target == "new":
            corr = (-0.4072 * sin(Mprimer) + 0.17241 * E * sin(Mr)
                    + 0.01608 * sin(2.0 * Mprimer) + 0.01039 * sin(2.0 * Fr)
                    + 0.00739 * E * sin(Mprimer - Mr)
                    - 0.00514 * E * sin(Mprimer + Mr)
                    + 0.00208 * E * E * sin(2.0 * Mr)
                    - 0.00111 * sin(Mprimer - 2.0 * Fr)
                    - 0.00057 * sin(Mprimer + 2.0 * Fr)
                    + 0.00056 * E * sin(2.0 * Mprimer + Mr)
                    - 0.00042 * sin(3.0 * Mprimer)
                    + 0.00042 * E * sin(Mr + 2.0 * Fr)
                    + 0.00038 * E * sin(Mr - 2.0 * Fr)
                    - 0.00024 * E * sin(2.0 * Mprimer - Mr)
                    - 0.00017 * sin(Omegar) - 0.00007 * sin(Mprimer + 2.0 * Mr)
                    + 0.00004 * sin(2.0 * (Mprimer - Fr))
                    + 0.00004 * sin(3.0 * Mr)
                    + 0.00003 * sin(Mprimer + Mr - 2.0 * Fr)
                    + 0.00003 * sin(2.0 * (Mprimer + Fr))
                    - 0.00003 * sin(Mprimer + Mr + 2.0 * Fr)
                    + 0.00003 * sin(Mprimer - Mr + 2.0 * Fr)
                    - 0.00002 * sin(Mprimer - Mr - 2.0 * Fr)
                    - 0.00002 * sin(3.0 * Mprimer + Mr)
                    + 0.00002 * sin(4.0 * Mprimer))
        elif target == "full":
            corr = (-0.40614 * sin(Mprimer) + 0.17302 * E * sin(Mr)
                    + 0.01614 * sin(2.0 * Mprimer) + 0.01043 * sin(2.0 * Fr)
                    + 0.00734 * E * sin(Mprimer - Mr)
                    - 0.00515 * E * sin(Mprimer + Mr)
                    + 0.00209 * E * E * sin(2.0 * Mr)
                    - 0.00111 * sin(Mprimer - 2.0 * Fr)
                    - 0.00057 * sin(Mprimer + 2.0 * Fr)
                    + 0.00056 * E * sin(2.0 * Mprimer + Mr)
                    - 0.00042 * sin(3.0 * Mprimer)
                    + 0.00042 * E * sin(Mr + 2.0 * Fr)
                    + 0.00038 * E * sin(Mr - 2.0 * Fr)
                    - 0.00024 * E * sin(2.0 * Mprimer - Mr)
                    - 0.00017 * sin(Omegar) - 0.00007 * sin(Mprimer + 2.0 * Mr)
                    + 0.00004 * sin(2.0 * (Mprimer - Fr))
                    + 0.00004 * sin(3.0 * Mr)
                    + 0.00003 * sin(Mprimer + Mr - 2.0 * Fr)
                    + 0.00003 * sin(2.0 * (Mprimer + Fr))
                    - 0.00003 * sin(Mprimer + Mr + 2.0 * Fr)
                    + 0.00003 * sin(Mprimer - Mr + 2.0 * Fr)
                    - 0.00002 * sin(Mprimer - Mr - 2.0 * Fr)
                    - 0.00002 * sin(3.0 * Mprimer + Mr)
                    + 0.00002 * sin(4.0 * Mprimer))
        elif target == "first" or target == "last":
            corr = (-0.62801 * sin(Mprimer) + 0.17172 * E * sin(Mr)
                    - 0.01183 * E * sin(Mprimer + Mr)
                    + 0.00862 * sin(2.0 * Mprimer) + 0.00804 * sin(2.0 * Fr)
                    + 0.00454 * E * sin(Mprimer - Mr)
                    + 0.00204 * E * E * sin(2.0 * Mr)
                    - 0.0018 * sin(Mprimer - 2.0 * Fr)
                    - 0.0007 * sin(Mprimer + 2.0 * Fr)
                    - 0.0004 * sin(3.0 * Mprimer)
                    - 0.00034 * E * sin(2.0 * Mprimer - Mr)
                    + 0.00032 * E * sin(Mr + 2.0 * Fr)
                    + 0.00032 * E * sin(Mr - 2.0 * Fr)
                    - 0.00028 * E * E * sin(Mprimer + 2.0 * Mr)
                    + 0.00027 * E * sin(2.0 * Mprimer + Mr)
                    - 0.00017 * sin(Omegar)
                    - 0.00005 * sin(Mprimer - Mr - 2.0 * Fr)
                    + 0.00004 * sin(2.0 * (Mprimer + Fr))
                    - 0.00004 * sin(Mprimer + Mr + 2.0 * Fr)
                    + 0.00004 * sin(Mprimer - 2.0 * Mr)
                    + 0.00003 * sin(Mprimer + Mr - 2.0 * Fr)
                    + 0.00003 * sin(3.0 * Mr)
                    + 0.00002 * sin(2.0 * (Mprimer - Fr))
                    + 0.00002 * sin(Mprimer - Mr + 2.0 * Fr)
                    - 0.00002 * sin(3.0 * Mprimer + Mr))
            w = (0.00306 - 0.00038 * E * cos(Mr) + 0.00026 * cos(Mprimer)
                 - 0.00002 * cos(Mprimer - Mr) + 0.00002 * cos(Mprimer + Mr)
                 + 0.00002 * cos(2.0 * Fr))
            if target == "last":
                w = -w
        # Additional corrections for all phases
        corr2 = (0.000325 * sin(a1r) + 0.000165 * sin(a2r)
                 + 0.000164 * sin(a3r) + 0.000126 * sin(a4r)
                 + 0.000110 * sin(a5r) + 0.000062 * sin(a6r)
                 + 0.000060 * sin(a7r) + 0.000056 * sin(a8r)
                 + 0.000047 * sin(a9r) + 0.000042 * sin(a10r)
                 + 0.000040 * sin(a11r) + 0.000037 * sin(a12r)
                 + 0.000035 * sin(a13r) + 0.000023 * sin(a14r))
        jde += corr + corr2 + w
        jde = Epoch(jde)
        return jde

    @staticmethod
    def moon_perigee_apogee(epoch, target="perigee"):
        """This method computes the approximate times when the distance between
        the Earth and the Moon is a minimum (perigee) or a maximum (apogee).
        The resulting times will be expressed in the uniform time scale of
        Dynamical Time (TT).

        :param epoch: Approximate epoch we want to compute the Moon's perigee
            or apogee for.
        :type year: :py:class:`Epoch`
        :param target: Either 'perigee' or 'apogee'. It's 'perigee' by default.
        :type target: str

        :returns: A tuple containing the instant of time when the perigee or
            apogee happens, as a :py:class:`Epoch` object, and the Moon's
            corresponding equatorial horizontal parallax, as a
            :py:class:`Angle` object.
        :rtype: tuple
        :raises: TypeError if input values are of wrong type.
        :raises: ValueError if 'target' value is invalid.

        >>> epoch = Epoch(1988, 10, 1.0)
        >>> apogee, parallax = Moon.moon_perigee_apogee(epoch, target="apogee")
        >>> y, m, d, h, mi, s = apogee.get_full_date()
        >>> print("{}/{}/{} {}:{}".format(y, m, d, h, mi))
        1988/10/7 20:30
        >>> print("{}".format(parallax.dms_str(n_dec=3)))
        54' 0.679''
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch) and isinstance(target, str)):
            raise TypeError("Invalid input types")
        # Second, check that the target is correct
        if (
            (target != "perigee")
            and (target != "apogee")
        ):
            raise ValueError("'target' value is invalid")
        # Let's start computing the year with decimals
        y, m, d = epoch.get_date()
        num_days_year = 365.0
        if Epoch.is_leap(y):
            num_days_year = 366.0
        doy = Epoch.get_doy(y, m, d)
        year = y + doy / num_days_year
        # We compute the 'k' parameter
        k = round((year - 1999.97) * 13.2555, 0)
        if target == "apogee":
            k += 0.5
        t = k / 1325.55
        # Compute the time of the 'mean' perigee or apogee
        jde = (2451534.6698 + 27.55454989 * k
               + (-0.0006691 + (0.000001098 + 0.0000000052 * t) * t) * t * t)
        # Moon's mean elongation at jde
        D = (171.9179 + 335.9106046 * k
             + (-0.0100383 + (-0.00001156 + 0.000000055 * t) * t) * t * t)
        # Sun's mean anomaly
        M = 347.3477 + 27.1577721 * k + (-0.000813 - 0.000001 * t) * t * t
        # Moon's argument of latitude
        F = 316.6109 + 364.5287911 * k + (-0.0125053 - 0.0000148 * t) * t * t
        D = Angle(Angle.reduce_deg(D)).to_positive()
        Dr = D.rad()
        M = Angle(Angle.reduce_deg(M)).to_positive()
        Mr = M.rad()
        F = Angle(Angle.reduce_deg(F)).to_positive()
        Fr = F.rad()
        corr = 0.0
        parallax = 0.0
        if target == "perigee":
            corr = (-1.6769 * sin(2.0 * Dr) + 0.4589 * sin(4.0 * Dr)
                    - 0.1856 * sin(6.0 * Dr) + 0.0883 * sin(8.0 * Dr)
                    + (-0.0773 + 0.00019 * t) * sin(2.0 * Dr - Mr)
                    + (0.0502 - 0.00013 * t) * sin(Mr) - 0.046 * sin(10.0 * Dr)
                    + (0.0422 - 0.00011 * t) * sin(4.0 * Dr - Mr)
                    - 0.0256 * sin(6.0 * Dr - Mr) + 0.0253 * sin(12.0 * Dr)
                    + 0.0237 * sin(Dr) + 0.0162 * sin(8.0 * Dr - Mr)
                    - 0.0145 * sin(14.0 * Dr) + 0.0129 * sin(2.0 * Fr)
                    - 0.0112 * sin(3.0 * Dr) - 0.0104 * sin(10.0 * Dr - Mr)
                    + 0.0086 * sin(16.0 * Dr) + 0.0069 * sin(12.0 * Dr - Mr)
                    + 0.0066 * sin(5.0 * Dr) - 0.0053 * sin(2.0 * (Dr + Fr))
                    - 0.0052 * sin(18.0 * Dr) - 0.0046 * sin(14.0 * Dr - Mr)
                    - 0.0041 * sin(7.0 * Dr) + 0.004 * sin(2.0 * Dr + Mr)
                    + 0.0032 * sin(20.0 * Dr) - 0.0032 * sin(Dr + Mr)
                    + 0.0031 * sin(16.0 * Dr - Mr)
                    - 0.0029 * sin(4.0 * Dr + Mr) + 0.0027 * sin(9.0 * Dr)
                    + 0.0027 * sin(4.0 * Dr + 2.0 * Fr)
                    - 0.0027 * sin(2.0 * (Dr - Mr))
                    + 0.0024 * sin(4.0 * Dr - 2.0 * Mr)
                    - 0.0021 * sin(6.0 * Dr - 2.0 * Mr)
                    - 0.0021 * sin(22.0 * Dr) - 0.0021 * sin(18.0 * Dr - Mr)
                    + 0.0019 * sin(6.0 * Dr + Mr) - 0.0018 * sin(11.0 * Dr)
                    - 0.0014 * sin(8.0 * Dr + Mr)
                    - 0.0014 * sin(4.0 * Dr - 2.0 * Fr)
                    - 0.0014 * sin(6.0 * Dr + 2.0 * Fr)
                    + 0.0014 * sin(3.0 * Dr + Mr) - 0.0014 * sin(5.0 * Dr + Mr)
                    + 0.0013 * sin(13.0 * Dr) + 0.0013 * sin(20.0 * Dr - Mr)
                    + 0.0011 * sin(3.0 * Dr + 2.0 * Mr)
                    - 0.0011 * sin(4.0 * Dr + 2.0 * Fr - 2.0 * Mr)
                    - 0.0010 * sin(Dr + 2.0 * Mr)
                    - 0.0009 * sin(22.0 * Dr - Mr) - 0.0008 * sin(4.0 * Fr)
                    + 0.0008 * sin(6.0 * Dr - 2.0 * Fr)
                    + 0.0008 * sin(2.0 * Dr - 2.0 * Fr + Mr)
                    + 0.0007 * sin(2.0 * Mr) + 0.0007 * sin(2.0 * Fr - Mr)
                    + 0.0007 * sin(2.0 * Dr + 4.0 * Fr)
                    - 0.0006 * sin(2.0 * (Fr - Mr))
                    - 0.0006 * sin(2.0 * (Dr - Fr + Mr))
                    + 0.0006 * sin(24.0 * Dr) + 0.0005 * sin(4.0 * (Dr - Fr))
                    + 0.0005 * sin(2.0 * (Dr + Mr)) - 0.0004 * sin(Dr - Mr))
            parallax = (3629.215 + 63.224 * cos(2.0 * Dr)
                        - 6.99 * cos(4.0 * Dr)
                        + (2.834 - 0.0071 * t) * cos(2.0 * Dr - Mr)
                        + 1.927 * cos(6.0 * Dr) - 1.263 * cos(Dr)
                        - 0.702 * cos(8.0 * Dr)
                        + (0.696 - 0.0017 * t) * cos(Mr) - 0.69 * cos(2.0 * Fr)
                        + (-0.629 + 0.0016 * t) * cos(4.0 * Dr - Mr)
                        - 0.392 * cos(2.0 * (Dr - Fr)) + 0.297 * cos(10.0 * Dr)
                        + 0.26 * cos(6.0 * Dr - Mr) + 0.201 * cos(3.0 * Dr)
                        - 0.161 * cos(2.0 * Dr + Mr) + 0.157 * cos(Dr + Mr)
                        - 0.138 * cos(12.0 * Dr) - 0.127 * cos(8.0 * Dr - Mr)
                        + 0.104 * cos(2.0 * (Dr + Fr))
                        + 0.104 * cos(2.0 * (Dr - Mr)) - 0.079 * cos(5.0 * Dr)
                        + 0.068 * cos(14.0 * Dr) + 0.067 * cos(10.0 * Dr - Mr)
                        + 0.054 * cos(4.0 * Dr + Mr)
                        - 0.038 * cos(12.0 * Dr - Mr)
                        - 0.038 * cos(4.0 * Dr - 2.0 * Mr)
                        + 0.037 * cos(7.0 * Dr)
                        - 0.037 * cos(4.0 * Dr + 2.0 * Fr)
                        - 0.035 * cos(16.0 * Dr) - 0.03 * cos(3.0 * Dr + Mr)
                        + 0.029 * cos(Dr - Mr) - 0.025 * cos(6.0 * Dr + Mr)
                        + 0.023 * cos(2.0 * Mr) + 0.023 * cos(14.0 * Dr - Mr)
                        - 0.023 * cos(2.0 * (Dr + Mr))
                        + 0.022 * cos(6.0 * Dr - 2.0 * Mr)
                        - 0.021 * cos(2.0 * (Dr - Fr) - Mr)
                        - 0.020 * cos(9.0 * Dr) + 0.019 * cos(18.0 * Dr)
                        + 0.017 * cos(6.0 * Dr + 2.0 * Fr)
                        + 0.014 * cos(2.0 * Fr - Mr)
                        - 0.014 * cos(16.0 * Dr - Mr)
                        + 0.013 * cos(4.0 * Dr - 2.0 * Fr)
                        + 0.012 * cos(8.0 * Dr + Mr) + 0.011 * cos(11.0 * Dr)
                        + 0.01 * cos(5.0 * Dr + Mr) - 0.01 * cos(20.0 * Dr))
        else:
            corr = (0.4392 * sin(2.0 * Dr)
                    + 0.0684 * sin(4.0 * Dr)
                    + (0.0456 - 0.00011 * t) * sin(Mr)
                    + (0.0426 - 0.00011 * t) * sin(2.0 * Dr - Mr)
                    + 0.0212 * sin(2.0 * Fr)
                    - 0.0189 * sin(Dr)
                    + 0.0144 * sin(6.0 * Dr)
                    + 0.0113 * sin(4.0 * Dr - Mr)
                    + 0.0047 * sin(2.0 * (Dr + Fr))
                    + 0.0036 * sin(Dr + Mr)
                    + 0.0035 * sin(8.0 * Dr)
                    + 0.0034 * sin(6.0 * Dr - Mr)
                    - 0.0034 * sin(2.0 * (Dr - Fr))
                    + 0.0022 * sin(2.0 * (Dr - Mr))
                    - 0.0017 * sin(3.0 * Dr)
                    + 0.0013 * sin(4.0 * Dr + 2.0 * Fr)
                    + 0.0011 * sin(8.0 * Dr - Mr)
                    + 0.0010 * sin(4.0 * Dr - 2.0 * Mr)
                    + 0.0009 * sin(10.0 * Dr)
                    + 0.0007 * sin(3.0 * Dr + Mr)
                    + 0.0006 * sin(2.0 * Mr)
                    + 0.0005 * sin(2.0 * Dr + Mr)
                    + 0.0005 * sin(2.0 * (Dr + Mr))
                    + 0.0004 * sin(6.0 * Dr + 2.0 * Fr)
                    + 0.0004 * sin(6.0 * Dr - 2.0 * Mr)
                    + 0.0004 * sin(10.0 * Dr - Mr)
                    - 0.0004 * sin(5.0 * Dr)
                    - 0.0004 * sin(4.0 * Dr - 2.0 * Fr)
                    + 0.0003 * sin(2.0 * Fr + Mr)
                    + 0.0003 * sin(12.0 * Dr)
                    + 0.0003 * sin(2.0 * (Dr + Fr) - Mr)
                    - 0.0003 * sin(Dr - Mr))
            parallax = (3245.251 - 9.147 * cos(2.0 * Dr) - 0.841 * cos(Dr)
                        + 0.697 * cos(2.0 * Fr)
                        + (-0.656 + 0.0016 * t) * cos(Mr)
                        + 0.355 * cos(4.0 * Dr) + 0.159 * cos(2.0 * Dr - Mr)
                        + 0.127 * cos(Dr + Mr) + 0.065 * cos(4.0 * Dr - Mr)
                        + 0.052 * cos(6.0 * Dr) + 0.043 * cos(2.0 * Dr + Mr)
                        + 0.031 * cos(2.0 * (Dr + Fr))
                        - 0.023 * cos(2.0 * (Dr - Fr))
                        + 0.022 * cos(2.0 * (Dr - Mr))
                        + 0.019 * cos(2.0 * (Dr + Mr)) - 0.016 * cos(2.0 * Mr)
                        + 0.014 * cos(6.0 * Dr - Mr) + 0.01 * cos(8.0 * Dr))
        jde += corr
        jde = Epoch(jde)
        parallax = Angle(0, 0, parallax)
        return jde, parallax

    @staticmethod
    def moon_passage_nodes(epoch, target="ascending"):
        """This method computes the approximate times when the center of the
        Moon passes through the ascending or descending node of its orbit. The
        resulting times will be expressed in the uniform time scale of
        Dynamical Time (TT).

        :param epoch: Approximate epoch we want to compute the Moon's passage
            through the ascending or descending node.
        :type year: :py:class:`Epoch`
        :param target: Either 'ascending' or 'descending'. It is 'ascending' by
            default.
        :type target: str

        :returns: The instant of time when the Moon passes thhrough the
            ascending or descending node.
        :rtype: :py:class:`Epoch`
        :raises: TypeError if input values are of wrong type.
        :raises: ValueError if 'target' value is invalid.

        >>> epoch = Epoch(1987, 5, 15.0)
        >>> passage = Moon.moon_passage_nodes(epoch, target="ascending")
        >>> y, m, d, h, mi, s = passage.get_full_date()
        >>> mi += s/60.0
        >>> print("{}/{}/{} {}:{}".format(y, m, d, h, round(mi)))
        1987/5/23 6:26
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch) and isinstance(target, str)):
            raise TypeError("Invalid input types")
        # Second, check that the target is correct
        if (
            (target != "ascending")
            and (target != "descending")
        ):
            raise ValueError("'target' value is invalid")
        # Let's start computing the year with decimals
        y, m, d = epoch.get_date()
        num_days_year = 365.0
        if Epoch.is_leap(y):
            num_days_year = 366.0
        doy = Epoch.get_doy(y, m, d)
        year = y + doy / num_days_year
        # Compute the 'k' parameter
        k = round((year - 2000.05) * 13.4223, 0)
        if target == "descending":
            k += 0.5
        t = k / 1342.23
        # Compute the time without the corrections
        jde = (2451565.1619 + 27.212220817 * k
               + (0.0002762 + (0.000000021 - 0.000000000088 * t) * t) * t * t)
        # Compute the following angles in degrees
        D = (183.638 + 331.73735682 * k
             + (0.0014852 + (0.00000209 - 0.00000001 * t) * t) * t * t)
        M = 17.4006 + 26.8203725 * k + (0.0001186 + 0.00000006 * t) * t * t
        Mprime = (38.3776 + 355.52747313 * k
                  + (0.0123499 + (0.000014627 - 0.000000069 * t) * t) * t * t)
        Omega = (123.9767 - 1.44098956 * k
                 + (0.0020608 + (0.00000214 - 0.000000016 * t) * t) * t * t)
        V = 299.75 + (132.85 - 0.009173 * t) * t
        P = Omega + 272.75 - 2.3 * t
        # Reduce the angles to the [0 360] range, and convert to radians
        D = Angle(Angle.reduce_deg(D)).to_positive()
        Dr = D.rad()
        M = Angle(Angle.reduce_deg(M)).to_positive()
        Mr = M.rad()
        Mprime = Angle(Angle.reduce_deg(Mprime)).to_positive()
        Mprimer = Mprime.rad()
        Omega = Angle(Angle.reduce_deg(Omega)).to_positive()
        Omegar = Omega.rad()
        V = Angle(Angle.reduce_deg(V)).to_positive()
        Vr = V.rad()
        P = Angle(Angle.reduce_deg(P)).to_positive()
        Pr = P.rad()
        # Eccentricity of Earth's orbit around the Sun
        E = 1.0 + (-0.002516 - 0.0000074 * t) * t
        # Compute the correction to jde
        corr = (-0.4721 * sin(Mprimer) - 0.1649 * sin(2.0 * Dr)
                - 0.0868 * sin(2.0 * Dr - Mprimer)
                + 0.0084 * sin(2.0 * Dr + Mprimer)
                - 0.0083 * E * sin(2.0 * Dr - Mr)
                - 0.0039 * E * sin(2.0 * Dr - Mr - Mprimer)
                + 0.0034 * sin(2.0 * Mprimer)
                - 0.0031 * sin(2.0 * (Dr - Mprimer))
                + 0.0030 * E * sin(2.0 * Dr + Mr)
                + 0.0028 * E * sin(Mr - Mprimer) + 0.0026 * E * sin(Mr)
                + 0.0025 * sin(4.0 * Dr) + 0.0024 * sin(Dr)
                + 0.0022 * E * sin(Mr + Mprimer) + 0.0017 * sin(Omegar)
                + 0.0014 * sin(4.0 * Dr - Mprimer)
                + 0.0005 * E * sin(2.0 * Dr + Mr - Mprimer)
                + 0.0004 * E * sin(2.0 * Dr - Mr + Mprimer)
                - 0.0003 * E * sin(2.0 * (Dr - Mr))
                + 0.0003 * E * sin(4.0 * Dr - Mr) + 0.0003 * sin(Vr)
                + 0.0003 * sin(Pr))
        jde += corr
        jde = Epoch(jde)
        return jde

    @staticmethod
    def moon_maximum_declination(epoch, target="northern"):
        """This method computes the approximate times when the Moon reaches
        its maximum declination (either 'northern' or 'southern'), as well as
        the values of these extreme declinations. The resulting times will be
        expressed in the uniform time scale of Dynamical Time (TT).

        :param epoch: Approximate epoch we want to compute the Moon's maximum
            declination.
        :type year: :py:class:`Epoch`
        :param target: Either 'northern' or 'southern', depending on the
            maximum declination being looked for. It is 'northern' by default.
        :type target: str

        :returns: A tuple containing the instant of time when the maximum
            declination happens, as a :py:class:`Epoch` object, and the angle
            value of such declination, as a :py:class:`Angle` object.
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1988, 12, 15.0)
        >>> epo, dec = Moon.moon_maximum_declination(epoch)
        >>> y, m, d, h, mi, s = epo.get_full_date()
        >>> print("{}/{}/{} {}:0{}".format(y, m, d, h, mi))
        1988/12/22 20:01
        >>> print("{}".format(dec.dms_str(n_dec=0)))
        28d 9' 22.0''
        >>> epoch = Epoch(2049, 4, 15.0)
        >>> epo, dec = Moon.moon_maximum_declination(epoch, target='southern')
        >>> y, m, d, h, mi, s = epo.get_full_date()
        >>> print("{}/{}/{} {}:{}".format(y, m, d, h, mi))
        2049/4/21 14:0
        >>> print("{}".format(dec.dms_str(n_dec=0)))
        -22d 8' 18.0''
        >>> epoch = Epoch(-4, 3, 15.0)
        >>> epo, dec = Moon.moon_maximum_declination(epoch, target='northern')
        >>> y, m, d, h, mi, s = epo.get_full_date()
        >>> print("{}/{}/{} {}h".format(y, m, d, h))
        -4/3/16 15h
        >>> print("{}".format(dec.dms_str(n_dec=0)))
        28d 58' 26.0''
        """

        # First check that input values are of correct types
        if not (isinstance(epoch, Epoch) and isinstance(target, str)):
            raise TypeError("Invalid input types")
        # Second, check that the target is correct
        if (
            (target != "northern")
            and (target != "southern")
        ):
            raise ValueError("'target' value is invalid")
        # Let's start computing the year with decimals
        y, m, d = epoch.get_date()
        num_days_year = 365.0
        if Epoch.is_leap(y):
            num_days_year = 366.0
        doy = Epoch.get_doy(y, m, d)
        year = y + doy / num_days_year
        # We compute the 'k' parameter
        k = round((year - 2000.03) * 13.3686, 0)
        t = k / 1336.86
        # Compute the following angles in degrees, plus 'jde' in days
        D = 333.0705546 * k + (-0.0004214 + 0.00000011 * t) * t * t
        M = 26.9281592 * k - (0.0000355 + 0.0000001 * t) * t * t
        Mprime = 356.9562794 * k + (0.0103066 + 0.00001251 * t) * t * t
        F = 1.4467807 * k - (0.002069 + 0.00000215 * t) * t * t
        jde = 27.321582247 * k + (0.000119804 - 0.000000141 * t) * t * t
        # Adjust the values according to northern of southern declination
        if (target == 'northern'):
            D += 152.2029
            M += 14.8591
            Mprime += 4.6881
            F += 325.8867
            jde += 2451562.5897
        else:
            D += 345.6676
            M += 1.13951
            Mprime += 186.21
            F += 145.1633
            jde += 2451548.9289
        # Reduce the angles to the [0 360] range, and convert to radians
        D = Angle(Angle.reduce_deg(D)).to_positive()
        Dr = D.rad()
        M = Angle(Angle.reduce_deg(M)).to_positive()
        Mr = M.rad()
        Mprime = Angle(Angle.reduce_deg(Mprime)).to_positive()
        Mprimer = Mprime.rad()
        F = Angle(Angle.reduce_deg(F)).to_positive()
        Fr = F.rad()
        # Eccentricity of Earth's orbit around the Sun
        E = 1.0 + (-0.002516 - 0.0000074 * t) * t
        corr = 0.0
        cor2 = 0.0
        # Compute the periodic terms for the time of maximum declination
        if (target == 'northern'):
            # Correction for the epoch
            corr = (0.8975 * cos(Fr) - 0.4726 * sin(Mprimer)
                    - 0.1030 * sin(2.0 * Fr) - 0.0976 * sin(2.0 * Dr - Mprimer)
                    - 0.0462 * cos(Mprimer - Fr) - 0.0461 * cos(Mprimer + Fr)
                    - 0.0438 * sin(2.0 * Dr) + 0.0162 * E * sin(Mr)
                    - 0.0157 * cos(3.0 * Fr) + 0.0145 * sin(Mprimer + 2.0 * Fr)
                    + 0.0136 * cos(2.0 * Dr - Fr)
                    - 0.0095 * cos(2.0 * Dr - Mprimer - Fr)
                    - 0.0091 * cos(2.0 * Dr - Mprimer + Fr)
                    - 0.0089 * cos(2.0 * Dr + Fr) + 0.0075 * sin(2.0 * Mprimer)
                    - 0.0068 * sin(Mprimer - 2.0 * Fr)
                    + 0.0061 * cos(2.0 * Mprimer - Fr)
                    - 0.0047 * sin(Mprimer + 3.0 * Fr)
                    - 0.0043 * E * sin(2.0 * Dr - Mr - Mprimer)
                    - 0.0040 * cos(Mprimer - 2.0 * Fr)
                    - 0.0037 * sin(2.0 * (Dr - Mprimer)) + 0.0031 * sin(Fr)
                    + 0.0030 * sin(2.0 * Dr + Mprimer)
                    - 0.0029 * cos(Mprimer + 2.0 * Fr)
                    - 0.0029 * E * sin(2.0 * Dr - Mr)
                    - 0.0027 * sin(Mprimer + Fr)
                    + 0.0024 * E * sin(Mr - Mprimer)
                    - 0.0021 * sin(Mprimer - 3.0 * Fr)
                    + 0.0019 * sin(2.0 * Mprimer + Fr)
                    + 0.0018 * cos(2.0 * (Dr - Mprimer) - Fr)
                    + 0.0018 * sin(3.0 * Fr) + 0.0017 * cos(Mprimer + 3.0 * Fr)
                    + 0.0017 * cos(2.0 * Mprimer)
                    - 0.0014 * cos(2.0 * Dr - Mprimer)
                    + 0.0013 * cos(2.0 * Dr + Mprimer + Fr)
                    + 0.0013 * cos(Mprimer) + 0.0012 * sin(3.0 * Mprimer + Fr)
                    + 0.0011 * sin(2.0 * Dr - Mprimer + Fr)
                    - 0.0011 * cos(2.0 * (Dr - Mprimer)) + 0.001 * cos(Dr + Fr)
                    + 0.0010 * E * sin(Mr + Mprimer)
                    - 0.0009 * sin(2.0 * (Dr - Fr))
                    + 0.0007 * cos(2.0 * Mprimer + Fr)
                    - 0.0007 * cos(3.0 * Mprimer + Fr))
            # Correction for the declination
            cor2 = (5.1093 * sin(Fr) + 0.2658 * cos(2.0 * Fr)
                    + 0.1448 * sin(2.0 * Dr - Fr) - 0.0322 * sin(3.0 * Fr)
                    + 0.0133 * cos(2.0 * (Dr - Fr)) + 0.0125 * cos(2.0 * Dr)
                    - 0.0124 * sin(Mprimer - Fr)
                    - 0.0101 * sin(Mprimer + 2.0 * Fr) + 0.0097 * cos(Fr)
                    - 0.0087 * E * sin(2.0 * Dr + Mr - Fr)
                    + 0.0074 * sin(Mprimer + 3.0 * Fr) + 0.0067 * sin(Dr + Fr)
                    + 0.0063 * sin(Mprimer - 2.0 * Fr)
                    + 0.0060 * E * sin(2.0 * Dr - Mr - Fr)
                    - 0.0057 * sin(2.0 * Dr - Mprimer - Fr)
                    - 0.0056 * cos(Mprimer + Fr)
                    + 0.0052 * cos(Mprimer + 2.0 * Fr)
                    + 0.0041 * cos(2.0 * Mprimer + Fr)
                    - 0.0040 * cos(Mprimer - 3.0 * Fr)
                    + 0.0038 * cos(2.0 * Mprimer - Fr)
                    - 0.0034 * cos(Mprimer - 2.0 * Fr)
                    - 0.0029 * sin(2.0 * Mprimer)
                    + 0.0029 * sin(3.0 * Mprimer + Fr)
                    - 0.0028 * E * cos(2.0 * Dr + Mr - Fr)
                    - 0.0028 * cos(Mprimer - Fr) - 0.0023 * cos(3.0 * Fr)
                    - 0.0021 * sin(2.0 * Dr + Fr)
                    + 0.0019 * cos(Mprimer + 3.0 * Fr) + 0.0018 * cos(Dr + Fr)
                    + 0.0017 * sin(2.0 * Mprimer - Fr)
                    + 0.0015 * cos(3.0 * Mprimer + Fr)
                    + 0.0014 * cos(2.0 * (Dr + Mprimer) + Fr)
                    - 0.0012 * sin(2.0 * (Dr - Mprimer) - Fr)
                    - 0.0012 * cos(2.0 * Mprimer) - 0.0010 * cos(Mprimer)
                    - 0.0010 * sin(2.0 * Fr) + 0.0006 * sin(Mprimer + Fr))
        else:
            # Correction for the epoch
            corr = (-0.8975 * cos(Fr) - 0.4726 * sin(Mprimer)
                    - 0.1030 * sin(2.0 * Fr) - 0.0976 * sin(2.0 * Dr - Mprimer)
                    + 0.0541 * cos(Mprimer - Fr) + 0.0516 * cos(Mprimer + Fr)
                    - 0.0438 * sin(2.0 * Dr) + 0.0112 * E * sin(Mr)
                    + 0.0157 * cos(3.0 * Fr) + 0.0023 * sin(Mprimer + 2.0 * Fr)
                    - 0.0136 * cos(2.0 * Dr - Fr)
                    + 0.0110 * cos(2.0 * Dr - Mprimer - Fr)
                    + 0.0091 * cos(2.0 * Dr - Mprimer + Fr)
                    + 0.0089 * cos(2.0 * Dr + Fr) + 0.0075 * sin(2.0 * Mprimer)
                    - 0.0030 * sin(Mprimer - 2.0 * Fr)
                    - 0.0061 * cos(2.0 * Mprimer - Fr)
                    - 0.0047 * sin(Mprimer + 3.0 * Fr)
                    - 0.0043 * E * sin(2.0 * Dr - Mr - Mprimer)
                    + 0.0040 * cos(Mprimer - 2.0 * Fr)
                    - 0.0037 * sin(2.0 * (Dr - Mprimer)) - 0.0031 * sin(Fr)
                    + 0.0030 * sin(2.0 * Dr + Mprimer)
                    + 0.0029 * cos(Mprimer + 2.0 * Fr)
                    - 0.0029 * E * sin(2.0 * Dr - Mr)
                    - 0.0027 * sin(Mprimer + Fr)
                    + 0.0024 * E * sin(Mr - Mprimer)
                    - 0.0021 * sin(Mprimer - 3.0 * Fr)
                    - 0.0019 * sin(2.0 * Mprimer + Fr)
                    - 0.0006 * cos(2.0 * (Dr - Mprimer) - Fr)
                    - 0.0018 * sin(3.0 * Fr) - 0.0017 * cos(Mprimer + 3.0 * Fr)
                    + 0.0017 * cos(2.0 * Mprimer)
                    + 0.0014 * cos(2.0 * Dr - Mprimer)
                    - 0.0013 * cos(2.0 * Dr + Mprimer + Fr)
                    - 0.0013 * cos(Mprimer) + 0.0012 * sin(3.0 * Mprimer + Fr)
                    + 0.0011 * sin(2.0 * Dr - Mprimer + Fr)
                    + 0.0011 * cos(2.0 * (Dr - Mprimer)) + 0.001 * cos(Dr + Fr)
                    + 0.0010 * E * sin(Mr + Mprimer)
                    - 0.0009 * sin(2.0 * (Dr - Fr))
                    - 0.0007 * cos(2.0 * Mprimer + Fr)
                    - 0.0007 * cos(3.0 * Mprimer + Fr))
            # Correction for the declination
            cor2 = (-5.1093 * sin(Fr) + 0.2658 * cos(2.0 * Fr)
                    - 0.1448 * sin(2.0 * Dr - Fr) + 0.0322 * sin(3.0 * Fr)
                    + 0.0133 * cos(2.0 * (Dr - Fr)) + 0.0125 * cos(2.0 * Dr)
                    - 0.0015 * sin(Mprimer - Fr)
                    + 0.0101 * sin(Mprimer + 2.0 * Fr) - 0.0097 * cos(Fr)
                    + 0.0087 * E * sin(2.0 * Dr + Mr - Fr)
                    + 0.0074 * sin(Mprimer + 3.0 * Fr) + 0.0067 * sin(Dr + Fr)
                    - 0.0063 * sin(Mprimer - 2.0 * Fr)
                    - 0.0060 * E * sin(2.0 * Dr - Mr - Fr)
                    + 0.0057 * sin(2.0 * Dr - Mprimer - Fr)
                    - 0.0056 * cos(Mprimer + Fr)
                    - 0.0052 * cos(Mprimer + 2.0 * Fr)
                    - 0.0041 * cos(2.0 * Mprimer + Fr)
                    - 0.0040 * cos(Mprimer - 3.0 * Fr)
                    - 0.0038 * cos(2.0 * Mprimer - Fr)
                    + 0.0034 * cos(Mprimer - 2.0 * Fr)
                    - 0.0029 * sin(2.0 * Mprimer)
                    + 0.0029 * sin(3.0 * Mprimer + Fr)
                    + 0.0028 * E * cos(2.0 * Dr + Mr - Fr)
                    - 0.0028 * cos(Mprimer - Fr) + 0.0023 * cos(3.0 * Fr)
                    + 0.0021 * sin(2.0 * Dr + Fr)
                    + 0.0019 * cos(Mprimer + 3.0 * Fr) + 0.0018 * cos(Dr + Fr)
                    - 0.0017 * sin(2.0 * Mprimer - Fr)
                    + 0.0015 * cos(3.0 * Mprimer + Fr)
                    + 0.0014 * cos(2.0 * (Dr + Mprimer) + Fr)
                    + 0.0012 * sin(2.0 * (Dr - Mprimer) - Fr)
                    - 0.0012 * cos(2.0 * Mprimer) + 0.0010 * cos(Mprimer)
                    - 0.0010 * sin(2.0 * Fr) + 0.0037 * sin(Mprimer + Fr))
        # Add the correction to 'jde'
        jde += corr
        jde = Epoch(jde)
        declination = 23.6961 - 0.013004 * t + cor2
        if (target == 'southern'):
            declination *= -1.0
        declination = Angle(Angle.reduce_deg(declination))
        return jde, declination

    @staticmethod
    def moon_librations(epoch):
        """This method computes the librations in longitude and latitude of the
        moon. There are several librations: The optical librations, that are
        the apparent oscillations in the hemisphere that the Moon turns towards
        the Earth, due to variations in the geometric position of the Earth
        relative to the lunar surface during the course of the orbital motion
        of the Moon. These variations allow to observe about 59% of the surface
        of the Moon from the Earth.

        There is also the physical libration of the Moon, i.e., the libration
        due to the actual rotational motion of the Moon about its mean
        rotation. The physical libration is much smaller than the optical
        libration, and can never be larger than 0.04 degree in both longitude
        and latitude.

        Finally, there is the total libration, which is the sum of the two
        librations mentioned above

        :param epoch: Epoch we want to compute the Moon's librations.
        :type year: :py:class:`Epoch`

        :returns: A tuple containing the optical libration in longitude and in
            latitude, the physical libration also in longitude and latitude,
            and the total librations which is the sum of the previouse ones,
            all of them as :py:class:`Angle` objects.
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1992, 4, 12.0)
        >>> lopt, bopt, lphys, bphys, ltot, btot = Moon.moon_librations(epoch)
        >>> print(round(lopt, 3))
        -1.206
        >>> print(round(bopt, 3))
        4.194
        >>> print(round(lphys, 3))
        -0.025
        >>> print(round(bphys, 3))
        0.006
        >>> print(round(ltot, 2))
        -1.23
        >>> print(round(btot, 3))
        4.2
        """

        # First check that input value is of correct type
        if not isinstance(epoch, Epoch):
            raise TypeError("Invalid input types")
        # Let's start computing some constants
        ir = Angle(1.54242).rad()
        sinI = sin(ir)
        cosI = cos(ir)
        # Now, let's call the method apparent_ecliptical_pos()
        Lambda, Beta, Delta, ppi = Moon.apparent_ecliptical_pos(epoch)
        # Compute the nutation in longitude (deltaPsi)
        deltaPsi = nutation_longitude(epoch)
        # Get the time from J2000.0 in Julian centuries
        t = (epoch - JDE2000) / 36525.0
        # Mean elongation of the Moon
        D = 297.8501921 + (445267.1114034
                           + (-0.0018819
                              + (1.0/545868.0 - t/113065000.0) * t) * t) * t
        # Sun's mean anomaly
        M = 357.5291092 + (35999.0502909 + (-0.0001536 + t/24490000.0) * t) * t
        # Moon's mean anomaly
        Mprime = 134.9633964 + (477198.8675055
                                + (0.0087414
                                   + (1.0/69699.9
                                      + t/14712000.0) * t) * t) * t
        # Moon's argument of latitude
        F = 93.2720950 + (483202.0175233
                          + (-0.0036539
                             + (-1.0/3526000.0 + t/863310000.0) * t) * t) * t
        F = Angle(Angle.reduce_deg(F)).to_positive()
        # Compute the mean longitude of the ascending node of lunar orbit
        Omega = Moon.longitude_mean_ascending_node(epoch)
        # Let's compute some additional arguments
        k1 = 119.75 + 131.849 * t
        k2 = 72.56 + 20.186 * t
        # Eccentricity of Earth's orbit around the Sun
        E = 1.0 + (-0.002516 - 0.0000074 * t) * t
        # Reduce the angles to a [0 360] range
        D = Angle(Angle.reduce_deg(D)).to_positive()
        Dr = D.rad()
        M = Angle(Angle.reduce_deg(M)).to_positive()
        Mr = M.rad()
        Mprime = Angle(Angle.reduce_deg(Mprime)).to_positive()
        Mprimer = Mprime.rad()
        F = Angle(Angle.reduce_deg(F)).to_positive()
        Fr = F.rad()
        Omegar = Omega.rad()
        k1 = Angle(Angle.reduce_deg(k1)).to_positive()
        k1r = k1.rad()
        k2 = Angle(Angle.reduce_deg(k2)).to_positive()
        k2r = k2.rad()
        # Let's compute 'w' and some additional parameters
        w = Lambda - deltaPsi - Omega
        w = w.to_positive()
        wr = w.rad()
        sinW = sin(wr)
        cosW = cos(wr)
        betar = Beta.rad()
        sinB = sin(betar)
        cosB = cos(betar)
        # Compute 'A'
        Ar = atan2((sinW * cosB * cosI - sinB * sinI), (cosW * cosB))
        A = Angle(Ar, radians=True).to_positive()
        lprime = A - F
        bprimer = asin(-sinW * cosB * sinI - sinB * cosI)
        bprime = Angle(bprimer, radians=True)
        # Compute the expressions from D.H. Eckhardt 1981
        rho = (-0.02752 * cos(Mprimer) - 0.02245 * sin(Fr)
               + 0.00684 * cos(Mprimer - 2.0 * Fr) - 0.00293 * cos(2.0 * Fr)
               - 0.00085 * cos(2.0 * (Fr - Dr))
               - 0.00054 * cos(Mprimer - 2.0 * Dr) - 0.0002 * sin(Mprimer + Fr)
               - 0.0002 * cos(Mprimer + 2.0 * Fr) - 0.0002 * cos(Mprimer - Fr)
               + 0.00014 * cos(Mprimer + 2.0 * (Fr - Dr)))
        rho = Angle(rho)
        sigma = (-0.02816 * sin(Mprimer) + 0.02244 * cos(Fr)
                 - 0.00682 * sin(Mprimer - 2.0 * Fr) - 0.00279 * sin(2.0 * Fr)
                 - 0.00083 * sin(2.0 * (Fr - Dr))
                 + 0.00069 * sin(Mprimer - 2.0 * Dr)
                 + 0.0004 * cos(Mprimer + Fr) - 0.00025 * sin(2.0 * Mprimer)
                 - 0.00023 * sin(Mprimer + 2.0 * Fr)
                 + 0.0002 * cos(Mprimer - Fr) + 0.00019 * sin(Mprimer - Fr)
                 + 0.00013 * sin(Mprimer + 2.0 * (Fr - Dr))
                 - 0.0001 * cos(Mprimer - 3.0 * Fr))
        sigma = Angle(sigma)
        tau = (0.0252 * E * sin(Mr) + 0.00473 * sin(2.0 * (Mprimer - Fr))
               - 0.00467 * sin(Mprimer) + 0.00396 * sin(k1r)
               + 0.00276 * sin(2.0 * (Mprimer - Dr)) + 0.00196 * sin(Omegar)
               - 0.00183 * cos(Mprimer - Fr)
               + 0.00115 * sin(Mprimer - 2.0 * Dr)
               - 0.00096 * sin(Mprimer - Dr) + 0.00046 * sin(2.0 * (Fr - Dr))
               - 0.00039 * sin(Mprimer - Fr) - 0.00032 * sin(Mprimer - Mr - Dr)
               + 0.00027 * sin(2.0 * (Mprimer - Dr) - Mr) + 0.00023 * sin(k2r)
               - 0.00014 * sin(2.0 * Dr) + 0.00014 * cos(2.0 * (Mprimer - Fr))
               - 0.00012 * sin(Mprimer - 2.0 * Fr)
               - 0.00012 * sin(2.0 * Mprimer)
               + 0.00011 * sin(2.0 * (Mprimer - Mr - Dr)))
        tau = Angle(tau)
        # Compute the physical librations
        lpp = -tau + (rho * cos(Ar) + sigma * sin(Ar)) * tan(bprimer)
        bpp = sigma * cos(Ar) - rho * sin(Ar)
        lt = lprime + lpp
        bt = bprime + bpp
        return lprime, bprime, lpp, bpp, lt, bt

    @staticmethod
    def moon_position_angle_axis(epoch):
        """This method computes the position angle of the Moon's axis of
        rotation. The effect of the physical libration is taken into account.

        :param epoch: Epoch we want to compute the position angle of the Moon's
            axis of rotation.
        :type year: :py:class:`Epoch`

        :returns: The position angle of the Moon's axis of rotation, as a
            :py:class:`Angle` object.
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.

        >>> epoch = Epoch(1992, 4, 12.0)
        >>> p = Moon.moon_position_angle_axis(epoch)
        >>> print(round(p, 2))
        15.08
        """

        # First check that input value is of correct type
        if not isinstance(epoch, Epoch):
            raise TypeError("Invalid input types")
        # Let's start computing some constants
        ir = Angle(1.54242).rad()
        sinI = sin(ir)
        # Compute the nutation in longitude (deltaPsi)
        deltaPsi = nutation_longitude(epoch)
        # Get the true obliquity of the ecliptic
        epsilon = true_obliquity(epoch)
        epsr = epsilon.rad()
        # Get the time from J2000.0 in Julian centuries
        t = (epoch - JDE2000) / 36525.0
        # Mean elongation of the Moon
        D = 297.8501921 + (445267.1114034
                           + (-0.0018819
                              + (1.0/545868.0 - t/113065000.0) * t) * t) * t
        # Moon's mean anomaly
        Mprime = 134.9633964 + (477198.8675055
                                + (0.0087414
                                   + (1.0/69699.9
                                      + t/14712000.0) * t) * t) * t
        # Moon's argument of latitude
        F = 93.2720950 + (483202.0175233
                          + (-0.0036539
                             + (-1.0/3526000.0 + t/863310000.0) * t) * t) * t
        F = Angle(Angle.reduce_deg(F)).to_positive()
        # Compute the mean longitude of the ascending node of lunar orbit
        Omega = Moon.longitude_mean_ascending_node(epoch)
        # Reduce the angles to a [0 360] range
        D = Angle(Angle.reduce_deg(D)).to_positive()
        Dr = D.rad()
        Mprime = Angle(Angle.reduce_deg(Mprime)).to_positive()
        Mprimer = Mprime.rad()
        F = Angle(Angle.reduce_deg(F)).to_positive()
        Fr = F.rad()
        # Compute the expressions from D.H. Eckhardt 1981
        rho = (-0.02752 * cos(Mprimer) - 0.02245 * sin(Fr)
               + 0.00684 * cos(Mprimer - 2.0 * Fr) - 0.00293 * cos(2.0 * Fr)
               - 0.00085 * cos(2.0 * (Fr - Dr))
               - 0.00054 * cos(Mprimer - 2.0 * Dr) - 0.0002 * sin(Mprimer + Fr)
               - 0.0002 * cos(Mprimer + 2.0 * Fr) - 0.0002 * cos(Mprimer - Fr)
               + 0.00014 * cos(Mprimer + 2.0 * (Fr - Dr)))
        rho = Angle(rho)
        rhor = rho.rad()
        sigma = (-0.02816 * sin(Mprimer) + 0.02244 * cos(Fr)
                 - 0.00682 * sin(Mprimer - 2.0 * Fr) - 0.00279 * sin(2.0 * Fr)
                 - 0.00083 * sin(2.0 * (Fr - Dr))
                 + 0.00069 * sin(Mprimer - 2.0 * Dr)
                 + 0.0004 * cos(Mprimer + Fr) - 0.00025 * sin(2.0 * Mprimer)
                 - 0.00023 * sin(Mprimer + 2.0 * Fr)
                 + 0.0002 * cos(Mprimer - Fr) + 0.00019 * sin(Mprimer - Fr)
                 + 0.00013 * sin(Mprimer + 2.0 * (Fr - Dr))
                 - 0.0001 * cos(Mprimer - 3.0 * Fr))
        sigma = Angle(sigma)
        # Compute the parameters 'v', 'x', 'y' and 'w'
        v = Omega + deltaPsi + (sigma / sinI)
        vr = v.rad()
        x = sin(ir + rhor) * sin(vr)
        y = sin(ir + rhor) * cos(vr) * cos(epsr) - cos(ir + rhor) * sin(epsr)
        w = atan2(x, y)
        # Now, let's call the method apparent_equatorial_pos()
        alpha, dec, Delta, ppi = Moon.apparent_equatorial_pos(epoch)
        alphar = alpha.rad()
        # Get the Moon librations
        lopt, bopt, lphys, bphys, ltot, btot = Moon.moon_librations(epoch)
        p = asin((sqrt(x * x + y * y) * cos(alphar - w)) / cos(btot.rad()))
        return Angle(p, radians=True)


def main():

    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Let's show some uses of Saturn class
    print("\n" + 35 * "*")
    print("*** Use of Moon class")
    print(35 * "*" + "\n")

    # Let's compute the Moon geocentric ecliiptical position for a given epoch
    epoch = Epoch(1992, 4, 12.0)
    Lambda, Beta, Delta, ppi = Moon.geocentric_ecliptical_pos(epoch)
    print_me("Longitude (Lambda)", round(Lambda, 6))    # 133.162655
    print_me("Latitude (Beta)", round(Beta, 6))         # -3.229126
    print_me("Distance (Delta)", round(Delta, 1))       # 368409.7
    print_me("Equatorial horizontal parallax (Pi)", round(ppi, 6))  # 0.991990

    print("")

    # Now let's compute the apparent ecliptical position
    epoch = Epoch(1992, 4, 12.0)
    Lambda, Beta, Delta, ppi = Moon.apparent_ecliptical_pos(epoch)
    print_me("Longitude (Lambda)", round(Lambda, 6))    # 133.167264
    print_me("Latitude (Beta)", round(Beta, 6))         # -3.229126
    print_me("Distance (Delta)", round(Delta, 1))       # 368409.7
    print_me("Equatorial horizontal parallax (Pi)", round(ppi, 6))  # 0.991990

    print("")

    # Get the apparent equatorial position
    epoch = Epoch(1992, 4, 12.0)
    ra, dec, Delta, ppi = Moon.apparent_equatorial_pos(epoch)
    print_me("Right Ascension (ra)", round(ra, 6))      # 134.688469
    print_me("Declination (dec)", round(dec, 6))        # 13.768367
    print_me("Distance (Delta)", round(Delta, 1))       # 368409.7
    print_me("Equatorial horizontal parallax (Pi)", round(ppi, 6))  # 0.991990

    print("")

    # Compute the longitude of the Moon's mean ascending node
    epoch = Epoch(1913, 5, 27.0)
    Omega = Moon.longitude_mean_ascending_node(epoch)
    print_me("Longitude of the mean ascending node", round(Omega, 1))   # 0.0
    epoch = Epoch(1959, 12, 7.0)
    Omega = Moon.longitude_mean_ascending_node(epoch)
    print_me("Longitude of the mean ascending node", round(Omega, 1))   # 180.0

    print("")

    # Get the longitude of the Moonś true ascending node
    epoch = Epoch(1913, 5, 27.0)
    Omega = Moon.longitude_true_ascending_node(epoch)
    print_me("Longitude of the true ascending node", round(Omega, 4))  # 0.8763

    print("")

    # Compute the longitude of the Moon's mean perigee
    epoch = Epoch(2021, 3, 5.0)
    Pi = Moon.longitude_mean_perigee(epoch)
    print_me("Longitude of the mean perigee", round(Pi, 5))     # 224.89194

    print("")

    # Compute the approximate illuminated fraction of the Moon's disk
    epoch = Epoch(1992, 4, 12.0)
    k = Moon.illuminated_fraction_disk(epoch)
    print_me("Approximate illuminated fraction of Moon's disk", round(k, 2))
    # 0.68

    print("")

    # Compute the position angle of the bright limb of the Moon
    epoch = Epoch(1992, 4, 12.0)
    xi = Moon.position_bright_limb(epoch)
    print_me("Position angle of the bright limb of the Moon", round(xi, 1))
    # 285.0

    print("")

    # Calculate the instant of a New Moon
    epoch = Epoch(1977, 2, 15.0)
    new_moon = Moon.moon_phase(epoch, target="new")
    y, m, d, h, mi, s = new_moon.get_full_date()
    print("New Moon: {}/{}/{} {}:{}:{}".format(y, m, d, h, mi, round(s)))
    # 1977/2/18 3:37:42

    # Calculate the time of a Last Quarter
    epoch = Epoch(2044, 1, 1.0)
    new_moon = Moon.moon_phase(epoch, target="last")
    y, m, d, h, mi, s = new_moon.get_full_date()
    print("Last Quarter: {}/{}/{} {}:{}:{}".format(y, m, d, h, mi, round(s)))
    # 2044/1/21 23:48:17

    print("")

    # Compute the time and parallax of apogee
    epoch = Epoch(1988, 10, 1.0)
    apogee, parallax = Moon.moon_perigee_apogee(epoch, target="apogee")
    y, m, d, h, mi, s = apogee.get_full_date()
    print("Apogee epoch: {}/{}/{} {}:{}".format(y, m, d, h, mi))
    # 1988/10/7 20:30
    print_me("Equatorial horizontal parallax", parallax.dms_str(n_dec=3))
    # 54' 0.679''

    print("")

    # Compute the time of passage by the ascending node
    epoch = Epoch(1987, 5, 15.0)
    passage = Moon.moon_passage_nodes(epoch, target="ascending")
    y, m, d, h, mi, s = passage.get_full_date()
    mi += s/60.0
    print("Passage by the ascending node: {}/{}/{} {}:{}".format(y,
                                                                 m,
                                                                 d,
                                                                 h,
                                                                 round(mi)))
    # 1987/5/23 6:26

    print("")

    # Compute the epoch and amplitude of maximum southern declination
    epoch = Epoch(2049, 4, 15.0)
    epo, dec = Moon.moon_maximum_declination(epoch, target='southern')
    y, m, d, h, mi, s = epo.get_full_date()
    print("Epoch of maximum declination: {}/{}/{} {}:{}".format(y, m, d, h,
                                                                mi))
    # 2049/4/21 14:0
    print_me("Amplitude of maximum declination", dec.dms_str(n_dec=0))
    # -22d 8' 18.0''

    print("")

    # Compute the librations of the Moon
    epoch = Epoch(1992, 4, 12.0)
    lopt, bopt, lphys, bphys, ltot, btot = Moon.moon_librations(epoch)
    print_me("Optical libration in longitude", round(lopt, 3))
    # -1.206
    print_me("Optical libration in latitude", round(bopt, 3))
    # 4.194
    print_me("Physical libration in longitude", round(lphys, 3))
    # -0.025
    print_me("Physical libration in latitude", round(bphys, 3))
    # 0.006
    print_me("Total libration in longitude", round(lphys, 2))
    # -1.23
    print_me("Total libration in latitude", round(bphys, 3))
    # 4.2

    print("")

    # Let's calculate the position angle of the Moon's axis of rotation
    epoch = Epoch(1992, 4, 12.0)
    p = Moon.moon_position_angle_axis(epoch)
    print_me("Position angle of Moon's axis of rotation", round(p, 2))
    # 15.08


if __name__ == "__main__":

    main()
