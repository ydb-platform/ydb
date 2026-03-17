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


from math import sin, cos, sqrt, asin, atan2

from pymeeus.Angle import Angle
from pymeeus.Epoch import Epoch, JDE2000
from pymeeus.Sun import Sun


"""
.. module:: Pluto
   :synopsis: Class to model Pluto minor planet
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Dagoberto Salazar
"""


PLUTO_ARGUMENT = [
    (0.0, 0.0, 1.0),
    (0.0, 0.0, 2.0),
    (0.0, 0.0, 3.0),
    (0.0, 0.0, 4.0),
    (0.0, 0.0, 5.0),
    (0.0, 0.0, 6.0),
    (0.0, 1.0, -1.0),
    (0.0, 1.0, 0.0),
    (0.0, 1.0, 1.0),
    (0.0, 1.0, 2.0),
    (0.0, 1.0, 3.0),
    (0.0, 2.0, -2.0),
    (0.0, 2.0, -1.0),
    (0.0, 2.0, 0.0),
    (1.0, -1.0, 0.0),
    (1.0, -1.0, 1.0),
    (1.0, 0.0, -3.0),
    (1.0, 0.0, -2.0),
    (1.0, 0.0, -1.0),
    (1.0, 0.0, 0.0),
    (1.0, 0.0, 1.0),
    (1.0, 0.0, 2.0),
    (1.0, 0.0, 3.0),
    (1.0, 0.0, 4.0),
    (1.0, 1.0, -3.0),
    (1.0, 1.0, -2.0),
    (1.0, 1.0, -1.0),
    (1.0, 1.0, 0.0),
    (1.0, 1.0, 1.0),
    (1.0, 1.0, 3.0),
    (2.0, 0.0, -6.0),
    (2.0, 0.0, -5.0),
    (2.0, 0.0, -4.0),
    (2.0, 0.0, -3.0),
    (2.0, 0.0, -2.0),
    (2.0, 0.0, -1.0),
    (2.0, 0.0, 0.0),
    (2.0, 0.0, 1.0),
    (2.0, 0.0, 2.0),
    (2.0, 0.0, 3.0),
    (3.0, 0.0, -2.0),
    (3.0, 0.0, -1.0),
    (3.0, 0.0, 0.0)
]
"""This table contains Pluto's argument coefficients according to Table 37.A in
Meeus' book, page 265."""


PLUTO_LONGITUDE = [
    (-19799805.0, 19850055.0),
    (897144.0, -4954829.0),
    (611149.0, 1211027.0),
    (-341243.0, -189585.0),
    (129287.0, -34992.0),
    (-38164.0, 30893.0),
    (20442.0, -9987.0),
    (-4063.0, -5071.0),
    (-6016.0, -3336.0),
    (-3956.0, 3039.0),
    (-667.0, 3572.0),
    (1276.0, 501.0),
    (1152.0, -917.0),
    (630.0, -1277.0),
    (2571.0, -459.0),
    (899.0, -1449.0),
    (-1016.0, 1043.0),
    (-2343.0, -1012.0),
    (7042.0, 788.0),
    (1199.0, -338.0),
    (418.0, -67.0),
    (120.0, -274.0),
    (-60.0, -159.0),
    (-82.0, -29.0),
    (-36.0, -29.0),
    (-40.0, 7.0),
    (-14.0, 22.0),
    (4.0, 13.0),
    (5.0, 2.0),
    (-1.0, 0.0),
    (2.0, 0.0),
    (-4.0, 5.0),
    (4.0, -7.0),
    (14.0, 24.0),
    (-49.0, -34.0),
    (163.0, -48.0),
    (9.0, -24.0),
    (-4.0, 1.0),
    (-3.0, 1.0),
    (1.0, 3.0),
    (-3.0, -1.0),
    (5.0, -3.0),
    (0.0, 0.0)
]
"""This table contains the periodic terms to compute Pluto's heliocentric
longitude according to Table 37.A in Meeus' book, page 265"""


PLUTO_LATITUDE = [
    (-5452852.0, -14974862),
    (3527812.0, 1672790.0),
    (-1050748.0, 327647.0),
    (178690.0, -292153.0),
    (18650.0, 100340.0),
    (-30697.0, -25823.0),
    (4878.0, 11248.0),
    (226.0, -64.0),
    (2030.0, -836.0),
    (69.0, -604.0),
    (-247.0, -567.0),
    (-57.0, 1.0),
    (-122.0, 175.0),
    (-49.0, -164.0),
    (-197.0, 199.0),
    (-25.0, 217.0),
    (589.0, -248.0),
    (-269.0, 711.0),
    (185.0, 193.0),
    (315.0, 807.0),
    (-130.0, -43.0),
    (5.0, 3.0),
    (2.0, 17.0),
    (2.0, 5.0),
    (2.0, 3.0),
    (3.0, 1.0),
    (2.0, -1.0),
    (1.0, -1.0),
    (0.0, -1.0),
    (0.0, 0.0),
    (0.0, -2.0),
    (2.0, 2.0),
    (-7.0, 0.0),
    (10.0, -8.0),
    (-3.0, 20.0),
    (6.0, 5.0),
    (14.0, 17.0),
    (-2.0, 0.0),
    (0.0, 0.0),
    (0.0, 0.0),
    (0.0, 1.0),
    (0.0, 0.0),
    (1.0, 0.0)
]
"""This table contains the periodic terms to compute Pluto's heliocentric
latitude according to Table 37.A in Meeus' book, page 265"""


PLUTO_RADIUS_VECTOR = [
    (66865439.0, 68951812.0),
    (-11827535.0, -332538.0),
    (1593179.0, -1438890.0),
    (-18444.0, 483220.0),
    (-65977.0, -85431.0),
    (31174.0, -6032.0),
    (-5794.0, 22161.0),
    (4601.0, 4032.0),
    (-1729.0, 234.0),
    (-415.0, 702.0),
    (239.0, 723.0),
    (67.0, -67.0),
    (1034.0, -451.0),
    (-129.0, 504.0),
    (480.0, -231.0),
    (2.0, -441.0),
    (-3359.0, 265.0),
    (7856.0, -7832.0),
    (36.0, 45763.0),
    (8663.0, 8547.0),
    (-809.0, -769.0),
    (263.0, -144.0),
    (-126.0, 32.0),
    (-35.0, -16.0),
    (-19.0, -4.0),
    (-15.0, 8.0),
    (-4.0, 12.0),
    (5.0, 6.0),
    (3.0, 1.0),
    (6.0, -2.0),
    (2.0, 2.0),
    (-2.0, -2.0),
    (14.0, 13.0),
    (-63.0, 13.0),
    (136.0, -236.0),
    (273.0, 1065.0),
    (251.0, 149.0),
    (-25.0, -9.0),
    (9.0, -2.0),
    (-8.0, 7.0),
    (2.0, -10.0),
    (19.0, 35.0),
    (10.0, 3.0)
]
"""This table contains the periodic terms to compute Pluto's heliocentric
radius vector according to Table 37.A in Meeus' book, page 265"""


class Pluto(object):
    """
    Class Pluto models that minor planet.
    """

    @staticmethod
    def geometric_heliocentric_position(epoch):
        """This method computes the geometric heliocentric position of planet
        Pluto for a given epoch.

        :param epoch: Epoch to compute Pluto position, as an Epoch object
        :type epoch: :py:class:`Epoch`

        :returns: A tuple with the heliocentric longitude and latitude (as
            :py:class:`Angle` objects), and the radius vector (as a float,
            in astronomical units), in that order
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.
        :raises: ValueError if input epoch outside the 1885-2099 range.

        >>> epoch = Epoch(1992, 10, 13.0)
        >>> l, b, r = Pluto.geometric_heliocentric_position(epoch)
        >>> print(round(l, 5))
        232.74071
        >>> print(round(b, 5))
        14.58782
        >>> print(round(r, 6))
        29.711111
        """

        # First check that input value is of correct types
        if not isinstance(epoch, Epoch):
            raise TypeError("Invalid input type")
        # Check that the input epoch is within valid range
        y = epoch.year()
        if y < 1885.0 or y > 2099.0:
            raise ValueError("Epoch outside the 1885-2099 range")
        t = (epoch - JDE2000) / 36525.0
        jj = 34.35 + 3034.9057 * t
        ss = 50.08 + 1222.1138 * t
        pp = 238.96 + 144.96 * t
        # Compute the arguments
        corr_lon = 0.0
        corr_lat = 0.0
        corr_rad = 0.0
        for n, argument in enumerate(PLUTO_ARGUMENT):
            iii, jjj, kkk = argument
            alpha = Angle(iii * jj + jjj * ss + kkk * pp).to_positive()
            alpha = alpha.rad()
            sin_a = sin(alpha)
            cos_a = cos(alpha)
            a_lon, b_lon = PLUTO_LONGITUDE[n]
            corr_lon += a_lon * sin_a + b_lon * cos_a
            a_lat, b_lat = PLUTO_LATITUDE[n]
            corr_lat += a_lat * sin_a + b_lat * cos_a
            a_rad, b_rad = PLUTO_RADIUS_VECTOR[n]
            corr_rad += a_rad * sin_a + b_rad * cos_a
        # The coefficients in the tables were scaled up. Let's scale them down
        corr_lon /= 1000000.0
        corr_lat /= 1000000.0
        corr_rad /= 10000000.0
        lon = Angle(238.958116 + 144.96 * t + corr_lon)
        lat = Angle(-3.908239 + corr_lat)
        radius = 40.7241346 + corr_rad
        return lon, lat, radius

    @staticmethod
    def geocentric_position(epoch):
        """This method computes the geocentric position of Pluto (right
        ascension and declination) for the given epoch, for the standard
        equinox J2000.0.

        :param epoch: Epoch to compute geocentric position, as an Epoch object
        :type epoch: :py:class:`Epoch`

        :returns: A tuple containing the right ascension and the declination as
            Angle objects
        :rtype: tuple
        :raises: TypeError if input value is of wrong type.
        :raises: ValueError if input epoch outside the 1885-2099 range.

        >>> epoch = Epoch(1992, 10, 13.0)
        >>> ra, dec = Pluto.geocentric_position(epoch)
        >>> print(ra.ra_str(n_dec=1))
        15h 31' 43.7''
        >>> print(dec.dms_str(n_dec=0))
        -4d 27' 29.0''
        """

        # First check that input value is of correct types
        if not isinstance(epoch, Epoch):
            raise TypeError("Invalid input type")
        # Check that the input epoch is within valid range
        y = epoch.year()
        if y < 1885.0 or y > 2099.0:
            raise ValueError("Epoch outside the 1885-2099 range")
        # Compute the heliocentric position of Pluto
        ll, b, r = Pluto.geometric_heliocentric_position(epoch)
        # Change angles to radians
        ll = ll.rad()
        b = b.rad()
        # Values corresponding to obliquity of ecliptic (epsilon) for J2000.0
        sine = 0.397777156
        cose = 0.917482062
        x = r * cos(ll) * cos(b)
        y = r * (sin(ll) * cos(b) * cose - sin(b) * sine)
        z = r * (sin(ll) * cos(b) * sine + sin(b) * cose)
        # Compute Sun's J2000.0 rectacngular coordinates
        xs, ys, zs = Sun.rectangular_coordinates_j2000(epoch)
        # Compute auxiliary quantities
        xi = x + xs
        eta = y + ys
        zeta = z + zs
        # Compute Pluto's distance to Earth
        delta = sqrt(xi * xi + eta * eta + zeta * zeta)
        # Get the light-time difference
        tau = 0.0057755183 * delta
        # Repeat the computations using the light-time correction
        ll, b, r = Pluto.geometric_heliocentric_position(epoch - tau)
        # Change angles to radians
        ll = ll.rad()
        b = b.rad()
        x = r * cos(ll) * cos(b)
        y = r * (sin(ll) * cos(b) * cose - sin(b) * sine)
        z = r * (sin(ll) * cos(b) * sine + sin(b) * cose)
        # Compute auxiliary quantities
        xi = x + xs
        eta = y + ys
        zeta = z + zs
        # Compute Pluto's distance to Earth
        delta = sqrt(xi * xi + eta * eta + zeta * zeta)
        # Compute right ascension and declination
        alpha = Angle(atan2(eta, xi), radians=True)
        dec = Angle(asin(zeta / delta), radians=True)
        return alpha.to_positive(), dec


def main():

    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Let's show some uses of Pluto class
    print("\n" + 35 * "*")
    print("*** Use of Pluto class")
    print(35 * "*" + "\n")

    # Let's now compute the heliocentric position for a given epoch
    epoch = Epoch(1992, 10, 13.0)
    lon, lat, r = Pluto.geometric_heliocentric_position(epoch)
    print_me("Geometric Heliocentric Longitude", lon.to_positive())
    print_me("Geometric Heliocentric Latitude", lat)
    print_me("Radius vector", r)

    print("")

    # Compute the geocentric position for 1992/10/13:
    epoch = Epoch(1992, 10, 13.0)
    ra, dec = Pluto.geocentric_position(epoch)
    print_me("Right ascension", ra.ra_str(n_dec=1))
    print_me("Declination", dec.dms_str(n_dec=1))


if __name__ == "__main__":

    main()
