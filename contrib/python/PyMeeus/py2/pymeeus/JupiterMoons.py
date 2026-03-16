# -*- coding: utf-8 -*-


# PyMeeus: Python module implementing astronomical algorithms.
# Copyright (C) 2020  Michael Lutz, Sophie Scholz, Vittorio Serra, Sebastian
# Veigl
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


from math import sin, cos, sqrt, atan, atan2, radians

from pymeeus.Epoch import Epoch
from pymeeus.Jupiter import Jupiter
from pymeeus.Sun import Sun


"""
.. module:: JupiterMoons
   :synopsis: Class to model Jupiter's galilean moons
   :license: GNU Lesser General Public License v3 (LGPLv3)

.. moduleauthor:: Michael Lutz, Sophie Scholz, Vittorio Serra, Sebastian Veigl
"""


class JupiterMoons(object):
    """
    Class JupiterMoons models the four galilean satellites of Jupiter. With:
    1: Io
    2: Europa
    3: Ganymede
    4: Callisto
    The algorithm used can be found in chapter 44 (high accuracy method) of
    Meeus' book Astronomic Algorithms
    """

    @staticmethod
    def jupiter_system_angles(epoch):
        """ This method computes the ascending node of Jupiter as well as
        the node of the equator of Jupiter on the ecliptic (psi).

        :param epoch: Epoch to compute satellites' positions, as an Epoch
            object
        :type epoch: :py:class:`Epoch`
        :returns: Two float values with the ascending node of Jupiter and psi
        :rtype: (float, float)

        >>> utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        >>> psi_corrected, OMEGA_ascending_node_jupiter = \
        JupiterMoons.jupiter_system_angles(utc_1992_12_16_00_00_00)
        >>> print(round(psi_corrected, 9))
        317.105800921
        >>> print(round(OMEGA_ascending_node_jupiter, 9))
        100.39249943
        """
        # Calculate solar coordinates
        O, beta, R = Sun.geometric_geocentric_position(epoch)

        # Compute distance Earth - Jupiter (DELTA) by iteration (start value:
        # DELTA = 5 AU)
        DELTA_old = -1.0
        DELTA = 5.0
        x = 0.0
        y = 0.0
        z = 0.0
        tau = 0.0
        iterations = 0

        while DELTA != DELTA_old and iterations < 5:
            # Calculate light-time delay
            tau = 0.0057755183 * DELTA

            l, b, r = Jupiter.geometric_heliocentric_position(epoch - tau)

            x = r * cos(b.rad()) * cos(l.rad()) + R * cos(O.rad())
            y = r * cos(b.rad()) * sin(l.rad()) + R * sin(O.rad())
            z = r * sin(b.rad()) + R * sin(beta.rad())

            DELTA_old = DELTA
            DELTA = sqrt(x ** 2 + y ** 2 + z ** 2)
            iterations += 1

        # t is time since JDE 2433000.5 - light time (tau)
        t = epoch.jde() - 2443000.5 - tau

        # Longitude of the node of the equator of Jupiter on the ecliptic
        psi = 316.5182 - 0.00000208 * t

        # Calculate precession since epoch B1950.0
        T_0 = (epoch.jde() - 2433282.423) / 36525

        # Precession in longitude from the epoch B1950.0 in deg
        P = 1.3966626 * T_0 + 0.0003088 * (T_0 ** 2)

        psi_corrected = psi + P

        # Calculate longitude of ascending node (
        # OMEGA_ascending_node_jupiter) and inclination on
        # the plane of the ecliptic (i_ecliptic_jupiter) in deg
        JC_jupiter_angles = (epoch.jde() - tau - 2451545) / 36525

        OMEGA_ascending_node_jupiter = 100.464407 + 1.0209774 * \
            JC_jupiter_angles + 0.00040315 * (
                JC_jupiter_angles ** 2) + \
            0.000000404 * (
                JC_jupiter_angles ** 3)

        return psi_corrected, OMEGA_ascending_node_jupiter

    @staticmethod
    def rectangular_positions_jovian_equatorial(
            epoch, tofk5=True, solar=False, do_correction=True):
        """This method computes the rectangular geocentric position of
        Jupiter's satellites for a given epoch, using the E5-theory.

        :param epoch: Epoch to compute satellites' positions, as an Epoch
            object
        :type epoch: :py:class:`Epoch`
        :param tofk5: Whether or not the small correction to convert to the FK5
            system will be applied or not
        :type tofk5: bool
        :param solar: Whether the satellites' positions are observed from the
            Earth (False) or the Sun (True)
        :type solar: bool
        :param do_correction: Whether the satellites' positions is corrected
            for the effect of different light-time and perspective effect or
            not
        :type do_correction: bool

        :returns: Four tuple with the rectangular X, Y and Z coordinates in
            Jupiter radii (float), where Z is positive if the satellite is more
            distant than Jupiter.
        :rtype: tuple
        :raises: TypeError if input values are of wrong type.

        >>> utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        >>> io, europa, ganymede, callisto = \
        JupiterMoons.rectangular_positions_jovian_equatorial( \
        utc_1992_12_16_00_00_00)
        >>> print(io)
        (-3.450168811390241, 0.21370246960509387, -4.818966623735296)
        >>> print(europa)
        (7.441869121153001, 0.27524463479625677, -5.747104399729193)
        >>> print(ganymede)
        (1.201111684800708, 0.5899903274317162, -14.940581367576527)
        >>> print(callisto)
        (7.072022641384802, 1.0289678450543338, -25.224420329457175)
        """
        # Calculate solar coordinates
        O, beta, R = Sun.geometric_geocentric_position(epoch, tofk5)

        if solar:  # Observed from the sun
            R = 0

        # Compute distance Earth - Jupiter (DELTA) by iteration (start value:
        # DELTA = 5 AU)
        DELTA_old = -1.0
        DELTA = 5.0
        x = 0.0
        y = 0.0
        z = 0.0
        tau = 0.0

        while DELTA != DELTA_old:
            # Calculate light-time delay
            tau = 0.0057755183 * DELTA

            l, b, r = Jupiter.geometric_heliocentric_position(epoch - tau)

            x = r * cos(b.rad()) * cos(l.rad()) + R * cos(O.rad())
            y = r * cos(b.rad()) * sin(l.rad()) + R * sin(O.rad())
            z = r * sin(b.rad()) + R * sin(beta.rad())

            DELTA_old = DELTA
            DELTA = sqrt(x ** 2 + y ** 2 + z ** 2)

        # Calculate Jupiter's geocentric longitude lambda_0 and latitute beta_0
        lambda_0 = atan2(y, x)
        beta_0 = atan(z / (sqrt(x ** 2 + y ** 2)))

        # t is time since JDE 2433000.5 - light time (tau)
        t = epoch.jde() - 2443000.5 - tau

        # Mean longitudes of the satellites
        l_1 = 106.07719 + 203.488955790 * t
        l_2 = 175.73161 + 101.374724735 * t
        l_3 = 120.55883 + 50.317609207 * t
        l_4 = 84.44459 + 21.571071177 * t

        # Longitudes of the perijoves
        pi_1 = 97.0881 + 0.16138586 * t
        pi_2 = 154.8663 + 0.04726307 * t
        pi_3 = 188.1840 + 0.00712734 * t
        pi_4 = 335.2868 + 0.00184000 * t

        # Longitudes of the nodes on the equatorial plane of Jupiter
        omega_1 = 312.3346 - 0.13279386 * t
        omega_2 = 100.4411 - 0.03263064 * t
        omega_3 = 119.1942 - 0.00717703 * t
        omega_4 = 322.6186 - 0.00175934 * t

        # Principal inequality in the longitude of Jupiter
        GAMMA = 0.33033 * sin(radians(163.679 + 0.0010512 * t)) + \
            0.03439 * sin(radians(34.486 - 0.0161731 * t))

        # Calculate libration for inner satellites
        PHI_lambda = 199.6766 + 0.17379190 * t

        # Longitude of the node of the equator of Jupiter on the ecliptic
        psi = 316.5182 - 0.00000208 * t

        # Mean anomalies of Jupiter and Saturn
        G = 30.23756 + 0.0830925701 * t + GAMMA
        G_ = 31.97853 + 0.0334597339 * t

        # Longitude of the perihelion of Jupiter
        PI = 13.469942

        # Periodic terms in the longitudes of the satellites
        sum1 = 0
        sum1 += 0.47259 * sin(2 * radians(l_1 - l_2))
        sum1 -= 0.03478 * sin(radians(pi_3 - pi_4))
        sum1 += 0.01081 * sin(radians(l_2 - 2 * l_3 + pi_3))
        sum1 += 0.00738 * sin(radians(PHI_lambda))
        sum1 += 0.00713 * sin(radians(l_2 - 2 * l_3 + pi_2))
        sum1 -= 0.00674 * sin(radians(pi_1 + pi_3 - 2 * PI - 2 * G))
        sum1 += 0.00666 * sin(radians(l_2 - 2 * l_3 + pi_4))
        sum1 += 0.00445 * sin(radians(l_1 - pi_3))
        sum1 -= 0.00354 * sin(radians(l_1 - l_2))
        sum1 -= 0.00317 * sin(radians(2 * psi - 2 * PI))
        sum1 += 0.00265 * sin(radians(l_1 - pi_4))
        sum1 -= 0.00186 * sin(radians(G))
        sum1 += 0.00162 * sin(radians(pi_2 - pi_3))
        sum1 += 0.00158 * sin(radians(4 * (l_1 - l_2)))
        sum1 -= 0.00155 * sin(radians(l_1 - l_3))
        sum1 -= 0.00138 * sin(radians(psi + omega_3 - 2 * PI - 2 * G))
        sum1 -= 0.00115 * sin(radians(2 * (l_1 - 2 * l_2 + omega_2)))
        sum1 += 0.00089 * sin(radians(pi_2 - pi_4))
        sum1 += 0.00085 * sin(radians(l_1 + pi_3 - 2 * PI - 2 * G))
        sum1 += 0.00083 * sin(radians(omega_2 - omega_3))
        sum1 += 0.00053 * sin(radians(psi - omega_2))

        sum2 = 0
        sum2 += 1.06476 * sin(radians(2 * (l_2 - l_3)))
        sum2 += 0.04256 * sin(radians(l_1 - 2 * l_2 + pi_3))
        sum2 += 0.03581 * sin(radians(l_2 - pi_3))
        sum2 += 0.02395 * sin(radians(l_1 - 2 * l_2 + pi_4))
        sum2 += 0.01984 * sin(radians(l_2 - pi_4))
        sum2 -= 0.01778 * sin(radians(PHI_lambda))
        sum2 += 0.01654 * sin(radians(l_2 - pi_2))
        sum2 += 0.01334 * sin(radians(l_2 - 2 * l_3 + pi_2))
        sum2 += 0.01294 * sin(radians(pi_3 - pi_4))
        sum2 -= 0.01142 * sin(radians(l_2 - l_3))
        sum2 -= 0.01057 * sin(radians(G))
        sum2 -= 0.00775 * sin(radians(2 * (psi - PI)))
        sum2 += 0.00524 * sin(radians(2 * (l_1 - l_2)))
        sum2 -= 0.00460 * sin(radians((l_1 - l_3)))
        sum2 += 0.00316 * sin(radians(psi - 2 * G + omega_3 - 2 * PI))
        sum2 -= 0.00203 * sin(radians(pi_1 + pi_3 - 2 * PI - 2 * G))
        sum2 += 0.00146 * sin(radians(psi - omega_3))
        sum2 -= 0.00145 * sin(radians(2 * G))
        sum2 += 0.00125 * sin(radians(psi - omega_4))
        sum2 -= 0.00115 * sin(radians(l_1 - 2 * l_3 + pi_3))
        sum2 -= 0.00094 * sin(radians(2 * (l_2 - omega_2)))
        sum2 += 0.00086 * sin(radians(2 * (l_1 - 2 * l_2 + omega_2)))
        sum2 -= 0.00086 * sin(radians(5 * G_ - 2 * G + 52.225))
        sum2 -= 0.00078 * sin(radians(l_2 - l_4))
        sum2 -= 0.00064 * sin(radians(3 * l_3 - 7 * l_4 + 4 * pi_4))
        sum2 += 0.00064 * sin(radians(pi_1 - pi_4))
        sum2 -= 0.00063 * sin(radians(l_1 - 2 * l_3 + pi_4))
        sum2 += 0.00058 * sin(radians(omega_3 - omega_4))
        sum2 += 0.00056 * sin(radians(2 * (psi - PI - G)))
        sum2 += 0.00056 * sin(radians(2 * (l_2 - l_4)))
        sum2 += 0.00055 * sin(radians(2 * (l_1 - l_3)))
        sum2 += 0.00052 * sin(radians(3 * l_3 - 7 * l_4 + pi_3 + 3 * pi_4))
        sum2 -= 0.00043 * sin(radians(l_1 - pi_3))
        sum2 += 0.00041 * sin(radians(5 * (l_2 - l_3)))
        sum2 += 0.00041 * sin(radians(pi_4 - PI))
        sum2 += 0.00032 * sin(radians(omega_2 - omega_3))
        sum2 += 0.00032 * sin(radians(2 * (l_3 - G - PI)))

        sum3 = 0
        sum3 += 0.16490 * sin(radians(l_3 - pi_3))
        sum3 += 0.09081 * sin(radians(l_3 - pi_4))
        sum3 -= 0.06907 * sin(radians(l_2 - l_3))
        sum3 += 0.03784 * sin(radians(pi_3 - pi_4))
        sum3 += 0.01846 * sin(radians(2 * (l_3 - l_4)))
        sum3 -= 0.01340 * sin(radians(G))
        sum3 -= 0.01014 * sin(radians(2 * (psi - PI)))
        sum3 += 0.00704 * sin(radians(l_2 - 2 * l_3 + pi_3))
        sum3 -= 0.00620 * sin(radians(l_2 - 2 * l_3 + pi_2))
        sum3 -= 0.00541 * sin(radians(l_3 - l_4))
        sum3 += 0.00381 * sin(radians(l_2 - 2 * l_3 + pi_4))
        sum3 += 0.00235 * sin(radians(psi - omega_3))
        sum3 += 0.00198 * sin(radians(psi - omega_4))
        sum3 += 0.00176 * sin(radians(PHI_lambda))
        sum3 += 0.00130 * sin(radians(3 * (l_3 - l_4)))
        sum3 += 0.00125 * sin(radians(l_1 - l_3))
        sum3 -= 0.00119 * sin(radians(5 * G_ - 2 * G + 52.225))
        sum3 += 0.00109 * sin(radians(l_1 - l_2))
        sum3 -= 0.00100 * sin(radians(3 * l_3 - 7 * l_4 + 4 * pi_4))
        sum3 += 0.00091 * sin(radians(omega_3 - omega_4))
        sum3 += 0.00080 * sin(radians(3 * l_3 - 7 * l_4 + pi_3 + 3 * pi_4))
        sum3 -= 0.00075 * sin(radians(2 * l_2 - 3 * l_3 + pi_3))
        sum3 += 0.00072 * sin(radians(pi_1 + pi_3 - 2 * PI - 2 * G))
        sum3 += 0.00069 * sin(radians(pi_4 - PI))
        sum3 -= 0.00058 * sin(radians(2 * l_3 - 3 * l_4 + pi_4))
        sum3 -= 0.00057 * sin(radians(l_3 - 2 * l_4 + pi_4))
        sum3 += 0.00056 * sin(radians(l_3 + pi_3 - 2 * PI - 2 * G))
        sum3 -= 0.00052 * sin(radians(l_2 - 2 * l_3 + pi_1))
        sum3 -= 0.00050 * sin(radians(pi_2 - pi_3))
        sum3 += 0.00048 * sin(radians(l_3 - 2 * l_4 + pi_3))
        sum3 -= 0.00045 * sin(radians(2 * l_2 - 3 * l_3 + pi_4))
        sum3 -= 0.00041 * sin(radians(pi_2 - pi_4))
        sum3 -= 0.00038 * sin(radians(2 * G))
        sum3 -= 0.00037 * sin(radians(pi_3 - pi_4 + omega_3 - omega_4))
        sum3 -= 0.00032 * sin(radians(3 * l_3 - 7 * l_4 + 2 * pi_3 + 2 * pi_4))
        sum3 += 0.00030 * sin(radians(4 * (l_3 - l_4)))
        sum3 += 0.00029 * sin(radians(l_3 + pi_4 - 2 * PI - 2 * G))
        sum3 -= 0.00028 * sin(radians(omega_3 + psi - 2 * PI - 2 * G))
        sum3 += 0.00026 * sin(radians(l_3 - PI - G))
        sum3 += 0.00024 * sin(radians(l_2 - 3 * l_3 + 2 * l_4))
        sum3 += 0.00021 * sin(radians(2 * (l_3 - PI - G)))
        sum3 -= 0.00021 * sin(radians(l_3 - pi_2))
        sum3 += 0.00017 * sin(radians(2 * (l_3 - pi_3)))

        sum4 = 0
        sum4 += 0.84287 * sin(radians(l_4 - pi_4))
        sum4 += 0.03431 * sin(radians(pi_4 - pi_3))
        sum4 -= 0.03305 * sin(radians(2 * (psi - PI)))
        sum4 -= 0.03211 * sin(radians(G))
        sum4 -= 0.01862 * sin(radians(l_4 - pi_3))
        sum4 += 0.01186 * sin(radians(psi - omega_4))
        sum4 += 0.00623 * sin(radians(l_4 + pi_4 - 2 * G - 2 * PI))
        sum4 += 0.00387 * sin(radians(2 * (l_4 - pi_4)))
        sum4 -= 0.00284 * sin(radians(5 * G_ - 2 * G + 52.225))
        sum4 -= 0.00234 * sin(radians(2 * (psi - pi_4)))
        sum4 -= 0.00223 * sin(radians(l_3 - l_4))
        sum4 -= 0.00208 * sin(radians(l_4 - PI))
        sum4 += 0.00178 * sin(radians(psi + omega_4 - 2 * pi_4))
        sum4 += 0.00134 * sin(radians(pi_4 - PI))
        sum4 += 0.00125 * sin(radians(2 * (l_4 - G - PI)))
        sum4 -= 0.00117 * sin(radians(2 * G))
        sum4 -= 0.00112 * sin(radians(2 * (l_3 - l_4)))
        sum4 += 0.00107 * sin(radians(3 * l_3 - 7 * l_4 + 4 * pi_4))
        sum4 += 0.00102 * sin(radians(l_4 - G - PI))
        sum4 += 0.00096 * sin(radians(2 * l_4 - psi - omega_4))
        sum4 += 0.00087 * sin(radians(2 * (psi - omega_4)))
        sum4 -= 0.00085 * sin(radians(3 * l_3 - 7 * l_4 + pi_3 + 3 * pi_4))
        sum4 += 0.00085 * sin(radians(l_3 - 2 * l_4 + pi_4))
        sum4 -= 0.00081 * sin(radians(2 * (l_4 - psi)))
        sum4 += 0.00071 * sin(radians(l_4 + pi_4 - 2 * PI - 3 * G))
        sum4 += 0.00061 * sin(radians(l_1 - l_4))
        sum4 -= 0.00056 * sin(radians(psi - omega_3))
        sum4 -= 0.00054 * sin(radians(l_3 - 2 * l_4 + pi_3))
        sum4 += 0.00051 * sin(radians(l_2 - l_4))
        sum4 += 0.00042 * sin(radians(2 * (psi - G - PI)))
        sum4 += 0.00039 * sin(radians(2 * (pi_4 - omega_4)))
        sum4 += 0.00036 * sin(radians(psi + PI - pi_4 - omega_4))
        sum4 += 0.00035 * sin(radians(2 * G_ - G + 188.37))
        sum4 -= 0.00035 * sin(radians(l_4 - pi_4 + 2 * PI - 2 * psi))
        sum4 -= 0.00032 * sin(radians(l_4 + pi_4 - 2 * PI - G))
        sum4 += 0.00030 * sin(radians(2 * G_ - 2 * G + 149.15))
        sum4 += 0.00029 * sin(radians(3 * l_3 - 7 * l_4 + 2 * pi_3 + 2 * pi_4))
        sum4 += 0.00028 * sin(radians(l_4 - pi_4 + 2 * psi - 2 * PI))
        sum4 -= 0.00028 * sin(radians(2 * (l_4 - omega_4)))
        sum4 -= 0.00027 * sin(radians(pi_3 - pi_4 + omega_3 - omega_4))
        sum4 -= 0.00026 * sin(radians(5 * G_ - 3 * G + 188.37))
        sum4 += 0.00025 * sin(radians(omega_4 - omega_3))
        sum4 -= 0.00025 * sin(radians(l_2 - 3 * l_3 + 2 * l_4))
        sum4 -= 0.00023 * sin(radians(3 * (l_3 - l_4)))
        sum4 += 0.00021 * sin(radians(2 * l_4 - 2 * PI - 3 * G))
        sum4 -= 0.00021 * sin(radians(2 * l_3 - 3 * l_4 + pi_4))
        sum4 += 0.00019 * sin(radians(l_4 - pi_4 - G))
        sum4 -= 0.00019 * sin(radians(2 * l_4 - pi_3 - pi_4))
        sum4 -= 0.00018 * sin(radians(l_4 - pi_4 + G))
        sum4 -= 0.00016 * sin(radians(l_4 + pi_3 - 2 * PI - 2 * G))

        # True longitudes of the satellites
        L1 = l_1 + sum1
        L2 = l_2 + sum2
        L3 = l_3 + sum3
        L4 = l_4 + sum4

        # Periodic terms in the latitudes of the satellites
        tan_B1 = +0.0006393 * sin(radians(L1 - omega_1)) \
                 + 0.0001825 * sin(radians(L1 - omega_2)) \
                 + 0.0000329 * sin(radians(L1 - omega_3)) \
            - 0.0000311 * sin(radians(L1 - psi)) \
                 + 0.0000093 * sin(radians(L1 - omega_4)) \
                 + 0.0000075 * sin(
            radians(3 * L1 - 4 * l_2 - 1.9927 * sum1 + omega_2)) \
            + 0.0000046 * sin(radians(L1 + psi - 2 * PI - 2 * G))

        tan_B2 = +0.0081004 * sin(radians(L2 - omega_2)) \
                 + 0.0004512 * sin(radians(L2 - omega_3)) \
            - 0.0003284 * sin(radians(L2 - psi)) \
                 + 0.0001160 * sin(radians(L2 - omega_4)) \
                 + 0.0000272 * sin(
            radians(l_1 - 2 * l_3 + 1.0146 * sum2 + omega_2)) \
            - 0.0000144 * sin(radians(L2 - omega_1)) \
            + 0.0000143 * sin(radians(L2 + psi - 2 * PI - 2 * G)) \
            + 0.0000035 * sin(radians(L2 - psi + G)) \
            - 0.0000028 * sin(
            radians(l_1 - 2 * l_3 + 1.0146 * sum2 + omega_3))

        tan_B3 = +0.0032402 * sin(radians(L3 - omega_3)) \
            - 0.0016911 * sin(radians(L3 - psi)) \
                 + 0.0006847 * sin(radians(L3 - omega_4)) \
            - 0.0002797 * sin(radians(L3 - omega_2)) \
                 + 0.0000321 * sin(radians(L3 + psi - 2 * PI - 2 * G)) \
                 + 0.0000051 * sin(radians(L3 - psi + G)) \
            - 0.0000045 * sin(radians(L3 - psi - G)) \
            - 0.0000045 * sin(radians(L3 + psi - 2 * PI)) \
                 + 0.0000037 * sin(radians(L3 + psi - 2 * PI - 3 * G)) \
                 + 0.0000030 * sin(
            radians(2 * l_2 - 3 * L3 + 4.03 * sum3 + omega_2)) \
            - 0.0000021 * \
            sin(radians(2 * l_2 - 3 * L3 + 4.03 * sum3 + omega_3))

        tan_B4 = -0.0076579 * sin(radians(L4 - psi)) \
            + 0.0044134 * sin(radians(L4 - omega_4)) \
                 - 0.0005112 * sin(radians(L4 - omega_3)) \
            + 0.0000773 * sin(radians(L4 + psi - 2 * PI - 2 * G)) \
            + 0.0000104 * sin(radians(L4 - psi + G)) \
                 - 0.0000102 * sin(radians(L4 - psi - G)) \
            + 0.0000088 * sin(radians(L4 + psi - 2 * PI - 3 * G)) \
                 - 0.0000038 * sin(radians(L4 + psi - 2 * PI - G))

        B_1_rad = atan(tan_B1)
        B_2_rad = atan(tan_B2)
        B_3_rad = atan(tan_B3)
        B_4_rad = atan(tan_B4)

        # Periodic terms for the radius vector
        sum_r_1 = -0.0041339 * cos(radians((2 * (l_1 - l_2))))
        sum_r_1 -= 0.0000387 * cos(radians((l_1 - pi_3)))
        sum_r_1 -= 0.0000214 * cos(radians((l_1 - pi_4)))
        sum_r_1 += 0.0000170 * cos(radians((l_1 - l_2)))
        sum_r_1 -= 0.0000131 * cos(radians((4 * (l_1 - l_2))))
        sum_r_1 += 0.0000106 * cos(radians((l_1 - l_3)))
        sum_r_1 -= 0.0000066 * cos(radians((l_1 + pi_3 - 2 * PI - 2 * G)))

        sum_r_2 = +0.0093848 * cos(radians((l_1 - l_2)))
        sum_r_2 -= 0.0003116 * cos(radians((l_2 - pi_3)))
        sum_r_2 -= 0.0001744 * cos(radians((l_2 - pi_4)))
        sum_r_2 -= 0.0001442 * cos(radians((l_2 - pi_2)))
        sum_r_2 += 0.0000553 * cos(radians((l_2 - l_3)))
        sum_r_2 += 0.0000523 * cos(radians((l_1 - l_3)))
        sum_r_2 -= 0.0000290 * cos(radians((2 * (l_1 - l_2))))
        sum_r_2 += 0.0000164 * cos(radians((2 * (l_2 - omega_2))))
        sum_r_2 += 0.0000107 * cos(radians((l_1 - 2 * l_3 + pi_3)))
        sum_r_2 -= 0.0000102 * cos(radians((l_2 - pi_1)))
        sum_r_2 -= 0.0000091 * cos(radians((2 * (l_1 - l_3))))

        sum_r_3 = -0.0014388 * cos(radians((l_3 - pi_3)))
        sum_r_3 -= 0.0007919 * cos(radians((l_3 - pi_4)))
        sum_r_3 += 0.0006342 * cos(radians((l_2 - l_3)))
        sum_r_3 -= 0.0001761 * cos(radians((2 * (l_3 - l_4))))
        sum_r_3 += 0.0000294 * cos(radians((l_3 - l_4)))
        sum_r_3 -= 0.0000156 * cos(radians((3 * (l_3 - l_4))))
        sum_r_3 += 0.0000156 * cos(radians((l_1 - l_3)))
        sum_r_3 -= 0.0000153 * cos(radians((l_1 - l_2)))
        sum_r_3 += 0.0000070 * cos(radians((2 * l_2 - 3 * l_3 + pi_3)))
        sum_r_3 -= 0.0000051 * cos(radians((l_3 + pi_3 - 2 * PI - 2 * G)))

        sum_r_4 = -0.0073546 * cos(radians((l_4 - pi_4)))
        sum_r_4 += 0.0001621 * cos(radians((l_4 - pi_3)))
        sum_r_4 += 0.0000974 * cos(radians((l_3 - l_4)))
        sum_r_4 -= 0.0000543 * cos(radians((l_4 + pi_4 - 2 * PI - 2 * G)))
        sum_r_4 -= 0.0000271 * cos(radians((2 * (l_4 - pi_4))))
        sum_r_4 += 0.0000182 * cos(radians((l_4 - PI)))
        sum_r_4 += 0.0000177 * cos(radians((2 * (l_3 - l_4))))
        sum_r_4 -= 0.0000167 * cos(radians((2 * l_4 - psi - omega_4)))
        sum_r_4 += 0.0000167 * cos(radians((psi - omega_4)))
        sum_r_4 -= 0.0000155 * cos(radians((2 * (l_4 - PI - G))))
        sum_r_4 += 0.0000142 * cos(radians((2 * (l_4 - psi))))
        sum_r_4 += 0.0000105 * cos(radians((l_1 - l_4)))
        sum_r_4 += 0.0000092 * cos(radians((l_2 - l_4)))
        sum_r_4 -= 0.0000089 * cos(radians((l_4 - PI - G)))
        sum_r_4 -= 0.0000062 * cos(radians((l_4 + pi_4 - 2 * PI - 3 * G)))
        sum_r_4 += 0.0000048 * cos(radians((2 * (l_4 - omega_4))))

        # Mean distances of the satellites
        a1 = 5.90569
        a2 = 9.39657
        a3 = 14.98832
        a4 = 26.36273

        # Radius vector of the satellites in equatorial radii of Jupiter
        R_1 = a1 * (1 + sum_r_1)
        R_2 = a2 * (1 + sum_r_2)
        R_3 = a3 * (1 + sum_r_3)
        R_4 = a4 * (1 + sum_r_4)

        # Calculate precession since epoch B1950.0
        T_0 = (epoch.jde() - 2433282.423) / 36525

        # Precession in longitude from the epoch B1950.0 in deg
        P = 1.3966626 * T_0 + 0.0003088 * (T_0 ** 2)

        # Correct all longitudes and psi by precession
        L1_corrected = L1 + P
        L2_corrected = L2 + P
        L3_corrected = L3 + P
        L4_corrected = L4 + P
        psi_corrected = psi + P

        # Cartesian coordinates of the four satellites referred to Jupiter in
        # Jupiter's radii
        X_1 = R_1 * cos(radians(L1_corrected - psi_corrected)) * cos(B_1_rad)
        Y_1 = R_1 * sin(radians(L1_corrected - psi_corrected)) * cos(B_1_rad)
        Z_1 = R_1 * sin(B_1_rad)

        X_2 = R_2 * cos(radians(L2_corrected - psi_corrected)) * cos(B_2_rad)
        Y_2 = R_2 * sin(radians(L2_corrected - psi_corrected)) * cos(B_2_rad)
        Z_2 = R_2 * sin(B_2_rad)

        X_3 = R_3 * cos(radians(L3_corrected - psi_corrected)) * cos(B_3_rad)
        Y_3 = R_3 * sin(radians(L3_corrected - psi_corrected)) * cos(B_3_rad)
        Z_3 = R_3 * sin(B_3_rad)

        X_4 = R_4 * cos(radians(L4_corrected - psi_corrected)) * cos(B_4_rad)
        Y_4 = R_4 * sin(radians(L4_corrected - psi_corrected)) * cos(B_4_rad)
        Z_4 = R_4 * sin(B_4_rad)

        # Fictional fifth satellite
        X_5 = 0
        Y_5 = 0
        Z_5 = 1

        # Calculate longitude of ascending node (
        # OMEGA_asc_node_jup) and inclination on
        # the plane of the ecliptic (i_ecliptic_jupiter) in deg
        JC_jupiter_angles = (epoch.jde() - tau - 2451545) / 36525

        OMEGA_asc_node_jup = 100.464407 + 1.0209774 * \
            JC_jupiter_angles + 0.00040315 * (
                JC_jupiter_angles ** 2) + \
            0.000000404 * (
                JC_jupiter_angles ** 3)
        i_ecliptic_jupiter = 1.303267 - 0.0054965 * JC_jupiter_angles + \
            0.00000466 * (
                JC_jupiter_angles ** 2) - 0.000000002 * (
                JC_jupiter_angles ** 3)

        # Calculate D with the fictional satellite
        D = JupiterMoons.apparent_rectangular_coordinates(epoch, X_5, Y_5, Z_5,
                                                          OMEGA_asc_node_jup,
                                                          psi_corrected,
                                                          i_ecliptic_jupiter,
                                                          lambda_0, beta_0,
                                                          isFictional=True)

        # Calculate rectangular Coordinates X, Y and Z in Jupiter's radii of
        # Io (1), Europa (2),
        # Ganimed (3) and Callisto (4)
        Io = JupiterMoons. \
            apparent_rectangular_coordinates(epoch, X_1, Y_1,
                                             Z_1,
                                             OMEGA_asc_node_jup,
                                             psi_corrected,
                                             i_ecliptic_jupiter,
                                             lambda_0, beta_0, D)
        Europa = JupiterMoons. \
            apparent_rectangular_coordinates(epoch, X_2, Y_2,
                                             Z_2,
                                             OMEGA_asc_node_jup,
                                             psi_corrected,
                                             i_ecliptic_jupiter,
                                             lambda_0,
                                             beta_0, D)
        Ganimed = JupiterMoons. \
            apparent_rectangular_coordinates(epoch, X_3,
                                             Y_3, Z_3,
                                             OMEGA_asc_node_jup,
                                             psi_corrected,
                                             i_ecliptic_jupiter,
                                             lambda_0,
                                             beta_0, D)
        Callisto = JupiterMoons. \
            apparent_rectangular_coordinates(epoch, X_4,
                                             Y_4, Z_4,
                                             OMEGA_asc_node_jup,
                                             psi_corrected,
                                             i_ecliptic_jupiter,
                                             lambda_0,
                                             beta_0, D)

        # Calculate corrected coordinates
        if do_correction:
            Io = JupiterMoons.correct_rectangular_positions(R_1, 1, DELTA, Io)
            Europa = JupiterMoons.correct_rectangular_positions(
                R_2, 2, DELTA, Europa)
            Ganimed = JupiterMoons.correct_rectangular_positions(
                R_3, 3, DELTA, Ganimed)
            Callisto = JupiterMoons.correct_rectangular_positions(
                R_4, 4, DELTA, Callisto)

        return Io, Europa, Ganimed, Callisto

    @staticmethod
    def apparent_rectangular_coordinates(
            epoch, X, Y, Z, OMEGA, psi, i, lambda_0, beta_0, D=0,
            isFictional=False):
        """This method computes the apparent rectangular coordinates of a
        Jupiter satellite for given coordinates.

        :param epoch: Epoch to compute satellite position, as an Epoch object
        :type epoch: :py:class:`Epoch`
        :param X: X-coordinate of the satellite in Jupiter radii
        :type X: float
        :param Y: Y-coordinate of the satellite in Jupiter radii
        :type Y: float
        :param Z: Z-coordinate of the satellite in Jupiter radii
        :type Z: float
        :param OMEGA: longitude of the node of Jupiter
        :type OMEGA: float
        :param psi: longitude of the node of Jupiter
        :type psi: float
        :param i: inclination on the plane of the ecliptic
        :type i: float
        :param beta_0: Jupiter’s geocentric latitude
        :type beta_0: float
        :param lambda_0: Jupiter’s geocentric longitude
        :type lambda_0: float
        :param D: parameter calculated by the fifth fictional satellite
            (fictional satellite has to be calculated first, in order to
            calculate the coordinates of the remaining "true" satellites)
        :type D: float
        :param isFictional: Whether or not the satellite is the fictional
            satellite No. 5
        :type isFictional: bool

        :returns: A tuple with the apparent rectangular coordinates of the
            satellite or parameter D if isFictional=True
        :rtype: tuple, float
        :raises: TypeError if input values are wrong type

        >>> utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        >>> psi_corrected = 317.1058009213959
        >>> i_ecliptic_jupiter = 1.3036541530886598
        >>> lambda_0 = -2.9355662143229146
        >>> beta_0 = 0.021667777174910842
        >>> OMEGA_ascending_node_jupiter= 100.39249942976576
        >>> X_1 =  5.60361395790844
        >>> Y_1 = 1.9398758261880644
        >>> Z_1 = 0.00449258104769796
        >>> X_5 =  0
        >>> Y_5 = 0
        >>> Z_5 = 1
        >>> d = JupiterMoons.apparent_rectangular_coordinates( \
        utc_1992_12_16_00_00_00, X_5, Y_5, Z_5, \
        OMEGA_ascending_node_jupiter, psi_corrected, i_ecliptic_jupiter, \
        lambda_0, beta_0, isFictional=True)
        >>> print(round(d, 10))
        -0.0320562469
        >>> io = JupiterMoons.apparent_rectangular_coordinates( \
        utc_1992_12_16_00_00_00, X_1, Y_1, Z_1, \
        OMEGA_ascending_node_jupiter, psi_corrected, i_ecliptic_jupiter, \
        lambda_0, beta_0, d)
        >>> print(io)
        (-3.4489935969836503, 0.21361563816963675, -4.818966623735296)
        """

        # Checking for type
        if not isinstance(epoch, Epoch):
            raise TypeError("Invalid input types")

        # Time in centuries since 1900.0
        time_JC_1900 = (epoch.jde() - 2415020.50000) / \
            36525  # leave out final .5

        # Inclination of Jupiter's orbit with respect to the orbital plane
        Inc = 3.120262 + 0.0006 * time_JC_1900

        # Rotate towards Jupiter's orbital plane
        A_1 = X
        B_1 = Y * cos(radians(Inc)) - Z * sin(radians(Inc))
        C_1 = Y * sin(radians(Inc)) + Z * cos(radians(Inc))

        # Rotate towards the ascending node of Jupiter's orbit
        PHI = psi - OMEGA
        A_2 = A_1 * cos(radians(PHI)) - B_1 * sin(radians(PHI))
        B_2 = A_1 * sin(radians(PHI)) + B_1 * cos(radians(PHI))
        C_2 = C_1

        # Rotate towards the plane of the ecliptic
        A_3 = A_2
        B_3 = B_2 * cos(radians(i)) - C_2 * sin(radians(i))
        C_3 = B_2 * sin(radians(i)) + C_2 * cos(radians(i))

        # Rotate towards the vernal equinox
        A_4 = A_3 * cos(radians(OMEGA)) - B_3 * sin(radians(OMEGA))
        B_4 = A_3 * sin(radians(OMEGA)) + B_3 * cos(radians(OMEGA))
        C_4 = C_3

        A_5 = A_4 * sin(lambda_0) - B_4 * cos(lambda_0)
        B_5 = A_4 * cos(lambda_0) + B_4 * sin(lambda_0)
        C_5 = C_4

        A_6 = A_5
        B_6 = C_5 * sin(beta_0) + B_5 * cos(beta_0)
        C_6 = C_5 * cos(beta_0) - B_5 * sin(beta_0)

        if isFictional:  # for fictional satellite No. 5
            # Calculate D
            zeta = A_6
            eta = C_6
            D = atan2(zeta, eta)

            return D
        else:  # for remaining true satellites No. 1-4
            X = A_6 * cos(D) - C_6 * sin(D)
            Y = A_6 * sin(D) + C_6 * cos(D)
            Z = B_6

            return X, Y, Z

    @staticmethod
    def calculate_delta(epoch):
        """This method calculates the distance between Earth and Jupiter
        (DELTA) for a given epoch by iteration.

        :param epoch: Epoch the distance should be calculated for.
        :type epoch: :py:class:`Epoch`

        :returns: Distance Earth-Jupiter in AU and light-time delay from
            Earth-Jupiter, together with Coordinates of Jupiter
        :rtype: tuple

        :raises: TypeError if input values are wrong type

        >>> utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        >>> delta, tau, l, b, r = JupiterMoons.calculate_delta( \
        utc_1992_12_16_00_00_00)
        >>> print(round(delta, 10))
        5.6611211815
        >>> print(round(tau, 10))
        0.032695909
        """

        # Calculate solar coordinates
        O, beta, R = Sun.geometric_geocentric_position(epoch)

        # Compute distance Earth - Jupiter (DELTA) by iteration (start value:
        # DELTA = 5 AU)
        DELTA_old = -1.0
        DELTA = 5.0
        x = 0.0
        y = 0.0
        z = 0.0
        tau = 0.0
        l, b, r = 0, 0, 0
        iterations = 0

        while DELTA != DELTA_old and iterations < 5:
            # Calculate light-time delay
            tau = 0.0057755183 * DELTA

            l, b, r = Jupiter.geometric_heliocentric_position(epoch - tau)

            x = r * cos(b.rad()) * cos(l.rad()) + R * cos(O.rad())
            y = r * cos(b.rad()) * sin(l.rad()) + R * sin(O.rad())
            z = r * sin(b.rad()) + R * sin(beta.rad())

            DELTA_old = DELTA
            DELTA = sqrt(x ** 2 + y ** 2 + z ** 2)
            iterations += 1

        return DELTA, tau, l, b, r

    @staticmethod
    def correct_rectangular_positions(
            R, i_sat, DELTA, X_coordinate, Y_coordinate=0, Z_coordinate=0):
        """This method corrects the given rectangular coordinates of a Jupiter
        satellite in order to obtain higher accuracy by considering
        differential light-time and the perspective effect.

        :param R: Radius vector of the satellite
        :type R: float
        :param i_sat: Number of the satellite
        :type i_sat: int
        :param DELTA: Distance Observer-Jupiter in AU
        :type DELTA: float
        :param X_coordinate: Uncorrected X-coordinate of the satellite in
            Jupiter's radii or tuple for all coordinates
        :type X_coordinate: float, tuple, list
        :param Y_coordinate: Uncorrected Y-coordinate of the satellite in
            Jupiter's radii
        :type Y_coordinate: float
        :param Z_coordinate: Uncorrected Z-coordinate of the satellite in
            Jupiter's radii
        :type Z_coordinate: float

        :returns: A tuple with the corrected rectangular coordinates (X, Y, Z)
            of the satellite in Jupiter's radii
        :rtype: tuple
        :raises: TypeError if input values are wrong type

        Calculate corrected rectangular Coordinates X, Y and Z in Jupiter's
        radii for Io (1)

        >>> R = 5.929892730360271
        >>> i_sat = 1
        >>> DELTA = 5.6611211815432645
        >>> X_coordinate = -3.4489935969836503
        >>> Y_coordinate = 0.21361563816963675
        >>> Z_coordinate = -4.818966623735296
        >>> io = JupiterMoons.correct_rectangular_positions(R, i_sat, DELTA, \
        X_coordinate, Y_coordinate, Z_coordinate)
        >>> print(io)
        (-3.450168811390241, 0.21370246960509387, -4.818966623735296)
        """
        # Check type
        if not isinstance(i_sat, int):
            raise TypeError("Invalid input types")

        # Handle tuple or seperate values for input coordinates
        if type(X_coordinate) in (list, tuple):  # input type: tuple or list
            X, Y, Z = X_coordinate
        else:  # input type: seperate values
            X = X_coordinate
            Y = Y_coordinate
            Z = Z_coordinate

        # Differential light-time correction:
        # Correction factors for the satellites (index + 1 = No. of satellite)
        K = [17295, 21819, 27558, 36548]

        # Correct X-coordinate
        X += (abs(Z) / K[i_sat - 1]) * sqrt(1 - (X / R) ** 2)

        # Perspective effect correction:
        # Compute correction factor
        W = DELTA / (DELTA + (Z / 2095))

        # Correct coordinates
        X *= W
        Y *= W

        return X, Y, Z

    @staticmethod
    def check_phenomena(epoch, check_all=True, i_sat=0):
        """This method returns the perspective distance to any phenomena of all
        satellites for the given epoch.

        :param epoch: Epoch the calculations should be made for
        :type epoch: :py:class:'Epoch'
        :param check_all: Whether all satellites should be checked
        :type check_all: bool
        :param i_sat: Which satellite should be checked
        :type i_sat: int

        :returns: Distance to the satellite being ecclipsed, occulted in
            penumbra
        :rtype: tuple

        :raises: TypeError if input values are wrong type

        >>> utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        >>> result_matrix = JupiterMoons.check_phenomena( \
        utc_1992_12_16_00_00_00)
        >>> print(result_matrix[0])
        [-3.457757270630766, -2.553301264153796, 0.0]
        >>> print(result_matrix[1])
        [-7.44770945299594, -8.33419997337025, 0.0]
        >>> print(result_matrix[2])
        [-1.3572840767173413, -3.817302564886177, 0.0]
        >>> print(result_matrix[3])
        [-7.157430454898491, -11.373611474420906, 0.0]

        >>> io_ecc_start_2021_02_12_14_19_14 = Epoch(2021, 2, 12.5966898148148)
        >>> result_matrix = JupiterMoons.check_phenomena( \
        io_ecc_start_2021_02_12_14_19_14)
        >>> print([round(result_matrix[0][0], 10), round(result_matrix[0][ \
        1], 10), round(result_matrix[0][2], 10)])
        [1.192605868, 0.8560277162, 0.0]
        >>> print([round(result_matrix[1][0], 10), round(result_matrix[1][ \
        1], 10), round(result_matrix[1][2], 10)])
        [-8.7397202369, -8.8930940921, 0.0]
        >>> print([round(result_matrix[2][0], 10), round(result_matrix[2][ \
        1], 10), round(result_matrix[2][2], 10)])
        [14.0691219925, 13.8323491768, 0.0]
        >>> print([round(result_matrix[3][0], 10), round(result_matrix[3][ \
        1], 10), round(result_matrix[3][2], 10)])
        [-2.9341209156, -3.9904598153, 0.0]
        """

        # Check input type
        if not isinstance(epoch, Epoch):
            raise TypeError("Invalid input type")

        # Calculate light-time delay
        # DELTA, tau = JupiterMoons.calculate_delta(epoch)

        # Calculate coordinates as seen from the Earth
        Coords_Earth = JupiterMoons.rectangular_positions_jovian_equatorial(
            epoch)
        # Calculate coordinates as seen from the Sun
        Coords_Sun = JupiterMoons.rectangular_positions_jovian_equatorial(
            epoch, solar=True)

        if check_all is True:
            # Result matrix, where each rows is for a satellite
            # Column 0: Occultation
            # Column 1: Eclipse
            # Column 2: No use
            result_matrix = [[0.0, 0.0, 0.0],
                             [0.0, 0.0, 0.0],
                             [0.0, 0.0, 0.0],
                             [0.0, 0.0, 0.0]]

            for i in range(len(result_matrix)):
                # Coordinates for the iterated satellite
                X = Coords_Earth[i][0]
                Y = Coords_Earth[i][1]
                Z = Coords_Earth[i][2]

                X_0 = Coords_Sun[i][0]
                Y_0 = Coords_Sun[i][1]
                Z_0 = Coords_Sun[i][2]

                # Check occultation
                result_matrix[i][0] = JupiterMoons.check_occultation(X, Y, Z)
                # Check eclipse
                result_matrix[i][1] = JupiterMoons.check_eclipse(X_0, Y_0, Z_0)

            return result_matrix
        else:
            return JupiterMoons.check_occultation(Coords_Earth[i_sat - 1][0],
                                                  Coords_Earth[i_sat - 1][0],
                                                  Coords_Earth[i_sat - 1][
                                                      0]), \
                JupiterMoons.check_eclipse(
                Coords_Sun[i_sat - 1][0], Coords_Sun[i_sat - 1][0],
                Coords_Sun[i_sat - 1][0])

    @staticmethod
    def is_phenomena(epoch):
        """This method checks if the given coordinates correspond with any
        satellite
        phenomena. It returns the type of phenomena for all satellites.

        :param epoch: Epoch the calculations should be made for
        :type epoch: :py:class:'Epoch'

        :returns: Result matrix for the four Galilean satellites
            Row 0: Io            Column 0: Occultation
            Row 1: Europa        Column 1: Eclipse
            Row 2: Ganymede      Column 2: No use
            Row 3: Callisto
        :rtype: tuple

        :raises: TypeError if input values are wrong type

        Calculation of result matrix for December 16 at 0h UTC

        >>> utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        >>> result_matrix = JupiterMoons.is_phenomena(utc_1992_12_16_00_00_00)
        >>> print(result_matrix[0])
        [False, False, False]
        >>> print(result_matrix[1])
        [False, False, False]
        >>> print(result_matrix[2])
        [False, False, False]
        >>> print(result_matrix[3])
        [False, False, False]
        >>> io_ecc_start_2021_02_12_14_19_14 = Epoch(2021, 2, 12.5966898148148)
        >>> result_matrix = JupiterMoons.is_phenomena( \
        io_ecc_start_2021_02_12_14_19_14)
        >>> print(result_matrix[0])
        [False, True, False]
        >>> print(result_matrix[1])
        [False, False, False]
        >>> print(result_matrix[2])
        [False, False, False]
        >>> print(result_matrix[3])
        [False, False, False]
        """

        # Get distance Matrix
        dist_matrix = JupiterMoons.check_phenomena(epoch)

        result_matrix = [[False, False, False],
                         [False, False, False],
                         [False, False, False],
                         [False, False, False]]

        for row in range(len(result_matrix)):
            for col in range(len(result_matrix[row]) - 1):
                result_matrix[row][col] = (1 >= dist_matrix[row][col] >= 0)

        return result_matrix

    @staticmethod
    def check_coordinates(X, Y):
        """This method checks if the given coordinates correspond with a
        satellite
        phenomena. It returns if the satellite with the given coordinates is
        hidden
        behind Jupiter or directly in front.

        :param X: X-coordinate of the satellite in Jupiter's radii
        :type X: float
        :param Y: Y-coordinate of the satellite in Jupiter's radii
        :type Y: float

        :returns: Perspective distance to Jupiter's center in Jupiter's radii
        :rtype: float

        Calculation of the perspective distance of the planet Io to the
        center of Jupiter
        for December 16 at 0h UTC as seen from the Earth

        >>> utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        >>> result_matrix = \
        JupiterMoons.rectangular_positions_jovian_equatorial( \
        utc_1992_12_16_00_00_00, solar=False)
        >>> io_radius_to_center_of_jupiter_earth = \
        JupiterMoons.check_coordinates(result_matrix[0][0], result_matrix[ \
        0][1])
        >>> print(round(io_radius_to_center_of_jupiter_earth, 10))
        3.4577572706
        """

        # Accounting for elliptical Jupiter disk
        Y *= 1.071374

        return sqrt(X ** 2 + Y ** 2)

    @staticmethod
    def check_occultation(X=0, Y=0, Z=0, epoch=None, i_sat=None):
        """This method checks if the given coordinates or Epoch correspond
        with a satellite being in occultation.

        :param X: X-coordinate of the satellite in Jupiter's radii
        :type X: float
        :param Y: Y-coordinate of the satellite in Jupiter's radii
        :type Y: float
        :param Z: Z-coordinate of the satellite in Jupiter's radii
        :type Z: float
        :param epoch: Epoch that should be checked
        :type epoch: :py:class:`Epoch`
        :param i_sat: Index of the satellite (only for given Epoch)
        :type i_sat: int

        :returns: Perspective distance to center of Jupiter in Jupiter radii
            as seen from the Earth (value of perspective distance is negative
            when the satellite is closer to the Earth than Jupiter, otherwise
            positive)
        :rtype: float
        :raises: TypeError if input values are wrong type


        Calculation of the perspective distance of the planet Io
        squareroot(X^2 + Y^2) to the center of Jupiter for December 16 at 0h
        UTC as seen from the Earth

        >>> utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        >>> result_matrix = \
        JupiterMoons.rectangular_positions_jovian_equatorial( \
        utc_1992_12_16_00_00_00, solar=False)
        >>> io_distance_to_center_of_jupiter_earthview = \
        JupiterMoons.check_occultation(result_matrix[0][0], result_matrix[ \
        0][1])
        >>> print(round(io_distance_to_center_of_jupiter_earthview, 10))
        -3.4577572706
        """

        # Check if Epoch is given
        if epoch is not None and i_sat is not None:
            # Check types
            if isinstance(epoch, Epoch) and isinstance(i_sat, int):
                # Calculate coordinates for given Epoch as seen from the Earth
                X, Y, Z = \
                    JupiterMoons.rectangular_positions_jovian_equatorial(
                        epoch)[
                        i_sat - 1]
            else:
                raise TypeError("Invalid input types")

        # Check if satellite is more distant than Jupiter and distance to
        # center
        if Z > 0:
            return JupiterMoons.check_coordinates(X, Y)
        else:
            return -1 * JupiterMoons.check_coordinates(X, Y)

    @staticmethod
    def check_eclipse(X_0=0, Y_0=0, Z_0=0, epoch=None, i_sat=None):
        """This method checks if the given coordinates or Epoch correspond
        with a satellite being in eclipse.

        :param X_0: X-coordinate of the satellite in Jupiter's radii
            observed from the sun
        :type X_0: float
        :param Y_0: Y-coordinate of the satellite in Jupiter's radii
            observed from the sun
        :type Y_0: float
        :param Z_0: Z-coordinate of the satellite in Jupiter's radii
            observed from the sun
        :type Z_0: float
        :param epoch: Epoch that should be checked
        :type epoch: :py:class:`Epoch`
        :param i_sat: Index of the satellite (only for given Epoch)
        :type i_sat: int

        :returns: perspective distance to center of Jupiter in Jupiter radii
            as seen from the Sun (value of perspective distance is negative
            when the satellite is closer to the Sun than Jupiter, otherwise
            positive)
        :rtype: float
        :raises: TypeError if input values are wrong type


        Calculation of the Perspective distance of the planet Io
        squareroot(X_0^2 + Y_0^2) to the center of Jupiter for December 16 at
        0h UTC as seen from the Sun

        >>> utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        >>> result_matrix = \
        JupiterMoons.rectangular_positions_jovian_equatorial( \
        utc_1992_12_16_00_00_00, solar=True)
        >>> io_radius_to_center_of_jupiter_sunview = \
        JupiterMoons.check_eclipse(result_matrix[0][0], result_matrix[0][1])
        >>> print(round(io_radius_to_center_of_jupiter_sunview, 10))
        -2.5533012642
        """

        # Check if Epoch is given
        if epoch is not None and i_sat is not None:
            # Check types
            if isinstance(epoch, Epoch) and isinstance(i_sat, int):
                # Calculate coordinates for given Epoch as seen from the Sun
                X_0, Y_0, Z_0 = \
                    JupiterMoons.rectangular_positions_jovian_equatorial(
                        epoch, solar=True)[i_sat - 1]
            else:
                raise TypeError("Invalid input types")

        # Check if satellite is more distant than Jupiter and distance to
        # center
        if Z_0 > 0:
            return JupiterMoons.check_coordinates(X_0, Y_0)
        else:
            return -1 * JupiterMoons.check_coordinates(X_0, Y_0)


def main():
    # Let's define a small helper function
    def print_me(msg, val):
        print("{}: {}".format(msg, val))

    # Let's show some uses of JupiterMoons functions
    print("\n" + 35 * "*")
    print("*** Use of JupiterMoons class")
    print(35 * "*" + "\n")

    # Lets compute the ascending node of Jupiter as well as the longitude of
    # the node of
    # the equator of Jupiter on the ecliptic (psi) .
    utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
    psi_corrected, OMEGA_ascending_node_jupiter = \
        JupiterMoons.jupiter_system_angles(
            utc_1992_12_16_00_00_00)
    print("Ascending node of Jupiter: ", OMEGA_ascending_node_jupiter)
    # 100.39249942976576
    print(
        "Longitude of the node of the equator of Jupiter on the ecliptic ("
        "psi):",
        psi_corrected)
    # 317.1058009213959t

    print("")

    # Lets compute the corrected rectangular geocentric position of
    # Jupiter's satellites
    # for a given epoch, using the E5-theory.
    utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
    io, europa, ganymede, callisto = \
        JupiterMoons.rectangular_positions_jovian_equatorial(
            utc_1992_12_16_00_00_00)

    print("Corrected rectangular geocentric position of Io [X, Y , Z]: ", io)
    # (-3.450168811390241, 0.21370246960509387, -4.818966623735296)

    print(
        "Corrected rectangular geocentric position of Europa [X, Y , Z]: ",
        europa)
    # (7.441869121153001, 0.27524463479625677, -5.747104399729193)

    print(
        "Corrected rectangular geocentric position of Ganymede [X, Y , Z]: ",
        ganymede)
    # (1.201111684800708, 0.5899903274317162, -14.940581367576527)

    print(
        "Corrected rectangular geocentric position of Callisto [X, Y , Z]: ",
        callisto)
    # (7.071943240286434, 1.0289562923230684, -25.224137724734955)

    print("")

    # Lets compute the uncorrected rectangular geocentric position of
    # Jupiter's satellites
    # for a given epoch, using the E5-theory.
    # So the effects of different light-time and perspective described in
    # Pymeeus page 313 - 314 are neglected
    utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
    io_uncorrected, europa_uncorrected, ganymede_uncorrected, \
        callisto_uncorrected = \
        JupiterMoons.rectangular_positions_jovian_equatorial(
            utc_1992_12_16_00_00_00, do_correction=False)

    print(
        "Uncorrected rectangular geocentric position of Io [X, Y , Z]: ",
        io_uncorrected)
    # (-3.4489935969836503, 0.21361563816963675, -4.818966623735296)

    print(
        "Uncorrected rectangular geocentric position of Europa [X, Y , Z]: ",
        europa_uncorrected)
    # (7.438101803124541, 0.2751112576349763, -5.747104399729193)

    print(
        "Uncorrected rectangular geocentric position of Ganymede [X, Y , Z]: ",
        ganymede_uncorrected)
    # (1.1990581804888616, 0.589247092847632, -14.940581367576527)

    print(
        "Uncorrected rectangular geocentric position of Callisto [X, Y , Z]: ",
        callisto_uncorrected)
    # (7.056237832405445, 1.0267678919629089, -25.224137724734955)

    print("")

    # Lets calculate the distance between Earth and Jupiter (DELTA) for a
    # given epoch.
    utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
    delta, tau, l, b, r = JupiterMoons.calculate_delta(utc_1992_12_16_00_00_00)

    print("Distance between Earth and Jupiter in AU: ", delta)
    # 5.6611211815432645

    print("Light-time from Earth to Jupiter in d (day): ", tau)
    # 0.03269590898252075

    print("")

    # Lets calculate the perspective distance in Jupiter radii of all
    # satellites
    # for an eclipse of Io.

    io_ecc_start_2021_02_12_14_19_14 = Epoch(2021, 2, 12.5966898148148)
    result_matrix = JupiterMoons.check_phenomena(
        io_ecc_start_2021_02_12_14_19_14)
    # Row 0: Io          Column 0: perspective distance as seen from the Earth
    # Row 1: Europa      Column 1: perspective distance as seen from the Sun
    # Row 2: Ganymede    Column 2: No use
    # Row 3: Callisto

    # print Row 0
    print(
        "(perspective distance of Io (Earth View), perspective distance of "
        "Io (Sun view), No use): ")
    print(result_matrix[0])
    # [1.1926058680144362, 0.856027716233023, 0.0]

    # print Row 1
    print(
        "(perspective distance of Europa (Earth View), perspective distance "
        "of Europa (Sun view), No use): ")
    print(result_matrix[1])
    # [-8.739720236890856, -8.893094092124032, 0.0]

    # print Row 2
    print(
        "(perspective distance of Ganymede (Earth View), perspective "
        "distance of Ganymede (Sun view), No use): ")
    print(result_matrix[2])
    # [14.069121992481382, 13.8323491767871, 0.0]

    # print Row 3
    print(
        "(perspective distance of Callisto (Earth View), perspective "
        "distance of Callisto (Sun view), No use): ")
    print(result_matrix[3])
    # [-2.934134686233644, -3.9904786452498144, 0.0]

    print("")

    # Lets check if an eclipse or\and occultation for any of the four Galilean
    # satellites is detected for a given epoch

    io_ecc_start_2021_02_12_14_19_14 = Epoch(2021, 2, 12.5966898148148)

    # Structure of result matrix
    # Row 0: Io          Column 0: Occultation True\False
    # Row 1: Europa      Column 1: Eclipse True\False
    # Row 2: Ganymede    Column 2: No use
    # Row 3: Callisto

    result_matrix = JupiterMoons.is_phenomena(io_ecc_start_2021_02_12_14_19_14)
    # print Row 0
    print("(Occultation of Io, Eclipse of Io, No use): ")
    print(result_matrix[0])
    # [False, True, False]

    # print Row 1
    print(" (Occultation of Europa, Eclipse of Europa, No use): ")
    print(result_matrix[1])
    # [False, False, False]

    # print Row 2
    print(" (Occultation of Ganymede, Eclipse of Gaymede, No use): ")
    print(result_matrix[2])
    # [False,False,False]

    # print Row 3
    print("(Occultation of Callisto, Eclipse of Callisto, No use): ")
    print(result_matrix[3])
    # [False,False,False]

    print("")

    # Calculation of the perspective distance ot the planet Io to the center
    # of Jupiter
    # for December 16 at 0h UTC as seen from the Sun
    utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
    result_matrix = JupiterMoons.rectangular_positions_jovian_equatorial(
        utc_1992_12_16_00_00_00, solar=True)
    io_radius_to_center_of_jupiter_sun = JupiterMoons.check_coordinates(
        result_matrix[0][0], result_matrix[0][1])

    print(
        "Perspective distance of Io as seen from the Sun in Jupiter radii: ",
        io_radius_to_center_of_jupiter_sun)
    # 3.457757270630766

    print("")

    # Calculation of the perspective distance ot the planet Io to the center
    # of Jupiter
    # for December 16 at 0h UTC as seen from the Earth
    utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
    result_matrix = JupiterMoons.rectangular_positions_jovian_equatorial(
        utc_1992_12_16_00_00_00, solar=False)
    io_radius_to_center_of_jupiter_earth = JupiterMoons.check_coordinates(
        result_matrix[0][0], result_matrix[0][1])

    print(
        "Perspective distance of Io as seen from the Earth in Jupiter radii: ",
        io_radius_to_center_of_jupiter_earth)
    # 2.553301264153796


if __name__ == "__main__":
    main()
