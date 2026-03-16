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


from unittest import TestCase

from pymeeus.JupiterMoons import JupiterMoons
from pymeeus.base import TOL
from pymeeus.Epoch import Epoch

EPOCH_1992_12_16_UTC = Epoch(1992, 12, 16, utc=True)
exp_prec = 4


class TestJupiterMoons(TestCase):

    def test_jupiter_system_angles(self):
        """This method tests the method jupiter_system_angles() that calculates
         the ascending node of Jupiter as well as psi. """
        utc_1992_12_16_00_00_00 = Epoch(1992, 12, 16, utc=True)
        psi_corrected, OMEGA_ascending_node_jupiter = \
            JupiterMoons.jupiter_system_angles(
                utc_1992_12_16_00_00_00)
        assert abs(round(psi_corrected, exp_prec) - round(317.1058009213959,
                                                          exp_prec)) < TOL, \
            """ERROR: psi_corrected of JupiterMoons.jupiter_system angles()
            test doesn't match"""
        assert abs(round(OMEGA_ascending_node_jupiter, exp_prec) - round(
            100.39249942976576, exp_prec)) < TOL, \
            """ERROR: OMEGA_ascending_node_jupiter of
            JupiterMoons.jupiter_system angles() test doesn't match"""

    def test_rectangular_positions(self):
        """This method tests the method
        rectangular_positions_jovian_equatorial() that calculates the
        rectangular geocentric
         position of Jupiter's satellites for a given epoch, using the
         E5-theory. """

        io_corr_true, europe_corr_true, ganymede_corr_true, \
            callisto_corr_true = \
            JupiterMoons.rectangular_positions_jovian_equatorial(
                EPOCH_1992_12_16_UTC, do_correction=True)

        assert abs(round(io_corr_true[0], exp_prec) - round(-3.45016881,
                                                            exp_prec)) < TOL, \
            """ERROR: 1st rectangular position (X) for Io of
            JupiterMoons.rectangular_position() test doesn't match"""

        assert abs(round(io_corr_true[1], exp_prec) - round(0.21370247,
                                                            exp_prec)) < TOL, \
            """ERROR: 2nd rectangular position (Y) for Io of
            JupiterMoons.rectangular_position() test doesn't match"""

        assert abs(round(io_corr_true[2], exp_prec) + round(4.81896662,
                                                            exp_prec)) < TOL, \
            """ERROR: 3rd rectangular position (Z) for Io of
            JupiterMoons.rectangular_position() test doesn't match"""

        assert abs(round(europe_corr_true[0], exp_prec) - round(7.44186912,
                                                                exp_prec)) <\
            TOL, \
            """ERROR: 1st rectangular position (X) for Europe of
            JupiterMoons.rectangular_position()
            test doesn't match"""

        assert abs(round(europe_corr_true[1], exp_prec) - round(0.27524463,
                                                                exp_prec)) <\
            TOL, \
            """ERROR: 2nd rectangular position for (Y) Europe of
            JupiterMoons.rectangular_position()
            test doesn't match"""

        assert abs(round(europe_corr_true[2], exp_prec) - round(-5.74710440,
                                                                exp_prec)) <\
            TOL, \
            """ERROR: 3rd rectangular position for (Z) Europe of
            JupiterMoons.rectangular_position()
            test doesn't match"""

        assert abs(round(ganymede_corr_true[0], exp_prec) - round(1.20111168,
                                                                  exp_prec))\
            < TOL, \
            """ERROR: 1st rectangular position (X) for Ganymede of
            JupiterMoons.rectangular_position()
            test doesn't match"""

        assert abs(round(ganymede_corr_true[1], exp_prec) - round(0.58999033,
                                                                  exp_prec))\
            < TOL, \
            """ERROR: 2nd rectangular position (Y) for Ganymede of
            JupiterMoons.rectangular_position()
            test doesn't match"""

        assert abs(round(ganymede_corr_true[2], exp_prec) - round(-14.94058137,
                                                                  exp_prec))\
            < TOL, \
            """ERROR: 3rd rectangular position (Z) for Ganymede of
            JupiterMoons.rectangular_position()
            test doesn't match"""

        assert abs(round(callisto_corr_true[0], exp_prec) - round(7.07202264,
                                                                  exp_prec))\
            < TOL, \
            """ERROR: 1st rectangular position (X) for Callisto of
            JupiterMoons.rectangular_position()
            test doesn't match"""

        assert abs(round(callisto_corr_true[1], exp_prec) - round(1.02895629,
                                                                  exp_prec))\
            < TOL, \
            """ERROR: 2nd rectangular position (Y) for  Callisto of
            JupiterMoons.rectangular_position()
            test doesn't match"""

        assert abs(round(callisto_corr_true[2], exp_prec) - round(-25.22442033,
                                                                  exp_prec))\
            < TOL, \
            """ERROR: 3rd rectangular position (Z) for  Callisto of
            JupiterMoons.rectangular_position()
            test doesn't match"""

    def test_calculate_delta(self):
        """This method tests calculate_delta() that calculates the distance
        between Earth and Jupiter (DELTA) for a given epoch by iteration. """

        """Epoch used for the calculations"""
        epoch = Epoch(1992, 12, 16, utc=True)

        delta, tau, l, b, r = JupiterMoons.calculate_delta(epoch)

        # value_reference
        delta_reference = 5.6611211815432645
        tau_reference = 0.032695909

        assert abs(round(delta, 4) - round(delta_reference, 4)) < TOL, \
            """ERROR: Distance between earth and Jupiter of
            JupiterMoons.calculate_delta() doesn't match"""

        assert abs(round(tau, 4) - round(tau_reference, 4)) < TOL, \
            """ERROR: Light time delay tau between earth and Jupiter of
            JupiterMoons.calculate_delta()doesn't match"""

    def test_correct_rectangular_positions(self):
        """This method tests the method correct_rectangular_positions() that
        corrects the rectangular geocentric
        position of Jupiter's satellites for a given epoch as described
        in astronomical algorithm chapter 44 page: 313 -314. """

        "Radius vector of Io in Jupiter radii"
        R = 5.929892730360271

        "distance Earth to Jupiter"
        DELTA = 5.6611211815432645

        "uncorrecte rectangular coordiantes of Io"
        X_coordinate = -3.4489935969836503
        Y_coordinate = 0.21361563816963675
        Z_coordinate = -4.818966623735296

        """ calculate corrected rectangular positions for the Jupiter moons
        Io"""
        i_sat = 1
        io = JupiterMoons.correct_rectangular_positions(R, i_sat, DELTA,
                                                        X_coordinate,
                                                        Y_coordinate,
                                                        Z_coordinate)

        assert abs(round(io[0], exp_prec) - round(-3.450168811390241,
                                                  exp_prec)) < TOL, \
            """ERROR: correction of 1st rectangular position (X) for Io test
            doesn't match"""
        assert abs(round(io[1], exp_prec) - round(0.21370246960509387,
                                                  exp_prec)) < TOL, \
            """ERROR: correction of 2nd rectangular position (Y) for Io test
            doesn't match"""
        assert abs(round(io[2], exp_prec) - round(-4.818966623735296,
                                                  exp_prec)) < TOL, \
            """ERROR: correction of 3rd rectangular position (Z) for Io test
            doesn't match"""

    def test_check_coordinates(self):
        """This method tests if the method check_coordinates() returns the
        right distance between Io and Jupiter
         as seen from the Earth"""

        """Calculation of the perspective distance of the planet Io to the
        center of Jupiter
        for December 16 at 0h UTC as seen from the Earth"""
        result_matrix = JupiterMoons.rectangular_positions_jovian_equatorial(
            EPOCH_1992_12_16_UTC, solar=False)
        io_radius_to_center_of_jupiter_earth = JupiterMoons.check_coordinates(
            result_matrix[0][0], result_matrix[0][1])
        assert abs(
            round(io_radius_to_center_of_jupiter_earth, exp_prec) - round(
                3.457757270630766, exp_prec)) < TOL, \
            """ERROR: test_check_coordinates() test doesn't match"""

    def test_check_occulation(self):
        """This method test if the method check_occultation() returns the
        right distance between Io and Jupiter
         as seen from the earth."""
        result_matrix = JupiterMoons.rectangular_positions_jovian_equatorial(
            EPOCH_1992_12_16_UTC, solar=False)
        io_distance_to_center_of_jupiter_earthview = \
            JupiterMoons.check_occultation(
                result_matrix[0][0],
                result_matrix[0][1])
        assert abs(
            round(io_distance_to_center_of_jupiter_earthview,
                  exp_prec) + round(3.457757270630766, exp_prec)) < TOL, \
            """ERROR: test_check_occultation() test doesn't match"""

    def test_check_eclipse(self):
        result_matrix = JupiterMoons.rectangular_positions_jovian_equatorial(
            EPOCH_1992_12_16_UTC, solar=True)
        io_distance_to_center_of_jupiter_sunview = JupiterMoons.check_eclipse(
            result_matrix[0][0],
            result_matrix[0][1])
        assert abs(
            round(io_distance_to_center_of_jupiter_sunview, exp_prec) + round(
                2.553301264153796, exp_prec)) < TOL, \
            """ERROR: test_check_eclipse() test doesn't match"""

    def test_check_phenomena(self):
        """This method tests if the method check_phenomena() returns the
        right perspective distances
         between the Galilean satellites and Jupiter"""

        utc_1992_12_16_00_00_00 = Epoch(EPOCH_1992_12_16_UTC)
        result_matrix = JupiterMoons.check_phenomena(utc_1992_12_16_00_00_00)
        result_matrix_expect = [[-3.457757270630766, -2.553301264153796, 0.0],
                                [-7.44770945299594, -8.33419997337025, 0.0],
                                [-1.3572840767173413, -3.817302564886177, 0.0],
                                [-7.15735009488433, -11.373483813510918, 0.0]]
        for row in range(3):
            for col in range(2):
                assert abs(
                    result_matrix[row][col] - result_matrix_expect[row][
                        col]) < TOL, \
                    """ERROR: check_phenomena() test doesn't match"""

    def test_is_phenomena(self):
        """This method tests if the method check_phenomena() returns the
        right perspective distances
         between the Galilean satellites and Jupiter"""

        utc_1992_12_16_00_00_00 = Epoch(EPOCH_1992_12_16_UTC, utc=True)
        result_matrix = JupiterMoons.is_phenomena(utc_1992_12_16_00_00_00)
        result_matrix_expect = [[False, False, False],
                                [False, False, False],
                                [False, False, False],
                                [False, False, False]]
        for row in range(3):
            for col in range(2):
                assert abs(
                    result_matrix[row][col] - result_matrix_expect[row][
                        col]) < TOL, \
                    """ERROR: is_phenomena() test one doesn't match"""

        io_ecc_start_2021_02_12_14_19_14 = Epoch(2021, 2, 12.5966898148148)
        result_matrix = JupiterMoons.is_phenomena(
            io_ecc_start_2021_02_12_14_19_14)
        result_matrix_expect[0][1] = True

        for row in range(3):
            for col in range(2):
                assert abs(
                    result_matrix[row][col] - result_matrix_expect[row][
                        col]) < TOL, \
                    """ERROR: is_phenomena() test two doesn't match"""

        """ Unit test weather the software detects the right phenomea or not
            Matrix:
            col 0: test case identifier
            col 1: Epoch to be tested
            col 2/3: row/col-index of expected 'True'
        All other entries are expected to be 'False'!"""
        test_set = [["IO_OCC_START_2021_01_17_00_55_11",
                     Epoch(2021, 1, 17.0383217592593), 0, 0],
                    ["IO_ECC_START_2021_02_12_14_19_14",
                     Epoch(2021, 2, 12.5966898148148), 0, 1],
                    ["EU_OCC_START_2021_10_02_13_04_19",
                     Epoch(2021, 10, 2.54466435185185), 1, 0],
                    ["EU_ECC_START_2021_02_13_14_26_18",
                     Epoch(2021, 2, 13.6015972222222), 1, 1],
                    ["GA_OCC_START_2021_03_29_00_43_46",
                     Epoch(2021, 3, 29.0303935185185), 2, 0],
                    ["GA_ECC_START_2021_02_06_16_53_50",
                     Epoch(2021, 2, 6.70405092592593), 2, 1],
                    ["CA_OCC_START_2021_03_09_13_26_47",
                     Epoch(2021, 3, 9.5602662037037), 3, 0],
                    ["CA_ECC_START_2021_04_28_13_32_36",
                     Epoch(2021, 4, 28.5643055555556), 3, 1],
                    ["IO_OCC_END_2021_02_14_11_20_01",
                     Epoch(2021, 2, 14.4722337962963), 0, 0],
                    ["IO_ECC_END_2021_10_09_14_44_31",
                     Epoch(2021, 10, 9.61424768518519), 0, 1],
                    ["EU_OCC_END_2021_03_07_02_14_12",
                     Epoch(2021, 3, 7.09319444444444), 1, 0],
                    ["EU_ECC_END_2021_10_06_07_12_45",
                     Epoch(2021, 10, 6.30052083333333), 1, 1],
                    ["GA_OCC_END_2021_12_18_23_58_55",
                     Epoch(2021, 12, 18.9992476851852), 2, 0],
                    ["GA_ECC_END_2021_12_04_20_34_04",
                     Epoch(2021, 12, 4.85699074074074), 2, 1],
                    ["CA_OCC_END_2021_11_15_07_29_43",
                     Epoch(2021, 11, 15.3123032407407), 3, 0],
                    ["CA_ECC_END_2021_08_24_00_58_49",
                     Epoch(2021, 8, 24.0408449074074), 3, 1]
                    ]

        for test_label in range(15):
            result = JupiterMoons.is_phenomena(test_set[test_label][1])
            for column in range(1):
                for row in range(3):
                    if column == test_set[test_label][3] and row == \
                            test_set[test_label][2]:
                        assert result[test_set[test_label][2]][
                            test_set[test_label][3]], \
                            """ERROR: existing phenomena is not detected """
                    else:
                        assert not result[row][column], \
                            """ERROR: non existing phenomena is detected"""
