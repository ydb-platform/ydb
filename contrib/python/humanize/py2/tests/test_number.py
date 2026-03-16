#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Number tests."""

from humanize import number

from .base import HumanizeTestCase


class NumberTestCase(HumanizeTestCase):
    def test_ordinal(self):
        test_list = (
            "1",
            "2",
            "3",
            "4",
            "11",
            "12",
            "13",
            "101",
            "102",
            "103",
            "111",
            "something else",
            None,
        )
        result_list = (
            "1st",
            "2nd",
            "3rd",
            "4th",
            "11th",
            "12th",
            "13th",
            "101st",
            "102nd",
            "103rd",
            "111th",
            "something else",
            None,
        )
        self.assertManyResults(number.ordinal, test_list, result_list)

    def test_intcomma(self):
        test_list = (
            100,
            1000,
            10123,
            10311,
            1000000,
            1234567.25,
            "100",
            "1000",
            "10123",
            "10311",
            "1000000",
            "1234567.1234567",
            None,
        )
        result_list = (
            "100",
            "1,000",
            "10,123",
            "10,311",
            "1,000,000",
            "1,234,567.25",
            "100",
            "1,000",
            "10,123",
            "10,311",
            "1,000,000",
            "1,234,567.1234567",
            None,
        )
        self.assertManyResults(number.intcomma, test_list, result_list)

    def test_intword(self):
        # make sure that powers & human_powers have the same number of items
        self.assertEqual(len(number.powers), len(number.human_powers))
        # test the result of intword
        test_list = (
            "100",
            "1000000",
            "1200000",
            "1290000",
            "1000000000",
            "2000000000",
            "6000000000000",
            "1300000000000000",
            "3500000000000000000000",
            "8100000000000000000000000000000000",
            None,
            ("1230000", "%0.2f"),
            10 ** 101,
        )
        result_list = (
            "100",
            "1.0 million",
            "1.2 million",
            "1.3 million",
            "1.0 billion",
            "2.0 billion",
            "6.0 trillion",
            "1.3 quadrillion",
            "3.5 sextillion",
            "8.1 decillion",
            None,
            "1.23 million",
            "1" + "0" * 101,
        )
        self.assertManyResults(number.intword, test_list, result_list)

    def test_apnumber(self):
        test_list = (1, 2, 4, 5, 9, 10, "7", None)
        result_list = ("one", "two", "four", "five", "nine", "10", "seven", None)
        self.assertManyResults(number.apnumber, test_list, result_list)

    def test_fractional(self):
        test_list = (1, 2.0, (4.0 / 3.0), (5.0 / 6.0), "7", "8.9", "ten", None)
        result_list = ("1", "2", "1 1/3", "5/6", "7", "8 9/10", "ten", None)
        self.assertManyResults(number.fractional, test_list, result_list)
