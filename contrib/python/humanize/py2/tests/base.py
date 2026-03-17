#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests base classes."""

from unittest import TestCase


class HumanizeTestCase(TestCase):
    def assertManyResults(self, function, args, results):
        """Goes through a list of arguments and makes sure that function called
        upon them lists a similarly ordered list of results.  If more than one
        argument is required, each position in args may be a tuple."""
        for arg, result in zip(args, results):
            if isinstance(arg, tuple):
                self.assertEqual(function(*arg), result)
            else:
                self.assertEqual(function(arg), result)

    def assertEqualDatetime(self, dt1, dt2):
        self.assertEqual((dt1 - dt2).seconds, 0)

    def assertEqualTimedelta(self, td1, td2):
        self.assertEqual(td1.days, td2.days)
        self.assertEqual(td1.seconds, td2.seconds)
