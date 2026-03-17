#!/usr/bin/env python
"""
Python tests for Mime-Type Parser.

This module loads a json file and converts the tests specified therein to a set
of PyUnitTestCases. Then it uses PyUnit to run them and report their status.
"""
import json
import unittest

import mimeparse

import yatest.common

TEST_DATA = yatest.common.test_source_path("testdata.json")


__version__ = "0.1"
__author__ = 'Ade Oshineye'
__email__ = "ade@oshineye.com"
__credits__ = ""


class MimeParseTestCase(unittest.TestCase):

    def setUp(self):
        super(MimeParseTestCase, self).setUp()
        with open(TEST_DATA) as f:
            self.test_data = json.load(f)

    def _test_parse_media_range(self, args, expected):
        expected = tuple(expected)
        result = mimeparse.parse_media_range(args)
        message = "Expected: '%s' but got %s" % (expected, result)
        self.assertEqual(expected, result, message)

    def _test_quality(self, args, expected):
        result = mimeparse.quality(args[0], args[1])
        message = "Expected: '%s' but got %s" % (expected, result)
        self.assertEqual(expected, result, message)

    def _test_best_match(self, args, expected, description):
        if expected is None:
            self.assertRaises(mimeparse.MimeTypeParseException,
                              mimeparse.best_match, args[0], args[1])
        else:
            result = mimeparse.best_match(args[0], args[1])
            message = \
                "Expected: '%s' but got %s. Description for this test: %s" % \
                (expected, result, description)
            self.assertEqual(expected, result, message)

    def _test_parse_mime_type(self, args, expected):
        if expected is None:
            self.assertRaises(mimeparse.MimeTypeParseException,
                              mimeparse.parse_mime_type, args)
        else:
            expected = tuple(expected)
            result = mimeparse.parse_mime_type(args)
            message = "Expected: '%s' but got %s" % (expected, result)
            self.assertEqual(expected, result, message)

    def test_parse_media_range(self):
        for args, expected in self.test_data['parse_media_range']:
            self._test_parse_media_range(args, expected)

    def test_quality(self):
        for args, expected in self.test_data['quality']:
            self._test_quality(args, expected)

    def test_best_match(self):
        for args, expected, description in self.test_data['best_match']:
            self._test_best_match(args, expected, description)

    def test_parse_mime_type(self):
        for args, expected in self.test_data['parse_mime_type']:
            self._test_parse_mime_type(args, expected)


if __name__ == '__main__':
    unittest.main()
