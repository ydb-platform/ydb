# -*- coding: utf-8 -*-

from __future__ import absolute_import
from __future__ import division, print_function, unicode_literals

import unittest
import pytest

from justext.core import JustextError, decode_html


class TestHtmlEncoding(unittest.TestCase):

    def assert_strings_equal(self, s1, s2):
        assert type(s1) == type(s2)
        assert s1 == s2

    def test_unicode(self):
        html = "ľščťžýáíéäňúô Ł€"
        decoded_html = decode_html(html)

        self.assert_strings_equal(html, decoded_html)

    def test_utf8_bytes(self):
        html = "ľščťžýáíéäňúô Ł€"
        decoded_html = decode_html(html.encode("utf8"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_1(self):
        html = '<meta http-equiv="Content-Type" content="text/html; charset=iso-8859-2"/> ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_2(self):
        html = '<meta content=text/html; charset=iso-8859-2 http-equiv="Content-Type"/> ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_3(self):
        html = '<meta content=\'text/html; charset=iso-8859-2\' http-equiv="Content-Type"/> ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_4(self):
        html = '<meta charset=iso-8859-2/> ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_5(self):
        html = '<meta charset="iso-8859-2"/> ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_6(self):
        html = '<meta charset=iso-8859-2/> ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_7(self):
        html = '<meta charset=iso-8859-2> ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_8(self):
        html = '<meta charset=iso-8859-2> ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_9(self):
        html = '<meta content=text/html; charset=iso-8859-2 http-equiv="Content-Type"/> ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_charset_outside_1(self):
        html = '<meta charset="iso-8859-2"/> charset="iso-fake-29" ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_charset_outside_2(self):
        html = '<meta content=text/html; charset=iso-8859-2 http-equiv="Content-Type"/> charset="iso-fake-29" ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_meta_detection_charset_outside_3(self):
        html = '<META HTTP-EQUIV="Content-Type" CONTENT="text/html; CHARSET=ISO-8859-2"> charset="iso-fake-29" ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"))

        self.assert_strings_equal(html, decoded_html)

    def test_unknown_encoding_in_strict_mode(self):
        html = 'ľščťžäňôě'
        with pytest.raises(JustextError):
            decode_html(html.encode("iso-8859-2"), errors='strict')

    def test_unknown_encoding_with_default_error_handler(self):
        html = 'ľščťžäňôě'
        decoded = decode_html(html.encode("iso-8859-2"), default_encoding="iso-8859-2")
        self.assertEqual(decoded, html)

    def test_default_encoding(self):
        html = 'ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"), default_encoding="iso-8859-2")

        self.assert_strings_equal(html, decoded_html)

    def test_given_encoding(self):
        html = 'ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"), encoding="iso-8859-2")

        self.assert_strings_equal(html, decoded_html)

    def test_given_wrong_encoding(self):
        html = 'ľščťžäňôě'
        decoded_html = decode_html(html.encode("iso-8859-2"), encoding="ASCII")

        self.assert_strings_equal("\ufffd" * len(html), decoded_html)

    def test_fake_encoding_in_meta(self):
        html = '<meta charset="iso-fake-2"/> ľščťžäňôě'

        with pytest.raises(JustextError):
            decode_html(html.encode("iso-8859-2"), errors='strict')
