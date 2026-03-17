# Copyright (C) Dnspython Contributors, see LICENSE for text of ISC license

# Copyright (C) 2006, 2007, 2009-2011 Nominum, Inc.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose with or without fee is hereby granted,
# provided that the above copyright notice and this permission notice
# appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND NOMINUM DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL NOMINUM BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

import unittest

from dns.exception import DNSException


class FormatedError(DNSException):
    fmt = "Custom format: {parameter}"
    supp_kwargs = {'parameter'}


class ExceptionTestCase(unittest.TestCase):

    def test_custom_message(self):
        msg = "this is a custom message"
        try:
            raise DNSException(msg)
        except DNSException as ex:
            self.assertEqual(str(ex), msg)

    def test_implicit_message(self):
        try:
            raise DNSException()
        except DNSException as ex:
            self.assertEqual(ex.__class__.__doc__, str(ex))

    def test_formatted_error(self):
        """Exceptions with explicit format has to respect it."""
        params = {'parameter': 'value'}
        try:
            raise FormatedError(**params)
        except FormatedError as ex:
            msg = FormatedError.fmt.format(**params)
            self.assertEqual(msg, str(ex))

    def test_kwargs_only(self):
        """Kwargs cannot be combined with args."""
        with self.assertRaises(AssertionError):
            raise FormatedError(1, a=2)

    def test_kwargs_unsupported(self):
        """Only supported kwargs are accepted."""
        with self.assertRaises(AssertionError):
            raise FormatedError(unsupported=2)

if __name__ == '__main__':
    unittest.main()
