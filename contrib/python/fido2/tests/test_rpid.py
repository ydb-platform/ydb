# coding=utf-8

# Copyright (c) 2013 Yubico AB
# All rights reserved.
#
#   Redistribution and use in source and binary forms, with or
#   without modification, are permitted provided that the following
#   conditions are met:
#
#    1. Redistributions of source code must retain the above copyright
#       notice, this list of conditions and the following disclaimer.
#    2. Redistributions in binary form must reproduce the above
#       copyright notice, this list of conditions and the following
#       disclaimer in the documentation and/or other materials provided
#       with the distribution.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
# FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
# COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
# LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN
# ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.

from fido2.rpid import verify_rp_id
import unittest


class TestRpId(unittest.TestCase):
    def test_valid_ids(self):
        self.assertTrue(verify_rp_id("example.com", "https://register.example.com"))
        self.assertTrue(verify_rp_id("example.com", "https://fido.example.com"))
        self.assertTrue(verify_rp_id("example.com", "https://www.example.com:444"))

    def test_invalid_ids(self):
        self.assertFalse(verify_rp_id("example.com", "http://example.com"))
        self.assertFalse(verify_rp_id("example.com", "http://www.example.com"))
        self.assertFalse(verify_rp_id("example.com", "https://example-test.com"))

        self.assertFalse(
            verify_rp_id("companyA.hosting.example.com", "https://register.example.com")
        )
        self.assertFalse(
            verify_rp_id(
                "companyA.hosting.example.com", "https://companyB.hosting.example.com"
            )
        )

    def test_suffix_list(self):
        self.assertFalse(verify_rp_id("co.uk", "https://foobar.co.uk"))
        self.assertTrue(verify_rp_id("foobar.co.uk", "https://site.foobar.co.uk"))
        self.assertFalse(verify_rp_id("appspot.com", "https://example.appspot.com"))
        self.assertTrue(
            verify_rp_id("example.appspot.com", "https://example.appspot.com")
        )
