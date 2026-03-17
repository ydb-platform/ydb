# Copyright (c) 2019 Yubico AB
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

from fido2.attestation.tpm import TpmAttestationFormat, TpmPublicFormat

import unittest


class TestTpmObject(unittest.TestCase):
    def test_parse_tpm(self):
        data = bytes.fromhex(
            "ff54434780170022000b68cec627cc6411099a1f809fde4379f649aa170c7072d1adf230de439efc80810014f7c8b0cdeb31328648130a19733d6fff16e76e1300000003ef605603446ed8c56aa7608d01a6ea5651ee67a8a20022000bdf681917e18529c61e1b85a1e7952f3201eb59c609ed5d8e217e5de76b228bbd0022000b0a10d216b0c3ab82bfdc1f0a016ab9493384c7aee1937ee8800f76b30c9b71a7"  # noqa
        )

        tpm = TpmAttestationFormat.parse(data)
        self.assertEqual(
            tpm.data, bytes.fromhex("f7c8b0cdeb31328648130a19733d6fff16e76e13")
        )

    def test_parse_too_short_of_a_tpm(self):
        with self.assertRaises(ValueError):
            TpmAttestationFormat.parse(bytes.fromhex("ff5443"))
        with self.assertRaises(ValueError) as e:
            data = bytes.fromhex(
                "ff54434780170022000b68cec627cc6411099a1f809fde4379f649aa170c7072d1adf230de439efc80810014f7c8b0cdeb31328648"  # noqa
            )
            TpmAttestationFormat.parse(data)
        self.assertEqual(
            e.exception.args[0], "Not enough data to read (need: 20, had: 9)."
        )

    def test_parse_public_ecc(self):
        data = bytes.fromhex(
            "0023000b00060472000000100010000300100020b9174cd199f77552afcffe6b1f069c032ffdc4f56068dec4e189e7967b3bf6b0002037bf8aa7d93fddb9507319141c6fa31c8e48a1c6da013603a9f6e3913d157c66"  # noqa
        )
        TpmPublicFormat.parse(data)
