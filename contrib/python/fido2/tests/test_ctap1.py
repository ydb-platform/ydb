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

import unittest
from unittest import mock
from fido2.ctap1 import Ctap1, ApduError


class TestCtap1(unittest.TestCase):
    def test_send_apdu_ok(self):
        ctap = Ctap1(mock.MagicMock())
        ctap.device.call.return_value = b"response\x90\x00"

        self.assertEqual(b"response", ctap.send_apdu(1, 2, 3, 4, b"foobar"))
        ctap.device.call.assert_called_with(0x03, b"\1\2\3\4\0\0\6foobar\0\0")

    def test_send_apdu_err(self):
        ctap = Ctap1(mock.MagicMock())
        ctap.device.call.return_value = b"err\x6a\x80"

        try:
            ctap.send_apdu(1, 2, 3, 4, b"foobar")
            self.fail("send_apdu did not raise error")
        except ApduError as e:
            self.assertEqual(e.code, 0x6A80)
            self.assertEqual(e.data, b"err")
        ctap.device.call.assert_called_with(0x03, b"\1\2\3\4\0\0\6foobar\0\0")

    def test_get_version(self):
        ctap = Ctap1(mock.MagicMock())
        ctap.device.call.return_value = b"U2F_V2\x90\x00"

        self.assertEqual("U2F_V2", ctap.get_version())
        ctap.device.call.assert_called_with(0x03, b"\0\3\0\0\0\0\0\0\0")

    def test_register(self):
        ctap = Ctap1(mock.MagicMock())
        ctap.device.call.return_value = (
            bytes.fromhex(
                "0504b174bc49c7ca254b70d2e5c207cee9cf174820ebd77ea3c65508c26da51b657c1cc6b952f8621697936482da0a6d3d3826a59095daf6cd7c03e2e60385d2f6d9402a552dfdb7477ed65fd84133f86196010b2215b57da75d315b7b9e8fe2e3925a6019551bab61d16591659cbaf00b4950f7abfe6660e2e006f76868b772d70c253082013c3081e4a003020102020a47901280001155957352300a06082a8648ce3d0403023017311530130603550403130c476e756262792050696c6f74301e170d3132303831343138323933325a170d3133303831343138323933325a3031312f302d0603550403132650696c6f74476e756262792d302e342e312d34373930313238303030313135353935373335323059301306072a8648ce3d020106082a8648ce3d030107034200048d617e65c9508e64bcc5673ac82a6799da3c1446682c258c463fffdf58dfd2fa3e6c378b53d795c4a4dffb4199edd7862f23abaf0203b4b8911ba0569994e101300a06082a8648ce3d0403020347003044022060cdb6061e9c22262d1aac1d96d8c70829b2366531dda268832cb836bcd30dfa0220631b1459f09e6330055722c8d89b7f48883b9089b88d60d1d9795902b30410df304502201471899bcc3987e62e8202c9b39c33c19033f7340352dba80fcab017db9230e402210082677d673d891933ade6f617e5dbde2e247e70423fd5ad7804a6d3d3961ef871"  # noqa E501
            )
            + b"\x90\x00"
        )

        client_param = bytes.fromhex(
            "4142d21c00d94ffb9d504ada8f99b721f4b191ae4e37ca0140f696b6983cfacb"
        )
        app_param = bytes.fromhex(
            "f0e6a6a97042a4f1f1c87f5f7d44315b2d852c2df5c7991cc66241bf7072d1c4"
        )

        resp = ctap.register(client_param, app_param)
        ctap.device.call.assert_called_with(
            0x03, b"\0\1\0\0\0\0\x40" + client_param + app_param + b"\0\0"
        )
        self.assertEqual(
            resp.public_key,
            bytes.fromhex(
                "04b174bc49c7ca254b70d2e5c207cee9cf174820ebd77ea3c65508c26da51b657c1cc6b952f8621697936482da0a6d3d3826a59095daf6cd7c03e2e60385d2f6d9"  # noqa E501
            ),
        )
        self.assertEqual(
            resp.key_handle,
            bytes.fromhex(
                "2a552dfdb7477ed65fd84133f86196010b2215b57da75d315b7b9e8fe2e3925a6019551bab61d16591659cbaf00b4950f7abfe6660e2e006f76868b772d70c25"  # noqa E501
            ),
        )
        self.assertEqual(
            resp.certificate,
            bytes.fromhex(
                "3082013c3081e4a003020102020a47901280001155957352300a06082a8648ce3d0403023017311530130603550403130c476e756262792050696c6f74301e170d3132303831343138323933325a170d3133303831343138323933325a3031312f302d0603550403132650696c6f74476e756262792d302e342e312d34373930313238303030313135353935373335323059301306072a8648ce3d020106082a8648ce3d030107034200048d617e65c9508e64bcc5673ac82a6799da3c1446682c258c463fffdf58dfd2fa3e6c378b53d795c4a4dffb4199edd7862f23abaf0203b4b8911ba0569994e101300a06082a8648ce3d0403020347003044022060cdb6061e9c22262d1aac1d96d8c70829b2366531dda268832cb836bcd30dfa0220631b1459f09e6330055722c8d89b7f48883b9089b88d60d1d9795902b30410df"  # noqa E501
            ),
        )
        self.assertEqual(
            resp.signature,
            bytes.fromhex(
                "304502201471899bcc3987e62e8202c9b39c33c19033f7340352dba80fcab017db9230e402210082677d673d891933ade6f617e5dbde2e247e70423fd5ad7804a6d3d3961ef871"  # noqa E501
            ),
        )

        resp.verify(app_param, client_param)

    def test_authenticate(self):
        ctap = Ctap1(mock.MagicMock())
        ctap.device.call.return_value = (
            bytes.fromhex(
                "0100000001304402204b5f0cd17534cedd8c34ee09570ef542a353df4436030ce43d406de870b847780220267bb998fac9b7266eb60e7cb0b5eabdfd5ba9614f53c7b22272ec10047a923f"  # noqa E501
            )
            + b"\x90\x00"
        )

        client_param = bytes.fromhex(
            "ccd6ee2e47baef244d49a222db496bad0ef5b6f93aa7cc4d30c4821b3b9dbc57"
        )
        app_param = bytes.fromhex(
            "4b0be934baebb5d12d26011b69227fa5e86df94e7d94aa2949a89f2d493992ca"
        )
        key_handle = b"\3" * 64

        resp = ctap.authenticate(client_param, app_param, key_handle)
        ctap.device.call.assert_called_with(
            0x03,
            b"\0\2\3\0\0\0\x81"
            + client_param
            + app_param
            + b"\x40"
            + key_handle
            + b"\0\0",
        )

        self.assertEqual(resp.user_presence, 1)
        self.assertEqual(resp.counter, 1)
        self.assertEqual(
            resp.signature,
            bytes.fromhex(
                "304402204b5f0cd17534cedd8c34ee09570ef542a353df4436030ce43d406de870b847780220267bb998fac9b7266eb60e7cb0b5eabdfd5ba9614f53c7b22272ec10047a923f"  # noqa E501
            ),
        )

        public_key = bytes.fromhex(
            "04d368f1b665bade3c33a20f1e429c7750d5033660c019119d29aa4ba7abc04aa7c80a46bbe11ca8cb5674d74f31f8a903f6bad105fb6ab74aefef4db8b0025e1d"  # noqa E501
        )
        resp.verify(app_param, client_param, public_key)

        key_handle = b"\4" * 8
        ctap.authenticate(client_param, app_param, key_handle)
        ctap.device.call.assert_called_with(
            0x03,
            b"\0\2\3\0\0\0\x49"
            + client_param
            + app_param
            + b"\x08"
            + key_handle
            + b"\0\0",
        )

        ctap.authenticate(client_param, app_param, key_handle, True)
        ctap.device.call.assert_called_with(
            0x03,
            b"\0\2\7\0\0\0\x49"
            + client_param
            + app_param
            + b"\x08"
            + key_handle
            + b"\0\0",
        )
