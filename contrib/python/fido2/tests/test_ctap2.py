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
from fido2.ctap1 import RegistrationData
from fido2.ctap2 import (
    Ctap2,
    ClientPin,
    PinProtocolV1,
    Info,
    AttestationResponse,
    AssertionResponse,
)
from fido2.webauthn import AttestationObject, AuthenticatorData, AttestedCredentialData
from fido2.attestation import Attestation
from fido2 import cbor
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import ec


_AAGUID = bytes.fromhex("F8A011F38C0A4D15800617111F9EDC7D")
_INFO = bytes.fromhex(
    "a60182665532465f5632684649444f5f325f3002826375766d6b686d61632d7365637265740350f8a011f38c0a4d15800617111f9edc7d04a462726bf5627570f564706c6174f469636c69656e7450696ef4051904b0068101"  # noqa E501
)
_INFO_EXTRA_KEY = bytes.fromhex(
    "A70182665532465F5632684649444F5F325F3002826375766D6B686D61632D7365637265740350F8A011F38C0A4D15800617111F9EDC7D04A462726BF5627570F564706C6174F469636C69656E7450696EF4051904B006810118631904D2"  # noqa E501
)


class TestInfo(unittest.TestCase):
    def test_parse_bytes(self):
        info = Info.from_dict(cbor.decode(_INFO))

        self.assertEqual(info.versions, ["U2F_V2", "FIDO_2_0"])
        self.assertEqual(info.extensions, ["uvm", "hmac-secret"])
        self.assertEqual(info.aaguid, _AAGUID)
        self.assertEqual(
            info.options, {"rk": True, "up": True, "plat": False, "clientPin": False}
        )
        self.assertEqual(info.max_msg_size, 1200)
        self.assertEqual(info.pin_uv_protocols, [1])
        assert info[0x01] == ["U2F_V2", "FIDO_2_0"]
        assert info[0x02] == ["uvm", "hmac-secret"]
        assert info[0x03] == _AAGUID
        assert info[0x04] == {
            "clientPin": False,
            "plat": False,
            "rk": True,
            "up": True,
        }
        assert info[0x05] == 1200
        assert info[0x06] == [1]

    def test_info_with_extra_field(self):
        info = Info.from_dict(cbor.decode(_INFO_EXTRA_KEY))
        self.assertEqual(info.versions, ["U2F_V2", "FIDO_2_0"])


_ATT_CRED_DATA = bytes.fromhex(
    "f8a011f38c0a4d15800617111f9edc7d0040fe3aac036d14c1e1c65518b698dd1da8f596bc33e11072813466c6bf3845691509b80fb76d59309b8d39e0a93452688f6ca3a39a76f3fc52744fb73948b15783a5010203262001215820643566c206dd00227005fa5de69320616ca268043a38f08bde2e9dc45a5cafaf225820171353b2932434703726aae579fa6542432861fe591e481ea22d63997e1a5290"  # noqa E501
)
_CRED_ID = bytes.fromhex(
    "fe3aac036d14c1e1c65518b698dd1da8f596bc33e11072813466c6bf3845691509b80fb76d59309b8d39e0a93452688f6ca3a39a76f3fc52744fb73948b15783"  # noqa E501
)
_PUB_KEY = {
    1: 2,
    3: -7,
    -1: 1,
    -2: bytes.fromhex(
        "643566c206dd00227005fa5de69320616ca268043a38f08bde2e9dc45a5cafaf"
    ),
    -3: bytes.fromhex(
        "171353b2932434703726aae579fa6542432861fe591e481ea22d63997e1a5290"
    ),
}


class TestAttestedCredentialData(unittest.TestCase):
    def test_parse_bytes(self):
        data = AttestedCredentialData(_ATT_CRED_DATA)
        self.assertEqual(data.aaguid, _AAGUID)
        self.assertEqual(data.credential_id, _CRED_ID)
        self.assertEqual(data.public_key, _PUB_KEY)

    def test_create_from_args(self):
        data = AttestedCredentialData.create(_AAGUID, _CRED_ID, _PUB_KEY)
        self.assertEqual(_ATT_CRED_DATA, data)


_AUTH_DATA_MC = bytes.fromhex(
    "0021F5FC0B85CD22E60623BCD7D1CA48948909249B4776EB515154E57B66AE12410000001CF8A011F38C0A4D15800617111F9EDC7D0040FE3AAC036D14C1E1C65518B698DD1DA8F596BC33E11072813466C6BF3845691509B80FB76D59309B8D39E0A93452688F6CA3A39A76F3FC52744FB73948B15783A5010203262001215820643566C206DD00227005FA5DE69320616CA268043A38F08BDE2E9DC45A5CAFAF225820171353B2932434703726AAE579FA6542432861FE591E481EA22D63997E1A5290"  # noqa E501
)
_AUTH_DATA_GA = bytes.fromhex(
    "0021F5FC0B85CD22E60623BCD7D1CA48948909249B4776EB515154E57B66AE12010000001D"
)
_RP_ID_HASH = bytes.fromhex(
    "0021F5FC0B85CD22E60623BCD7D1CA48948909249B4776EB515154E57B66AE12"
)


class TestAuthenticatorData(unittest.TestCase):
    def test_parse_bytes_make_credential(self):
        data = AuthenticatorData(_AUTH_DATA_MC)
        self.assertEqual(data.rp_id_hash, _RP_ID_HASH)
        self.assertEqual(data.flags, 0x41)
        self.assertEqual(data.counter, 28)
        self.assertEqual(data.credential_data, _ATT_CRED_DATA)
        self.assertIsNone(data.extensions)

    def test_parse_bytes_get_assertion(self):
        data = AuthenticatorData(_AUTH_DATA_GA)
        self.assertEqual(data.rp_id_hash, _RP_ID_HASH)
        self.assertEqual(data.flags, 0x01)
        self.assertEqual(data.counter, 29)
        self.assertIsNone(data.credential_data)
        self.assertIsNone(data.extensions)


_MC_RESP = bytes.fromhex(
    "a301667061636b65640258c40021f5fc0b85cd22e60623bcd7d1ca48948909249b4776eb515154e57b66ae12410000001cf8a011f38c0a4d15800617111f9edc7d0040fe3aac036d14c1e1c65518b698dd1da8f596bc33e11072813466c6bf3845691509b80fb76d59309b8d39e0a93452688f6ca3a39a76f3fc52744fb73948b15783a5010203262001215820643566c206dd00227005fa5de69320616ca268043a38f08bde2e9dc45a5cafaf225820171353b2932434703726aae579fa6542432861fe591e481ea22d63997e1a529003a363616c67266373696758483046022100cc1ef43edf07de8f208c21619c78a565ddcf4150766ad58781193be8e0a742ed022100f1ed7c7243e45b7d8e5bda6b1abf10af7391789d1ef21b70bd69fed48dba4cb163783563815901973082019330820138a003020102020900859b726cb24b4c29300a06082a8648ce3d0403023047310b300906035504061302555331143012060355040a0c0b59756269636f205465737431223020060355040b0c1941757468656e74696361746f72204174746573746174696f6e301e170d3136313230343131353530305a170d3236313230323131353530305a3047310b300906035504061302555331143012060355040a0c0b59756269636f205465737431223020060355040b0c1941757468656e74696361746f72204174746573746174696f6e3059301306072a8648ce3d020106082a8648ce3d03010703420004ad11eb0e8852e53ad5dfed86b41e6134a18ec4e1af8f221a3c7d6e636c80ea13c3d504ff2e76211bb44525b196c44cb4849979cf6f896ecd2bb860de1bf4376ba30d300b30090603551d1304023000300a06082a8648ce3d0403020349003046022100e9a39f1b03197525f7373e10ce77e78021731b94d0c03f3fda1fd22db3d030e7022100c4faec3445a820cf43129cdb00aabefd9ae2d874f9c5d343cb2f113da23723f3"  # noqa E501
)
_GA_RESP = bytes.fromhex(
    "a301a26269645840fe3aac036d14c1e1c65518b698dd1da8f596bc33e11072813466c6bf3845691509b80fb76d59309b8d39e0a93452688f6ca3a39a76f3fc52744fb73948b1578364747970656a7075626c69632d6b65790258250021f5fc0b85cd22e60623bcd7d1ca48948909249b4776eb515154e57b66ae12010000001d035846304402206765cbf6e871d3af7f01ae96f06b13c90f26f54b905c5166a2c791274fc2397102200b143893586cc799fba4da83b119eaea1bd80ac3ce88fcedb3efbd596a1f4f63"  # noqa E501
)
_CRED_ID = bytes.fromhex(
    "FE3AAC036D14C1E1C65518B698DD1DA8F596BC33E11072813466C6BF3845691509B80FB76D59309B8D39E0A93452688F6CA3A39A76F3FC52744FB73948B15783"  # noqa E501
)
_CRED = {"type": "public-key", "id": _CRED_ID}
_SIGNATURE = bytes.fromhex(
    "304402206765CBF6E871D3AF7F01AE96F06B13C90F26F54B905C5166A2C791274FC2397102200B143893586CC799FBA4DA83B119EAEA1BD80AC3CE88FCEDB3EFBD596A1F4F63"  # noqa E501
)


class TestAttestationObject(unittest.TestCase):
    def test_fido_u2f_attestation(self):
        att = AttestationObject.from_ctap1(
            bytes.fromhex(
                "1194228DA8FDBDEEFD261BD7B6595CFD70A50D70C6407BCF013DE96D4EFB17DE"
            ),
            RegistrationData(
                bytes.fromhex(
                    "0504E87625896EE4E46DC032766E8087962F36DF9DFE8B567F3763015B1990A60E1427DE612D66418BDA1950581EBC5C8C1DAD710CB14C22F8C97045F4612FB20C91403EBD89BF77EC509755EE9C2635EFAAAC7B2B9C5CEF1736C3717DA48534C8C6B654D7FF945F50B5CC4E78055BDD396B64F78DA2C5F96200CCD415CD08FE4200383082024A30820132A0030201020204046C8822300D06092A864886F70D01010B0500302E312C302A0603550403132359756269636F2055324620526F6F742043412053657269616C203435373230303633313020170D3134303830313030303030305A180F32303530303930343030303030305A302C312A302806035504030C2159756269636F205532462045452053657269616C203234393138323332343737303059301306072A8648CE3D020106082A8648CE3D030107034200043CCAB92CCB97287EE8E639437E21FCD6B6F165B2D5A3F3DB131D31C16B742BB476D8D1E99080EB546C9BBDF556E6210FD42785899E78CC589EBE310F6CDB9FF4A33B3039302206092B0601040182C40A020415312E332E362E312E342E312E34313438322E312E323013060B2B0601040182E51C020101040403020430300D06092A864886F70D01010B050003820101009F9B052248BC4CF42CC5991FCAABAC9B651BBE5BDCDC8EF0AD2C1C1FFB36D18715D42E78B249224F92C7E6E7A05C49F0E7E4C881BF2E94F45E4A21833D7456851D0F6C145A29540C874F3092C934B43D222B8962C0F410CEF1DB75892AF116B44A96F5D35ADEA3822FC7146F6004385BCB69B65C99E7EB6919786703C0D8CD41E8F75CCA44AA8AB725AD8E799FF3A8696A6F1B2656E631B1E40183C08FDA53FA4A8F85A05693944AE179A1339D002D15CABD810090EC722EF5DEF9965A371D415D624B68A2707CAD97BCDD1785AF97E258F33DF56A031AA0356D8E8D5EBCADC74E071636C6B110ACE5CC9B90DFEACAE640FF1BB0F1FE5DB4EFF7A95F060733F530450220324779C68F3380288A1197B6095F7A6EB9B1B1C127F66AE12A99FE8532EC23B9022100E39516AC4D61EE64044D50B415A6A4D4D84BA6D895CB5AB7A1AA7D081DE341FA"  # noqa E501
                )
            ),
        )
        Attestation.for_type(att.fmt)().verify(
            att.att_stmt,
            att.auth_data,
            bytes.fromhex(
                "687134968222EC17202E42505F8ED2B16AE22F16BB05B88C25DB9E602645F141"
            ),
        )

    def test_packed_attestation(self):
        att = AttestationResponse.from_dict(
            cbor.decode(
                bytes.fromhex(
                    "a301667061636b65640258c40021f5fc0b85cd22e60623bcd7d1ca48948909249b4776eb515154e57b66ae124100000003f8a011f38c0a4d15800617111f9edc7d004060a386206a3aacecbdbb22d601853d955fdc5d11adfbd1aa6a950d966b348c7663d40173714a9f987df6461beadfb9cd6419ffdfe4d4cf2eec1aa605a4f59bdaa50102032620012158200edb27580389494d74d2373b8f8c2e8b76fa135946d4f30d0e187e120b423349225820e03400d189e85a55de9ab0f538ed60736eb750f5f0306a80060fe1b13010560d03a363616c6726637369675847304502200d15daf337d727ab4719b4027114a2ac43cd565d394ced62c3d9d1d90825f0b3022100989615e7394c87f4ad91f8fdae86f7a3326df332b3633db088aac76bffb9a46b63783563815902bb308202b73082019fa00302010202041d31330d300d06092a864886f70d01010b0500302a3128302606035504030c1f59756269636f2050726576696577204649444f204174746573746174696f6e301e170d3138303332383036333932345a170d3139303332383036333932345a306e310b300906035504061302534531123010060355040a0c0959756269636f20414231223020060355040b0c1941757468656e74696361746f72204174746573746174696f6e3127302506035504030c1e59756269636f205532462045452053657269616c203438393736333539373059301306072a8648ce3d020106082a8648ce3d030107034200047d71e8367cafd0ea6cf0d61e4c6a416ba5bb6d8fad52db2389ad07969f0f463bfdddddc29d39d3199163ee49575a3336c04b3309d607f6160c81e023373e0197a36c306a302206092b0601040182c40a020415312e332e362e312e342e312e34313438322e312e323013060b2b0601040182e51c0201010404030204303021060b2b0601040182e51c01010404120410f8a011f38c0a4d15800617111f9edc7d300c0603551d130101ff04023000300d06092a864886f70d01010b050003820101009b904ceadbe1f1985486fead02baeaa77e5ab4e6e52b7e6a2666a4dc06e241578169193b63dadec5b2b78605a128b2e03f7fe2a98eaeb4219f52220995f400ce15d630cf0598ba662d7162459f1ad1fc623067376d4e4091be65ac1a33d8561b9996c0529ec1816d1710786384d5e8783aa1f7474cb99fe8f5a63a79ff454380361c299d67cb5cc7c79f0d8c09f8849b0500f6d625408c77cbbc26ddee11cb581beb7947137ad4f05aaf38bd98da10042ddcac277604a395a5b3eaa88a5c8bb27ab59c8127d59d6bbba5f11506bf7b75fda7561a0837c46f025fd54dcf1014fc8d17c859507ac57d4b1dea99485df0ba8f34d00103c3eef2ef3bbfec7a6613de"  # noqa E501
                )
            )
        )
        Attestation.for_type(att.fmt)().verify(
            att.att_stmt,
            att.auth_data,
            bytes.fromhex(
                "985B6187D042FB1258892ED637CEC88617DDF5F6632351A545617AA2B75261BF"
            ),
        )


class TestCtap2(unittest.TestCase):
    def mock_ctap(self):
        device = mock.MagicMock()
        device.call.return_value = b"\0" + _INFO
        return Ctap2(device)

    def test_send_cbor_ok(self):
        ctap = self.mock_ctap()
        ctap.device.call.return_value = b"\0" + cbor.encode({1: b"response"})

        self.assertEqual({1: b"response"}, ctap.send_cbor(2, b"foobar"))
        ctap.device.call.assert_called_with(
            0x10, b"\2" + cbor.encode(b"foobar"), mock.ANY, None
        )

    def test_get_info(self):
        ctap = self.mock_ctap()

        info = ctap.get_info()
        ctap.device.call.assert_called_with(0x10, b"\4", mock.ANY, None)
        self.assertIsInstance(info, Info)

    def test_make_credential(self):
        ctap = self.mock_ctap()
        ctap.device.call.return_value = b"\0" + _MC_RESP

        resp = ctap.make_credential(1, 2, 3, 4)
        ctap.device.call.assert_called_with(
            0x10, b"\1" + cbor.encode({1: 1, 2: 2, 3: 3, 4: 4}), mock.ANY, None
        )

        self.assertIsInstance(resp, AttestationResponse)
        self.assertEqual(resp, AttestationResponse.from_dict(cbor.decode(_MC_RESP)))
        self.assertEqual(resp.fmt, "packed")
        self.assertEqual(resp.auth_data, _AUTH_DATA_MC)
        self.assertSetEqual(set(resp.att_stmt.keys()), {"alg", "sig", "x5c"})

    def test_get_assertion(self):
        ctap = self.mock_ctap()
        ctap.device.call.return_value = b"\0" + _GA_RESP

        resp = ctap.get_assertion(1, 2)
        ctap.device.call.assert_called_with(
            0x10, b"\2" + cbor.encode({1: 1, 2: 2}), mock.ANY, None
        )

        self.assertIsInstance(resp, AssertionResponse)
        self.assertEqual(resp, AssertionResponse.from_dict(cbor.decode(_GA_RESP)))
        self.assertEqual(resp.credential, _CRED)
        self.assertEqual(resp.auth_data, _AUTH_DATA_GA)
        self.assertEqual(resp.signature, _SIGNATURE)
        self.assertIsNone(resp.user)
        self.assertIsNone(resp.number_of_credentials)


EC_PRIV = 0x7452E599FEE739D8A653F6A507343D12D382249108A651402520B72F24FE7684
EC_PUB_X = bytes.fromhex(
    "44D78D7989B97E62EA993496C9EF6E8FD58B8B00715F9A89153DDD9C4657E47F"
)
EC_PUB_Y = bytes.fromhex(
    "EC802EE7D22BD4E100F12E48537EB4E7E96ED3A47A0A3BD5F5EEAB65001664F9"
)
DEV_PUB_X = bytes.fromhex(
    "0501D5BC78DA9252560A26CB08FCC60CBE0B6D3B8E1D1FCEE514FAC0AF675168"
)
DEV_PUB_Y = bytes.fromhex(
    "D551B3ED46F665731F95B4532939C25D91DB7EB844BD96D4ABD4083785F8DF47"
)
SHARED = bytes.fromhex(
    "c42a039d548100dfba521e487debcbbb8b66bb7496f8b1862a7a395ed83e1a1c"
)
TOKEN_ENC = bytes.fromhex("7A9F98E31B77BE90F9C64D12E9635040")
TOKEN = bytes.fromhex("aff12c6dcfbf9df52f7a09211e8865cd")
PIN_HASH_ENC = bytes.fromhex("afe8327ce416da8ee3d057589c2ce1a9")


class TestClientPin(unittest.TestCase):
    @mock.patch("cryptography.hazmat.primitives.asymmetric.ec.generate_private_key")
    def test_establish_shared_secret(self, patched_generate):
        ctap = mock.MagicMock()
        ctap.info.options = {"clientPin": True}
        prot = ClientPin(ctap, PinProtocolV1())

        patched_generate.return_value = ec.derive_private_key(
            EC_PRIV, ec.SECP256R1(), default_backend()
        )

        ctap.client_pin.return_value = {
            1: {1: 2, 3: -25, -1: 1, -2: DEV_PUB_X, -3: DEV_PUB_Y}
        }

        key_agreement, shared = prot._get_shared_secret()

        self.assertEqual(shared, SHARED)
        self.assertEqual(key_agreement[-2], EC_PUB_X)
        self.assertEqual(key_agreement[-3], EC_PUB_Y)

    def test_get_pin_token(self):
        ctap = mock.MagicMock()
        ctap.info.options = {"clientPin": True}
        prot = ClientPin(ctap, PinProtocolV1())

        prot._get_shared_secret = mock.Mock(return_value=({}, SHARED))
        prot.ctap.client_pin.return_value = {2: TOKEN_ENC}

        self.assertEqual(prot.get_pin_token("1234"), TOKEN)
        prot.ctap.client_pin.assert_called_once()
        self.assertEqual(
            prot.ctap.client_pin.call_args[1]["pin_hash_enc"], PIN_HASH_ENC
        )

    def test_set_pin(self):
        ctap = mock.MagicMock()
        ctap.info.options = {"clientPin": True}
        prot = ClientPin(ctap, PinProtocolV1())

        prot._get_shared_secret = mock.Mock(return_value=({}, SHARED))

        prot.set_pin("1234")
        prot.ctap.client_pin.assert_called_with(
            1,
            3,
            key_agreement={},
            new_pin_enc=bytes.fromhex(
                "0222fc42c6dd76a274a7057858b9b29d98e8a722ec2dc6668476168c5320473cec9907b4cd76ce7943c96ba5683943211d84471e64d9c51e54763488cd66526a"  # noqa E501
            ),
            pin_uv_param=bytes.fromhex("7b40c084ccc5794194189ab57836475f"),
        )

    def test_change_pin(self):
        ctap = mock.MagicMock()
        ctap.info.options = {"clientPin": True}
        prot = ClientPin(ctap, PinProtocolV1())

        prot._get_shared_secret = mock.Mock(return_value=({}, SHARED))

        prot.change_pin("1234", "4321")
        prot.ctap.client_pin.assert_called_with(
            1,
            4,
            key_agreement={},
            new_pin_enc=bytes.fromhex(
                "4280e14aac4fcbf02dd079985f0c0ffc9ea7d5f9c173fd1a4c843826f7590cb3c2d080c6923e2fe6d7a52c31ea1309d3fcca3dedae8a2ef14b6330cafc79339e"  # noqa E501
            ),
            pin_uv_param=bytes.fromhex("fb97e92f3724d7c85e001d7f93e6490a"),
            pin_hash_enc=bytes.fromhex("afe8327ce416da8ee3d057589c2ce1a9"),
        )

    def test_short_pin(self):
        ctap = mock.MagicMock()
        ctap.info.options = {"clientPin": True}
        prot = ClientPin(ctap, PinProtocolV1())

        with self.assertRaises(ValueError):
            prot.set_pin("123")

    def test_long_pin(self):
        ctap = mock.MagicMock()
        ctap.info.options = {"clientPin": True}
        prot = ClientPin(ctap, PinProtocolV1())

        with self.assertRaises(ValueError):
            prot.set_pin("1" * 256)
