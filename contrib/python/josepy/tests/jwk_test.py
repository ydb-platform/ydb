"""Tests for josepy.jwk."""

import binascii
import sys
import unittest
from typing import TYPE_CHECKING

import pytest
import test_util

from josepy import errors, json_util, util

# The approach used here and below is based on
# https://github.com/certbot/certbot/pull/8748.
if TYPE_CHECKING:
    from typing_extensions import Protocol
else:
    Protocol = object

DSA_PEM = test_util.load_vector("dsa512_key.pem")
RSA256_KEY = test_util.load_rsa_private_key("rsa256_key.pem")
RSA512_KEY = test_util.load_rsa_private_key("rsa512_key.pem")
EC_P256_KEY = test_util.load_ec_private_key("ec_p256_key.pem")
EC_P384_KEY = test_util.load_ec_private_key("ec_p384_key.pem")
EC_P521_KEY = test_util.load_ec_private_key("ec_p521_key.pem")


class JWKTest(unittest.TestCase):
    """Tests for josepy.jwk.JWK."""

    def test_load(self) -> None:
        from josepy.jwk import JWK

        with pytest.raises(errors.Error):
            JWK.load(DSA_PEM)

    def test_load_subclass_wrong_type(self) -> None:
        from josepy.jwk import JWKRSA

        with pytest.raises(errors.Error):
            JWKRSA.load(DSA_PEM)


class JWKSubclassTest(Protocol):
    from josepy.jwk import JWK

    jwk: JWK
    thumbprint: bytes


class JWKTestBaseMixin:
    """Mixin test for JWK subclass tests."""

    thumbprint: bytes = NotImplemented

    def test_thumbprint_private(self: JWKSubclassTest) -> None:
        assert self.thumbprint == self.jwk.thumbprint()

    def test_thumbprint_public(self: JWKSubclassTest) -> None:
        assert self.thumbprint == self.jwk.public_key().thumbprint()


class JWKOctTest(unittest.TestCase, JWKTestBaseMixin):
    """Tests for josepy.jwk.JWKOct."""

    thumbprint = (
        b"\xf3\xe7\xbe\xa8`\xd2\xdap\xe9}\x9c\xce>"
        b"\xd0\xfcI\xbe\xcd\x92'\xd4o\x0e\xf41\xea"
        b"\x8e(\x8a\xb2i\x1c"
    )

    def setUp(self) -> None:
        from josepy.jwk import JWKOct

        self.jwk = JWKOct(key=b"foo")
        self.jobj = {"kty": "oct", "k": json_util.encode_b64jose(b"foo")}

    def test_to_partial_json(self) -> None:
        assert self.jwk.to_partial_json() == self.jobj

    def test_from_json(self) -> None:
        from josepy.jwk import JWKOct

        assert self.jwk == JWKOct.from_json(self.jobj)

    def test_from_json_hashable(self) -> None:
        from josepy.jwk import JWKOct

        hash(JWKOct.from_json(self.jobj))

    def test_load(self) -> None:
        from josepy.jwk import JWKOct

        assert self.jwk == JWKOct.load(b"foo")

    def test_public_key(self) -> None:
        assert self.jwk.public_key() is self.jwk


class JWKRSATest(unittest.TestCase, JWKTestBaseMixin):
    """Tests for josepy.jwk.JWKRSA."""

    thumbprint = (
        b"\x83K\xdc#3\x98\xca\x98\xed\xcb\x80\x80<\x0c"
        b"\xf0\x95\xb9H\xb2*l\xbd$\xe5&|O\x91\xd4 \xb0Y"
    )

    def setUp(self) -> None:
        from josepy.jwk import JWKRSA

        self.jwk256 = JWKRSA(key=RSA256_KEY.public_key())
        self.jwk256json = {
            "kty": "RSA",
            "e": "AQAB",
            "n": "m2Fylv-Uz7trgTW8EBHP3FQSMeZs2GNQ6VRo1sIVJEk",
        }
        self.jwk256_not_comparable = JWKRSA(key=RSA256_KEY.public_key()._wrapped)
        self.jwk512 = JWKRSA(key=RSA512_KEY.public_key())
        self.jwk512json = {
            "kty": "RSA",
            "e": "AQAB",
            "n": "rHVztFHtH92ucFJD_N_HW9AsdRsUuHUBBBDlHwNlRd3fp5"
            "80rv2-6QWE30cWgdmJS86ObRz6lUTor4R0T-3C5Q",
        }
        self.private = JWKRSA(key=RSA256_KEY)
        self.private_json_small = self.jwk256json.copy()
        self.private_json_small["d"] = "lPQED_EPTV0UIBfNI3KP2d9Jlrc2mrMllmf946bu-CE"
        self.private_json = self.jwk256json.copy()
        self.private_json.update(
            {
                "d": "lPQED_EPTV0UIBfNI3KP2d9Jlrc2mrMllmf946bu-CE",
                "p": "zUVNZn4lLLBD1R6NE8TKNQ",
                "q": "wcfKfc7kl5jfqXArCRSURQ",
                "dp": "CWJFq43QvT5Bm5iN8n1okQ",
                "dq": "bHh2u7etM8LKKCF2pY2UdQ",
                "qi": "oi45cEkbVoJjAbnQpFY87Q",
            }
        )
        self.jwk = self.private

    def test_init_auto_comparable(self) -> None:
        assert isinstance(self.jwk256_not_comparable.key, util.ComparableRSAKey)
        assert self.jwk256 == self.jwk256_not_comparable

    def test_encode_param_zero(self) -> None:
        from josepy.jwk import JWKRSA

        # TODO: move encode/decode _param to separate class
        assert "AA" == JWKRSA._encode_param(0)

    def test_equals(self) -> None:
        assert self.jwk256 == self.jwk256
        assert self.jwk512 == self.jwk512

    def test_not_equals(self) -> None:
        assert self.jwk256 != self.jwk512
        assert self.jwk512 != self.jwk256

    def test_load(self) -> None:
        from josepy.jwk import JWKRSA

        assert self.private == JWKRSA.load(test_util.load_vector("rsa256_key.pem"))

    def test_public_key(self) -> None:
        assert self.jwk256 == self.private.public_key()

    def test_to_partial_json(self) -> None:
        assert self.jwk256.to_partial_json() == self.jwk256json
        assert self.jwk512.to_partial_json() == self.jwk512json
        assert self.private.to_partial_json() == self.private_json

    def test_from_json(self) -> None:
        from josepy.jwk import JWK

        assert self.jwk256 == JWK.from_json(self.jwk256json)
        assert self.jwk512 == JWK.from_json(self.jwk512json)
        assert self.private == JWK.from_json(self.private_json)

    def test_from_json_private_small(self) -> None:
        from josepy.jwk import JWK

        assert self.private == JWK.from_json(self.private_json_small)

    def test_from_json_missing_one_additional(self) -> None:
        from josepy.jwk import JWK

        del self.private_json["q"]
        with pytest.raises(errors.Error):
            JWK.from_json(self.private_json)

    def test_from_json_hashable(self) -> None:
        from josepy.jwk import JWK

        hash(JWK.from_json(self.jwk256json))

    def test_from_json_non_schema_errors(self) -> None:
        # valid against schema, but still failing
        from josepy.jwk import JWK

        with pytest.raises(errors.DeserializationError):
            JWK.from_json({"kty": "RSA", "e": "AQAB", "n": ""})
        with pytest.raises(errors.DeserializationError):
            JWK.from_json({"kty": "RSA", "e": "AQAB", "n": "1"})

    def test_thumbprint_go_jose(self) -> None:
        # https://github.com/square/go-jose/blob/4ddd71883fa547d37fbf598071f04512d8bafee3/jwk.go#L155
        # https://github.com/square/go-jose/blob/4ddd71883fa547d37fbf598071f04512d8bafee3/jwk_test.go#L331-L344
        # https://github.com/square/go-jose/blob/4ddd71883fa547d37fbf598071f04512d8bafee3/jwk_test.go#L384
        from josepy.jwk import JWKRSA

        key = JWKRSA.json_loads(
            """{
    "kty": "RSA",
    "kid": "bilbo.baggins@hobbiton.example",
    "use": "sig",
    "n": "n4EPtAOCc9AlkeQHPzHStgAbgs7bTZLwUBZdR8_KuKPEHLd4rHVTeT-O-XV2jRojdNhxJWTDvNd7nqQ0VEiZQHz_AJmSCpMaJMRBSFKrKb2wqVwGU_NsYOYL-QtiWN2lbzcEe6XC0dApr5ydQLrHqkHHig3RBordaZ6Aj-oBHqFEHYpPe7Tpe-OfVfHd1E6cS6M1FZcD1NNLYD5lFHpPI9bTwJlsde3uhGqC0ZCuEHg8lhzwOHrtIQbS0FVbb9k3-tVTU4fg_3L_vniUFAKwuCLqKnS2BYwdq_mzSnbLY7h_qixoR7jig3__kRhuaxwUkRz5iaiQkqgc5gHdrNP5zw",
    "e": "AQAB"
}"""  # noqa
        )
        assert (
            binascii.hexlify(key.thumbprint())
            == b"f63838e96077ad1fc01c3f8405774dedc0641f558ebb4b40dccf5f9b6d66a932"
        )


class JWKECTest(unittest.TestCase, JWKTestBaseMixin):
    """Tests for josepy.jwk.JWKEC."""

    # pylint: disable=too-many-instance-attributes

    thumbprint = (
        b"\x06\xceL\x1b\xa8\x8d\x86\x1flF\x99J\x8b\xe0$\t\xbbj"
        b"\xd8\xf6O\x1ed\xdeR\x8f\x97\xff\xf6\xa2\x86\xd3"
    )

    def setUp(self) -> None:
        from josepy.jwk import JWKEC

        self.jwk256 = JWKEC(key=EC_P256_KEY.public_key())
        self.jwk384 = JWKEC(key=EC_P384_KEY.public_key())
        self.jwk521 = JWKEC(key=EC_P521_KEY.public_key())
        self.jwk256_not_comparable = JWKEC(key=EC_P256_KEY.public_key()._wrapped)
        self.jwk256json = {
            "kty": "EC",
            "crv": "P-256",
            "x": "jjQtV-fA7J_tK8dPzYq7jRPNjF8r5p6LW2R25S2Gw5U",
            "y": "EPAw8_8z7PYKsHH6hlGSlsWxFoFl7-0vM0QRGbmnvCc",
        }
        self.jwk384json = {
            "kty": "EC",
            "crv": "P-384",
            "x": "tIhpNtEXkadUbrY84rYGgApFM1X_3l3EWQRuOP1IWtxlTftrZQwneJZF0k0eRn00",
            "y": "KW2Gp-TThDXmZ-9MJPnD8hv-X130SVvfZRl1a04HPVwIbvLe87mvA_iuOa-myUyv",
        }
        self.jwk521json = {
            "kty": "EC",
            "crv": "P-521",
            "x": "AFkdl6cKzBmP18U8fffpP4IZN2eED45hDcwRPl5ZeClwHcLtnMBMuWYFFO_Nzm6DL2MhpN0zI2bcMLJd95aY2tPs",  # noqa
            "y": "AYvZq3wByjt7nQd8nYMqhFNCL3j_-U6GPWZet1hYBY_XZHrC4yIV0R4JnssRAY9eqc1EElpCc4hziis1jiV1iR4W",  # noqa
        }
        self.private = JWKEC(key=EC_P256_KEY)
        self.private_json = {
            "d": "xReNQBKqqTthG8oTmBdhp4EQYImSK1dVqfa2yyMn2rc",
            "x": "jjQtV-fA7J_tK8dPzYq7jRPNjF8r5p6LW2R25S2Gw5U",
            "y": "EPAw8_8z7PYKsHH6hlGSlsWxFoFl7-0vM0QRGbmnvCc",
            "crv": "P-256",
            "kty": "EC",
        }
        self.jwk = self.private

    def test_init_auto_comparable(self) -> None:
        assert isinstance(self.jwk256_not_comparable.key, util.ComparableECKey)
        assert self.jwk256 == self.jwk256_not_comparable

    def test_encode_param_zero(self) -> None:
        from josepy.jwk import JWKEC

        # pylint: disable=protected-access
        # TODO: move encode/decode _param to separate class
        assert "AA" == JWKEC._encode_param(0, 1)

    def test_equals(self) -> None:
        assert self.jwk256 == self.jwk256
        assert self.jwk384 == self.jwk384
        assert self.jwk521 == self.jwk521

    def test_not_equals(self) -> None:
        assert self.jwk256 != self.jwk384
        assert self.jwk256 != self.jwk521
        assert self.jwk384 != self.jwk256
        assert self.jwk384 != self.jwk521
        assert self.jwk521 != self.jwk256
        assert self.jwk521 != self.jwk384

    def test_load(self) -> None:
        from josepy.jwk import JWKEC

        assert self.private == JWKEC.load(test_util.load_vector("ec_p256_key.pem"))

    def test_public_key(self) -> None:
        assert self.jwk256 == self.private.public_key()

    def test_to_partial_json(self) -> None:
        assert self.jwk256.to_partial_json() == self.jwk256json
        assert self.jwk384.to_partial_json() == self.jwk384json
        assert self.jwk521.to_partial_json() == self.jwk521json
        assert self.private.to_partial_json() == self.private_json

    def test_from_json(self) -> None:
        from josepy.jwk import JWK

        assert self.jwk256 == JWK.from_json(self.jwk256json)
        assert self.jwk384 == JWK.from_json(self.jwk384json)
        assert self.jwk521 == JWK.from_json(self.jwk521json)
        assert self.private == JWK.from_json(self.private_json)

    def test_from_json_missing_x_coordinate(self) -> None:
        from josepy.jwk import JWK

        del self.private_json["x"]
        with pytest.raises(KeyError):
            JWK.from_json(self.private_json)

    def test_from_json_missing_y_coordinate(self) -> None:
        from josepy.jwk import JWK

        del self.private_json["y"]
        with pytest.raises(KeyError):
            JWK.from_json(self.private_json)

    def test_from_json_hashable(self) -> None:
        from josepy.jwk import JWK

        hash(JWK.from_json(self.jwk256json))

    def test_from_json_non_schema_errors(self) -> None:
        # valid against schema, but still failing
        from josepy.jwk import JWK

        with pytest.raises(errors.DeserializationError):
            JWK.from_json(
                {
                    "kty": "EC",
                    "crv": "P-256",
                    "x": "AQAB",
                    "y": "m2Fylv-Uz7trgTW8EBHP3FQSMeZs2GNQ6VRo1sIVJEk",
                }
            )
        with pytest.raises(errors.DeserializationError):
            JWK.from_json(
                {
                    "kty": "EC",
                    "crv": "P-256",
                    "x": "jjQtV-fA7J_tK8dPzYq7jRPNjF8r5p6LW2R25S2Gw5U",
                    "y": "1",
                }
            )

    def test_unknown_crv_name(self) -> None:
        from josepy.jwk import JWK

        with pytest.raises(errors.DeserializationError):
            JWK.from_json(
                {
                    "kty": "EC",
                    "crv": "P-255",
                    "x": "jjQtV-fA7J_tK8dPzYq7jRPNjF8r5p6LW2R25S2Gw5U",
                    "y": "EPAw8_8z7PYKsHH6hlGSlsWxFoFl7-0vM0QRGbmnvCc",
                }
            )

    def test_encode_y_leading_zero_p256(self) -> None:
        import josepy
        from josepy.jwk import JWK, JWKEC

        data = b"""-----BEGIN EC PRIVATE KEY-----
MHcCAQEEICZ7LCI99Na2KZ/Fq8JmJROakGJ5+J7rHiGSPoO36kOAoAoGCCqGSM49
AwEHoUQDQgAEGS5RvStca15z2FEanCM3juoX7tE/LB7iD44GWawGE40APAl/iZuH
31wQfst4glTZpxkpEI/MzNZHjiYnqrGeSw==
-----END EC PRIVATE KEY-----"""
        key = JWKEC.load(data)
        json = key.to_partial_json()
        y = josepy.json_util.decode_b64jose(json["y"])
        assert y[0] == 0
        assert len(y) == 32
        JWK.from_json(json)


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] + [__file__]))  # pragma: no cover
