"""Tests for josepy.jws."""

import base64
import sys
import unittest
from unittest import mock

import OpenSSL
import pytest
import test_util

from josepy import errors, json_util, jwa, jwk

CERT = test_util.load_comparable_cert("cert.pem")
KEY = jwk.JWKRSA.load(test_util.load_vector("rsa512_key.pem"))


class MediaTypeTest(unittest.TestCase):
    """Tests for josepy.jws.MediaType."""

    def test_decode(self) -> None:
        from josepy.jws import MediaType

        assert "application/app" == MediaType.decode("application/app")
        assert "application/app" == MediaType.decode("app")
        with pytest.raises(errors.DeserializationError):
            MediaType.decode("app;foo")

    def test_encode(self) -> None:
        from josepy.jws import MediaType

        assert "app" == MediaType.encode("application/app")
        assert "application/app;foo" == MediaType.encode("application/app;foo")


class HeaderTest(unittest.TestCase):
    """Tests for josepy.jws.Header."""

    def setUp(self) -> None:
        from josepy.jws import Header

        self.header1 = Header(jwk="foo")
        self.header2 = Header(jwk="bar")
        self.crit = Header(crit=("a", "b"))
        self.empty = Header()

    def test_add_non_empty(self) -> None:
        from josepy.jws import Header

        assert Header(jwk="foo", crit=("a", "b")) == self.header1 + self.crit

    def test_add_empty(self) -> None:
        assert self.header1 == self.header1 + self.empty
        assert self.header1 == self.empty + self.header1

    def test_add_overlapping_error(self) -> None:
        with pytest.raises(TypeError):
            self.header1.__add__(self.header2)

    def test_add_wrong_type_error(self) -> None:
        with pytest.raises(TypeError):
            self.header1.__add__("xxx")

    def test_crit_decode_always_errors(self) -> None:
        from josepy.jws import Header

        with pytest.raises(errors.DeserializationError):
            Header.from_json({"crit": ["a", "b"]})

    def test_x5c_decoding(self) -> None:
        from josepy.jws import Header

        header = Header(x5c=(CERT, CERT))
        jobj = header.to_partial_json()
        assert isinstance(CERT.wrapped, OpenSSL.crypto.X509)
        cert_asn1 = OpenSSL.crypto.dump_certificate(OpenSSL.crypto.FILETYPE_ASN1, CERT.wrapped)
        cert_b64 = base64.b64encode(cert_asn1)
        assert jobj == {"x5c": [cert_b64, cert_b64]}
        assert header == Header.from_json(jobj)
        jobj["x5c"][0] = base64.b64encode(b"xxx" + cert_asn1)
        with pytest.raises(errors.DeserializationError):
            Header.from_json(jobj)

    def test_find_key(self) -> None:
        assert "foo" == self.header1.find_key()
        assert "bar" == self.header2.find_key()
        with pytest.raises(errors.Error):
            self.crit.find_key()


class SignatureTest(unittest.TestCase):
    """Tests for josepy.jws.Signature."""

    def test_from_json(self) -> None:
        from josepy.jws import Header, Signature

        assert Signature(signature=b"foo", header=Header(alg=jwa.RS256)) == Signature.from_json(
            {"signature": "Zm9v", "header": {"alg": "RS256"}}
        )

    def test_from_json_no_alg_error(self) -> None:
        from josepy.jws import Signature

        with pytest.raises(errors.DeserializationError):
            Signature.from_json({"signature": "foo"})


class JWSTest(unittest.TestCase):
    """Tests for josepy.jws.JWS."""

    def setUp(self) -> None:
        self.privkey = KEY
        self.pubkey = self.privkey.public_key()

        from josepy.jws import JWS

        self.unprotected = JWS.sign(payload=b"foo", key=self.privkey, alg=jwa.RS256)
        self.protected = JWS.sign(
            payload=b"foo", key=self.privkey, alg=jwa.RS256, protect=frozenset(["jwk", "alg"])
        )
        self.mixed = JWS.sign(
            payload=b"foo", key=self.privkey, alg=jwa.RS256, protect=frozenset(["alg"])
        )

    def test_pubkey_jwk(self) -> None:
        assert self.unprotected.signature.combined.jwk == self.pubkey
        assert self.protected.signature.combined.jwk == self.pubkey
        assert self.mixed.signature.combined.jwk == self.pubkey

    def test_sign_unprotected(self) -> None:
        assert self.unprotected.verify() is True

    def test_sign_protected(self) -> None:
        assert self.protected.verify() is True

    def test_sign_mixed(self) -> None:
        assert self.mixed.verify() is True

    def test_compact_lost_unprotected(self) -> None:
        compact = self.mixed.to_compact()
        assert (
            b"eyJhbGciOiAiUlMyNTYifQ.Zm9v.OHdxFVj73l5LpxbFp1AmYX4yJM0Pyb"
            b"_893n1zQjpim_eLS5J1F61lkvrCrCDErTEJnBGOGesJ72M7b6Ve1cAJA" == compact
        )

        from josepy.jws import JWS

        mixed = JWS.from_compact(compact)

        assert self.mixed != mixed
        assert {"alg"} == set(mixed.signature.combined.not_omitted())

    def test_from_compact_missing_components(self) -> None:
        from josepy.jws import JWS

        with pytest.raises(errors.DeserializationError):
            JWS.from_compact(b".")

    def test_json_omitempty(self) -> None:
        protected_jobj = self.protected.to_partial_json(flat=True)
        unprotected_jobj = self.unprotected.to_partial_json(flat=True)

        assert "protected" not in unprotected_jobj
        assert "header" not in protected_jobj

        unprotected_jobj["header"] = unprotected_jobj["header"].to_json()

        from josepy.jws import JWS

        assert JWS.from_json(protected_jobj) == self.protected
        assert JWS.from_json(unprotected_jobj) == self.unprotected

    def test_json_flat(self) -> None:
        jobj_to = {
            "signature": json_util.encode_b64jose(self.mixed.signature.signature),
            "payload": json_util.encode_b64jose(b"foo"),
            "header": self.mixed.signature.header,
            "protected": json_util.encode_b64jose(self.mixed.signature.protected.encode("utf-8")),
        }

        from josepy.jws import Header

        jobj_from = jobj_to.copy()
        header = jobj_from["header"]
        assert isinstance(header, Header)
        jobj_from["header"] = header.to_json()

        assert self.mixed.to_partial_json(flat=True) == jobj_to
        from josepy.jws import JWS

        assert self.mixed == JWS.from_json(jobj_from)

    def test_json_not_flat(self) -> None:
        jobj_to = {
            "signatures": (self.mixed.signature,),
            "payload": json_util.encode_b64jose(b"foo"),
        }
        jobj_from = jobj_to.copy()
        signature = jobj_to["signatures"][0]
        from josepy.jws import Signature

        assert isinstance(signature, Signature)
        jobj_from["signatures"] = [signature.to_json()]

        assert self.mixed.to_partial_json(flat=False) == jobj_to
        from josepy.jws import JWS

        assert self.mixed == JWS.from_json(jobj_from)

    def test_from_json_mixed_flat(self) -> None:
        from josepy.jws import JWS

        with pytest.raises(errors.DeserializationError):
            JWS.from_json({"signatures": (), "signature": "foo"})

    def test_from_json_hashable(self) -> None:
        from josepy.jws import JWS

        hash(JWS.from_json(self.mixed.to_json()))


class CLITest(unittest.TestCase):
    def setUp(self) -> None:
        self.key_path = test_util.vector_path("rsa512_key.pem")

    def test_unverified(self) -> None:
        from josepy.jws import CLI

        with mock.patch("sys.stdin") as sin:
            sin.read.return_value = '{"payload": "foo", "signature": "xxx"}'
            with mock.patch("sys.stdout"):
                assert CLI.run(["verify"]) is False

    def test_json(self) -> None:
        from josepy.jws import CLI

        with mock.patch("sys.stdin") as sin:
            sin.read.return_value = "foo"
            with mock.patch("sys.stdout") as sout:
                CLI.run(["sign", "-k", self.key_path, "-a", "RS256", "-p", "jwk"])
                sin.read.return_value = sout.write.mock_calls[0][1][0]
                assert 0 == CLI.run(["verify"])

    def test_compact(self) -> None:
        from josepy.jws import CLI

        with mock.patch("sys.stdin") as sin:
            sin.read.return_value = "foo"
            with mock.patch("sys.stdout") as sout:
                CLI.run(["--compact", "sign", "-k", self.key_path])
                sin.read.return_value = sout.write.mock_calls[0][1][0]
                assert 0 == CLI.run(["--compact", "verify", "--kty", "RSA", "-k", self.key_path])


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] + [__file__]))  # pragma: no cover
