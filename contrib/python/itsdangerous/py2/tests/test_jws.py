from datetime import timedelta
from functools import partial

import pytest
from __tests__.test_serializer import TestSerializer
from __tests__.test_timed import TestTimedSerializer

from itsdangerous.exc import BadData
from itsdangerous.exc import BadHeader
from itsdangerous.exc import BadPayload
from itsdangerous.exc import BadSignature
from itsdangerous.exc import SignatureExpired
from itsdangerous.jws import JSONWebSignatureSerializer
from itsdangerous.jws import TimedJSONWebSignatureSerializer


class TestJWSSerializer(TestSerializer):
    @pytest.fixture()
    def serializer_factory(self):
        return partial(JSONWebSignatureSerializer, secret_key="secret-key")

    test_signer_cls = None
    test_signer_kwargs = None
    test_fallback_signers = None
    test_iter_unsigners = None

    @pytest.mark.parametrize("algorithm_name", ("HS256", "HS384", "HS512", "none"))
    def test_algorithm(self, serializer_factory, algorithm_name):
        serializer = serializer_factory(algorithm_name=algorithm_name)
        assert serializer.loads(serializer.dumps("value")) == "value"

    def test_invalid_algorithm(self, serializer_factory):
        with pytest.raises(NotImplementedError) as exc_info:
            serializer_factory(algorithm_name="invalid")

        assert "not supported" in str(exc_info.value)

    def test_algorithm_mismatch(self, serializer_factory, serializer):
        other = serializer_factory(algorithm_name="HS256")
        other.algorithm = serializer.algorithm
        signed = other.dumps("value")

        with pytest.raises(BadHeader) as exc_info:
            serializer.loads(signed)

        assert "mismatch" in str(exc_info.value)

    @pytest.mark.parametrize(
        ("value", "exc_cls", "match"),
        (
            ("ab", BadPayload, '"."'),
            ("a.b", BadHeader, "base64 decode"),
            ("ew.b", BadPayload, "base64 decode"),
            ("ew.ab", BadData, "malformed"),
            ("W10.ab", BadHeader, "JSON object"),
        ),
    )
    def test_load_payload_exceptions(self, serializer, value, exc_cls, match):
        signer = serializer.make_signer()
        signed = signer.sign(value)

        with pytest.raises(exc_cls) as exc_info:
            serializer.loads(signed)

        assert match in str(exc_info.value)


class TestTimedJWSSerializer(TestJWSSerializer, TestTimedSerializer):
    @pytest.fixture()
    def serializer_factory(self):
        return partial(
            TimedJSONWebSignatureSerializer, secret_key="secret-key", expires_in=10
        )

    def test_default_expires_in(self, serializer_factory):
        serializer = serializer_factory(expires_in=None)
        assert serializer.expires_in == serializer.DEFAULT_EXPIRES_IN

    test_max_age = None

    def test_exp(self, serializer, value, ts, freeze):
        signed = serializer.dumps(value)
        freeze.tick()
        assert serializer.loads(signed) == value
        freeze.tick(timedelta(seconds=10))

        with pytest.raises(SignatureExpired) as exc_info:
            serializer.loads(signed)

        assert exc_info.value.date_signed == ts
        assert exc_info.value.payload == value

    test_return_payload = None

    def test_return_header(self, serializer, value, ts):
        signed = serializer.dumps(value)
        payload, header = serializer.loads(signed, return_header=True)
        date_signed = serializer.get_issue_date(header)
        assert (payload, date_signed) == (value, ts)

    def test_missing_exp(self, serializer):
        header = serializer.make_header(None)
        del header["exp"]
        signer = serializer.make_signer()
        signed = signer.sign(serializer.dump_payload(header, "value"))

        with pytest.raises(BadSignature):
            serializer.loads(signed)

    @pytest.mark.parametrize("exp", ("invalid", -1))
    def test_invalid_exp(self, serializer, exp):
        header = serializer.make_header(None)
        header["exp"] = exp
        signer = serializer.make_signer()
        signed = signer.sign(serializer.dump_payload(header, "value"))

        with pytest.raises(BadHeader) as exc_info:
            serializer.loads(signed)

        assert "IntDate" in str(exc_info.value)

    def test_invalid_iat(self, serializer):
        header = serializer.make_header(None)
        header["iat"] = "invalid"
        assert serializer.get_issue_date(header) is None
