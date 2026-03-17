"""Tests for josepy.util."""

import functools
import sys
import unittest
import warnings

import pytest
import test_util


class ComparableX509Test(unittest.TestCase):
    """Tests for josepy.util.ComparableX509."""

    def setUp(self) -> None:
        # test_util.load_comparable_{csr,cert} return ComparableX509
        self.req1 = test_util.load_comparable_csr("csr.pem")
        self.req2 = test_util.load_comparable_csr("csr.pem")
        self.req_other = test_util.load_comparable_csr("csr-san.pem")

        self.cert1 = test_util.load_comparable_cert("cert.pem")
        self.cert2 = test_util.load_comparable_cert("cert.pem")
        self.cert_other = test_util.load_comparable_cert("cert-san.pem")

    def test_getattr_proxy(self) -> None:
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                category=DeprecationWarning,
                message=".*Use timezone-aware objects to represent datetimes",
            )
            assert self.cert1.has_expired() is True

    def test_eq(self) -> None:
        assert self.req1 == self.req2
        assert self.cert1 == self.cert2

    def test_ne(self) -> None:
        assert self.req1 != self.req_other
        assert self.cert1 != self.cert_other

    def test_ne_wrong_types(self) -> None:
        assert self.req1 != 5
        assert self.cert1 != 5

    def test_hash(self) -> None:
        assert hash(self.req1) == hash(self.req2)
        assert hash(self.req1) != hash(self.req_other)

        assert hash(self.cert1) == hash(self.cert2)
        assert hash(self.cert1) != hash(self.cert_other)

    def test_repr(self) -> None:
        for x509 in self.req1, self.cert1:
            assert repr(x509) == "<ComparableX509({0!r})>".format(x509.wrapped)


class ComparableRSAKeyTest(unittest.TestCase):
    """Tests for josepy.util.ComparableRSAKey."""

    def setUp(self) -> None:
        # test_utl.load_rsa_private_key return ComparableRSAKey
        self.key = test_util.load_rsa_private_key("rsa256_key.pem")
        self.key_same = test_util.load_rsa_private_key("rsa256_key.pem")
        self.key2 = test_util.load_rsa_private_key("rsa512_key.pem")

    def test_getattr_proxy(self) -> None:
        assert 256 == self.key.key_size

    def test_eq(self) -> None:
        assert self.key == self.key_same

    def test_ne(self) -> None:
        assert self.key != self.key2

    def test_ne_different_types(self) -> None:
        assert self.key != 5

    def test_ne_not_wrapped(self) -> None:
        assert self.key != self.key_same._wrapped

    def test_ne_no_serialization(self) -> None:
        from josepy.util import ComparableRSAKey

        assert ComparableRSAKey(5) != ComparableRSAKey(5)  # type: ignore

    def test_hash(self) -> None:
        assert isinstance(hash(self.key), int)
        assert hash(self.key) == hash(self.key_same)
        assert hash(self.key) != hash(self.key2)

    def test_repr(self) -> None:
        assert repr(self.key).startswith("<ComparableRSAKey(<cryptography.hazmat.") is True

    def test_public_key(self) -> None:
        from josepy.util import ComparableRSAKey

        assert isinstance(self.key.public_key(), ComparableRSAKey)


class ComparableECKeyTest(unittest.TestCase):
    """Tests for josepy.util.ComparableECKey."""

    def setUp(self) -> None:
        # test_utl.load_ec_private_key return ComparableECKey
        self.p256_key = test_util.load_ec_private_key("ec_p256_key.pem")
        self.p256_key_same = test_util.load_ec_private_key("ec_p256_key.pem")
        self.p384_key = test_util.load_ec_private_key("ec_p384_key.pem")
        self.p521_key = test_util.load_ec_private_key("ec_p521_key.pem")

    def test_getattr_proxy(self) -> None:
        assert 256 == self.p256_key.key_size

    def test_eq(self) -> None:
        assert self.p256_key == self.p256_key_same

    def test_ne(self) -> None:
        assert self.p256_key != self.p384_key
        assert self.p256_key != self.p521_key

    def test_ne_different_types(self) -> None:
        assert self.p256_key != 5

    def test_ne_not_wrapped(self) -> None:
        assert self.p256_key != self.p256_key_same._wrapped

    def test_ne_no_serialization(self) -> None:
        from josepy.util import ComparableECKey

        assert ComparableECKey(5) != ComparableECKey(5)  # type: ignore

    def test_hash(self) -> None:
        assert isinstance(hash(self.p256_key), int)
        assert hash(self.p256_key) == hash(self.p256_key_same)
        assert hash(self.p256_key) != hash(self.p384_key)
        assert hash(self.p256_key) != hash(self.p521_key)

    def test_repr(self) -> None:
        assert repr(self.p256_key).startswith("<ComparableECKey(<cryptography.hazmat.") is True

    def test_public_key(self) -> None:
        from josepy.util import ComparableECKey

        assert isinstance(self.p256_key.public_key(), ComparableECKey)


class ImmutableMapTest(unittest.TestCase):
    """Tests for josepy.util.ImmutableMap."""

    def setUp(self) -> None:
        from josepy.util import ImmutableMap

        class A(ImmutableMap):
            x: int
            y: int
            __slots__ = ("x", "y")

        class B(ImmutableMap):
            x: int
            y: int
            __slots__ = ("x", "y")

        self.A = A
        self.B = B

        self.a1 = self.A(x=1, y=2)
        self.a1_swap = self.A(y=2, x=1)
        self.a2 = self.A(x=3, y=4)
        self.b = self.B(x=1, y=2)

    def test_update(self) -> None:
        assert self.A(x=2, y=2) == self.a1.update(x=2)
        assert self.a2 == self.a1.update(x=3, y=4)

    def test_get_missing_item_raises_key_error(self) -> None:
        with pytest.raises(KeyError):
            self.a1.__getitem__("z")

    def test_order_of_args_does_not_matter(self) -> None:
        assert self.a1 == self.a1_swap

    def test_type_error_on_missing(self) -> None:
        with pytest.raises(TypeError):
            self.A(x=1)
        with pytest.raises(TypeError):
            self.A(y=2)

    def test_type_error_on_unrecognized(self) -> None:
        with pytest.raises(TypeError):
            self.A(x=1, z=2)
        with pytest.raises(TypeError):
            self.A(x=1, y=2, z=3)

    def test_get_attr(self) -> None:
        assert 1 == self.a1.x
        assert 2 == self.a1.y
        assert 1 == self.a1_swap.x
        assert 2 == self.a1_swap.y

    def test_set_attr_raises_attribute_error(self) -> None:
        with pytest.raises(AttributeError):
            functools.partial(self.a1.__setattr__, "x")(10)

    def test_equal(self) -> None:
        assert self.a1 == self.a1
        assert self.a2 == self.a2
        assert self.a1 != self.a2

    def test_hash(self) -> None:
        assert hash((1, 2)) == hash(self.a1)

    def test_unhashable(self) -> None:
        with pytest.raises(TypeError):
            self.A(x=1, y={}).__hash__()

    def test_repr(self) -> None:
        assert "A(x=1, y=2)" == repr(self.a1)
        assert "A(x=1, y=2)" == repr(self.a1_swap)
        assert "B(x=1, y=2)" == repr(self.b)
        assert "B(x='foo', y='bar')" == repr(self.B(x="foo", y="bar"))


class frozendictTest(unittest.TestCase):
    """Tests for josepy.util.frozendict."""

    def setUp(self) -> None:
        from josepy.util import frozendict

        self.fdict = frozendict(x=1, y="2")

    def test_init_dict(self) -> None:
        from josepy.util import frozendict

        assert self.fdict == frozendict({"x": 1, "y": "2"})

    def test_init_other_raises_type_error(self) -> None:
        from josepy.util import frozendict

        # specifically fail for generators...
        with pytest.raises(TypeError):
            frozendict({"a": "b"}.items())

    def test_len(self) -> None:
        assert 2 == len(self.fdict)

    def test_hash(self) -> None:
        assert isinstance(hash(self.fdict), int)

    def test_getattr_proxy(self) -> None:
        assert 1 == self.fdict.x
        assert "2" == self.fdict.y

    def test_getattr_raises_attribute_error(self) -> None:
        with pytest.raises(AttributeError):
            self.fdict.__getattr__("z")

    def test_setattr_immutable(self) -> None:
        with pytest.raises(AttributeError):
            self.fdict.__setattr__("z", 3)

    def test_repr(self) -> None:
        assert "frozendict(x=1, y='2')" == repr(self.fdict)


if __name__ == "__main__":
    sys.exit(pytest.main(sys.argv[1:] + [__file__]))  # pragma: no cover
