import sys
import uuid
from dataclasses import dataclass
from enum import Enum, auto
from hashlib import md5, sha1, sha256
from ipaddress import IPv4Address, IPv4Network, IPv6Address, IPv6Network

import pytest

from dirty_equals import (
    FunctionCheck,
    IsDataclass,
    IsDataclassType,
    IsEnum,
    IsHash,
    IsInt,
    IsIP,
    IsJson,
    IsPartialDataclass,
    IsStr,
    IsStrictDataclass,
    IsUrl,
    IsUUID,
)


class FooEnum(Enum):
    a = auto()
    b = auto()
    c = 'c'


@dataclass
class Foo:
    a: int
    b: int
    c: str


foo = Foo(1, 2, 'c')


@dataclass
class Address:
    street: str
    zip_code: str


@dataclass
class Person:
    name: str
    address: Address


person = Person(name='Alice', address=Address(street='123 Main St', zip_code='12345'))


@pytest.mark.parametrize(
    'other,dirty',
    [
        (uuid.uuid4(), IsUUID()),
        (uuid.uuid4(), IsUUID),
        (uuid.uuid4(), IsUUID(4)),
        ('edf9f29e-45c7-431c-99db-28ea44df9785', IsUUID),
        ('edf9f29e-45c7-431c-99db-28ea44df9785', IsUUID(4)),
        ('edf9f29e45c7431c99db28ea44df9785', IsUUID(4)),
        (uuid.uuid3(uuid.UUID('edf9f29e-45c7-431c-99db-28ea44df9785'), 'abc'), IsUUID),
        (uuid.uuid3(uuid.UUID('edf9f29e-45c7-431c-99db-28ea44df9785'), 'abc'), IsUUID(3)),
        (uuid.uuid1(), IsUUID(1)),
        (str(uuid.uuid1()), IsUUID(1)),
        ('ea9e828d-fd18-3898-99f3-5a46dbcee036', IsUUID(3)),
    ],
)
def test_is_uuid_true(other, dirty):
    assert other == dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        ('foobar', IsUUID()),
        ([1, 2, 3], IsUUID()),
        ('edf9f29e-45c7-431c-99db-28ea44df9785', IsUUID(5)),
        (uuid.uuid3(uuid.UUID('edf9f29e-45c7-431c-99db-28ea44df9785'), 'abc'), IsUUID(4)),
        (uuid.uuid1(), IsUUID(4)),
        ('edf9f29e-45c7-431c-99db-28ea44df9785', IsUUID(1)),
        ('ea9e828d-fd18-3898-99f3-5a46dbcee036', IsUUID(4)),
    ],
)
def test_is_uuid_false(other, dirty):
    assert other != dirty


def test_is_uuid_false_repr():
    is_uuid = IsUUID()
    with pytest.raises(AssertionError):
        assert '123' == is_uuid
    assert str(is_uuid) == 'IsUUID()'


def test_is_uuid4_false_repr():
    is_uuid = IsUUID(4)
    with pytest.raises(AssertionError):
        assert '123' == is_uuid
    assert str(is_uuid) == 'IsUUID(4)'


@pytest.mark.parametrize('json_value', ['null', '"xyz"', '[1, 2, 3]', '{"a": 1}'])
def test_is_json_any_true(json_value):
    assert json_value == IsJson()
    assert json_value == IsJson


def test_is_json_any_false():
    is_json = IsJson()
    with pytest.raises(AssertionError):
        assert 'foobar' == is_json
    assert str(is_json) == 'IsJson()'


@pytest.mark.parametrize(
    'json_value,expected_value',
    [
        ('null', None),
        ('"xyz"', 'xyz'),
        ('[1, 2, 3]', [1, 2, 3]),
        ('{"a": 1}', {'a': 1}),
    ],
)
def test_is_json_specific_true(json_value, expected_value):
    assert json_value == IsJson(expected_value)
    assert json_value == IsJson[expected_value]


def test_is_json_invalid():
    assert 'invalid json' != IsJson
    assert 123 != IsJson
    assert [1, 2] != IsJson


def test_is_json_kwargs():
    assert '{"a": 1, "b": 2}' == IsJson(a=1, b=2)
    assert '{"a": 1, "b": 3}' != IsJson(a=1, b=2)


def test_is_json_specific_false():
    is_json = IsJson([1, 2, 3])
    with pytest.raises(AssertionError):
        assert '{"a": 1}' == is_json
    assert str(is_json) == 'IsJson([1, 2, 3])'


def test_equals_function():
    func_argument = None

    def foo(v):
        nonlocal func_argument
        func_argument = v
        return v % 2 == 0

    assert 4 == FunctionCheck(foo)
    assert func_argument == 4
    assert 5 != FunctionCheck(foo)


def test_equals_function_fail():
    def foobar(v):
        return False

    c = FunctionCheck(foobar)

    with pytest.raises(AssertionError):
        assert 4 == c

    assert str(c) == 'FunctionCheck(foobar)'


def test_json_both():
    with pytest.raises(TypeError, match='IsJson requires either an argument or kwargs, not both'):
        IsJson(1, a=2)


@pytest.mark.parametrize(
    'other,dirty',
    [
        (IPv4Address('127.0.0.1'), IsIP()),
        pytest.param(
            IPv4Network('43.48.0.0/12'),
            IsIP(),
            marks=pytest.mark.xfail(
                sys.version_info >= (3, 14), reason='https://github.com/python/cpython/issues/141647'
            ),
        ),
        (IPv6Address('::eeff:ae3f:d473'), IsIP()),
        pytest.param(
            IPv6Network('::eeff:ae3f:d473/128'),
            IsIP(),
            marks=pytest.mark.xfail(
                sys.version_info >= (3, 14), reason='https://github.com/python/cpython/issues/141647'
            ),
        ),
        ('2001:0db8:0a0b:12f0:0000:0000:0000:0001', IsIP()),
        ('179.27.154.96', IsIP),
        ('43.62.123.119', IsIP(version=4)),
        ('::ffff:2b3e:7b77', IsIP(version=6)),
        ('0:0:0:0:0:ffff:2b3e:7b77', IsIP(version=6)),
        ('54.43.53.219/10', IsIP(version=4, netmask='255.192.0.0')),
        ('::ffff:aebf:d473/12', IsIP(version=6, netmask='fff0::')),
        ('2001:0db8:0a0b:12f0:0000:0000:0000:0001', IsIP(version=6)),
        (3232235521, IsIP()),
        (b'\xc0\xa8\x00\x01', IsIP()),
        (338288524927261089654018896845572831328, IsIP(version=6)),
        (b'\x20\x01\x06\x58\x02\x2a\xca\xfe\x02\x00\x00\x00\x00\x00\x00\x01', IsIP(version=6)),
    ],
    ids=repr,
)
def test_is_ip_true(other, dirty):
    assert other == dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        ('foobar', IsIP()),
        ([1, 2, 3], IsIP()),
        ('210.115.28.193', IsIP(version=6)),
        ('::ffff:d273:1cc1', IsIP(version=4)),
        ('210.115.28.193/12', IsIP(version=6, netmask='255.255.255.0')),
        ('::ffff:d273:1cc1', IsIP(version=6, netmask='fff0::')),
        (3232235521, IsIP(version=6)),
        (338288524927261089654018896845572831328, IsIP(version=4)),
    ],
)
def test_is_ip_false(other, dirty):
    assert other != dirty


def test_not_ip_repr():
    is_ip = IsIP()
    with pytest.raises(AssertionError):
        assert '123' == is_ip
    assert str(is_ip) == 'IsIP()'


def test_ip_bad_netmask():
    with pytest.raises(TypeError, match='To check the netmask you must specify the IP version'):
        IsIP(netmask='255.255.255.0')


@pytest.mark.parametrize(
    'other,dirty',
    [
        ('f1e069787ECE74531d112559945c6871', IsHash('md5')),
        ('40bd001563085fc35165329ea1FF5c5ecbdbbeef', IsHash('sha-1')),
        ('a665a45920422f9d417e4867eFDC4fb8a04a1f3fff1fa07e998e86f7f7a27ae3', IsHash('sha-256')),
        (b'f1e069787ECE74531d112559945c6871', IsHash('md5')),
        (bytearray(b'f1e069787ECE74531d112559945c6871'), IsHash('md5')),
    ],
)
def test_is_hash_true(other, dirty):
    assert other == dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        ('foobar', IsHash('md5')),
        (b'\x81 UnicodeDecodeError', IsHash('md5')),
        ([1, 2, 3], IsHash('sha-1')),
        ('f1e069787ECE74531d112559945c6871d', IsHash('md5')),
        ('400bd001563085fc35165329ea1FF5c5ecbdbbeef', IsHash('sha-1')),
        ('a665a45920422g9d417e4867eFDC4fb8a04a1f3fff1fa07e998e86f7f7a27ae3', IsHash('sha-256')),
    ],
)
def test_is_hash_false(other, dirty):
    assert other != dirty


@pytest.mark.parametrize(
    'hash_type',
    ['md5', 'sha-1', 'sha-256'],
)
def test_is_hash_md5_false_repr(hash_type):
    is_hash = IsHash(hash_type)
    with pytest.raises(AssertionError):
        assert '123' == is_hash
    assert str(is_hash) == f"IsHash('{hash_type}')"


@pytest.mark.parametrize(
    'hash_func, hash_type',
    [(md5, 'md5'), (sha1, 'sha-1'), (sha256, 'sha-256')],
)
def test_hashlib_hashes(hash_func, hash_type):
    assert hash_func(b'dirty equals').hexdigest() == IsHash(hash_type)


def test_wrong_hash_type():
    with pytest.raises(ValueError, match='Hash type must be one of the following values: md5, sha-1, sha-256'):
        assert '123' == IsHash('ntlm')


@pytest.mark.parametrize(
    'other,dirty',
    [
        ('https://example.com', IsUrl),
        ('https://example.com', IsUrl(scheme='https')),
        ('postgres://user:pass@localhost:5432/app', IsUrl(postgres_dsn=True)),
    ],
)
def test_is_url_true(other, dirty):
    assert other == dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        ('https://example.com', IsUrl(postgres_dsn=True)),
        ('https://example.com', IsUrl(scheme='http')),
        ('definitely not a url', IsUrl),
        (42, IsUrl),
        ('https://anotherexample.com', IsUrl(postgres_dsn=True)),
    ],
)
def test_is_url_false(other, dirty):
    assert other != dirty


def test_is_url_invalid_kwargs():
    with pytest.raises(
        TypeError,
        match='IsURL only checks these attributes: scheme, host, host_type, user, password, port, path, query, '
        'fragment',
    ):
        IsUrl(https=True)


def test_is_url_too_many_url_types():
    with pytest.raises(
        ValueError,
        match='You can only check against one Pydantic url type at a time',
    ):
        assert 'https://example.com' == IsUrl(any_url=True, http_url=True, postgres_dsn=True)


@pytest.mark.parametrize(
    'other,dirty',
    [
        (Foo, IsDataclassType),
        (Foo, IsDataclassType()),
    ],
)
def test_is_dataclass_type_true(other, dirty):
    assert other == dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        (foo, IsDataclassType),
        (foo, IsDataclassType()),
        (Foo, IsDataclass),
    ],
)
def test_is_dataclass_type_false(other, dirty):
    assert other != dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        (foo, IsDataclass),
        (foo, IsDataclass()),
        (foo, IsDataclass(a=IsInt, b=2, c=IsStr)),
        (foo, IsDataclass(a=1, c='c', b=2).settings(strict=False, partial=False)),
        (foo, IsDataclass(a=1, b=2, c='c').settings(strict=True, partial=False)),
        (foo, IsStrictDataclass(a=1, b=2, c='c')),
        (foo, IsDataclass(c='c', a=1).settings(strict=False, partial=True)),
        (foo, IsPartialDataclass(c='c', a=1)),
        (foo, IsDataclass(b=2, c='c').settings(strict=True, partial=True)),
        (foo, IsStrictDataclass(b=2, c='c').settings(partial=True)),
        (foo, IsPartialDataclass(b=2, c='c').settings(strict=True)),
    ],
)
def test_is_dataclass_true(other, dirty):
    assert other == dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        (foo, IsDataclassType),
        (Foo, IsDataclass),
        (foo, IsDataclass(a=1)),
        (foo, IsDataclass(a=IsStr, b=IsInt, c=IsStr)),
        (foo, IsDataclass(b=2, a=1, c='c').settings(strict=True)),
        (foo, IsStrictDataclass(b=2, a=1, c='c')),
        (foo, IsDataclass(b=2, a=1).settings(partial=False)),
    ],
)
def test_is_dataclass_false(other, dirty):
    assert other != dirty


def test_is_dataclass_nested():
    assert person == IsDataclass(name='Alice', address=IsDataclass(street='123 Main St', zip_code='12345'))


@pytest.mark.parametrize(
    'other,dirty',
    [
        (FooEnum.a, IsEnum),
        (FooEnum.b, IsEnum(FooEnum)),
        (2, IsEnum(FooEnum)),
        ('c', IsEnum(FooEnum)),
    ],
)
def test_is_enum_true(other, dirty):
    assert other == dirty


@pytest.mark.parametrize(
    'other,dirty',
    [
        (FooEnum, IsEnum),
        (FooEnum, IsEnum(FooEnum)),
        (4, IsEnum(FooEnum)),
    ],
)
def test_is_enum_false(other, dirty):
    assert other != dirty
