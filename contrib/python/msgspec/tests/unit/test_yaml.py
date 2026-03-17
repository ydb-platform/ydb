import dataclasses
import datetime
import enum
import sys
import uuid
from decimal import Decimal
from typing import Dict, FrozenSet, List, Set, Tuple

import pytest

import msgspec

try:
    import yaml  # noqa
except ImportError:
    pytestmark = pytest.mark.skip(reason="PyYAML is not installed")


UTC = datetime.timezone.utc


class ExStruct(msgspec.Struct):
    x: int
    y: str


@dataclasses.dataclass
class ExDataclass:
    x: int
    y: str


class ExEnum(enum.Enum):
    one = "one"
    two = "two"


class ExIntEnum(enum.IntEnum):
    one = 1
    two = 2


def test_module_dir():
    assert set(dir(msgspec.yaml)) == {"encode", "decode"}


def test_pyyaml_not_installed_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "yaml", None)

    with pytest.raises(ImportError, match="PyYAML"):
        msgspec.yaml.encode(1)

    with pytest.raises(ImportError, match="PyYAML"):
        msgspec.yaml.decode("1", type=int)


@pytest.mark.parametrize(
    "val",
    [
        None,
        True,
        False,
        1,
        1.5,
        "fizz",
        datetime.datetime(2022, 1, 2, 3, 4, 5, 6),
        datetime.datetime(2022, 1, 2, 3, 4, 5, 6, UTC),
        datetime.date(2022, 1, 2),
        [1, 2],
        {"one": 2},
        {1: "two"},
    ],
)
def test_roundtrip_any(val):
    msg = msgspec.yaml.encode(val)
    res = msgspec.yaml.decode(msg)
    assert res == val


@pytest.mark.parametrize(
    "val, type",
    [
        (None, None),
        (True, bool),
        (False, bool),
        (1, int),
        (1.5, float),
        ("fizz", str),
        (b"fizz", bytes),
        (b"fizz", bytearray),
        (datetime.datetime(2022, 1, 2, 3, 4, 5, 6), datetime.datetime),
        (datetime.datetime(2022, 1, 2, 3, 4, 5, 6, UTC), datetime.datetime),
        (datetime.date(2022, 1, 2), datetime.date),
        (datetime.time(12, 34), datetime.time),
        (uuid.uuid4(), uuid.UUID),
        (ExEnum.one, ExEnum),
        (ExIntEnum.one, ExIntEnum),
        ([1, 2], List[int]),
        ((1, 2), Tuple[int, ...]),
        ({1, 2}, Set[int]),
        (frozenset({1, 2}), FrozenSet[int]),
        (("one", 2), Tuple[str, int]),
        ({"one": 2}, Dict[str, int]),
        ({1: "two"}, Dict[int, str]),
        (ExStruct(1, "two"), ExStruct),
        (ExDataclass(1, "two"), ExDataclass),
    ],
)
def test_roundtrip_typed(val, type):
    msg = msgspec.yaml.encode(val)
    res = msgspec.yaml.decode(msg, type=type)
    assert res == val


def test_encode_error():
    class Oops:
        pass

    with pytest.raises(TypeError, match="Encoding objects of type Oops is unsupported"):
        msgspec.yaml.encode(Oops())


def test_encode_enc_hook():
    msg = msgspec.yaml.encode(Decimal(1.5), enc_hook=str)
    assert msgspec.yaml.decode(msg) == "1.5"


@pytest.mark.parametrize("order", [None, "deterministic"])
def test_encode_order(order):
    msg = {"y": 1, "x": 2, "z": 3}
    res = msgspec.yaml.encode(msg, order=order)
    sol = yaml.safe_dump(msg, sort_keys=bool(order)).encode("utf-8")
    assert res == sol


def test_decode_str_or_bytes_like():
    assert msgspec.yaml.decode("[1, 2]") == [1, 2]
    assert msgspec.yaml.decode(b"[1, 2]") == [1, 2]
    assert msgspec.yaml.decode(bytearray(b"[1, 2]")) == [1, 2]
    assert msgspec.yaml.decode(memoryview(b"[1, 2]")) == [1, 2]
    with pytest.raises(TypeError):
        msgspec.yaml.decode(1)


@pytest.mark.parametrize("msg", [b"{{", b"!!binary 123"])
def test_decode_parse_error(msg):
    with pytest.raises(msgspec.DecodeError):
        msgspec.yaml.decode(msg)


def test_decode_validation_error():
    with pytest.raises(msgspec.ValidationError, match="Expected `str`"):
        msgspec.yaml.decode(b"[1, 2, 3]", type=List[str])


@pytest.mark.parametrize("strict", [True, False])
def test_decode_strict_or_lax(strict):
    msg = b"a: ['1', '2']"
    typ = Dict[str, List[int]]

    if strict:
        with pytest.raises(msgspec.ValidationError, match="Expected `int`"):
            msgspec.yaml.decode(msg, type=typ, strict=strict)
    else:
        res = msgspec.yaml.decode(msg, type=typ, strict=strict)
        assert res == {"a": [1, 2]}


def test_decode_dec_hook():
    def dec_hook(typ, val):
        if typ is Decimal:
            return Decimal(val)
        raise TypeError

    res = msgspec.yaml.decode("'1.5'", type=Decimal, dec_hook=dec_hook)
    assert res == Decimal("1.5")
