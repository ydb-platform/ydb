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
    import tomllib
except ImportError:
    try:
        import tomli as tomllib
    except ImportError:
        tomllib = None

try:
    import tomli_w
except ImportError:
    tomli_w = None


needs_decode = pytest.mark.skipif(
    tomllib is None, reason="Neither tomllib or tomli are installed"
)
needs_encode = pytest.mark.skipif(tomli_w is None, reason="tomli_w is not installed")

PY311 = sys.version_info[:2] >= (3, 11)

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
    assert set(dir(msgspec.toml)) == {"encode", "decode"}


@pytest.mark.skipif(PY311, reason="tomllib is builtin in 3.11+")
def test_tomli_not_installed_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "tomli", None)

    with pytest.raises(ImportError, match="conda install"):
        msgspec.toml.decode("a = 1", type=int)


def test_tomli_w_not_installed_error(monkeypatch):
    monkeypatch.setitem(sys.modules, "tomli_w", None)

    with pytest.raises(ImportError, match="conda install"):
        msgspec.toml.encode({"a": 1})


@pytest.mark.parametrize(
    "val",
    [
        True,
        False,
        1,
        1.5,
        "fizz",
        datetime.datetime(2022, 1, 2, 3, 4, 5, 6),
        datetime.datetime(2022, 1, 2, 3, 4, 5, 6, UTC),
        datetime.date(2022, 1, 2),
        datetime.time(12, 34),
        [1, 2],
        {"one": 2},
    ],
)
@needs_encode
@needs_decode
def test_roundtrip_any(val):
    msg = msgspec.toml.encode({"x": val})
    res = msgspec.toml.decode(msg)["x"]
    assert res == val


@pytest.mark.parametrize(
    "val, type",
    [
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
@needs_encode
@needs_decode
def test_roundtrip_typed(val, type):
    msg = msgspec.toml.encode({"x": val})
    res = msgspec.toml.decode(msg, type=Dict[str, type])["x"]
    assert res == val


@needs_encode
def test_encode_output_type():
    msg = msgspec.toml.encode({"x": 1})
    assert isinstance(msg, bytes)


@needs_encode
def test_encode_error():
    class Oops:
        pass

    with pytest.raises(TypeError, match="Encoding objects of type Oops is unsupported"):
        msgspec.toml.encode({"x": Oops()})


@needs_encode
@needs_decode
def test_encode_enc_hook():
    msg = msgspec.toml.encode({"x": Decimal(1.5)}, enc_hook=str)
    assert msgspec.toml.decode(msg) == {"x": "1.5"}


@needs_encode
@pytest.mark.parametrize("order", [None, "deterministic"])
def test_encode_order(order):
    msg = {"y": 1, "x": ({"n": 1, "m": 2},), "z": [{"b": 1, "a": 2}]}
    res = msgspec.toml.encode(msg, order=order)
    if order:
        sol_msg = {"x": ({"m": 2, "n": 1},), "y": 1, "z": [{"a": 2, "b": 1}]}
    else:
        sol_msg = msg
    sol = tomli_w.dumps(sol_msg).encode("utf-8")
    assert res == sol


@needs_decode
def test_decode_str_or_bytes_like():
    assert msgspec.toml.decode("a = 1") == {"a": 1}
    assert msgspec.toml.decode(b"a = 1") == {"a": 1}
    assert msgspec.toml.decode(bytearray(b"a = 1")) == {"a": 1}
    assert msgspec.toml.decode(memoryview(b"a = 1")) == {"a": 1}
    with pytest.raises(TypeError):
        msgspec.toml.decode(1)


@needs_decode
@pytest.mark.parametrize("msg", [b"{{", b"!!binary 123"])
def test_decode_parse_error(msg):
    with pytest.raises(msgspec.DecodeError):
        msgspec.toml.decode(msg)


@needs_decode
def test_decode_validation_error():
    with pytest.raises(msgspec.ValidationError, match="Expected `str`"):
        msgspec.toml.decode(b"a = [1, 2, 3]", type=Dict[str, List[str]])


@needs_decode
@pytest.mark.parametrize("strict", [True, False])
def test_decode_strict_or_lax(strict):
    msg = b"a = ['1', '2']"
    typ = Dict[str, List[int]]

    if strict:
        with pytest.raises(msgspec.ValidationError, match="Expected `int`"):
            msgspec.toml.decode(msg, type=typ, strict=strict)
    else:
        res = msgspec.toml.decode(msg, type=typ, strict=strict)
        assert res == {"a": [1, 2]}


@needs_decode
def test_decode_dec_hook():
    def dec_hook(typ, val):
        if typ is Decimal:
            return Decimal(val)
        raise TypeError

    res = msgspec.toml.decode("a = '1.5'", type=Dict[str, Decimal], dec_hook=dec_hook)
    assert res == {"a": Decimal("1.5")}
