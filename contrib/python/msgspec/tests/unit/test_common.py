from __future__ import annotations

import base64
import collections
import datetime
import decimal
import enum
import gc
import sys
import typing
import uuid
import weakref
from collections import namedtuple
from dataclasses import dataclass, field, make_dataclass
from datetime import timedelta
from typing import (
    Annotated,
    ClassVar,
    Deque,
    Dict,
    Final,
    Generic,
    List,
    Literal,
    NamedTuple,
    NewType,
    Optional,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
)

import pytest

from .utils import max_call_depth, temp_module

try:
    import attrs
except ImportError:
    attrs = None

import msgspec
from msgspec import UNSET, Meta, Struct, UnsetType, ValidationError

UTC = datetime.timezone.utc

PY310 = sys.version_info[:2] >= (3, 10)
PY311 = sys.version_info[:2] >= (3, 11)
PY312 = sys.version_info[:2] >= (3, 12)

py310_plus = pytest.mark.skipif(not PY310, reason="3.10+ only")
py311_plus = pytest.mark.skipif(not PY311, reason="3.11+ only")
py312_plus = pytest.mark.skipif(not PY312, reason="3.12+ only")

T = TypeVar("T")


def assert_eq(x, y):
    assert x == y
    assert type(x) is type(y)


@pytest.fixture(params=["json", "msgpack"])
def proto(request):
    if request.param == "json":
        return msgspec.json
    elif request.param == "msgpack":
        return msgspec.msgpack


try:
    from enum import StrEnum
except ImportError:

    class StrEnum(str, enum.Enum):
        pass


class FruitInt(enum.IntEnum):
    APPLE = 1
    BANANA = 2


class FruitStr(enum.Enum):
    APPLE = "apple"
    BANANA = "banana"


class VeggieInt(enum.IntEnum):
    CARROT = 1
    LETTUCE = 2


class VeggieStr(enum.Enum):
    CARROT = "carrot"
    LETTUCE = "banana"


class Person(Struct):
    first: str
    last: str
    age: int


class PersonArray(Struct, array_like=True):
    first: str
    last: str
    age: int


class PersonDict(TypedDict):
    first: str
    last: str
    age: int


@dataclass
class PersonDataclass:
    first: str
    last: str
    age: int


class PersonTuple(NamedTuple):
    first: str
    last: str
    age: int


class Custom:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y


class TestEncodeSubclasses:
    def test_encode_dict_subclass(self, proto):
        class subclass(dict):
            pass

        for msg in [{}, {"a": 1, "b": 2}]:
            assert proto.encode(subclass(msg)) == proto.encode(msg)

    @pytest.mark.parametrize("cls", [list, tuple, set, frozenset])
    def test_encode_sequence_subclass(self, cls, proto):
        class subclass(cls):
            pass

        for msg in [[], [1, 2]]:
            assert proto.encode(subclass(msg)) == proto.encode(cls(msg))


class TestDecoder:
    def test_decoder_runtime_type_parameters(self, proto):
        dec = proto.Decoder[int](int)
        assert isinstance(dec, proto.Decoder)
        msg = proto.encode(2)
        assert dec.decode(msg) == 2

    def test_decoder_dec_hook_attribute(self, proto):
        def dec_hook(typ, obj):
            pass

        dec = proto.Decoder()
        assert dec.dec_hook is None

        dec = proto.Decoder(dec_hook=None)
        assert dec.dec_hook is None

        dec = proto.Decoder(dec_hook=dec_hook)
        assert dec.dec_hook is dec_hook

    def test_decoder_dec_hook_not_callable(self, proto):
        with pytest.raises(TypeError):
            proto.Decoder(dec_hook=1)

    def test_decode_dec_hook(self, proto):
        def dec_hook(typ, obj):
            assert typ is Custom
            return typ(*obj)

        msg = proto.encode([1, 2])
        res = proto.decode(msg, type=Custom, dec_hook=dec_hook)
        assert res == Custom(1, 2)
        assert isinstance(res, Custom)

    def test_decoder_dec_hook(self, proto):
        called = False

        def dec_hook(typ, obj):
            nonlocal called
            called = True
            assert typ is Custom
            return Custom(*obj)

        dec = proto.Decoder(type=List[Custom], dec_hook=dec_hook)
        buf = proto.encode([[1, 2], [3, 4], [5, 6]])
        msg = dec.decode(buf)
        assert called
        assert msg == [Custom(1, 2), Custom(3, 4), Custom(5, 6)]
        assert isinstance(msg[0], Custom)

    def test_decoder_dec_hook_optional_custom_type(self, proto):
        called = False

        def dec_hook(typ, obj):
            nonlocal called
            called = True

        dec = proto.Decoder(type=Optional[Custom], dec_hook=dec_hook)
        msg = dec.decode(proto.encode(None))
        assert not called
        assert msg is None

    @pytest.mark.parametrize("err_cls", [TypeError, ValueError])
    def test_decode_dec_hook_errors_wrapped(self, err_cls, proto):
        def dec_hook(typ, obj):
            assert obj == "some string"
            raise err_cls("Oh no!")

        msg = proto.encode("some string")
        with pytest.raises(msgspec.ValidationError, match="Oh no!") as rec:
            proto.decode(msg, type=Custom, dec_hook=dec_hook)

        assert rec.value.__cause__ is rec.value.__context__
        assert type(rec.value.__cause__) is err_cls

        msg = proto.encode(["some string"])
        with pytest.raises(msgspec.ValidationError, match=r"Oh no! - at `\$\[0\]`"):
            proto.decode(msg, type=List[Custom], dec_hook=dec_hook)

    def test_decode_dec_hook_errors_passthrough(self, proto):
        def dec_hook(typ, obj):
            assert obj == "some string"
            raise NotImplementedError("Oh no!")

        msg = proto.encode("some string")
        with pytest.raises(NotImplementedError, match="Oh no!"):
            proto.decode(msg, type=Custom, dec_hook=dec_hook)

        msg = proto.encode(["some string"])
        with pytest.raises(NotImplementedError, match=r"Oh no!"):
            proto.decode(msg, type=List[Custom], dec_hook=dec_hook)

    def test_decode_dec_hook_wrong_type(self, proto):
        dec = proto.Decoder(type=Custom, dec_hook=lambda t, o: o)

        msg = proto.encode([1, 2])
        with pytest.raises(
            msgspec.ValidationError,
            match="Expected `Custom`, got `list`",
        ):
            dec.decode(msg)

    def test_decode_dec_hook_wrong_type_in_struct(self, proto):
        class Test(Struct):
            point: Custom
            other: int

        dec = proto.Decoder(type=Test, dec_hook=lambda t, o: o)

        msg = proto.encode({"point": [1, 2], "other": 3})
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msg)

        assert "Expected `Custom`, got `list` - at `$.point`" == str(rec.value)

    def test_decode_dec_hook_wrong_type_generic(self, proto):
        dec = proto.Decoder(type=Deque[int], dec_hook=lambda t, o: o)

        msg = proto.encode([1, 2, 3])
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msg)

        assert "Expected `collections.deque`, got `list`" == str(rec.value)

    def test_decode_dec_hook_isinstance_errors(self, proto):
        class Metaclass(type):
            def __instancecheck__(self, obj):
                raise TypeError("Oh no!")

        class Custom(metaclass=Metaclass):
            pass

        dec = proto.Decoder(type=Custom)

        msg = proto.encode(1)
        with pytest.raises(TypeError, match="Oh no!"):
            dec.decode(msg)


@pytest.mark.skipif(
    PY312,
    reason=(
        "Python 3.12 harcodes the C recursion limit, making this "
        "behavior harder to test in CI"
    ),
)
class TestRecursion:
    @staticmethod
    def nested(n, is_array):
        if is_array:
            obj = []
            for _ in range(n):
                obj = [obj]
        else:
            obj = {}
            for _ in range(n):
                obj = {"": obj}
        return obj

    @pytest.mark.parametrize("is_array", [True, False])
    def test_encode_highly_recursive_msg_errors(self, is_array, proto):
        N = 200
        obj = self.nested(N, is_array)

        # Errors if above the recursion limit
        with max_call_depth(N // 2):
            with pytest.raises(RecursionError):
                proto.encode(obj)

        # Works if below the recursion limit
        with max_call_depth(N * 2):
            proto.encode(obj)

    @pytest.mark.parametrize("is_array", [True, False])
    def test_decode_highly_recursive_msg_errors(self, is_array, proto):
        """Ensure recursion is properly handled when decoding.
        Test case seen in https://github.com/ijl/orjson/issues/458."""
        N = 200
        obj = self.nested(N, is_array)

        with max_call_depth(N * 2):
            msg = proto.encode(obj)

        # Errors if above the recursion limit
        with max_call_depth(N // 2):
            with pytest.raises(RecursionError):
                proto.decode(msg)

        # Works if below the recursion limit
        with max_call_depth(N * 2):
            obj2 = proto.decode(msg)

        assert obj2


class TestThreadSafe:
    def test_encode_threadsafe(self, proto):
        class Nested:
            def __init__(self, x):
                self.x = x

        def enc_hook(obj):
            return base64.b64encode(enc.encode(obj.x)).decode("utf-8")

        enc = proto.Encoder(enc_hook=enc_hook)
        res = enc.encode({"x": Nested(1)})
        sol = proto.encode({"x": base64.b64encode(proto.encode(1)).decode("utf-8")})
        assert res == sol

    def test_decode_threadsafe(self, proto):
        class Custom:
            def __init__(self, node):
                self.node = node

            def __eq__(self, other):
                return type(other) is Custom and self.node == other.node

        def dec_hook(typ, obj):
            msg = base64.b64decode(obj)
            return Custom(dec.decode(msg))

        dec = proto.Decoder(Tuple[Union[Custom, None], int], dec_hook=dec_hook)
        msg = proto.encode(
            (base64.b64encode(proto.encode((None, 1))).decode("utf-8"), 2)
        )
        sol = (Custom((None, 1)), 2)
        res = dec.decode(msg)
        assert res == sol


class TestIntEnum:
    def test_empty_errors(self, proto):
        class Empty(enum.IntEnum):
            pass

        with pytest.raises(TypeError, match="Enum types must have at least one item"):
            proto.Decoder(Empty)

    @pytest.mark.parametrize("base_cls", [enum.IntEnum, enum.Enum])
    def test_encode(self, proto, base_cls):
        class Test(base_cls):
            A = 1
            B = 2

        assert proto.encode(Test.A) == proto.encode(1)

    @pytest.mark.parametrize("base_cls", [enum.IntEnum, enum.Enum])
    def test_decode(self, proto, base_cls):
        class Test(base_cls):
            A = 1
            B = 2

        dec = proto.Decoder(Test)
        assert dec.decode(proto.encode(1)) is Test.A
        assert dec.decode(proto.encode(2)) is Test.B

        with pytest.raises(ValidationError, match="Invalid enum value 3"):
            dec.decode(proto.encode(3))

    def test_decode_nested(self, proto):
        class Test(Struct):
            fruit: FruitInt

        dec = proto.Decoder(Test)

        dec.decode(proto.encode({"fruit": 1})) == Test(FruitInt.APPLE)

        with pytest.raises(
            ValidationError, match=r"Invalid enum value 3 - at `\$.fruit`"
        ):
            dec.decode(proto.encode({"fruit": 3}))

    def test_intenum_missing(self, proto):
        class Ex(enum.IntEnum):
            A = 1
            B = 2

            @classmethod
            def _missing_(cls, val):
                if val == 3:
                    return cls.A
                elif val == -4:
                    return cls.B
                elif val == 5:
                    raise ValueError("oh no!")
                else:
                    return None

        dec = proto.Decoder(Ex)

        def roundtrip(msg):
            return dec.decode(proto.encode(msg))

        assert roundtrip(1) is Ex.A
        assert roundtrip(3) is Ex.A
        assert roundtrip(-4) is Ex.B
        with pytest.raises(ValidationError, match="Invalid enum value 5"):
            roundtrip(5)
        with pytest.raises(ValidationError, match="Invalid enum value 6"):
            roundtrip(6)

    def test_intflag(self, proto):
        class Ex(enum.IntFlag):
            A = 0b001
            B = 0b010
            C = 0b100

        obj = Ex.A | Ex.C
        msg = proto.encode(obj)
        assert msg == proto.encode(int(obj))
        assert proto.decode(msg, type=Ex) == obj

    def test_int_lookup_reused(self):
        class Test(enum.IntEnum):
            A = 1
            B = 2

        dec = msgspec.msgpack.Decoder(Test)  # noqa
        count = sys.getrefcount(Test.__msgspec_cache__)
        dec2 = msgspec.msgpack.Decoder(Test)
        count2 = sys.getrefcount(Test.__msgspec_cache__)
        assert count2 == count + 1

        # Reference count decreases when decoder is dropped
        del dec2
        gc.collect()
        count3 = sys.getrefcount(Test.__msgspec_cache__)
        assert count == count3

    def test_int_lookup_gc(self):
        class Test(enum.IntEnum):
            A = 1
            B = 2

        dec = msgspec.msgpack.Decoder(Test)
        assert gc.is_tracked(Test.__msgspec_cache__)

        # Deleting all references and running GC cleans up cycle
        ref = weakref.ref(Test)
        del Test
        del dec
        gc.collect()
        assert ref() is None

    @pytest.mark.parametrize(
        "values",
        [
            [0, 1, 2, -(2**63) - 1],
            [0, 1, 2, 2**63],
        ],
    )
    def test_int_lookup_values_out_of_range(self, values):
        myenum = enum.IntEnum("myenum", [(f"x{i}", v) for i, v in enumerate(values)])

        with pytest.raises(NotImplementedError):
            msgspec.msgpack.Decoder(myenum)

    def test_msgspec_cache_overwritten(self):
        class Test(enum.IntEnum):
            A = 1

        Test.__msgspec_cache__ = 1

        with pytest.raises(RuntimeError, match="__msgspec_cache__"):
            msgspec.msgpack.Decoder(Test)

    @pytest.mark.parametrize(
        "values",
        [
            [0],
            [1],
            [-1],
            [3, 4, 5, 2, 1],
            [4, 3, 1, 2, 7],
            [-4, -3, -2, -1, 0, 1, 2, 3, 4],
            [-4, -3, -1, -2, -7],
            [-4, -3, 1, 0, -2, -1],
            [2**63 - 1, 2**63 - 2, 2**63 - 3],
            [-(2**63) + 1, -(2**63) + 2, -(2**63) + 3],
        ],
    )
    def test_compact(self, values):
        myenum = enum.IntEnum("myenum", [(f"x{i}", v) for i, v in enumerate(values)])
        dec = msgspec.msgpack.Decoder(myenum)

        assert hasattr(myenum, "__msgspec_cache__")

        for val in myenum:
            msg = msgspec.msgpack.encode(val)
            val2 = dec.decode(msg)
            assert val == val2

        for bad in [-1000, min(values) - 1, max(values) + 1, 1000]:
            with pytest.raises(ValidationError):
                dec.decode(msgspec.msgpack.encode(bad))

    @pytest.mark.parametrize(
        "values",
        [
            [-(2**63), 2**63 - 1, 0],
            [2**63 - 2, 2**63 - 3, 2**63 - 1],
            [2**63 - 2, 2**63 - 3, 2**63 - 1, 0, 2, 3, 4, 5, 6],
        ],
    )
    def test_hashtable(self, values):
        myenum = enum.IntEnum("myenum", [(f"x{i}", v) for i, v in enumerate(values)])
        dec = msgspec.msgpack.Decoder(myenum)

        assert hasattr(myenum, "__msgspec_cache__")

        for val in myenum:
            msg = msgspec.msgpack.encode(val)
            val2 = dec.decode(msg)
            assert val == val2

        for bad in [-2000, -1, 1, 2000]:
            with pytest.raises(ValidationError):
                dec.decode(msgspec.msgpack.encode(bad))

    @pytest.mark.parametrize(
        "values",
        [
            [8, 16, 24, 32, 40, 48],
            [-8, -16, -24, -32, -40, -48],
        ],
    )
    def test_hashtable_collisions(self, values):
        myenum = enum.IntEnum("myenum", [(f"x{i}", v) for i, v in enumerate(values)])
        dec = msgspec.msgpack.Decoder(myenum)

        for val in myenum:
            msg = msgspec.msgpack.encode(val)
            val2 = dec.decode(msg)
            assert val == val2

        for bad in [0, 7, 9, 56, -min(values), -max(values), 2**64 - 1, -(2**63)]:
            with pytest.raises(ValidationError):
                dec.decode(msgspec.msgpack.encode(bad))


class TestEnum:
    def test_empty_errors(self, proto):
        class Empty(enum.Enum):
            pass

        with pytest.raises(TypeError, match="Enum types must have at least one item"):
            proto.Decoder(Empty)

    def test_encode_complex(self, proto):
        class Complex(enum.Enum):
            A = 1.5

        res = proto.encode(Complex.A)
        sol = proto.encode(1.5)
        assert res == sol

        res = proto.encode({Complex.A: 1})
        sol = proto.encode({1.5: 1})
        assert res == sol

    def test_decode_complex_errors(self, proto):
        class Complex(enum.Enum):
            A = 1.5

        with pytest.raises(TypeError) as rec:
            proto.Decoder(Complex)

        assert "Enums must contain either all str or all int values" in str(rec.value)
        assert repr(Complex) in str(rec.value)

    @pytest.mark.parametrize(
        "values",
        [
            [("A", 1), ("B", 2), ("C", "c")],
            [("A", "a"), ("B", "b"), ("C", 3)],
        ],
    )
    def test_mixed_value_types_errors(self, values, proto):
        Bad = enum.Enum("Bad", values)

        with pytest.raises(TypeError) as rec:
            proto.Decoder(Bad)

        assert "Enums must contain either all str or all int values" in str(rec.value)
        assert repr(Bad) in str(rec.value)

    @pytest.mark.parametrize("base_cls", [StrEnum, enum.Enum])
    def test_encode(self, proto, base_cls):
        class Test(base_cls):
            A = "apple"
            B = "banana"

        assert proto.encode(Test.A) == proto.encode("apple")

    @pytest.mark.parametrize("base_cls", [StrEnum, enum.Enum])
    def test_decode(self, proto, base_cls):
        class Test(base_cls):
            A = "apple"
            B = "banana"

        dec = proto.Decoder(Test)
        assert dec.decode(proto.encode("apple")) is Test.A
        assert dec.decode(proto.encode("banana")) is Test.B

        with pytest.raises(ValidationError, match="Invalid enum value 'cherry'"):
            dec.decode(proto.encode("cherry"))

    def test_decode_nested(self, proto):
        class Test(Struct):
            fruit: FruitStr

        dec = proto.Decoder(Test)

        dec.decode(proto.encode({"fruit": "apple"})) == Test(FruitStr.APPLE)

        with pytest.raises(
            ValidationError,
            match=r"Invalid enum value 'cherry' - at `\$.fruit`",
        ):
            dec.decode(proto.encode({"fruit": "cherry"}))

    def test_str_lookup_reused(self):
        class Test(enum.Enum):
            A = "a"
            B = "b"

        dec = msgspec.msgpack.Decoder(Test)  # noqa
        count = sys.getrefcount(Test.__msgspec_cache__)
        dec2 = msgspec.msgpack.Decoder(Test)
        count2 = sys.getrefcount(Test.__msgspec_cache__)
        assert count2 == count + 1

        # Reference count decreases when decoder is dropped
        del dec2
        gc.collect()
        count3 = sys.getrefcount(Test.__msgspec_cache__)
        assert count == count3

    def test_str_lookup_gc(self):
        class Test(enum.Enum):
            A = "a"
            B = "b"

        dec = msgspec.msgpack.Decoder(Test)
        assert gc.is_tracked(Test.__msgspec_cache__)

        # Deleting all references and running GC cleans up cycle
        ref = weakref.ref(Test)
        del Test
        del dec
        gc.collect()
        assert ref() is None

    def test_msgspec_cache_overwritten(self):
        class Test(enum.Enum):
            A = 1

        Test.__msgspec_cache__ = 1

        with pytest.raises(RuntimeError, match="__msgspec_cache__"):
            msgspec.msgpack.Decoder(Test)

    @pytest.mark.parametrize("length", [2, 8, 16])
    @pytest.mark.parametrize("nitems", [1, 3, 6, 12, 24, 48])
    def test_random_enum_same_lengths(self, rand, length, nitems):
        def strgen(length):
            """Yields unique random fixed-length strings"""
            seen = set()
            while True:
                x = rand.str(length)
                if x in seen:
                    continue
                seen.add(x)
                yield x

        unique_str = strgen(length).__next__

        myenum = enum.Enum(
            "myenum", [(unique_str(), unique_str()) for _ in range(nitems)]
        )
        dec = msgspec.msgpack.Decoder(myenum)

        for val in myenum:
            msg = msgspec.msgpack.encode(val.value)
            val2 = dec.decode(msg)
            assert val == val2

        for _ in range(10):
            key = unique_str()
            with pytest.raises(ValidationError):
                dec.decode(msgspec.msgpack.encode(key))

        # Try bad of different lengths
        for bad_length in [1, 7, 15, 30]:
            assert bad_length != length
            key = rand.str(bad_length)
            with pytest.raises(ValidationError):
                dec.decode(msgspec.msgpack.encode(key))

    @pytest.mark.parametrize("nitems", [1, 3, 6, 12, 24, 48])
    def test_random_enum_different_lengths(self, rand, nitems):
        def strgen():
            """Yields unique random strings"""
            seen = set()
            while True:
                x = rand.str(1, 32)
                if x in seen:
                    continue
                seen.add(x)
                yield x

        unique_str = strgen().__next__

        myenum = enum.Enum(
            "myenum", [(unique_str(), unique_str()) for _ in range(nitems)]
        )
        dec = msgspec.msgpack.Decoder(myenum)

        for val in myenum:
            msg = msgspec.msgpack.encode(val.value)
            val2 = dec.decode(msg)
            assert val == val2

        for _ in range(10):
            key = unique_str()
            with pytest.raises(ValidationError):
                dec.decode(msgspec.msgpack.encode(key))

    def test_enum_missing(self, proto):
        class Ex(enum.Enum):
            A = "a"
            B = "b"

            @classmethod
            def _missing_(cls, val):
                if val == "return-A":
                    return cls.A
                elif val == "return-B":
                    return cls.B
                elif val == "error":
                    raise ValueError("oh no!")
                else:
                    return None

        dec = proto.Decoder(Ex)

        def roundtrip(msg):
            return dec.decode(proto.encode(msg))

        assert roundtrip("a") is Ex.A
        assert roundtrip("return-A") is Ex.A
        assert roundtrip("return-B") is Ex.B
        with pytest.raises(ValidationError, match="Invalid enum value 'error'"):
            roundtrip("error")
        with pytest.raises(ValidationError, match="Invalid enum value 'other'"):
            roundtrip("other")


class TestLiterals:
    def test_empty_errors(self):
        with pytest.raises(
            TypeError, match="Literal types must have at least one item"
        ):
            msgspec.msgpack.Decoder(Literal[()])

    @pytest.mark.parametrize(
        "values",
        [
            [0, 1, 2, 2**63],
            [0, 1, 2, -(2**63) - 1],
        ],
    )
    def test_int_literal_values_out_of_range(self, values):
        literal = Literal[tuple(values)]

        with pytest.raises(NotImplementedError):
            msgspec.msgpack.Decoder(literal)

    @pytest.mark.parametrize(
        "typ",
        [
            Literal[1, False],
            Literal["ok", b"bad"],
            Literal[1, object()],
            Union[Literal[1, 2], Literal[3, False]],
            Union[Literal["one", "two"], Literal[3, False]],
            Literal[Literal[1, 2], Literal[3, False]],
            Literal[Literal["one", "two"], Literal[3, False]],
            Literal[1, 2, List[int]],
            Literal[1, 2, List],
        ],
    )
    def test_invalid_values(self, typ):
        with pytest.raises(TypeError, match="not supported"):
            msgspec.msgpack.Decoder(typ)

    def test_decode_literal_int_str_and_none_uncached_and_cached(self):
        values = (45987, "an_unlikely_string", None)
        literal = Literal[values]
        assert not hasattr(literal, "__msgspec_cache__")
        uncached = msgspec.msgpack.Decoder(literal)
        assert hasattr(literal, "__msgspec_cache__")
        cached = msgspec.msgpack.Decoder(literal)

        for val in values:
            assert uncached.decode(msgspec.msgpack.encode(val)) == val
            assert cached.decode(msgspec.msgpack.encode(val)) == val

    def test_cache_refcounts(self):
        literal = Literal[1, 2, "three", "four"]
        dec = msgspec.msgpack.Decoder(literal)  # noqa
        cache = literal.__msgspec_cache__
        count = sys.getrefcount(cache)
        dec2 = msgspec.msgpack.Decoder(literal)
        assert sys.getrefcount(cache) == count
        del dec2
        gc.collect()
        assert sys.getrefcount(cache) == count

    @pytest.mark.parametrize("val", [None, (), (1,), (1, 2), (1, 2, 3)])
    def test_msgspec_cache_overwritten(self, val):
        literal = Literal["a", "highly", "improbable", "set", "of", "strings"]

        literal.__msgspec_cache__ = val

        with pytest.raises(RuntimeError, match="__msgspec_cache__"):
            msgspec.msgpack.Decoder(literal)

    def test_multiple_literals(self):
        integers = Literal[-1, -2, -3]
        strings = Literal["apple", "banana"]
        both = Union[integers, strings]

        dec = msgspec.msgpack.Decoder(both)

        assert not hasattr(both, "__msgspec_cache__")

        for val in [-1, -2, -3, "apple", "banana"]:
            assert dec.decode(msgspec.msgpack.encode(val)) == val

        with pytest.raises(ValidationError, match="Invalid enum value 4"):
            dec.decode(msgspec.msgpack.encode(4))

        with pytest.raises(ValidationError, match="Invalid enum value 'carrot'"):
            dec.decode(msgspec.msgpack.encode("carrot"))

    def test_nested_literals(self):
        integers = Literal[-1, -2, -3]
        strings = Literal["apple", "banana"]
        both = Literal[integers, strings]

        dec = msgspec.msgpack.Decoder(both)

        assert hasattr(both, "__msgspec_cache__")

        for val in [-1, -2, -3, "apple", "banana"]:
            assert dec.decode(msgspec.msgpack.encode(val)) == val

        with pytest.raises(ValidationError, match="Invalid enum value 4"):
            dec.decode(msgspec.msgpack.encode(4))

        with pytest.raises(ValidationError, match="Invalid enum value 'carrot'"):
            dec.decode(msgspec.msgpack.encode("carrot"))

    def test_mix_int_and_int_literal(self):
        dec = msgspec.msgpack.Decoder(Union[Literal[-1, 1], int])
        for x in [-1, 1, 10]:
            assert dec.decode(msgspec.msgpack.encode(x)) == x

    def test_mix_str_and_str_literal(self):
        dec = msgspec.msgpack.Decoder(Union[Literal["a", "b"], str])
        for x in ["a", "b", "c"]:
            assert dec.decode(msgspec.msgpack.encode(x)) == x


class TestUnionTypeErrors:
    def test_decoder_unsupported_type(self, proto):
        with pytest.raises(TypeError):
            proto.Decoder(1)

    def test_decoder_validates_struct_definition_unsupported_types(self, proto):
        """Struct definitions aren't validated until first use"""

        class Test(Struct):
            a: 1

        with pytest.raises(TypeError):
            proto.Decoder(Test)

    @pytest.mark.parametrize("typ", [Union[int, Deque], Union[Deque, int]])
    def test_err_union_with_custom_type(self, typ, proto):
        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)
        assert "custom type" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize(
        "typ",
        [
            Union[dict, Person],
            Union[Person, dict],
            Union[PersonDict, dict],
            Union[PersonDataclass, dict],
            Union[Person, PersonDict],
        ],
    )
    def test_err_union_with_multiple_dict_like_types(self, typ, proto):
        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)
        assert "more than one dict-like type" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize(
        "typ",
        [
            Union[PersonArray, list],
            Union[tuple, PersonArray],
            Union[PersonArray, PersonTuple],
            Union[PersonTuple, frozenset],
        ],
    )
    def test_err_union_with_struct_array_like_and_array(self, typ, proto):
        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)
        assert "more than one array-like type" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize("types", [(FruitInt, int), (FruitInt, Literal[1, 2])])
    def test_err_union_with_multiple_int_like_types(self, types, proto):
        typ = Union[types]
        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)
        assert "int-like" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize(
        "typ",
        [
            str,
            Literal["one", "two"],
            datetime.datetime,
            datetime.date,
            datetime.time,
            uuid.UUID,
        ],
    )
    def test_err_union_with_multiple_str_like_types(self, typ, proto):
        union = Union[FruitStr, typ]
        with pytest.raises(TypeError) as rec:
            proto.Decoder(union)
        assert "str-like" in str(rec.value)
        assert repr(union) in str(rec.value)

    @pytest.mark.parametrize(
        "typ,kind",
        [
            (Union[FruitInt, VeggieInt], "int enum"),
            (Union[FruitStr, VeggieStr], "str enum"),
            (Union[Dict[int, float], dict], "dict"),
            (Union[List[int], List[float]], "array-like"),
            (Union[List[int], tuple], "array-like"),
            (Union[set, tuple], "array-like"),
            (Union[Tuple[int, ...], list], "array-like"),
            (Union[Tuple[int, float, str], set], "array-like"),
            (Union[Deque, int, Custom], "custom"),
        ],
    )
    def test_err_union_conflicts(self, typ, kind, proto):
        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)
        assert f"more than one {kind}" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @py310_plus
    def test_310_union_types(self, proto):
        dec = proto.Decoder(int | str | None)
        for msg in [1, "abc", None]:
            assert dec.decode(proto.encode(msg)) == msg
        with pytest.raises(ValidationError):
            assert dec.decode(proto.encode(1.5))


class TestStructUnion:
    def test_err_union_struct_mix_array_like(self, proto):
        class Test1(Struct, tag=True, array_like=True):
            x: int

        class Test2(Struct, tag=True, array_like=False):
            x: int

        typ = Union[Test1, Test2]

        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)

        assert "not supported" in str(rec.value)
        assert "array_like" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize("array_like", [False, True])
    @pytest.mark.parametrize("tag1", [False, True])
    def test_err_union_struct_not_tagged(self, array_like, tag1, proto):
        class Test1(Struct, tag=tag1, array_like=array_like):
            x: int

        class Test2(Struct, array_like=array_like):
            x: int

        typ = Union[Test1, Test2]

        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)

        assert "not supported" in str(rec.value)
        assert "must be tagged" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize("array_like", [False, True])
    def test_err_union_conflict_with_basic_type(self, array_like, proto):
        class Test1(Struct, tag=True, array_like=array_like):
            x: int

        class Test2(Struct, tag=True, array_like=array_like):
            x: int

        other = list if array_like else dict

        typ = Union[Test1, Test2, other]

        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)

        assert "not supported" in str(rec.value)
        if array_like:
            assert "more than one array-like type" in str(rec.value)
        else:
            assert "more than one dict-like type" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize("array_like", [False, True])
    def test_err_union_struct_different_fields(self, proto, array_like):
        class Test1(Struct, tag_field="foo", array_like=array_like):
            x: int

        class Test2(Struct, tag_field="bar", array_like=array_like):
            x: int

        typ = Union[Test1, Test2]

        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)

        assert "not supported" in str(rec.value)
        assert "the same `tag_field`" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize("array_like", [False, True])
    def test_err_union_struct_mix_int_str_tags(self, proto, array_like):
        class Test1(Struct, tag=1, array_like=array_like):
            x: int

        class Test2(Struct, tag="two", array_like=array_like):
            x: int

        typ = Union[Test1, Test2]

        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)

        assert "not supported" in str(rec.value)
        assert "both `int` and `str` tags" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize("array_like", [False, True])
    @pytest.mark.parametrize(
        "tags",
        [
            ("a", "b", "b"),
            ("a", "a", "b"),
            ("a", "b", "a"),
            (1, 2, 2),
            (1, 1, 2),
            (1, 2, 1),
        ],
    )
    def test_err_union_struct_non_unique_tag_values(self, proto, array_like, tags):
        class Test1(Struct, tag=tags[0], array_like=array_like):
            x: int

        class Test2(Struct, tag=tags[1], array_like=array_like):
            x: int

        class Test3(Struct, tag=tags[2], array_like=array_like):
            x: int

        typ = Union[Test1, Test2, Test3]

        with pytest.raises(TypeError) as rec:
            proto.Decoder(typ)

        assert "not supported" in str(rec.value)
        assert "unique `tag`" in str(rec.value)
        assert repr(typ) in str(rec.value)

    @pytest.mark.parametrize(
        "tag1, tag2, unknown",
        [
            ("Test1", "Test2", "Test3"),
            (0, 1, 2),
            (123, -123, 0),
        ],
    )
    def test_decode_struct_union(self, proto, tag1, tag2, unknown):
        class Test1(Struct, tag=tag1):
            a: int
            b: int
            c: int = 0

        class Test2(Struct, tag=tag2):
            x: int
            y: int

        dec = proto.Decoder(Union[Test1, Test2])
        enc = proto.Encoder()

        # Tag can be in any position
        assert dec.decode(enc.encode({"type": tag1, "a": 1, "b": 2})) == Test1(1, 2)
        assert dec.decode(enc.encode({"a": 1, "type": tag1, "b": 2})) == Test1(1, 2)
        assert dec.decode(enc.encode({"x": 1, "y": 2, "type": tag2})) == Test2(1, 2)

        # Optional fields still work
        assert dec.decode(enc.encode({"type": tag1, "a": 1, "b": 2, "c": 3})) == Test1(
            1, 2, 3
        )
        assert dec.decode(enc.encode({"a": 1, "b": 2, "c": 3, "type": tag1})) == Test1(
            1, 2, 3
        )

        # Extra fields still ignored
        assert dec.decode(enc.encode({"a": 1, "b": 2, "d": 4, "type": tag1})) == Test1(
            1, 2
        )

        # Tag missing
        with pytest.raises(ValidationError) as rec:
            dec.decode(enc.encode({"a": 1, "b": 2}))
        assert "missing required field `type`" in str(rec.value)

        # Tag wrong type
        with pytest.raises(ValidationError) as rec:
            dec.decode(enc.encode({"type": 123.456, "a": 1, "b": 2}))
        assert f"Expected `{type(tag1).__name__}`" in str(rec.value)
        assert "`$.type`" in str(rec.value)

        # Tag unknown
        with pytest.raises(ValidationError) as rec:
            dec.decode(enc.encode({"type": unknown, "a": 1, "b": 2}))
        assert f"Invalid value {unknown!r} - at `$.type`" == str(rec.value)

    @pytest.mark.parametrize(
        "tag1, tag2, tag3, unknown",
        [
            ("Test1", "Test2", "Test3", "Test4"),
            (0, 1, 2, 3),
            (123, -123, 0, -1),
        ],
    )
    def test_decode_struct_array_union(self, proto, tag1, tag2, tag3, unknown):
        class Test1(Struct, tag=tag1, array_like=True):
            a: int
            b: int
            c: int = 0

        class Test2(Struct, tag=tag2, array_like=True):
            x: int
            y: int

        class Test3(Struct, tag=tag3, array_like=True):
            pass

        dec = proto.Decoder(Union[Test1, Test2, Test3])
        enc = proto.Encoder()

        # Decoding works
        assert dec.decode(enc.encode([tag1, 1, 2])) == Test1(1, 2)
        assert dec.decode(enc.encode([tag2, 3, 4])) == Test2(3, 4)
        assert dec.decode(enc.encode([tag3])) == Test3()

        # Optional & Extra fields still respected
        assert dec.decode(enc.encode([tag1, 1, 2, 3])) == Test1(1, 2, 3)
        assert dec.decode(enc.encode([tag1, 1, 2, 3, 4])) == Test1(1, 2, 3)

        # Missing required field
        with pytest.raises(ValidationError) as rec:
            dec.decode(enc.encode([tag1, 1]))
        assert "Expected `array` of at least length 3, got 2" in str(rec.value)

        # Type error has correct field index
        with pytest.raises(ValidationError) as rec:
            dec.decode(enc.encode([tag1, 1, "bad", 2]))
        assert "Expected `int`, got `str` - at `$[2]`" == str(rec.value)

        # Tag missing
        with pytest.raises(ValidationError) as rec:
            dec.decode(enc.encode([]))
        assert "Expected `array` of at least length 1, got 0" == str(rec.value)

        # Tag wrong type
        with pytest.raises(ValidationError) as rec:
            dec.decode(enc.encode([123.456, 2, 3, 4]))
        assert f"Expected `{type(tag1).__name__}`" in str(rec.value)
        assert "`$[0]`" in str(rec.value)

        # Tag unknown
        with pytest.raises(ValidationError) as rec:
            dec.decode(enc.encode([unknown, 1, 2, 3]))
        assert f"Invalid value {unknown!r} - at `$[0]`" == str(rec.value)

    @pytest.mark.parametrize("array_like", [False, True])
    def test_decode_struct_union_with_non_struct_types(self, array_like, proto):
        class Test1(Struct, tag=True, array_like=array_like):
            a: int
            b: int

        class Test2(Struct, tag=True, array_like=array_like):
            x: int
            y: int

        dec = proto.Decoder(Union[Test1, Test2, None, int, str])
        enc = proto.Encoder()

        for msg in [Test1(1, 2), Test2(3, 4), None, 5, 6]:
            assert dec.decode(enc.encode(msg)) == msg

        with pytest.raises(ValidationError) as rec:
            dec.decode(enc.encode(True))

        typ = "array" if array_like else "object"

        assert f"Expected `int | str | {typ} | null`, got `bool`" == str(rec.value)

    @pytest.mark.parametrize("array_like", [False, True])
    def test_struct_union_cached(self, array_like, proto):
        from msgspec._core import _struct_lookup_cache as cache

        cache.clear()

        class Test1(Struct, tag=True, array_like=array_like):
            a: int
            b: int

        class Test2(Struct, tag=True, array_like=array_like):
            x: int
            y: int

        typ1 = Union[Test2, Test1]
        typ2 = Union[Test1, Test2]
        typ3 = Union[Test1, Test2, int, None]

        for typ in [typ1, typ2, typ3]:
            for msg in [Test1(1, 2), Test2(3, 4)]:
                assert proto.decode(proto.encode(msg), type=typ) == msg

        assert len(cache) == 1
        assert frozenset((Test1, Test2)) in cache

    def test_struct_union_cache_evicted(self, proto):
        from msgspec._core import _struct_lookup_cache as cache

        MAX_CACHE_SIZE = 64  # XXX: update if hardcoded value in `_core.c` changes

        cache.clear()

        def call_with_new_types():
            class Test1(Struct, tag=True):
                a: int

            class Test2(Struct, tag=True):
                x: int

            typ = (Test1, Test2)

            proto.decode(proto.encode(Test1(1)), type=Union[typ])

            return frozenset(typ)

        first = call_with_new_types()
        assert first in cache

        # Fill up the cache
        for _ in range(MAX_CACHE_SIZE - 1):
            call_with_new_types()

        # Check that first item is still in cache and is first in order
        assert len(cache) == MAX_CACHE_SIZE
        assert first in cache
        assert first == list(cache.keys())[0]

        # Add a new item, causing an item to be popped from the cache
        new = call_with_new_types()

        assert len(cache) == MAX_CACHE_SIZE
        assert first not in cache
        assert frozenset(new) in cache


class TestGenericStruct:
    def test_generic_struct_info_cached(self, proto):
        class Ex(Struct, Generic[T]):
            x: T

        typ = Ex[int]
        assert Ex[int] is typ

        dec = proto.Decoder(typ)
        info = typ.__msgspec_cache__
        assert info is not None
        assert sys.getrefcount(info) <= 4  # info + attr + decoder + func call
        dec2 = proto.Decoder(typ)
        assert typ.__msgspec_cache__ is info
        assert sys.getrefcount(info) <= 5

        del dec
        del dec2
        assert sys.getrefcount(info) <= 3

    def test_generic_struct_invalid_types_not_cached(self, proto):
        class Ex(Struct, Generic[T]):
            x: Union[List[T], Tuple[float]]

        for typ in [Ex, Ex[int]]:
            for _ in range(2):
                with pytest.raises(TypeError, match="not supported"):
                    proto.Decoder(typ)

            assert not hasattr(typ, "__msgspec_cache__")

    def test_msgspec_cache_overwritten(self, proto):
        class Ex(Struct, Generic[T]):
            x: T

        typ = Ex[int]
        typ.__msgspec_cache__ = 1

        with pytest.raises(RuntimeError, match="__msgspec_cache__"):
            proto.Decoder(typ)

    @pytest.mark.parametrize("array_like", [False, True])
    def test_generic_struct(self, proto, array_like):
        class Ex(Struct, Generic[T], array_like=array_like):
            x: T
            y: List[T]

        sol = Ex(1, [1, 2])
        msg = proto.encode(sol)

        res = proto.decode(msg, type=Ex)
        assert res == sol

        res = proto.decode(msg, type=Ex[int])
        assert res == sol

        res = proto.decode(msg, type=Ex[Union[int, str]])
        assert res == sol

        res = proto.decode(msg, type=Ex[float])
        assert type(res.x) is float

        with pytest.raises(ValidationError, match="Expected `str`, got `int`"):
            proto.decode(msg, type=Ex[str])

    @pytest.mark.parametrize("array_like", [False, True])
    def test_recursive_generic_struct(self, proto, array_like):
        source = f"""
        from __future__ import annotations
        from typing import Union, Generic, TypeVar
        from msgspec import Struct

        T = TypeVar("T")

        class Ex(Struct, Generic[T], array_like={array_like}):
            a: T
            b: Union[Ex[T], None]
        """

        with temp_module(source) as mod:
            msg = mod.Ex(a=1, b=mod.Ex(a=2, b=None))
            msg2 = mod.Ex(a=1, b=mod.Ex(a="bad", b=None))
            assert proto.decode(proto.encode(msg), type=mod.Ex) == msg
            assert proto.decode(proto.encode(msg2), type=mod.Ex) == msg2
            assert proto.decode(proto.encode(msg), type=mod.Ex[int]) == msg

            with pytest.raises(ValidationError) as rec:
                proto.decode(proto.encode(msg2), type=mod.Ex[int])
            if array_like:
                assert "`$[1][0]`" in str(rec.value)
            else:
                assert "`$.b.a`" in str(rec.value)
            assert "Expected `int`, got `str`" in str(rec.value)

    @pytest.mark.parametrize("array_like", [False, True])
    def test_generic_struct_union(self, proto, array_like):
        class Test1(Struct, Generic[T], tag=True, array_like=array_like):
            a: Union[T, None]
            b: int

        class Test2(Struct, Generic[T], tag=True, array_like=array_like):
            x: T
            y: int

        typ = Union[Test1[T], Test2[T]]

        msg1 = Test1(1, 2)
        s1 = proto.encode(msg1)
        msg2 = Test2("three", 4)
        s2 = proto.encode(msg2)
        msg3 = Test1(None, 4)
        s3 = proto.encode(msg3)

        assert proto.decode(s1, type=typ) == msg1
        assert proto.decode(s2, type=typ) == msg2
        assert proto.decode(s3, type=typ) == msg3

        assert proto.decode(s1, type=typ[int]) == msg1
        assert proto.decode(s3, type=typ[int]) == msg3
        assert proto.decode(s2, type=typ[str]) == msg2
        assert proto.decode(s3, type=typ[str]) == msg3

        with pytest.raises(ValidationError) as rec:
            proto.decode(s1, type=typ[str])
        assert "Expected `str | null`, got `int`" in str(rec.value)
        loc = "$[1]" if array_like else "$.a"
        assert loc in str(rec.value)

        with pytest.raises(ValidationError) as rec:
            proto.decode(s2, type=typ[int])
        assert "Expected `int`, got `str`" in str(rec.value)
        loc = "$[1]" if array_like else "$.x"
        assert loc in str(rec.value)

    def test_unbound_typevars_use_bound_if_set(self, proto):
        T = TypeVar("T", bound=Union[int, str])

        dec = proto.Decoder(List[T])
        sol = [1, "two", 3, "four"]
        msg = proto.encode(sol)
        assert dec.decode(msg) == sol

        bad = proto.encode([1, {}])
        with pytest.raises(
            ValidationError,
            match=r"Expected `int \| str`, got `object` - at `\$\[1\]`",
        ):
            dec.decode(bad)

    def test_unbound_typevars_with_constraints_unsupported(self, proto):
        T = TypeVar("T", int, str)
        with pytest.raises(TypeError) as rec:
            proto.Decoder(List[T])

        assert "Unbound TypeVar `~T` has constraints" in str(rec.value)


class TestStructPostInit:
    @pytest.mark.parametrize("array_like", [False, True])
    @pytest.mark.parametrize("union", [False, True])
    def test_struct_post_init(self, array_like, union, proto):
        count = 0
        singleton = object()

        class Ex(Struct, array_like=array_like, tag=union):
            x: int

            def __post_init__(self):
                nonlocal count
                count += 1
                return singleton

        if union:

            class Ex2(Struct, array_like=array_like, tag=True):
                pass

            typ = Union[Ex, Ex2]
        else:
            typ = Ex

        msg = Ex(1)
        buf = proto.encode(msg)
        res = proto.decode(buf, type=typ)
        assert res == msg
        assert count == 2  # 1 for Ex(), 1 for decode
        assert sys.getrefcount(singleton) <= 2  # 1 for ref, 1 for call

    @pytest.mark.parametrize("array_like", [False, True])
    @pytest.mark.parametrize("union", [False, True])
    @pytest.mark.parametrize("exc_class", [ValueError, TypeError, OSError])
    def test_struct_post_init_errors(self, array_like, union, exc_class, proto):
        error = False

        class Ex(Struct, array_like=array_like, tag=union):
            x: int

            def __post_init__(self):
                if error:
                    raise exc_class("Oh no!")

        if union:

            class Ex2(Struct, array_like=array_like, tag=True):
                pass

            typ = Union[Ex, Ex2]
        else:
            typ = Ex

        msg = proto.encode([Ex(1)])
        error = True

        if exc_class in (ValueError, TypeError):
            expected = ValidationError
        else:
            expected = exc_class

        with pytest.raises(expected, match="Oh no!") as rec:
            proto.decode(msg, type=List[typ])

        if expected is ValidationError:
            assert "- at `$[0]`" in str(rec.value)


@pytest.fixture(params=["dataclass", "attrs"])
def decorator(request):
    if request.param == "dataclass":
        return dataclass
    elif request.param == "attrs":
        if attrs is None:
            pytest.skip(reason="attrs not installed")
        return attrs.define


class TestGenericDataclassOrAttrs:
    def test_generic_info_cached(self, decorator, proto):
        @decorator
        class Ex(Generic[T]):
            x: T

        typ = Ex[int]
        assert Ex[int] is typ

        dec = proto.Decoder(typ)
        info = typ.__msgspec_cache__
        assert info is not None
        assert sys.getrefcount(info) <= 4  # info + attr + decoder + func call
        dec2 = proto.Decoder(typ)
        assert typ.__msgspec_cache__ is info
        assert sys.getrefcount(info) <= 5

        del dec
        del dec2
        assert sys.getrefcount(info) <= 3

    def test_generic_invalid_types_not_cached(self, decorator, proto):
        @decorator
        class Ex(Generic[T]):
            x: Union[List[T], Tuple[float]]

        for typ in [Ex, Ex[int]]:
            for _ in range(2):
                with pytest.raises(TypeError, match="not supported"):
                    proto.Decoder(typ)

            assert not hasattr(typ, "__msgspec_cache__")

    def test_msgspec_cache_overwritten(self, decorator, proto):
        @decorator
        class Ex(Generic[T]):
            x: T

        typ = Ex[int]
        typ.__msgspec_cache__ = 1

        with pytest.raises(RuntimeError, match="__msgspec_cache__"):
            proto.Decoder(typ)

    def test_generic_dataclass(self, decorator, proto):
        @decorator
        class Ex(Generic[T]):
            x: T
            y: List[T]

        sol = Ex(1, [1, 2])
        msg = proto.encode(sol)

        res = proto.decode(msg, type=Ex)
        assert res == sol

        res = proto.decode(msg, type=Ex[int])
        assert res == sol

        res = proto.decode(msg, type=Ex[Union[int, str]])
        assert res == sol

        res = proto.decode(msg, type=Ex[float])
        assert type(res.x) is float

        with pytest.raises(ValidationError, match="Expected `str`, got `int`"):
            proto.decode(msg, type=Ex[str])

    @pytest.mark.parametrize("module", ["dataclasses", "attrs"])
    def test_recursive_generic(self, module, proto):
        pytest.importorskip(module)
        if module == "dataclasses":
            import_ = "from dataclasses import dataclass as decorator"
        else:
            import_ = "from attrs import define as decorator"

        source = f"""
        from __future__ import annotations
        from typing import Union, Generic, TypeVar
        from msgspec import Struct
        {import_}

        T = TypeVar("T")

        @decorator
        class Ex(Generic[T]):
            a: T
            b: Union[Ex[T], None]
        """

        with temp_module(source) as mod:
            msg = mod.Ex(a=1, b=mod.Ex(a=2, b=None))
            msg2 = mod.Ex(a=1, b=mod.Ex(a="bad", b=None))
            assert proto.decode(proto.encode(msg), type=mod.Ex) == msg
            assert proto.decode(proto.encode(msg2), type=mod.Ex) == msg2
            assert proto.decode(proto.encode(msg), type=mod.Ex[int]) == msg

            with pytest.raises(ValidationError) as rec:
                proto.decode(proto.encode(msg2), type=mod.Ex[int])
            assert "`$.b.a`" in str(rec.value)
            assert "Expected `int`, got `str`" in str(rec.value)

    def test_unbound_typevars_use_bound_if_set(self, proto):
        T = TypeVar("T", bound=Union[int, str])

        dec = proto.Decoder(List[T])
        sol = [1, "two", 3, "four"]
        msg = proto.encode(sol)
        assert dec.decode(msg) == sol

        bad = proto.encode([1, {}])
        with pytest.raises(
            ValidationError,
            match=r"Expected `int \| str`, got `object` - at `\$\[1\]`",
        ):
            dec.decode(bad)

    def test_unbound_typevars_with_constraints_unsupported(self, proto):
        T = TypeVar("T", int, str)
        with pytest.raises(TypeError) as rec:
            proto.Decoder(List[T])

        assert "Unbound TypeVar `~T` has constraints" in str(rec.value)


class TestStructOmitDefaults:
    def test_omit_defaults(self, proto):
        class Test(Struct, omit_defaults=True):
            a: int = 0
            b: bool = False
            c: Optional[str] = None
            d: list = []
            e: Union[list, set] = set()
            f: dict = {}

        cases = [
            (Test(), {}),
            (Test(1), {"a": 1}),
            (Test(1, False), {"a": 1}),
            (Test(1, True), {"a": 1, "b": True}),
            (Test(1, c=None), {"a": 1}),
            (Test(1, c="test"), {"a": 1, "c": "test"}),
            (Test(1, d=[1]), {"a": 1, "d": [1]}),
            (Test(1, e={1}), {"a": 1, "e": [1]}),
            (Test(1, e=[]), {"a": 1, "e": []}),
            (Test(1, f={"a": 1}), {"a": 1, "f": {"a": 1}}),
        ]

        for obj, sol in cases:
            res = proto.decode(proto.encode(obj))
            assert res == sol

    @pytest.mark.parametrize("typ", [tuple, list, set, frozenset, dict])
    def test_omit_defaults_collections(self, proto, typ):
        """Check that using empty collections as default values are detected
        regardless if they're specified by value or as a default_factory."""

        class Test(Struct, omit_defaults=True):
            a: typ = msgspec.field(default_factory=typ)
            b: typ = msgspec.field(default=typ())
            c: typ = typ()

        ex = {"x": 1} if typ is dict else [1]

        assert proto.encode(Test()) == proto.encode({})
        for n in ["a", "b", "c"]:
            assert proto.encode(Test(**{n: typ(ex)})) == proto.encode({n: ex})

    def test_omit_defaults_positional(self, proto):
        class Test(Struct, omit_defaults=True):
            a: int
            b: bool = False

        cases = [
            (Test(1), {"a": 1}),
            (Test(1, False), {"a": 1}),
            (Test(1, True), {"a": 1, "b": True}),
        ]

        for obj, sol in cases:
            res = proto.decode(proto.encode(obj))
            assert res == sol

    def test_omit_defaults_tagged(self, proto):
        class Test(Struct, omit_defaults=True, tag=True):
            a: int
            b: bool = False

        cases = [
            (Test(1), {"type": "Test", "a": 1}),
            (Test(1, False), {"type": "Test", "a": 1}),
            (Test(1, True), {"type": "Test", "a": 1, "b": True}),
        ]

        for obj, sol in cases:
            res = proto.decode(proto.encode(obj))
            assert res == sol

    def test_omit_defaults_ignored_for_array_like(self, proto):
        class Test(Struct, omit_defaults=True, array_like=True):
            a: int
            b: bool = False

        cases = [
            (Test(1), [1, False]),
            (Test(1, False), [1, False]),
            (Test(1, True), [1, True]),
        ]

        for obj, sol in cases:
            res = proto.decode(proto.encode(obj))
            assert res == sol


class TestStructForbidUnknownFields:
    def test_forbid_unknown_fields(self, proto):
        class Test(Struct, forbid_unknown_fields=True):
            x: int
            y: int

        good = Test(1, 2)
        assert proto.decode(proto.encode(good), type=Test) == good

        bad = proto.encode({"x": 1, "y": 2, "z": 3})
        with pytest.raises(ValidationError, match="Object contains unknown field `z`"):
            proto.decode(bad, type=Test)

    def test_forbid_unknown_fields_array_like(self, proto):
        class Test(Struct, forbid_unknown_fields=True, array_like=True):
            x: int
            y: int

        good = Test(1, 2)
        assert proto.decode(proto.encode(good), type=Test) == good

        bad = proto.encode([1, 2, 3])
        with pytest.raises(
            ValidationError, match="Expected `array` of at most length 2"
        ):
            proto.decode(bad, type=Test)


class PointUpper(Struct, rename="upper"):
    x: int
    y: int


class TestStructRename:
    def test_rename_encode_struct(self, proto):
        res = proto.encode(PointUpper(1, 2))
        exp = proto.encode({"X": 1, "Y": 2})
        assert res == exp

    def test_rename_decode_struct(self, proto):
        msg = proto.encode({"X": 1, "Y": 2})
        res = proto.decode(msg, type=PointUpper)
        assert res == PointUpper(1, 2)

    def test_rename_decode_struct_wrong_type(self, proto):
        msg = proto.encode({"X": 1, "Y": "bad"})
        with pytest.raises(ValidationError) as rec:
            proto.decode(msg, type=PointUpper)
        assert "Expected `int`, got `str` - at `$.Y`" == str(rec.value)

    def test_rename_decode_struct_missing_field(self, proto):
        msg = proto.encode({"X": 1})
        with pytest.raises(ValidationError) as rec:
            proto.decode(msg, type=PointUpper)
        assert "Object missing required field `Y`" == str(rec.value)


class TestStructKeywordOnly:
    def test_keyword_only_object(self, proto):
        class Test(Struct, kw_only=True):
            a: int
            b: int = 2
            c: int
            d: int = 4

        sol = Test(a=1, b=2, c=3, d=4)
        msg = proto.encode({"a": 1, "b": 2, "c": 3, "d": 4})
        res = proto.decode(msg, type=Test)
        assert res == sol

        msg = proto.encode({"a": 1, "c": 3})
        res = proto.decode(msg, type=Test)
        assert res == sol

        sol = Test(a=1, b=3, c=5)
        msg = proto.encode({"a": 1, "b": 3, "c": 5})
        res = proto.decode(msg, type=Test)
        assert res == sol

        msg = proto.encode({"a": 1, "b": 2})
        with pytest.raises(
            ValidationError,
            match="missing required field `c`",
        ):
            proto.decode(msg, type=Test)

        msg = proto.encode({"c": 1, "b": 2})
        with pytest.raises(
            ValidationError,
            match="missing required field `a`",
        ):
            proto.decode(msg, type=Test)

    def test_keyword_only_array(self, proto):
        class Test(Struct, kw_only=True, array_like=True):
            a: int
            b: int = 2
            c: int
            d: int = 4

        msg = proto.encode([5, 6, 7, 8])
        res = proto.decode(msg, type=Test)
        assert res == Test(a=5, b=6, c=7, d=8)

        msg = proto.encode([5, 6, 7])
        res = proto.decode(msg, type=Test)
        assert res == Test(a=5, b=6, c=7, d=4)

        msg = proto.encode([5, 6])
        with pytest.raises(
            ValidationError,
            match="Expected `array` of at least length 3, got 2",
        ):
            proto.decode(msg, type=Test)

        msg = proto.encode([])
        with pytest.raises(
            ValidationError,
            match="Expected `array` of at least length 3, got 0",
        ):
            proto.decode(msg, type=Test)


class TestStructDefaults:
    def test_struct_defaults(self, proto):
        class Test(Struct):
            a: int = 1
            b: list = []
            c: int = msgspec.field(default=2)
            d: dict = msgspec.field(default_factory=dict)

        sol = Test()

        res = proto.decode(proto.encode(sol), type=Test)
        assert res == sol

        res = proto.decode(proto.encode({}), type=Test)
        assert res == sol

    def test_struct_default_factory_errors(self, proto):
        def bad():
            raise ValueError("Oh no!")

        class Test(Struct):
            a: int = msgspec.field(default_factory=bad)

        msg = proto.encode({})
        with pytest.raises(Exception, match="Oh no!"):
            proto.decode(msg, type=Test)


class TestTypedDict:
    def test_type_cached(self, proto):
        class Ex(TypedDict):
            a: int
            b: str

        msg = {"a": 1, "b": "two"}

        dec = proto.Decoder(Ex)
        info = Ex.__msgspec_cache__
        assert info is not None
        dec2 = proto.Decoder(Ex)
        assert Ex.__msgspec_cache__ is info

        assert dec.decode(proto.encode(msg)) == msg
        assert dec2.decode(proto.encode(msg)) == msg

    def test_msgspec_cache_overwritten(self, proto):
        class Ex(TypedDict):
            x: int

        Ex.__msgspec_cache__ = 1

        with pytest.raises(RuntimeError, match="__msgspec_cache__"):
            proto.Decoder(Ex)

    def test_multiple_typeddict_errors(self, proto):
        class Ex1(TypedDict):
            a: int

        class Ex2(TypedDict):
            b: int

        with pytest.raises(TypeError, match="may not contain more than one TypedDict"):
            proto.Decoder(Union[Ex1, Ex2])

    def test_subtype_error(self, proto):
        class Ex(TypedDict):
            a: int
            b: Union[list, tuple]

        with pytest.raises(TypeError, match="may not contain more than one array-like"):
            proto.Decoder(Ex)
        assert not hasattr(Ex, "__msgspec_cache__")

    def test_recursive_type(self, proto):
        source = """
        from __future__ import annotations
        from typing import TypedDict, Union

        class Ex(TypedDict):
            a: int
            b: Union[Ex, None]
        """

        with temp_module(source) as mod:
            msg = {"a": 1, "b": {"a": 2, "b": None}}
            dec = proto.Decoder(mod.Ex)
            assert dec.decode(proto.encode(msg)) == msg

            with pytest.raises(ValidationError) as rec:
                dec.decode(proto.encode({"a": 1, "b": {"a": "bad"}}))
            assert "`$.b.a`" in str(rec.value)
            assert "Expected `int`, got `str`" in str(rec.value)

    def test_total_true(self, proto):
        class Ex(TypedDict):
            a: int
            b: str

        dec = proto.Decoder(Ex)

        x = {"a": 1, "b": "two"}
        assert dec.decode(proto.encode(x)) == x

        x2 = {"a": 1, "b": "two", "c": "extra"}
        assert dec.decode(proto.encode(x2)) == x

        with pytest.raises(ValidationError) as rec:
            dec.decode(proto.encode({"b": "two"}))
        assert "Object missing required field `a`" == str(rec.value)

        with pytest.raises(ValidationError) as rec:
            dec.decode(proto.encode({"a": 1, "b": 2}))
        assert "Expected `str`, got `int` - at `$.b`" == str(rec.value)

    def test_duplicate_keys(self, proto):
        """Validating if all required keys are present is done with a count. We
        need to ensure that duplicate required keys don't increment the count,
        masking a missing field."""

        class Ex(TypedDict):
            a: int
            b: str

        dec = proto.Decoder(Ex)

        temp = proto.encode({"a": 1, "b": "two", "x": 2})

        msg = temp.replace(b"x", b"a")
        assert dec.decode(msg) == {"a": 2, "b": "two"}

        msg = temp.replace(b"x", b"a").replace(b"b", b"c")
        with pytest.raises(ValidationError) as rec:
            dec.decode(msg)
        assert "Object missing required field `b`" == str(rec.value)

    def test_total_false(self, proto):
        class Ex(TypedDict, total=False):
            a: int
            b: str

        dec = proto.Decoder(Ex)

        x = {"a": 1, "b": "two"}
        assert dec.decode(proto.encode(x)) == x

        x2 = {"a": 1, "b": "two", "c": "extra"}
        assert dec.decode(proto.encode(x2)) == x

        x3 = {"b": "two"}
        assert dec.decode(proto.encode(x3)) == x3

        x4 = {}
        assert dec.decode(proto.encode(x4)) == x4

    @pytest.mark.parametrize("use_typing_extensions", [False, True])
    def test_total_partially_optional(self, proto, use_typing_extensions):
        if use_typing_extensions:
            tex = pytest.importorskip("typing_extensions")
            cls = tex.TypedDict
        else:
            cls = TypedDict

        class Base(cls):
            a: int
            b: str

        class Ex(Base, total=False):
            c: str

        dec = proto.Decoder(Ex)

        x = {"a": 1, "b": "two", "c": "extra"}
        assert dec.decode(proto.encode(x)) == x

        x2 = {"a": 1, "b": "two"}
        assert dec.decode(proto.encode(x2)) == x2

        with pytest.raises(ValidationError) as rec:
            dec.decode(proto.encode({"b": "two"}))
        assert "Object missing required field `a`" == str(rec.value)

    @pytest.mark.parametrize("use_typing_extensions", [False, True])
    def test_broken_typeddict(self, proto, use_typing_extensions):
        # Check that we don't crash if a TypedDict has incorrect
        # introspection data.
        if use_typing_extensions:
            tex = pytest.importorskip("typing_extensions")
            cls = tex.TypedDict
        else:
            cls = TypedDict

        class Ex(cls, total=False):
            c: str

        Ex.__annotations__ = {"c": "str"}
        Ex.__required_keys__ = {"a", "b"}

        with pytest.raises(RuntimeError):
            proto.Decoder(Ex)

    @pytest.mark.parametrize("use_typing_extensions", [False, True])
    def test_required_and_notrequired(self, proto, use_typing_extensions):
        if use_typing_extensions:
            module = "typing_extensions"
        else:
            module = "typing"

        ns = pytest.importorskip(module)

        if not hasattr(ns, "Required"):
            pytest.skip(f"{module}.Required is not available")

        source = f"""
        from __future__ import annotations
        from {module} import TypedDict, Required, NotRequired

        class Base(TypedDict):
            a: int
            b: NotRequired[str]

        class Ex(Base, total=False):
            c: str
            d: Required[bool]
        """

        with temp_module(source) as mod:
            dec = proto.Decoder(mod.Ex)

            x = {"a": 1, "b": "two", "c": "extra", "d": False}
            assert dec.decode(proto.encode(x)) == x

            x2 = {"a": 1, "d": False}
            assert dec.decode(proto.encode(x2)) == x2

            with pytest.raises(ValidationError) as rec:
                dec.decode(proto.encode({"d": False}))
            assert "Object missing required field `a`" == str(rec.value)

            with pytest.raises(ValidationError) as rec:
                dec.decode(proto.encode({"a": 2}))
            assert "Object missing required field `d`" == str(rec.value)

    def test_keys_are_their_interned_values(self, proto):
        """Ensure that we're not allocating new keys here, but reusing the
        existing keys on the TypedDict schema"""

        class Ex(TypedDict):
            key_name_1: int
            key_name_2: int

        dec = proto.Decoder(Ex)
        msg = dec.decode(proto.encode({"key_name_1": 1, "key_name_2": 2}))
        for k1, k2 in zip(sorted(Ex.__annotations__), sorted(msg)):
            assert k1 is k2

    def test_generic_typeddict_info_cached(self, proto):
        TypedDict = pytest.importorskip("typing_extensions").TypedDict

        class Ex(TypedDict, Generic[T]):
            x: T

        typ = Ex[int]
        assert Ex[int] is typ

        dec = proto.Decoder(typ)
        info = typ.__msgspec_cache__
        assert info is not None
        assert sys.getrefcount(info) <= 4  # info + attr + decoder + func call
        dec2 = proto.Decoder(typ)
        assert typ.__msgspec_cache__ is info
        assert sys.getrefcount(info) <= 5

        del dec
        del dec2
        assert sys.getrefcount(info) <= 3

    def test_generic_typeddict_invalid_types_not_cached(self, proto):
        TypedDict = pytest.importorskip("typing_extensions").TypedDict

        class Ex(TypedDict, Generic[T]):
            x: Union[List[T], Tuple[float]]

        for typ in [Ex, Ex[int]]:
            for _ in range(2):
                with pytest.raises(TypeError, match="not supported"):
                    proto.Decoder(typ)

            assert not hasattr(typ, "__msgspec_cache__")

    def test_generic_typeddict(self, proto):
        TypedDict = pytest.importorskip("typing_extensions").TypedDict

        class Ex(TypedDict, Generic[T]):
            x: T
            y: List[T]

        sol = Ex(x=1, y=[1, 2])
        msg = proto.encode(sol)

        res = proto.decode(msg, type=Ex)
        assert res == sol

        res = proto.decode(msg, type=Ex[int])
        assert res == sol

        res = proto.decode(msg, type=Ex[Union[int, str]])
        assert res == sol

        res = proto.decode(msg, type=Ex[float])
        assert type(res["x"]) is float

        with pytest.raises(ValidationError, match="Expected `str`, got `int`"):
            proto.decode(msg, type=Ex[str])

    def test_recursive_generic_typeddict(self, proto):
        pytest.importorskip("typing_extensions")

        source = """
        from __future__ import annotations
        from typing import Union, Generic, TypeVar
        from typing_extensions import TypedDict

        T = TypeVar("T")

        class Ex(TypedDict, Generic[T]):
            a: T
            b: Union[Ex[T], None]
        """

        with temp_module(source) as mod:
            msg = mod.Ex(a=1, b=mod.Ex(a=2, b=None))
            msg2 = mod.Ex(a=1, b=mod.Ex(a="bad", b=None))
            assert proto.decode(proto.encode(msg), type=mod.Ex) == msg
            assert proto.decode(proto.encode(msg2), type=mod.Ex) == msg2
            assert proto.decode(proto.encode(msg), type=mod.Ex[int]) == msg

            with pytest.raises(ValidationError) as rec:
                proto.decode(proto.encode(msg2), type=mod.Ex[int])
            assert "`$.b.a`" in str(rec.value)
            assert "Expected `int`, got `str`" in str(rec.value)


class TestNamedTuple:
    def test_type_cached(self, proto):
        class Ex(NamedTuple):
            a: int
            b: str

        msg = (1, "two")

        dec = proto.Decoder(Ex)
        info = Ex.__msgspec_cache__
        assert info is not None
        dec2 = proto.Decoder(Ex)
        assert Ex.__msgspec_cache__ is info

        assert dec.decode(proto.encode(msg)) == msg
        assert dec2.decode(proto.encode(msg)) == msg

    def test_msgspec_cache_overwritten(self, proto):
        class Ex(NamedTuple):
            x: int

        Ex.__msgspec_cache__ = 1

        with pytest.raises(RuntimeError, match="__msgspec_cache__"):
            proto.Decoder(Ex)

    def test_multiple_namedtuple_errors(self, proto):
        class Ex1(NamedTuple):
            a: int

        class Ex2(NamedTuple):
            b: int

        with pytest.raises(TypeError, match="may not contain more than one NamedTuple"):
            proto.Decoder(Union[Ex1, Ex2])

    def test_subtype_error(self, proto):
        class Ex(NamedTuple):
            a: int
            b: Union[list, tuple]

        with pytest.raises(TypeError, match="may not contain more than one array-like"):
            proto.Decoder(Ex)
        assert not hasattr(Ex, "__msgspec_cache__")

    def test_recursive_type(self, proto):
        source = """
        from __future__ import annotations
        from typing import NamedTuple, Union

        class Ex(NamedTuple):
            a: int
            b: Union[Ex, None]
        """

        with temp_module(source) as mod:
            msg = mod.Ex(1, mod.Ex(2, None))
            dec = proto.Decoder(mod.Ex)
            assert dec.decode(proto.encode(msg)) == msg

            with pytest.raises(ValidationError) as rec:
                dec.decode(proto.encode(mod.Ex(1, ("bad", "two"))))
            assert "`$[1][0]`" in str(rec.value)
            assert "Expected `int`, got `str`" in str(rec.value)

    @pytest.mark.parametrize("use_typing", [True, False])
    def test_decode_namedtuple_no_defaults(self, proto, use_typing):
        if use_typing:

            class Example(NamedTuple):
                a: int
                b: int
                c: int

        else:
            Example = namedtuple("Example", "a b c")

        dec = proto.Decoder(Example)
        msg = Example(1, 2, 3)
        res = dec.decode(proto.encode(msg))
        assert res == msg

        suffix = ", got 1" if proto is msgspec.msgpack else ""
        with pytest.raises(ValidationError, match=f"length 3{suffix}"):
            dec.decode(proto.encode((1,)))

        suffix = ", got 6" if proto is msgspec.msgpack else ""
        with pytest.raises(ValidationError, match=f"length 3{suffix}"):
            dec.decode(proto.encode((1, 2, 3, 4, 5, 6)))

    @pytest.mark.parametrize("use_typing", [True, False])
    def test_decode_namedtuple_with_defaults(self, proto, use_typing):
        if use_typing:

            class Example(NamedTuple):
                a: int
                b: int
                c: int = -3
                d: int = -4
                e: int = -5

        else:
            Example = namedtuple("Example", "a b c d e", defaults=(-3, -4, -5))

        dec = proto.Decoder(Example)
        for args in [(1, 2), (1, 2, 3), (1, 2, 3, 4), (1, 2, 3, 4, 5)]:
            msg = Example(*args)
            res = dec.decode(proto.encode(msg))
            assert res == msg

        suffix = ", got 1" if proto is msgspec.msgpack else ""
        with pytest.raises(ValidationError, match=f"length 2 to 5{suffix}"):
            dec.decode(proto.encode((1,)))

        suffix = ", got 6" if proto is msgspec.msgpack else ""
        with pytest.raises(ValidationError, match=f"length 2 to 5{suffix}"):
            dec.decode(proto.encode((1, 2, 3, 4, 5, 6)))

    def test_decode_namedtuple_field_wrong_type(self, proto):
        dec = proto.Decoder(PersonTuple)
        msg = proto.encode((1, "bad", 2))
        with pytest.raises(
            ValidationError, match=r"Expected `str`, got `int` - at `\$\[0\]`"
        ):
            dec.decode(msg)

    def test_decode_namedtuple_not_array(self, proto):
        dec = proto.Decoder(PersonTuple)
        msg = proto.encode({})
        with pytest.raises(ValidationError, match="Expected `array`, got `object`"):
            dec.decode(msg)

    def test_generic_namedtuple_info_cached(self, proto):
        NamedTuple = pytest.importorskip("typing_extensions").NamedTuple

        class Ex(NamedTuple, Generic[T]):
            x: T

        typ = Ex[int]
        assert Ex[int] is typ

        dec = proto.Decoder(typ)
        info = typ.__msgspec_cache__
        assert info is not None
        assert sys.getrefcount(info) <= 4  # info + attr + decoder + func call
        dec2 = proto.Decoder(typ)
        assert typ.__msgspec_cache__ is info
        assert sys.getrefcount(info) <= 5

        del dec
        del dec2
        assert sys.getrefcount(info) <= 3

    def test_generic_namedtuple_invalid_types_not_cached(self, proto):
        NamedTuple = pytest.importorskip("typing_extensions").NamedTuple

        class Ex(NamedTuple, Generic[T]):
            x: Union[List[T], Tuple[float]]

        for typ in [Ex, Ex[int]]:
            for _ in range(2):
                with pytest.raises(TypeError, match="not supported"):
                    proto.Decoder(typ)

            assert not hasattr(typ, "__msgspec_cache__")

    def test_generic_namedtuple(self, proto):
        NamedTuple = pytest.importorskip("typing_extensions").NamedTuple

        class Ex(NamedTuple, Generic[T]):
            x: T
            y: List[T]

        sol = Ex(1, [1, 2])
        msg = proto.encode(sol)

        res = proto.decode(msg, type=Ex)
        assert res == sol

        res = proto.decode(msg, type=Ex[int])
        assert res == sol

        res = proto.decode(msg, type=Ex[Union[int, str]])
        assert res == sol

        res = proto.decode(msg, type=Ex[float])
        assert type(res.x) is float

        with pytest.raises(ValidationError, match="Expected `str`, got `int`"):
            proto.decode(msg, type=Ex[str])

    def test_recursive_generic_namedtuple(self, proto):
        pytest.importorskip("typing_extensions")

        source = """
        from __future__ import annotations
        from typing import Union, Generic, TypeVar
        from typing_extensions import NamedTuple

        T = TypeVar("T")

        class Ex(NamedTuple, Generic[T]):
            a: T
            b: Union[Ex[T], None]
        """

        with temp_module(source) as mod:
            msg = mod.Ex(a=1, b=mod.Ex(a=2, b=None))
            msg2 = mod.Ex(a=1, b=mod.Ex(a="bad", b=None))
            assert proto.decode(proto.encode(msg), type=mod.Ex) == msg
            assert proto.decode(proto.encode(msg2), type=mod.Ex) == msg2
            assert proto.decode(proto.encode(msg), type=mod.Ex[int]) == msg

            with pytest.raises(ValidationError) as rec:
                proto.decode(proto.encode(msg2), type=mod.Ex[int])
            assert "`$[1][0]`" in str(rec.value)
            assert "Expected `int`, got `str`" in str(rec.value)


class TestDataclass:
    def test_encode_dataclass_err_invalid_dataclass_fields(self, proto):
        @dataclass
        class Ex:
            x: int

        Ex.__dataclass_fields__ = ()

        with pytest.raises(RuntimeError, match="is not a dict"):
            proto.encode(Ex(1))

    def test_encode_dataclass_class_errors(self, proto):
        @dataclass
        class Ex:
            x: int

        with pytest.raises(TypeError, match="Encoding objects of type type"):
            proto.encode(Ex)

    def test_encode_dataclass_no_slots(self, proto):
        @dataclass
        class Test:
            x: int
            y: int

        x = Test(1, 2)
        res = proto.encode(x)
        sol = proto.encode({"x": 1, "y": 2})
        assert res == sol

    @py310_plus
    def test_encode_dataclass_slots(self, proto):
        @dataclass(slots=True)
        class Test:
            x: int
            y: int

        x = Test(1, 2)
        res = proto.encode(x)
        sol = proto.encode({"x": 1, "y": 2})
        assert res == sol

    @py310_plus
    @pytest.mark.parametrize("slots", [True, False])
    def test_encode_dataclass_missing_fields(self, proto, slots):
        @dataclass(slots=slots)
        class Test:
            x: int
            y: int
            z: int

        x = Test(1, 2, 3)
        sol = {"x": 1, "y": 2, "z": 3}
        for key in "xyz":
            delattr(x, key)
            del sol[key]
            res = proto.decode(proto.encode(x))
            assert res == sol

    @py310_plus
    @pytest.mark.parametrize("slots_base", [True, False])
    @pytest.mark.parametrize("slots", [True, False])
    def test_encode_dataclass_subclasses(self, proto, slots_base, slots):
        @dataclass(slots=slots_base)
        class Base:
            x: int
            y: int

        @dataclass(slots=slots)
        class Test(Base):
            y: int
            z: int

        x = Test(1, 2, 3)
        res = proto.decode(proto.encode(x))
        assert res == {"x": 1, "y": 2, "z": 3}

        # Missing attribute ignored
        del x.y
        res = proto.decode(proto.encode(x))
        assert res == {"x": 1, "z": 3}

    @py311_plus
    def test_encode_dataclass_weakref_slot(self, proto):
        @dataclass(slots=True, weakref_slot=True)
        class Test:
            x: int
            y: int

        x = Test(1, 2)
        ref = weakref.ref(x)  # noqa
        res = proto.decode(proto.encode(x))
        assert res == {"x": 1, "y": 2}

    def test_encode_dataclass_classvars_ignored(self, proto):
        @dataclass
        class Ex:
            a: int
            b: ClassVar[int] = 2

        msg = proto.encode(Ex(a=1))
        assert msg == proto.encode({"a": 1})

    def test_encode_dataclass_extra_fields_ignored(self, proto):
        @dataclass
        class Ex:
            a: int
            b: int

        x = Ex(1, 2)
        x.c = 3

        msg = proto.encode(Ex(1, 2))
        assert msg == proto.encode({"a": 1, "b": 2})

    @pytest.mark.parametrize("order", ["acb", "bca", "cba"])
    def test_encode_dataclass_dict_reordered(self, proto, order):
        @dataclass
        class Ex:
            a: int
            b: int
            c: int

        x = Ex(1, 2, 3)
        x.__dict__.clear()
        x.__dict__.update(dict(zip(order, range(3))))
        res = proto.encode(x)
        sol = proto.encode(dict(sorted(zip(order, range(3)))))
        assert res == sol

    @pytest.mark.parametrize("present", ["ab", "a", "b", ""])
    def test_encode_dataclass_ducktyped(self, proto, present):
        """gel.Object looks like a dataclass, but the implementation doesn't
        match the one from dataclasses. This ducktyped implementation tries to
        mirror the one in gel for testing purposes. Ref:
        https://docs.geldata.com/reference/using/python/api/types#gel.Object"""

        @dataclass
        class Ex:
            a: int
            b: int

        msg = {k: v for k, v in zip("ab", range(2)) if k in present}

        class Ex2:
            def __getattr__(self, key):
                return msg[key]

            __dataclass_fields__ = {}

        x = Ex2()
        x.__dataclass_fields__ = Ex.__dataclass_fields__
        res = proto.encode(x)
        sol = proto.encode(msg)
        assert res == sol

    @pytest.mark.parametrize("field", "xyz")
    def test_encode_dataclass_invalid_field_errors(self, proto, field):
        @dataclass
        class Test:
            x: int
            y: int
            z: int

        x = Test(1, 2, 3)
        setattr(x, field, object())
        with pytest.raises(TypeError, match="unsupported"):
            proto.encode(x)

    def test_type_cached(self, proto):
        @dataclass
        class Ex:
            a: int
            b: str

        msg = Ex(a=1, b="two")

        dec = proto.Decoder(Ex)
        info = Ex.__msgspec_cache__
        assert info is not None
        dec2 = proto.Decoder(Ex)
        assert Ex.__msgspec_cache__ is info

        assert dec.decode(proto.encode(msg)) == msg
        assert dec2.decode(proto.encode(msg)) == msg

    def test_decode_dataclass_subclasses(self, proto):
        @dataclass
        class Base:
            x: int

        @dataclass
        class Sub(Base):
            y: int

        msg = proto.encode({"x": 1, "y": 2})

        assert proto.decode(msg, type=Base) == Base(1)
        assert proto.decode(msg, type=Sub) == Sub(1, 2)

    def test_multiple_dataclasses_errors(self, proto):
        @dataclass
        class Ex1:
            a: int

        @dataclass
        class Ex2:
            b: int

        with pytest.raises(TypeError, match="may not contain more than one dataclass"):
            proto.Decoder(Union[Ex1, Ex2])

    def test_subtype_error(self, proto):
        @dataclass
        class Ex:
            a: int
            b: Union[list, tuple]

        with pytest.raises(TypeError, match="may not contain more than one array-like"):
            proto.Decoder(Ex)
        assert not hasattr(Ex, "__msgspec_cache__")

    def test_recursive_type(self, proto):
        source = """
        from __future__ import annotations
        from typing import Union
        from dataclasses import dataclass

        @dataclass
        class Ex:
            a: int
            b: Union[Ex, None]
        """

        with temp_module(source) as mod:
            msg = mod.Ex(a=1, b=mod.Ex(a=2, b=None))
            dec = proto.Decoder(mod.Ex)
            assert dec.decode(proto.encode(msg)) == msg

            with pytest.raises(ValidationError) as rec:
                dec.decode(proto.encode({"a": 1, "b": {"a": "bad"}}))
            assert "`$.b.a`" in str(rec.value)
            assert "Expected `int`, got `str`" in str(rec.value)

    def test_classvars_ignored(self, proto):
        source = """
        from __future__ import annotations

        from typing import ClassVar
        from dataclasses import dataclass

        @dataclass
        class Ex:
            a: int
            other: ClassVar[int]
        """
        with temp_module(source) as mod:
            msg = mod.Ex(a=1)
            dec = proto.Decoder(mod.Ex)
            res = dec.decode(proto.encode({"a": 1, "other": 2}))
            assert res == msg
            assert not hasattr(res, "other")

    def test_initvars_forbidden(self, proto):
        source = """
        from dataclasses import dataclass, InitVar

        @dataclass
        class Ex:
            a: int
            other: InitVar[int]
        """
        with temp_module(source) as mod:
            with pytest.raises(TypeError, match="`InitVar` fields are not supported"):
                proto.Decoder(mod.Ex)

    @pytest.mark.parametrize("slots", [False, True])
    def test_decode_dataclass(self, proto, slots):
        if slots:
            if not PY310:
                pytest.skip(reason="Python 3.10+ required")
            kws = {"slots": True}
        else:
            kws = {}

        @dataclass(**kws)
        class Example:
            a: int
            b: int
            c: int

        dec = proto.Decoder(Example)
        msg = Example(1, 2, 3)
        res = dec.decode(proto.encode(msg))
        assert res == msg

        # Extra fields ignored
        res = dec.decode(
            proto.encode({"x": -1, "a": 1, "y": -2, "b": 2, "z": -3, "c": 3, "": -4})
        )
        assert res == msg

        # Missing fields error
        with pytest.raises(ValidationError, match="missing required field `b`"):
            dec.decode(proto.encode({"a": 1}))

        # Incorrect field types error
        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$.a`"
        ):
            dec.decode(proto.encode({"a": "bad"}))

    @pytest.mark.parametrize("frozen", [False, True])
    @pytest.mark.parametrize("slots", [False, True])
    def test_decode_dataclass_defaults(self, proto, frozen, slots):
        if slots:
            if not PY310:
                pytest.skip(reason="Python 3.10+ required")
            kws = {"slots": True}
        else:
            kws = {}

        @dataclass(frozen=frozen, **kws)
        class Example:
            a: int
            b: int
            c: int = -3
            d: int = -4
            e: int = field(default_factory=lambda: -1000)

        dec = proto.Decoder(Example)
        for args in [(1, 2), (1, 2, 3), (1, 2, 3, 4), (1, 2, 3, 4, 5)]:
            sol = Example(*args)
            msg = dict(zip("abcde", args))
            res = dec.decode(proto.encode(msg))
            assert res == sol

        # Missing fields error
        with pytest.raises(ValidationError, match="missing required field `a`"):
            dec.decode(proto.encode({"c": 1, "d": 2, "e": 3}))

    def test_decode_dataclass_default_factory_errors(self, proto):
        def bad():
            raise ValueError("Oh no!")

        @dataclass
        class Example:
            a: int = field(default_factory=bad)

        with pytest.raises(ValueError, match="Oh no!"):
            proto.decode(proto.encode({}), type=Example)

    def test_decode_dataclass_frozen(self, proto):
        @dataclass(frozen=True)
        class Point:
            x: int
            y: int

        msg = proto.encode(Point(1, 2))
        res = proto.decode(msg, type=Point)
        assert res == Point(1, 2)

    def test_decode_dataclass_post_init(self, proto):
        called = False

        @dataclass
        class Example:
            a: int

            def __post_init__(self):
                nonlocal called
                called = True

        res = proto.decode(proto.encode({"a": 1}), type=Example)
        assert res.a == 1
        assert called

    @pytest.mark.parametrize("exc_class", [ValueError, TypeError, OSError])
    def test_decode_dataclass_post_init_errors(self, proto, exc_class):
        @dataclass
        class Example:
            a: int

            def __post_init__(self):
                raise exc_class("Oh no!")

        expected = (
            ValidationError if exc_class in (ValueError, TypeError) else exc_class
        )

        with pytest.raises(expected, match="Oh no!") as rec:
            proto.decode(proto.encode([{"a": 1}]), type=List[Example])

        if expected is ValidationError:
            assert "- at `$[0]`" in str(rec.value)

    def test_decode_dataclass_not_object(self, proto):
        @dataclass
        class Example:
            a: int
            b: int

        dec = proto.Decoder(Example)
        msg = proto.encode([])
        with pytest.raises(ValidationError, match="Expected `object`, got `array`"):
            dec.decode(msg)


@pytest.mark.skipif(attrs is None, reason="attrs not installed")
class TestAttrs:
    def test_factory_takes_self_not_implemented(self, proto):
        """This feature is doable, but not yet implemented"""

        @attrs.define
        class Test:
            x: int = attrs.Factory(lambda self: 0, takes_self=True)

        with pytest.raises(NotImplementedError):
            proto.Decoder(Test)

    @pytest.mark.parametrize("slots", [True, False])
    def test_encode_attrs(self, proto, slots):
        @attrs.define(slots=slots)
        class Test:
            x: int
            y: int

        x = Test(1, 2)
        res = proto.encode(x)
        sol = proto.encode({"x": 1, "y": 2})
        assert res == sol

    @pytest.mark.parametrize("slots", [True, False])
    def test_encode_attrs_missing_fields(self, proto, slots):
        @attrs.define(slots=slots)
        class Test:
            x: int
            y: int
            z: int

        x = Test(1, 2, 3)
        sol = {"x": 1, "y": 2, "z": 3}
        for key in "xyz":
            delattr(x, key)
            del sol[key]
            res = proto.decode(proto.encode(x))
            assert res == sol

    @pytest.mark.parametrize("slots_base", [True, False])
    @pytest.mark.parametrize("slots", [True, False])
    def test_encode_attrs_subclasses(self, proto, slots_base, slots):
        @attrs.define(slots=slots_base)
        class Base:
            x: int
            y: int

        @attrs.define(slots=slots)
        class Test(Base):
            y: int
            z: int

        x = Test(1, 2, 3)
        res = proto.decode(proto.encode(x))
        assert res == {"x": 1, "y": 2, "z": 3}

        # Missing attribute ignored
        del x.y
        res = proto.decode(proto.encode(x))
        assert res == {"x": 1, "z": 3}

    def test_encode_attrs_weakref_slot(self, proto):
        @attrs.define(slots=True, weakref_slot=True)
        class Test:
            x: int
            y: int

        x = Test(1, 2)
        ref = weakref.ref(x)  # noqa
        res = proto.decode(proto.encode(x))
        assert res == {"x": 1, "y": 2}

    @pytest.mark.parametrize("slots", [True, False])
    def test_encode_attrs_skip_leading_underscore(self, proto, slots):
        @attrs.define(slots=slots)
        class Test:
            x: int
            y: int
            _z: int

        x = Test(1, 2, 3)
        res = proto.encode(x)
        sol = proto.encode({"x": 1, "y": 2})
        assert res == sol

    @pytest.mark.parametrize("slots", [False, True])
    def test_decode_attrs(self, proto, slots):
        @attrs.define(slots=slots)
        class Example:
            a: int
            b: int
            c: int

        dec = proto.Decoder(Example)
        msg = Example(1, 2, 3)
        res = dec.decode(proto.encode(msg))
        assert res == msg

        # Extra fields ignored
        res = dec.decode(
            proto.encode({"x": -1, "a": 1, "y": -2, "b": 2, "z": -3, "c": 3, "": -4})
        )
        assert res == msg

        # Missing fields error
        with pytest.raises(ValidationError, match="missing required field `b`"):
            dec.decode(proto.encode({"a": 1}))

        # Incorrect field types error
        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$.a`"
        ):
            dec.decode(proto.encode({"a": "bad"}))

    @pytest.mark.parametrize("frozen", [False, True])
    @pytest.mark.parametrize("slots", [False, True])
    def test_decode_attrs_defaults(self, proto, frozen, slots):
        @attrs.define(frozen=frozen, slots=slots)
        class Example:
            a: int
            b: int
            c: int = -3
            d: int = -4
            e: int = attrs.field(factory=lambda: -1000)

        dec = proto.Decoder(Example)
        for args in [(1, 2), (1, 2, 3), (1, 2, 3, 4), (1, 2, 3, 4, 5)]:
            sol = Example(*args)
            msg = dict(zip("abcde", args))
            res = dec.decode(proto.encode(msg))
            assert res == sol

        # Missing fields error
        with pytest.raises(ValidationError, match="missing required field `a`"):
            dec.decode(proto.encode({"c": 1, "d": 2, "e": 3}))

    def test_decode_attrs_default_factory_errors(self, proto):
        def bad():
            raise ValueError("Oh no!")

        @attrs.define
        class Example:
            a: int = attrs.field(factory=bad)

        with pytest.raises(ValueError, match="Oh no!"):
            proto.decode(proto.encode({}), type=Example)

    def test_decode_attrs_frozen(self, proto):
        @attrs.define(frozen=True)
        class Example:
            x: int
            y: int

        msg = Example(1, 2)
        res = proto.decode(proto.encode(msg), type=Example)
        assert res == Example(1, 2)

    def test_decode_attrs_post_init(self, proto):
        called = False

        @attrs.define
        class Example:
            a: int

            def __attrs_post_init__(self):
                nonlocal called
                called = True

        res = proto.decode(proto.encode({"a": 1}), type=Example)
        assert res.a == 1
        assert called

    @pytest.mark.parametrize("exc_class", [ValueError, TypeError, OSError])
    def test_decode_attrs_post_init_errors(self, proto, exc_class):
        @attrs.define
        class Example:
            a: int

            def __attrs_post_init__(self):
                raise exc_class("Oh no!")

        expected = (
            ValidationError if exc_class in (ValueError, TypeError) else exc_class
        )

        with pytest.raises(expected, match="Oh no!") as rec:
            proto.decode(proto.encode([{"a": 1}]), type=List[Example])

        if expected is ValidationError:
            assert "- at `$[0]`" in str(rec.value)

    def test_decode_attrs_pre_init(self, proto):
        called = False

        @attrs.define
        class Example:
            a: int

            def __attrs_pre_init__(self):
                nonlocal called
                called = True

        res = proto.decode(proto.encode({"a": 1}), type=Example)
        assert res.a == 1
        assert called

    def test_decode_attrs_pre_init_errors(self, proto):
        @attrs.define
        class Example:
            a: int

            def __attrs_pre_init__(self):
                raise ValueError("Oh no!")

        with pytest.raises(ValueError, match="Oh no!"):
            proto.decode(proto.encode({"a": 1}), type=Example)

    def test_decode_attrs_validators(self, proto):
        def not2(self, attr, value):
            if value == 2:
                raise ValueError("Oh no!")

        @attrs.define
        class Example:
            a: int = attrs.field(validator=[attrs.validators.gt(0), not2])

        res = proto.decode(proto.encode({"a": 1}), type=Example)
        assert res.a == 1

        with pytest.raises(ValidationError):
            res = proto.decode(proto.encode({"a": -1}), type=Example)

        with pytest.raises(ValidationError, match="Oh no!"):
            res = proto.decode(proto.encode({"a": 2}), type=Example)

    def test_decode_attrs_not_object(self, proto):
        @attrs.define
        class Example:
            a: int
            b: int

        dec = proto.Decoder(Example)
        msg = proto.encode([])
        with pytest.raises(ValidationError, match="Expected `object`, got `array`"):
            dec.decode(msg)


class TestDate:
    def test_encode_date(self, proto):
        # All fields, zero padded
        x = datetime.date(1, 2, 3)
        s = proto.decode(proto.encode(x))
        assert s == "0001-02-03"

        # All fields, no zeros
        x = datetime.date(1234, 12, 31)
        s = proto.decode(proto.encode(x))
        assert s == "1234-12-31"

    @pytest.mark.parametrize(
        "s",
        [
            "0001-01-01",
            "9999-12-31",
            "0001-02-03",
            "2020-02-29",
        ],
    )
    def test_decode_date(self, proto, s):
        sol = datetime.date.fromisoformat(s)
        res = proto.decode(proto.encode(s), type=datetime.date)
        assert type(res) is datetime.date
        assert res == sol

    def test_decode_date_wrong_type(self, proto):
        msg = proto.encode([])
        with pytest.raises(ValidationError, match="Expected `date`, got `array`"):
            proto.decode(msg, type=datetime.date)

    @pytest.mark.parametrize(
        "s",
        [
            # Incorrect field lengths
            "001-02-03",
            "0001-2-03",
            "0001-02-3",
            # Trailing data
            "0001-02-0300",
            # Truncated
            "0001-02-",
            # Invalid characters
            "000a-02-03",
            "0001-0a-03",
            "0001-02-0a",
            # Year out of range
            "0000-02-03",
            # Month out of range
            "0001-00-03",
            "0001-13-03",
            # Day out of range for month
            "0001-02-00",
            "0001-02-29",
            "2000-02-30",
        ],
    )
    def test_decode_date_malformed(self, proto, s):
        msg = proto.encode(s)
        with pytest.raises(ValidationError, match="Invalid RFC3339"):
            proto.decode(msg, type=datetime.date)


class TestTime:
    @staticmethod
    def parse(t_str):
        t_str = t_str.replace("Z", "+00:00")
        return datetime.time.fromisoformat(t_str)

    @pytest.mark.parametrize(
        "t",
        [
            "00:00:00",
            "01:02:03",
            "01:02:03.000004",
            "12:34:56.789000",
            "23:59:59.999999",
        ],
    )
    def test_encode_time_naive(self, proto, t):
        res = proto.encode(self.parse(t))
        sol = proto.encode(t)
        assert res == sol

    @pytest.mark.parametrize(
        "t",
        [
            "00:00:00",
            "01:02:03",
            "01:02:03.000004",
            "12:34:56.789000",
            "23:59:59.999999",
        ],
    )
    def test_decode_time_naive(self, proto, t):
        sol = self.parse(t)
        res = proto.decode(proto.encode(t), type=datetime.time)
        assert type(res) is datetime.time
        assert res == sol

    def test_decode_time_wrong_type(self, proto):
        msg = proto.encode([])
        with pytest.raises(ValidationError, match="Expected `time`, got `array`"):
            proto.decode(msg, type=datetime.time)

    @pytest.mark.parametrize(
        "offset",
        [
            datetime.timedelta(0),
            datetime.timedelta(days=1, microseconds=-1),
            datetime.timedelta(days=-1, microseconds=1),
            datetime.timedelta(days=1, seconds=-29),
            datetime.timedelta(days=-1, seconds=29),
            datetime.timedelta(days=0, seconds=30),
            datetime.timedelta(days=0, seconds=-30),
        ],
    )
    def test_encode_time_offset_is_appx_equal_to_utc(self, proto, offset):
        x = datetime.time(14, 56, 27, 123456, datetime.timezone(offset))
        res = proto.encode(x)
        sol = proto.encode("14:56:27.123456Z")
        assert res == sol

    @pytest.mark.parametrize(
        "offset, t_str",
        [
            (
                datetime.timedelta(days=1, seconds=-30),
                "14:56:27.123456+23:59",
            ),
            (
                datetime.timedelta(days=-1, seconds=30),
                "14:56:27.123456-23:59",
            ),
            (
                datetime.timedelta(minutes=19, seconds=32, microseconds=130000),
                "14:56:27.123456+00:20",
            ),
        ],
    )
    def test_encode_time_offset_rounds_to_nearest_minute(self, proto, offset, t_str):
        x = datetime.time(14, 56, 27, 123456, datetime.timezone(offset))
        res = proto.encode(x)
        sol = proto.encode(t_str)
        assert res == sol

    def test_encode_time_zoneinfo(self):
        import zoneinfo

        try:
            x = datetime.time(1, 2, 3, 456789, zoneinfo.ZoneInfo("America/Chicago"))
        except zoneinfo.ZoneInfoNotFoundError:
            pytest.skip(reason="Failed to load timezone")
        sol = msgspec.json.encode(x.isoformat())
        res = msgspec.json.encode(x)
        assert res == sol

    @pytest.mark.parametrize(
        "dt",
        [
            "04:05:06.000007",
            "04:05:06.007",
            "04:05:06",
            "21:19:22.123456",
        ],
    )
    @pytest.mark.parametrize("suffix", ["", "Z", "+00:00", "-00:00"])
    def test_decode_time_utc(self, proto, dt, suffix):
        dt += suffix
        sol = self.parse(dt)
        msg = proto.encode(sol)
        res = proto.decode(msg, type=datetime.time)
        assert res == sol

    @pytest.mark.parametrize("t", ["00:00:01", "12:01:01"])
    @pytest.mark.parametrize("sign", ["-", "+"])
    @pytest.mark.parametrize("hour", [0, 8, 12, 16, 23])
    @pytest.mark.parametrize("minute", [0, 30])
    def test_decode_time_with_timezone(self, proto, t, sign, hour, minute):
        s = f"{t}{sign}{hour:02}:{minute:02}"
        msg = proto.encode(s)
        res = proto.decode(msg, type=datetime.time)
        sol = self.parse(s)
        assert res == sol

    @pytest.mark.parametrize("z", ["Z", "z"])
    def test_decode_time_not_case_sensitive(self, proto, z):
        """Z can be upper/lowercase"""
        sol = datetime.time(4, 5, 6, 7, UTC)
        res = proto.decode(proto.encode(f"04:05:06.000007{z}"), type=datetime.time)
        assert res == sol

    @pytest.mark.parametrize(
        "lax, strict",
        [
            ("03:04:05+0102", "03:04:05+01:02"),
            ("03:04:05-0102", "03:04:05-01:02"),
        ],
    )
    def test_decode_time_rfc3339_relaxed(self, lax, strict, proto):
        """msgspec supports a few relaxations of the RFC3339 format."""
        sol = datetime.time.fromisoformat(strict)
        msg = proto.encode(lax)
        res = proto.decode(msg, type=datetime.time)
        assert res == sol

    @pytest.mark.parametrize(
        "t, sol",
        [
            (
                "03:04:05.1234564Z",
                datetime.time(3, 4, 5, 123456, UTC),
            ),
            (
                "03:04:05.1234565Z",
                datetime.time(3, 4, 5, 123457, UTC),
            ),
            (
                "03:04:05.12345650000000000001Z",
                datetime.time(3, 4, 5, 123457, UTC),
            ),
            (
                "03:04:05.9999995Z",
                datetime.time(3, 4, 6, 0, UTC),
            ),
            (
                "03:04:59.9999995Z",
                datetime.time(3, 5, 0, 0, UTC),
            ),
            (
                "03:59:59.9999995Z",
                datetime.time(4, 0, 0, 0, UTC),
            ),
            (
                "23:59:59.9999995Z",
                datetime.time(0, 0, 0, 0, UTC),
            ),
        ],
    )
    def test_decode_time_nanos(self, proto, t, sol):
        msg = proto.encode(t)
        res = proto.decode(msg, type=datetime.time)
        assert res == sol

    @pytest.mark.parametrize(
        "s",
        [
            # Incorrect field lengths
            "1:02:03.0000004Z",
            "01:2:03.0000004Z",
            "01:02:3.0000004Z",
            "01:02:03.0000004+5:06",
            "01:02:03.0000004+05:6",
            "01:02:03.0000004+056",
            "01:02:03.0000004+05600",
            # Trailing data
            "01:02:030",
            "01:02:03a",
            "01:02:03.a",
            "01:02:03.0a",
            "01:02:03.0000004a",
            "01:02:03.0000004+00:000",
            "01:02:03.0000004+00000",
            "01:02:03.0000004Z0",
            # Truncated
            "01:02:3",
            # Missing +/-
            "01:02:0300:00",
            # Missing digits after decimal
            "01:02:03.",
            "01:02:03.Z",
            # Invalid characters
            "0a:02:03.004+05:06",
            "01:0a:03.004+05:06",
            "01:02:0a.004+05:06",
            "01:02:03.00a+05:06",
            "01:02:03.004+0a:06",
            "01:02:03.004+05:0a",
            "01:02:03.004+0a06",
            "01:02:03.004+050a",
            # Hour out of range
            "24:02:03.004",
            # Minute out of range
            "01:60:03.004",
            # Second out of range
            "01:02:60.004",
            # Timezone hour out of range
            "01:02:03.004+24:00",
            "01:02:03.004-24:00",
            # Timezone minute out of range
            "01:02:03.004+00:60",
            "01:02:03.004-00:60",
        ],
    )
    def test_decode_time_malformed(self, proto, s):
        msg = proto.encode(s)
        with pytest.raises(ValidationError, match="Invalid RFC3339"):
            proto.decode(msg, type=datetime.time)


class TestTimeDelta:
    @pytest.mark.parametrize("neg", [False, True])
    @pytest.mark.parametrize(
        "td, msg",
        [
            (timedelta(), "P0D"),
            (timedelta(1), "P1D"),
            (timedelta(10), "P10D"),
            (timedelta(123456789), "P123456789D"),
            (timedelta(0, 1), "PT1S"),
            (timedelta(0, 10), "PT10S"),
            (timedelta(0, 12345), "PT12345S"),
            (timedelta(0, 0, 1), "PT0.000001S"),
            (timedelta(0, 0, 10), "PT0.00001S"),
            (timedelta(0, 0, 100), "PT0.0001S"),
            (timedelta(0, 0, 1000), "PT0.001S"),
            (timedelta(0, 0, 10000), "PT0.01S"),
            (timedelta(0, 0, 100000), "PT0.1S"),
            (timedelta(123456789, 54321, 123456), "P123456789DT54321.123456S"),
            (timedelta(0, 86399, 999999), "PT86399.999999S"),
        ],
    )
    def test_roundtrip_timedelta(self, proto, td, msg, neg):
        if neg and td:
            td = -td
            msg = "-" + msg

        buf = proto.encode(td)

        res = proto.decode(buf)
        assert res == msg

        td2 = proto.decode(buf, type=timedelta)
        assert td2 == td

    @pytest.mark.parametrize(
        "msg, sol",
        [
            ("PT0S", timedelta()),
            ("+P1DT2S", timedelta(1, 2)),
            ("-P1DT2S", -timedelta(1, 2)),
            ("P000DT000.000S", timedelta()),
            ("-P000DT000.000S", timedelta()),
            ("P00012DT0045.670000000S", timedelta(12, 45, 670000)),
            ("P123456789.12345678912D", timedelta(123456789, 10666, 666580)),
            ("P123456789.12345678912999D", timedelta(123456789, 10666, 666580)),
            ("P123456789.12345678913D", timedelta(123456789, 10666, 666581)),
            ("PT0123H", timedelta(0, 123 * 60 * 60)),
            ("PT0123.456H", timedelta(0, 123.456 * 60 * 60)),
            ("PT0123M", timedelta(0, 123 * 60)),
            ("PT0123.456M", timedelta(0, 123.456 * 60)),
        ],
    )
    def test_decode_timedelta(self, proto, msg, sol):
        buf = proto.encode(msg)
        res = proto.decode(buf, type=timedelta)
        assert res == sol

    def test_decode_timedelta_case_insensitive(self, proto):
        buf = proto.encode("p1dt2h3m4s")
        res = proto.decode(buf, type=timedelta)
        assert res == timedelta(1, 2 * 60 * 60 + 3 * 60 + 4)

    @pytest.mark.parametrize(
        "msg",
        [
            "P999999999DT86399.999999S",
            "P999999998DT24H86399.999999S",
            "P999999999DT86399.9999994S",
        ],
    )
    def test_decode_timedelta_max(self, proto, msg):
        buf = proto.encode(msg)
        res = proto.decode(buf, type=timedelta)
        assert res == timedelta.max

    @pytest.mark.parametrize(
        "msg",
        [
            "-P999999999D",
            "-P999999998DT24H",
            "-P999999998DT23H3600S",
            "-P999999998DT86399.9999995S",
        ],
    )
    def test_decode_timedelta_min(self, proto, msg):
        buf = proto.encode(msg)
        res = proto.decode(buf, type=timedelta)
        assert res == timedelta.min

    def test_decode_timedelta_wrong_type(self, proto):
        bad = proto.encode([])
        with pytest.raises(ValidationError, match="Expected `duration`, got `array`"):
            proto.decode(bad, type=timedelta)

    @pytest.mark.parametrize(
        "msg",
        [
            # No P
            "",
            "-",
            "+",
            # Just P
            "P",
            "-P",
            "+P",
            # Missing Number
            "PD",
            "P.0D",
            # Missing digit after decimal place
            "P123.",
            "P123.D",
            # Missing Unit
            "P0",
            "P0.0",
            "P0.00",
            "P0.000000000000123",
            # Trailing T
            "PT",
            "P0DT",
            # Missing T
            "P1D2H",
            "P1D2S",
            # Repeat T
            "PTT0S",
            # Repeat Units
            "P1D2D",
            "PT1H2H",
            "PT1M2M",
            "PT1S2S",
            # Units in wrong order
            "PT1H1D",
            "PT1M1H",
            "PT1S1M",
            # Non-fractional after fractional
            "PT1.2H1M",
            "P1.2DT1H",
            "PT1.2H0S",
            # Invalid characters
            "1P1D",
            "P-1D",
            "P1.-D",
            "P1.0-D",
            "P1.000000000000123-D",
            "P1D-",
        ],
    )
    def test_decode_timedelta_malformed(self, proto, msg):
        encoded = proto.encode(msg)
        with pytest.raises(ValidationError, match="Invalid ISO8601 duration"):
            proto.decode(encoded, type=timedelta)

    @pytest.mark.parametrize(
        "msg",
        [
            "P1000000000D",
            "PT140737488355329S",
            "P999999999DT86399.9999995S",
            "-P999999999DT0.0000005S",
            "P999999998DT48H",
            "-P999999998DT24H01S",
        ],
    )
    def test_decode_timedelta_out_of_range(self, proto, msg):
        encoded = proto.encode(msg)
        with pytest.raises(ValidationError, match="Duration is out of range"):
            proto.decode(encoded, type=timedelta)

    @pytest.mark.parametrize("unit", ["Y", "M", "W"])
    def test_decode_timedelta_unsupported_unit(self, proto, unit):
        upper = f"P1{unit}"
        for msg in [upper, upper.lower()]:
            encoded = proto.encode(msg)
            with pytest.raises(ValidationError, match="Only units 'D'"):
                proto.decode(encoded, type=timedelta)


class TestUUID:
    def test_encoder_uuid_format(self, proto):
        assert proto.Encoder().uuid_format == "canonical"
        assert proto.Encoder(uuid_format="canonical").uuid_format == "canonical"
        assert proto.Encoder(uuid_format="hex").uuid_format == "hex"

        if proto is msgspec.msgpack:
            assert proto.Encoder(uuid_format="bytes").uuid_format == "bytes"
        else:
            with pytest.raises(
                ValueError,
                match="`uuid_format` must be 'canonical' or 'hex', got 'bytes'",
            ):
                proto.Encoder(uuid_format="bytes")

    def test_encoder_invalid_uuid_format(self, proto):
        if proto is msgspec.json:
            msg = "`uuid_format` must be 'canonical' or 'hex', got {!r}"
        else:
            msg = "`uuid_format` must be 'canonical', 'hex', or 'bytes', got {!r}"

        for bad in ["bad", 1]:
            with pytest.raises(ValueError, match=msg.format(bad)):
                proto.Encoder(uuid_format=bad)

    @pytest.mark.parametrize("format", ["canonical", "hex"])
    def test_encode_uuid(self, format, proto):
        u = uuid.uuid4()
        enc = proto.Encoder(uuid_format=format)
        res = enc.encode(u)
        if format == "canonical":
            sol = enc.encode(str(u))
        else:
            sol = enc.encode(u.hex)
        assert res == sol

    def test_encode_uuid_bytes(self):
        u = uuid.uuid4()
        enc = msgspec.msgpack.Encoder(uuid_format="bytes")
        res = enc.encode(u)
        sol = enc.encode(u.bytes)
        assert res == sol

    def test_encode_uuid_subclass(self, proto):
        class Ex(uuid.UUID):
            pass

        s = "4184defa-4d1a-4497-a140-fd1ec0b22383"
        assert proto.encode(Ex(s)) == proto.encode(s)

    def test_encode_uuid_malformed_internals(self, proto):
        """Ensure that if some other code mutates the uuid object, we error
        nicely rather than segfaulting"""
        u = uuid.uuid4()
        object.__delattr__(u, "int")

        with pytest.raises(AttributeError):
            proto.encode(u)

        u = uuid.uuid4()
        object.__setattr__(u, "int", "oops")

        with pytest.raises(TypeError):
            proto.encode(u)

    @pytest.mark.parametrize("upper", [False, True])
    @pytest.mark.parametrize("hyphens", [False, True])
    def test_decode_uuid(self, proto, upper, hyphens):
        u = uuid.uuid4()
        s = str(u) if hyphens else u.hex
        if upper:
            s = s.upper()
        msg = proto.encode(s)
        res = proto.decode(msg, type=uuid.UUID)
        assert res == u
        assert res.is_safe == u.is_safe

    def test_decode_uuid_from_bytes(self):
        sol = uuid.uuid4()
        msg = msgspec.msgpack.encode(sol.bytes)
        res = msgspec.msgpack.decode(msg, type=uuid.UUID)
        assert res == sol

        bad_msg = msgspec.msgpack.encode(b"x" * 8)
        with pytest.raises(msgspec.ValidationError, match="Invalid UUID bytes"):
            msgspec.msgpack.decode(bad_msg, type=uuid.UUID)

    @pytest.mark.parametrize(
        "uuid_str",
        [
            # Truncated
            "12345678-1234-1234-1234-1234567890a",
            "123456781234123412341234567890a",
            # Truncated segments
            "1234567-1234-1234-1234-1234567890abc",
            "12345678-123-1234-1234-1234567890abc",
            "12345678-1234-123-1234-1234567890abc",
            "12345678-1234-1234-123-1234567890abc",
            "12345678-1234-1234-1234-1234567890a-",
            # Invalid character
            "123456x81234123412341234567890ab",
            "123456x8-1234-1234-1234-1234567890ab",
            "1234567x-1234-1234-1234-1234567890ab",
            "12345678-123x-1234-1234-1234567890ab",
            "12345678-1234-123x-1234-1234567890ab",
            "12345678-1234-1234-123x-1234567890ab",
            "12345678-1234-1234-1234-1234567890ax",
            # Invalid dash
            "12345678.1234-1234-1234-1234567890ab",
            "12345678-1234.1234-1234-1234567890ab",
            "12345678-1234-1234.1234-1234567890ab",
            "12345678-1234-1234-1234.1234567890ab",
            # Trailing data
            "12345678-1234-1234-1234-1234567890ab-",
            "12345678-1234-1234-1234-1234567890abc",
        ],
    )
    def test_decode_uuid_malformed(self, proto, uuid_str):
        msg = proto.encode(uuid_str)
        with pytest.raises(ValidationError, match="Invalid UUID"):
            proto.decode(msg, type=uuid.UUID)


class TestNewType:
    def test_decode_newtype(self, proto):
        UserId = NewType("UserId", int)
        assert proto.decode(proto.encode(1), type=UserId) == 1

        with pytest.raises(ValidationError):
            proto.decode(proto.encode("bad"), type=UserId)

        # Nested NewId works
        UserId2 = NewType("UserId2", UserId)
        assert proto.decode(proto.encode(1), type=UserId2) == 1

        with pytest.raises(ValidationError):
            proto.decode(proto.encode("bad"), type=UserId2)

    def test_decode_annotated_newtype(self, proto):
        UserId = NewType("UserId", int)
        dec = proto.Decoder(Annotated[UserId, msgspec.Meta(ge=0)])
        assert dec.decode(proto.encode(1)) == 1

        with pytest.raises(ValidationError):
            dec.decode(proto.encode(-1))

    def test_decode_newtype_annotated(self, proto):
        UserId = NewType("UserId", Annotated[int, msgspec.Meta(ge=0)])
        dec = proto.Decoder(UserId)
        assert dec.decode(proto.encode(1)) == 1

        with pytest.raises(ValidationError):
            dec.decode(proto.encode(-1))

    def test_decode_annotated_newtype_annotated(self, proto):
        UserId = Annotated[
            NewType("UserId", Annotated[int, msgspec.Meta(ge=0)]), msgspec.Meta(le=10)
        ]
        dec = proto.Decoder(UserId)
        assert dec.decode(proto.encode(1)) == 1

        for bad in [-1, 11]:
            with pytest.raises(ValidationError):
                dec.decode(proto.encode(bad))


class TestTypeAlias:
    @py312_plus
    def test_simple(self, proto):
        with temp_module("type Ex = str | None") as mod:
            dec = proto.Decoder(mod.Ex)
            assert dec.decode(proto.encode("test")) == "test"
            assert dec.decode(proto.encode(None)) is None
            with pytest.raises(ValidationError):
                dec.decode(proto.encode(1))

    @py312_plus
    def test_generic(self, proto):
        with temp_module("type Pair[T] = tuple[T, T]") as mod:
            dec = proto.Decoder(mod.Pair)
            assert dec.decode(proto.encode((1, 2))) == (1, 2)
            for bad in [1, [1, 2, 3]]:
                with pytest.raises(ValidationError):
                    dec.decode(proto.encode(bad))

    @py312_plus
    def test_parametrized_generic(self, proto):
        with temp_module("type Pair[T] = tuple[T, T]") as mod:
            dec = proto.Decoder(mod.Pair[int])
            assert dec.decode(proto.encode((1, 2))) == (1, 2)
            for bad in [1, [1, 2, 3], [1, "a"]]:
                with pytest.raises(ValidationError):
                    dec.decode(proto.encode(bad))

    @py312_plus
    def test_typealias_wrapping_typealias(self, proto):
        src = """
        type Pair[T] = tuple[T, T]
        type Pairs[T] = list[Pair[T]]
        """
        with temp_module(src) as mod:
            dec = proto.Decoder(mod.Pairs)
            for good in [[], [(1, 2), (3, 4)]]:
                assert dec.decode(proto.encode(good)) == good
            for bad in [1, [1], [(1, 2, 3)]]:
                with pytest.raises(ValidationError):
                    dec.decode(proto.encode(bad))

            dec = proto.Decoder(mod.Pairs[int])
            for good in [[], [(1, 2)], [(1, 2), (3, 4)]]:
                assert dec.decode(proto.encode(good)) == good
            for bad in [1, [1], [(1, "a")]]:
                with pytest.raises(ValidationError):
                    dec.decode(proto.encode(bad))

    @py312_plus
    def test_typealias_with_constraints(self, proto):
        src = """
        import msgspec
        from typing import Annotated
        type Key = Annotated[str, msgspec.Meta(max_length=4)]
        """
        with temp_module(src) as mod:
            dec = proto.Decoder(mod.Key)
            for good in ["", "abc", "abcd"]:
                assert dec.decode(proto.encode(good)) == good
            for bad in [1, "abcde"]:
                with pytest.raises(ValidationError):
                    dec.decode(proto.encode(bad))

    @py312_plus
    def test_typealias_parametrized_generic_too_many_parameters(self):
        with temp_module("type Pair[T] = tuple[T, T]") as mod:
            with pytest.raises(TypeError):
                msgspec.json.Decoder(mod.Pair[int, int])

    @py312_plus
    @pytest.mark.parametrize(
        "src",
        [
            "type Ex = Ex | None",
            "type Ex = tuple[Ex, int]",
            "type Ex[T] = tuple[T, Ex[T]]",
            "type Temp[T] = tuple[T, Temp[T]]; Ex = Temp[int]",
            "type Temp[T] = tuple[T, Ex[T]]; type Ex[T] = tuple[Temp[T], T];",
        ],
    )
    def test_recursive_typealias_errors(self, src):
        """Eventually we should support this, but for now just test that it
        errors cleanly"""
        with temp_module(src) as mod:
            with pytest.raises(RecursionError):
                msgspec.json.Decoder(mod.Ex)

    @py312_plus
    def test_typealias_invalid_type(self):
        with temp_module("type Ex = int | complex") as mod:
            with pytest.raises(TypeError):
                msgspec.json.Decoder(mod.Ex)


class TestDecimal:
    def test_encoder_decimal_format(self, proto):
        assert proto.Encoder().decimal_format == "string"
        assert proto.Encoder(decimal_format="string").decimal_format == "string"
        assert proto.Encoder(decimal_format="number").decimal_format == "number"

    def test_encoder_invalid_decimal_format(self, proto):
        with pytest.raises(ValueError, match="must be 'string' or 'number', got 'bad'"):
            proto.Encoder(decimal_format="bad")

        with pytest.raises(ValueError, match="must be 'string' or 'number', got 1"):
            proto.Encoder(decimal_format=1)

    def test_encoder_encode_decimal(self, proto):
        enc = proto.Encoder()
        d = decimal.Decimal("1.5")
        s = str(d)
        assert enc.encode(d) == enc.encode(s)

    def test_Encoder_encode_decimal_string(self, proto):
        enc = proto.Encoder(decimal_format="string")
        d = decimal.Decimal("1.5")
        sol = enc.encode(str(d))

        assert enc.encode(d) == sol

        buf = bytearray()
        enc.encode_into(d, buf)
        assert buf == sol

    def test_Encoder_encode_decimal_number(self, proto):
        enc = proto.Encoder(decimal_format="number")
        d = decimal.Decimal("1.5")
        sol = enc.encode(float(d))

        assert enc.encode(d) == sol

        buf = bytearray()
        enc.encode_into(d, buf)
        assert buf == sol

    def test_encode_decimal(self, proto):
        d = decimal.Decimal("1.5")
        s = str(d)
        assert proto.encode(d) == proto.encode(s)

    @pytest.mark.parametrize(
        "val", ["1.5", "InF", "-iNf", "iNfInItY", "-InFiNiTy", "NaN"]
    )
    def test_decode_decimal_str(self, val, proto):
        sol = decimal.Decimal(val)
        msg = proto.encode(sol)
        res = proto.decode(msg, type=decimal.Decimal)
        assert str(res) == str(sol)
        assert type(res) is decimal.Decimal

    def test_decode_decimal_str_invalid(self, proto):
        msg = proto.encode("1..5")
        with pytest.raises(ValidationError, match="Invalid decimal string"):
            proto.decode(msg, type=decimal.Decimal)

    @pytest.mark.parametrize("val", [-1, -1234, 1, 1234])
    def test_decode_decimal_int(self, val, proto):
        msg = proto.encode(val)
        sol = decimal.Decimal(str(val))
        res = proto.decode(msg, type=decimal.Decimal)
        assert type(res) is decimal.Decimal
        assert res == sol

    @pytest.mark.parametrize(
        "val", [0.0, 1.3, float("nan"), float("inf"), float("-inf")]
    )
    def test_decode_decimal_float(self, val, proto):
        msg = proto.encode(val)
        if msg == b"null":
            pytest.skip("nonfinite values not supported")
        sol = decimal.Decimal(str(val))
        res = proto.decode(msg, type=decimal.Decimal)
        assert str(res) == str(sol)
        assert type(res) is decimal.Decimal


class TestAbstractTypes:
    @pytest.mark.parametrize(
        "typ",
        [
            typing.Collection,
            typing.MutableSequence,
            typing.Sequence,
            collections.abc.Collection,
            collections.abc.MutableSequence,
            collections.abc.Sequence,
            typing.MutableSet,
            typing.AbstractSet,
            collections.abc.MutableSet,
            collections.abc.Set,
        ],
    )
    def test_abstract_sequence(self, proto, typ):
        # Hacky, but it works
        if "Set" in str(typ):
            sol = {1, 2}
        else:
            sol = [1, 2]
        msg = proto.encode(sol)
        assert proto.decode(msg, type=typ) == sol
        with pytest.raises(ValidationError, match="Expected `array`, got `str`"):
            proto.decode(proto.encode("a"), type=typ)

        assert proto.decode(msg, type=typ[int]) == sol
        with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
            proto.decode(proto.encode(["a"]), type=typ[int])

    @pytest.mark.parametrize(
        "typ",
        [
            typing.MutableMapping,
            typing.Mapping,
            collections.abc.MutableMapping,
            collections.abc.Mapping,
        ],
    )
    def test_abstract_mapping(self, proto, typ):
        sol = {"x": 1, "y": 2}
        msg = proto.encode(sol)
        assert proto.decode(msg, type=typ) == sol
        with pytest.raises(ValidationError, match="Expected `object`, got `str`"):
            proto.decode(proto.encode("a"), type=typ)

        assert proto.decode(msg, type=typ[str, int]) == sol
        with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
            proto.decode(proto.encode({"a": "b"}), type=typ[str, int])


class TestUnset:
    def test_unset_type_annotation_ignored(self, proto):
        class Ex(Struct):
            x: Union[int, UnsetType]

        dec = proto.Decoder(Ex)
        msg = proto.encode({"x": 1})
        assert dec.decode(msg) == Ex(1)

    def test_encode_unset_errors_other_contexts(self, proto):
        with pytest.raises(TypeError):
            proto.encode(UNSET)

    @pytest.mark.parametrize("kind", ["struct", "dataclass", "attrs"])
    def test_unset_encode(self, kind, proto):
        if kind == "struct":

            class Ex(Struct):
                x: Union[int, UnsetType]
                y: Union[int, UnsetType]

        elif kind == "dataclass":

            @dataclass
            class Ex:
                x: Union[int, UnsetType]
                y: Union[int, UnsetType]

        elif kind == "attrs":
            attrs = pytest.importorskip("attrs")

            @attrs.define
            class Ex:
                x: Union[int, UnsetType]
                y: Union[int, UnsetType]

        res = proto.encode(Ex(1, UNSET))
        sol = proto.encode({"x": 1})
        assert res == sol

        res = proto.encode(Ex(UNSET, 2))
        sol = proto.encode({"y": 2})
        assert res == sol

        res = proto.encode(Ex(UNSET, UNSET))
        sol = proto.encode({})
        assert res == sol

    def test_unset_encode_struct_omit_defaults(self, proto):
        class Ex(Struct, omit_defaults=True):
            x: Union[int, UnsetType] = UNSET
            y: Union[int, UnsetType] = UNSET
            z: int = 0

        for x, y in [(Ex(), {}), (Ex(y=2), {"y": 2}), (Ex(z=1), {"z": 1})]:
            res = proto.encode(x)
            sol = proto.encode(y)
            assert res == sol


class TestOrder:
    def test_encoder_order_attribute(self, proto):
        enc = proto.Encoder()
        assert enc.order is None

        enc = proto.Encoder(order=None)
        assert enc.order is None

        enc = proto.Encoder(order="deterministic")
        assert enc.order == "deterministic"

        enc = proto.Encoder(order="sorted")
        assert enc.order == "sorted"

    def test_order_invalid(self, proto):
        with pytest.raises(ValueError, match="`order` must be one of"):
            proto.Encoder(order="bad")

        with pytest.raises(ValueError, match="`order` must be one of"):
            proto.encode(1, order="bad")

    @pytest.mark.parametrize("msg", [{}, {"y": 1, "x": 2, "z": 3}])
    @pytest.mark.parametrize("order", [None, "deterministic", "sorted"])
    @pytest.mark.parametrize("use_encoder", [False, True])
    def test_order_dict(self, msg, order, use_encoder, proto):
        if use_encoder:
            res = proto.Encoder(order=order).encode(msg)
        else:
            res = proto.encode(msg, order=order)

        if order is not None:
            sol = proto.encode(dict(sorted(msg.items())))
        else:
            sol = proto.encode(msg)

        assert res == sol

    def test_order_dict_non_str_errors(self, proto):
        with pytest.raises(TypeError, match="Only dicts with str keys"):
            proto.encode({"b": 2, 1: "a"}, order="deterministic")

    def test_order_dict_unsortable(self, proto):
        with pytest.raises(TypeError):
            proto.encode({"x": 1, 1: 2}, order="deterministic")

    @pytest.mark.parametrize("typ", [set, frozenset])
    @pytest.mark.parametrize("order", ["deterministic", "sorted"])
    def test_order_set(self, typ, proto, rand, order):
        assert proto.encode(typ(), order=order) == proto.encode([])

        msg = typ(rand.str(10) for _ in range(20))

        res = proto.encode(msg, order=order)
        sol = proto.encode(list(sorted(msg)))
        assert res == sol

        res = proto.encode(msg)
        sol = proto.encode(list(msg))
        assert res == sol

    def test_order_set_unsortable(self, proto):
        with pytest.raises(TypeError):
            proto.encode({"x", 1}, order="deterministic")

    @pytest.mark.parametrize("n", [0, 1, 2])
    @pytest.mark.parametrize(
        "kind",
        [
            "struct",
            "dataclass",
            "attrs",
            "attrs-dict",
        ],
    )
    def test_order_object(self, kind, n, proto):
        fields = [f"x{i}" for i in range(n)]
        fields.reverse()
        if kind == "struct":
            cls = msgspec.defstruct("Test", fields)
        elif kind == "dataclass":
            cls = make_dataclass("Test", fields)
        else:
            attrs = pytest.importorskip("attrs")
            cls = attrs.make_class("Test", fields, slots=(kind == "attrs"))
        msg = cls(*range(n))

        if kind in ("struct", "dataclass"):
            # we currently don't guarantee field order with attrs types
            sol = proto.encode(dict(zip(fields, range(n))))
            res = proto.encode(msg)
            assert res == sol

            res = proto.encode(msg, order="deterministic")
            assert res == sol

        res = proto.encode(msg, order="sorted")
        sol = proto.encode(dict(sorted(zip(fields, range(n)))))
        assert res == sol

    @pytest.mark.parametrize("kind", ["struct", "dataclass", "attrs", "attrs-dict"])
    def test_order_unset(self, kind, proto):
        if kind == "struct":

            class Ex(Struct):
                z: Union[int, UnsetType] = UNSET
                x: Union[int, UnsetType] = UNSET
        elif kind == "dataclass":

            @dataclass
            class Ex:
                z: Union[int, UnsetType] = UNSET
                x: Union[int, UnsetType] = UNSET
        else:
            attrs = pytest.importorskip("attrs")

            @attrs.define(slots=(kind == "attrs"))
            class Ex:
                z: Union[int, UnsetType] = UNSET
                x: Union[int, UnsetType] = UNSET

        res = proto.encode(Ex(), order="sorted")
        sol = proto.encode({})
        assert res == sol

        res = proto.encode(Ex(z=10), order="sorted")
        sol = proto.encode({"z": 10})
        assert res == sol

        res = proto.encode(Ex(z=10, x=-1), order="sorted")
        sol = proto.encode({"x": -1, "z": 10})
        assert res == sol

    def test_order_struct_omit_defaults(self, proto):
        class Ex(Struct, omit_defaults=True):
            z: int = 0
            x: int = 1
            y: int = 2

        res = proto.encode(Ex(), order="sorted")
        sol = proto.encode({})
        assert res == sol

        res = proto.encode(Ex(z=10), order="sorted")
        sol = proto.encode({"z": 10})
        assert res == sol

        res = proto.encode(Ex(z=10, x=-1), order="sorted")
        sol = proto.encode({"x": -1, "z": 10})
        assert res == sol

    def test_order_struct_tag(self, proto):
        class Ex(Struct, tag_field="y", tag=2):
            z: int
            x: int

        res = proto.encode(Ex(0, 1), order="sorted")
        sol = proto.encode({"x": 1, "y": 2, "z": 0})
        assert res == sol

    @pytest.mark.parametrize("n", [0, 2, 3, 7, 15, 16, 17, 32, 100, 500, 1000, 10000])
    def test_order_sort_implementation(self, rand, n):
        keys = [f"x_{i}" for i in range(n)]
        rand.shuffle(keys)
        msg = dict(zip(keys, range(n)))
        res = msgspec.json.encode(msg, order="deterministic")
        sol = msgspec.json.encode(dict(sorted(msg.items())))
        assert res == sol


class TestFinal:
    def test_decode_final(self, proto):
        dec = proto.Decoder(Final[int])

        assert dec.decode(proto.encode(1)) == 1
        with pytest.raises(ValidationError):
            dec.decode(proto.encode("bad"))

    def test_decode_final_annotated(self, proto):
        dec = proto.Decoder(Final[Annotated[int, msgspec.Meta(ge=0)]])

        assert dec.decode(proto.encode(1)) == 1
        with pytest.raises(ValidationError):
            dec.decode(proto.encode(-1))

    def test_decode_final_newtype(self, proto):
        UserId = NewType("UserId", int)
        dec = proto.Decoder(Final[UserId])

        assert dec.decode(proto.encode(1)) == 1
        with pytest.raises(ValidationError):
            dec.decode(proto.encode("bad"))


class TestLax:
    @pytest.mark.parametrize("strict", [True, False])
    def test_strict_lax_decoder(self, proto, strict):
        dec = proto.Decoder(List[int], strict=strict)

        assert dec.strict is strict

        msg = proto.encode(["1", "2"])

        if strict:
            with pytest.raises(ValidationError):
                dec.decode(msg)
        else:
            assert dec.decode(msg) == [1, 2]

    def test_lax_none(self, proto):
        for x in ["null", "Null", "nUll", "nuLl", "nulL"]:
            msg = proto.encode(x)
            assert proto.decode(msg, type=None, strict=False) is None

        for x in ["xull", "nxll", "nuxl", "nulx"]:
            msg = proto.encode(x)
            with pytest.raises(ValidationError, match="Expected `null`, got `str`"):
                proto.decode(msg, type=None, strict=False)

    def test_lax_bool_true(self, proto):
        for x in [1, "1", "true", "True", "tRue", "trUe", "truE"]:
            msg = proto.encode(x)
            assert proto.decode(msg, type=bool, strict=False) is True

        for x in [-1, 3, "x", "xx", "xrue", "txue", "trxe", "trux"]:
            msg = proto.encode(x)
            typ = type(x).__name__
            with pytest.raises(ValidationError, match=f"Expected `bool`, got `{typ}`"):
                assert proto.decode(msg, type=bool, strict=False)

    def test_lax_bool_false(self, proto):
        for x in [0, "0", "false", "False", "fAlse", "faLse", "falSe", "falsE"]:
            msg = proto.encode(x)
            assert proto.decode(msg, type=bool, strict=False) is False

        for x in [-1, 3, "x", "xx", "xalse", "fxlse", "faxse", "falxe", "falsx"]:
            msg = proto.encode(x)
            typ = type(x).__name__
            with pytest.raises(ValidationError, match=f"Expected `bool`, got `{typ}`"):
                assert proto.decode(msg, type=bool, strict=False)

    def test_lax_int(self, proto):
        for x in ["1", "-1", "123456"]:
            msg = proto.encode(x)
            assert proto.decode(msg, type=int, strict=False) == int(x)

        for x in ["a", "1a", "1.5", "1..", "nan", "inf"]:
            msg = proto.encode(x)
            with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
                proto.decode(msg, type=int, strict=False)

    def test_lax_int_from_float(self, proto):
        bound = float(1 << 53)
        for x in [-bound, -1.0, -0.0, 0.0, 1.0, bound]:
            msg = proto.encode(x)
            assert proto.decode(msg, type=int, strict=False) == int(x)

        for x in [-bound - 2, -1.5, 0.001, 1.5, bound + 2]:
            msg = proto.encode(x)
            with pytest.raises(ValidationError, match="Expected `int`, got `float`"):
                proto.decode(msg, type=int, strict=False)

    def test_lax_int_constr(self, proto):
        typ = Annotated[int, Meta(ge=0)]
        msg = proto.encode("1")
        assert proto.decode(msg, type=typ, strict=False) == 1

        msg = proto.encode("-1")
        with pytest.raises(ValidationError):
            proto.decode(msg, type=typ, strict=False)

    def test_lax_int_enum(self, proto):
        class Ex(enum.IntEnum):
            x = 1
            y = -2

        def roundtrip(msg):
            return proto.decode(proto.encode(msg), type=Ex, strict=False)

        assert roundtrip("1") is Ex.x
        assert roundtrip("-2") is Ex.y
        with pytest.raises(ValidationError, match="Invalid enum value 3"):
            roundtrip("3")
        with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
            roundtrip("A")

    def test_lax_int_literal(self, proto):
        typ = Literal[1, -2]

        def roundtrip(msg):
            return proto.decode(proto.encode(msg), type=typ, strict=False)

        assert roundtrip("1") == 1
        assert roundtrip("-2") == -2
        with pytest.raises(ValidationError, match="Invalid enum value 3"):
            roundtrip("3")
        with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
            roundtrip("A")

    def test_lax_float(self, proto):
        for x in ["1", "-1", "123456", "1.5", "-1.5", "inf"]:
            msg = proto.encode(x)
            assert proto.decode(msg, type=float, strict=False) == float(x)

        for x in ["a", "1a", "1.0.0", "1.."]:
            msg = proto.encode(x)
            with pytest.raises(ValidationError, match="Expected `float`, got `str`"):
                proto.decode(msg, type=float, strict=False)

    def test_lax_float_constr(self, proto):
        msg = proto.encode("1.5")
        assert proto.decode(msg, type=Annotated[float, Meta(ge=0)], strict=False) == 1.5

        msg = proto.encode("-1.0")
        with pytest.raises(ValidationError):
            proto.decode(msg, type=Annotated[float, Meta(ge=0)], strict=False)

    def test_lax_str(self, proto):
        for x in ["1", "1.5", "false", "null"]:
            msg = proto.encode(x)
            assert proto.decode(msg, type=str, strict=False) == x

    def test_lax_str_constr(self, proto):
        typ = Annotated[str, Meta(max_length=10)]
        msg = proto.encode("xxx")
        assert proto.decode(msg, type=typ, strict=False) == "xxx"

        msg = proto.encode("x" * 20)
        with pytest.raises(ValidationError):
            proto.decode(msg, type=typ, strict=False)

    @pytest.mark.parametrize(
        "x",
        [
            1234.0000004,
            1234.0000006,
            1234.000567,
            1234.567,
            1234.0,
            0.123,
            0.0,
            1234,
            0,
        ],
    )
    @pytest.mark.parametrize("sign", [-1, 1])
    @pytest.mark.parametrize("transform", [None, str])
    def test_lax_datetime(self, x, sign, transform, proto):
        timestamp = x * sign
        msg = proto.encode(transform(timestamp) if transform else timestamp)
        sol = datetime.datetime.fromtimestamp(timestamp, UTC)
        res = proto.decode(msg, type=datetime.datetime, strict=False)
        assert res == sol

    def test_lax_datetime_nonfinite_values(self, proto):
        values = ["nan", "-inf", "inf"]
        if proto is msgspec.msgpack:
            values.extend([float(v) for v in values])
        for val in values:
            msg = proto.encode(val)
            with pytest.raises(ValidationError, match="Invalid epoch timestamp"):
                proto.decode(msg, type=datetime.datetime, strict=False)

    @pytest.mark.parametrize("val", [-62135596801, 253402300801])
    @pytest.mark.parametrize("type", [int, float, str])
    def test_lax_datetime_out_of_range(self, val, type, proto):
        msg = proto.encode(type(val))
        with pytest.raises(ValidationError, match="out of range"):
            proto.decode(msg, type=datetime.datetime, strict=False)

    def test_lax_datetime_invalid_numeric_str(self, proto):
        for bad in ["", "12e", "1234a", "1234-1", "1234.a"]:
            msg = proto.encode(bad)
            with pytest.raises(ValidationError, match="Invalid"):
                proto.decode(msg, type=datetime.datetime, strict=False)

    @pytest.mark.parametrize("val", [123, -123, 123.456, "123.456"])
    def test_lax_datetime_naive_required(self, val, proto):
        msg = proto.encode(val)
        with pytest.raises(ValidationError, match="no timezone component"):
            proto.decode(
                msg, type=Annotated[datetime.datetime, Meta(tz=False)], strict=False
            )

    @pytest.mark.parametrize(
        "x",
        [
            1234.0000004,
            1234.0000006,
            1234.000567,
            1234.567,
            1234.0,
            0.123,
            0.0,
            1234,
            0,
        ],
    )
    @pytest.mark.parametrize("sign", [-1, 1])
    @pytest.mark.parametrize("transform", [None, str])
    def test_lax_timedelta(self, x, sign, transform, proto):
        timestamp = x * sign
        msg = proto.encode(transform(timestamp) if transform else timestamp)
        sol = datetime.timedelta(seconds=timestamp)
        res = proto.decode(msg, type=datetime.timedelta, strict=False)
        assert res == sol

    def test_lax_timedelta_nonfinite_values(self, proto):
        values = ["nan", "-inf", "inf"]
        if proto is msgspec.msgpack:
            values.extend([float(v) for v in values])
        for val in values:
            msg = proto.encode(val)
            with pytest.raises(ValidationError, match="out of range"):
                proto.decode(msg, type=datetime.timedelta, strict=False)

    @pytest.mark.parametrize("val", [86400000000001, -86399999913601])
    @pytest.mark.parametrize("type", [int, float, str])
    def test_lax_timedelta_out_of_range(self, val, type, proto):
        msg = proto.encode(type(val))
        with pytest.raises(ValidationError, match="out of range"):
            proto.decode(msg, type=datetime.timedelta, strict=False)

    def test_lax_timedelta_invalid_numeric_str(self, proto):
        for bad in ["", "12e", "1234a", "1234-1", "1234.a"]:
            msg = proto.encode(bad)
            with pytest.raises(ValidationError, match="Invalid"):
                proto.decode(msg, type=datetime.timedelta, strict=False)

    @pytest.mark.parametrize(
        "x, sol",
        [
            ("1", 1),
            ("0", 0),
            ("-1", -1),
            ("12.5", 12.5),
            ("inf", float("inf")),
            ("true", True),
            ("false", False),
            ("null", None),
        ],
    )
    def test_lax_union_valid(self, x, sol, proto):
        typ = Union[int, float, bool, None]
        msg = proto.encode(x)
        assert_eq(proto.decode(msg, type=typ, strict=False), sol)

    @pytest.mark.parametrize("x", ["1a", "1.5a", "falsx", "trux", "nulx"])
    def test_lax_union_invalid(self, x, proto):
        typ = Union[int, float, bool, None]
        msg = proto.encode(x)
        with pytest.raises(
            ValidationError, match="Expected `int | float | bool | null`"
        ):
            proto.decode(msg, type=typ, strict=False)

    @pytest.mark.parametrize(
        "x, err",
        [
            ("-1", "`int` >= 0"),
            ("2000", "`int` <= 1000"),
            ("18446744073709551616", "`int` <= 1000"),
            ("-9223372036854775809", "`int` >= 0"),
            ("100.5", "`float` <= 100.0"),
        ],
    )
    def test_lax_union_invalid_constr(self, x, err, proto):
        """Ensure that values that parse properly but don't meet the specified
        constraints error with a specific constraint error"""
        msg = proto.encode(x)
        typ = Union[
            Annotated[int, Meta(ge=0), Meta(le=1000)],
            Annotated[float, Meta(le=100)],
        ]
        with pytest.raises(ValidationError, match=err):
            proto.decode(msg, type=typ, strict=False)

    @pytest.mark.parametrize(
        "x, sol",
        [
            ("1", 1),
            ("1.5", 1.5),
            ("false", False),
            ("true", True),
            ("null", None),
            ("2022-05-02", datetime.date(2022, 5, 2)),
        ],
    )
    def test_lax_union_extended(self, proto, x, sol):
        typ = Union[int, float, bool, None, datetime.date]
        msg = proto.encode(x)
        assert_eq(proto.decode(msg, type=typ, strict=False), sol)
