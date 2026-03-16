from __future__ import annotations

import datetime
import enum
import gc
import itertools
import math
import pickle
import struct
import sys
from typing import (
    Any,
    Dict,
    FrozenSet,
    List,
    Literal,
    Optional,
    Set,
    Tuple,
    Union,
)

import pytest

import msgspec

UTC = datetime.timezone.utc


class FruitInt(enum.IntEnum):
    APPLE = 1
    BANANA = 2


class FruitStr(enum.Enum):
    APPLE = "apple"
    BANANA = "banana"


class Person(msgspec.Struct):
    first: str
    last: str
    age: int
    prefect: bool = False


class PersonArray(msgspec.Struct, array_like=True):
    first: str
    last: str
    age: int
    prefect: bool = False


PERSON = Person("harry", "potter", 13)
PERSON_AA = PersonArray("harry", "potter", 13)


class Node(msgspec.Struct):
    left: Optional[Node] = None
    right: Optional[Node] = None


class Custom:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __eq__(self, other):
        return self.x == other.x and self.y == other.y


INTS = [
    -(2**63),
    -(2**31 + 1),
    -(2**31),
    -(2**15 + 1),
    -(2**15),
    -(2**7 + 1),
    -(2**7),
    -(2**5 + 1),
    -(2**5),
    -1,
    0,
    1,
    2**7 - 1,
    2**7,
    2**8 - 1,
    2**8,
    2**16 - 1,
    2**16,
    2**32 - 1,
    2**32,
    2**63 - 1,
    2**64 - 1,
]

FLOATS = [
    -1.5,
    0.0,
    1.5,
    -float("inf"),
    float("inf"),
    float("nan"),
    sys.float_info.max,
    sys.float_info.min,
    -sys.float_info.max,
    -sys.float_info.min,
]

SIZES = [0, 1, 31, 32, 2**8 - 1, 2**8, 2**16 - 1, 2**16]


def assert_eq(x, y):
    if isinstance(x, float) and math.isnan(x):
        assert math.isnan(y)
    else:
        assert x == y


class TestEncodeFunction:
    def test_encode(self):
        dec = msgspec.msgpack.Decoder()
        assert dec.decode(msgspec.msgpack.encode(1)) == 1

    def test_encode_bad_arguments(self):
        with pytest.raises(TypeError, match="Missing 1 required arguments"):
            msgspec.msgpack.encode()

        with pytest.raises(TypeError, match="Extra positional"):
            msgspec.msgpack.encode(1, 2)

        with pytest.raises(TypeError, match="enc_hook must be callable"):
            msgspec.msgpack.encode(1, enc_hook="bad")

    def test_encode_error(self):
        with pytest.raises(TypeError):
            msgspec.msgpack.encode(object())

    def test_encode_large_object(self):
        """Check that buffer resize works"""
        data = b"x" * 4097
        dec = msgspec.msgpack.Decoder()
        assert dec.decode(msgspec.msgpack.encode(data)) == data

    def test_encode_no_enc_hook(self):
        class Foo:
            pass

        with pytest.raises(
            TypeError, match="Encoding objects of type Foo is unsupported"
        ):
            msgspec.msgpack.encode(Foo())

        with pytest.raises(
            TypeError, match="Encoding objects of type Foo is unsupported"
        ):
            msgspec.msgpack.encode(Foo(), enc_hook=None)

    def test_encode_enc_hook(self):
        unsupported = object()

        def enc_hook(x):
            assert x is unsupported
            return "hello"

        orig_refcount = sys.getrefcount(enc_hook)

        res = msgspec.msgpack.encode(unsupported, enc_hook=enc_hook)
        assert msgspec.msgpack.encode("hello") == res
        assert sys.getrefcount(enc_hook) == orig_refcount

    def test_encode_enc_hook_errors(self):
        def enc_hook(x):
            raise TypeError("bad")

        orig_refcount = sys.getrefcount(enc_hook)

        with pytest.raises(TypeError, match="bad"):
            msgspec.msgpack.encode(object(), enc_hook=enc_hook)

        assert sys.getrefcount(enc_hook) == orig_refcount

    def test_encode_parse_arguments_errors(self):
        with pytest.raises(TypeError, match="Missing 1 required argument"):
            msgspec.msgpack.encode()

        with pytest.raises(TypeError, match="Extra positional arguments"):
            msgspec.msgpack.encode(1, lambda x: None)

        with pytest.raises(TypeError, match="Extra positional arguments"):
            msgspec.msgpack.encode(1, 2, 3)

        with pytest.raises(TypeError, match="Extra keyword arguments"):
            msgspec.msgpack.encode(1, bad=1)

        with pytest.raises(TypeError, match="Extra keyword arguments"):
            msgspec.msgpack.encode(1, enc_hook=lambda x: None, extra="extra")


class TestDecodeFunction:
    def setup_method(self):
        self.buf = msgspec.msgpack.encode([1, 2, 3])

    def test_decode(self):
        assert msgspec.msgpack.decode(self.buf) == [1, 2, 3]

    def test_decode_type_keyword(self):
        assert msgspec.msgpack.decode(self.buf, type=List[int]) == [1, 2, 3]

        with pytest.raises(msgspec.ValidationError):
            assert msgspec.msgpack.decode(self.buf, type=List[str])

    def test_decode_type_any(self):
        assert msgspec.msgpack.decode(self.buf, type=Any) == [1, 2, 3]

    @pytest.mark.parametrize("array_like", [False, True])
    def test_decode_type_struct(self, array_like):
        class Point(msgspec.Struct, array_like=array_like):
            x: int
            y: int

        msg = msgspec.msgpack.encode(Point(1, 2))

        for _ in range(2):
            assert msgspec.msgpack.decode(msg, type=Point) == Point(1, 2)

    def test_decode_type_struct_not_json_compatible(self):
        class Test(msgspec.Struct):
            x: Dict[int, str]

        msg = msgspec.msgpack.encode(Test({1: "two"}))
        msgspec.msgpack.decode(msg, type=Test) == Test({1, "two"})

    def test_decode_type_struct_invalid_type(self):
        class Test(msgspec.Struct):
            x: 1

        with pytest.raises(TypeError):
            msgspec.msgpack.decode(b"{}", type=Test)

    def test_decode_invalid_type(self):
        with pytest.raises(TypeError, match="Type '1' is not supported"):
            msgspec.msgpack.decode(self.buf, type=1)

    def test_decode_invalid_buf(self):
        with pytest.raises(TypeError):
            msgspec.msgpack.decode(1)

    def test_decode_parse_arguments_errors(self):
        with pytest.raises(TypeError, match="Missing 1 required argument"):
            msgspec.msgpack.decode()

        with pytest.raises(TypeError, match="Extra positional arguments"):
            msgspec.msgpack.decode(self.buf, List[int])

        with pytest.raises(TypeError, match="Extra positional arguments"):
            msgspec.msgpack.decode(self.buf, 2, 3)

        with pytest.raises(TypeError, match="Extra keyword arguments"):
            msgspec.msgpack.decode(self.buf, bad=1)

        with pytest.raises(TypeError, match="Extra keyword arguments"):
            msgspec.msgpack.decode(self.buf, type=List[int], extra=1)

    def test_decode_with_trailing_characters_errors(self):
        msg = msgspec.msgpack.encode([1, 2, 3]) + b"trailing"

        with pytest.raises(msgspec.DecodeError):
            msgspec.msgpack.decode(msg)


class TestEncoderMisc:
    def test_encoder_init_errors(self):
        with pytest.raises(TypeError):
            msgspec.msgpack.Encoder(bad=1)

        with pytest.raises(TypeError, match="enc_hook must be callable"):
            msgspec.msgpack.Encoder(enc_hook=1)

    @pytest.mark.parametrize("x", [-(2**63) - 1, 2**64])
    def test_encode_integer_limits(self, x):
        enc = msgspec.msgpack.Encoder()
        with pytest.raises(OverflowError):
            enc.encode(x)

    def rec_obj1(self):
        o = []
        o.append(o)
        return o

    def rec_obj2(self):
        o = ([],)
        o[0].append(o)
        return o

    def rec_obj3(self):
        o = {}
        o["a"] = o
        return o

    def rec_obj4(self):
        class Box(msgspec.Struct):
            a: "Box"

        o = Box(None)
        o.a = o
        return o

    @pytest.mark.parametrize("case", [1, 2, 3, 4])
    def test_encode_infinite_recursive_object_errors(self, case):
        enc = msgspec.msgpack.Encoder()
        o = getattr(self, "rec_obj%d" % case)()
        with pytest.raises(RecursionError):
            enc.encode(o)

    def test_encode_no_enc_hook(self):
        class Foo:
            pass

        enc = msgspec.msgpack.Encoder()
        assert enc.enc_hook is None

        enc = msgspec.msgpack.Encoder(enc_hook=None)
        assert enc.enc_hook is None

        with pytest.raises(
            TypeError, match="Encoding objects of type Foo is unsupported"
        ):
            enc.encode(Foo())

    def test_encode_enc_hook(self):
        unsupported = object()

        def enc_hook(x):
            assert x is unsupported
            return "hello"

        orig_refcount = sys.getrefcount(enc_hook)

        enc = msgspec.msgpack.Encoder(enc_hook=enc_hook)

        assert enc.enc_hook is enc_hook
        assert sys.getrefcount(enc.enc_hook) == orig_refcount + 2
        assert sys.getrefcount(enc_hook) == orig_refcount + 1

        res = enc.encode(unsupported)
        assert enc.encode("hello") == res

        del enc
        assert sys.getrefcount(enc_hook) == orig_refcount

    def test_encode_enc_hook_errors(self):
        def enc_hook(x):
            raise TypeError("bad")

        enc = msgspec.msgpack.Encoder(enc_hook=enc_hook)

        with pytest.raises(TypeError, match="bad"):
            enc.encode(object())

    def test_encode_enc_hook_recurses(self):
        class Node:
            def __init__(self, a):
                self.a = a

        def enc_hook(x):
            return {"type": "Node", "a": x.a}

        enc = msgspec.msgpack.Encoder(enc_hook=enc_hook)

        msg = enc.encode(Node(Node(1)))
        res = msgspec.msgpack.decode(msg)
        assert res == {"type": "Node", "a": {"type": "Node", "a": 1}}

    def test_encode_enc_hook_recursion_error(self):
        enc = msgspec.msgpack.Encoder(enc_hook=lambda x: x)

        with pytest.raises(RecursionError):
            enc.encode(object())

    def test_encode_bad_arguments(self):
        enc = msgspec.msgpack.Encoder()

        with pytest.raises(TypeError, match="Missing 1 required arguments"):
            enc.encode()

        with pytest.raises(TypeError, match="Extra positional"):
            enc.encode(1, 2)

    def test_encode_into_bad_arguments(self):
        enc = msgspec.msgpack.Encoder()

        with pytest.raises(TypeError, match="bytearray"):
            enc.encode_into(1, b"test")

        with pytest.raises(TypeError):
            enc.encode_into(1, bytearray(), "bad")

        with pytest.raises(ValueError, match="offset"):
            enc.encode_into(1, bytearray(), -2)

        with pytest.raises(TypeError, match="Missing 1 required arguments"):
            enc.encode_into(1)

        with pytest.raises(TypeError, match="Extra positional"):
            enc.encode_into(1, bytearray(), 2, 3)

    @pytest.mark.parametrize("buf_size", [0, 1, 16, 55, 60])
    def test_encode_into(self, buf_size):
        enc = msgspec.msgpack.Encoder()

        msg = {"key": "x" * 48}
        encoded = msgspec.msgpack.encode(msg)

        buf = bytearray(buf_size)
        out = enc.encode_into(msg, buf)
        assert out is None
        assert buf == encoded

    def test_encode_into_offset(self):
        enc = msgspec.msgpack.Encoder()
        msg = {"key": "value"}
        encoded = enc.encode(msg)

        # Offset 0 is default
        buf = bytearray()
        enc.encode_into(msg, buf, 0)
        assert buf == encoded

        # Offset in bounds uses the provided offset
        buf = bytearray(b"01234")
        enc.encode_into(msg, buf, 2)
        assert buf == b"01" + encoded

        # Offset out of bounds extends
        buf = bytearray(b"01234")
        enc.encode_into(msg, buf, 10)
        assert buf[:5] == b"01234"
        assert buf[10:] == encoded

        # Offset -1 means append at end
        buf = bytearray(b"01234")
        enc.encode_into(msg, buf, -1)
        assert buf == b"01234" + encoded

    def test_encode_into_handles_errors_properly(self):
        enc = msgspec.msgpack.Encoder()
        out1 = enc.encode([1, 2, 3])

        msg = [1, 2, object()]
        buf = bytearray()
        with pytest.raises(TypeError):
            enc.encode_into(msg, buf)

        assert buf  # buffer isn't reset upon error

        # Encoder still works
        out2 = enc.encode([1, 2, 3])
        assert out1 == out2

    @pytest.mark.parametrize(
        "dt, dt_str",
        [
            (datetime.datetime(1, 2, 3, 4, 5, 6), "0001-02-03T04:05:06"),
            (
                datetime.datetime(1234, 12, 31, 14, 56, 27, 123456),
                "1234-12-31T14:56:27.123456",
            ),
        ],
    )
    def test_encode_datetime_naive(self, dt, dt_str):
        res = msgspec.msgpack.encode(dt)
        sol = msgspec.msgpack.encode(dt_str)
        assert res == sol

    def test_encode_datetime_non_utc_tzinfo(self):
        tzinfo = datetime.timezone(datetime.timedelta(hours=1))
        x = datetime.datetime.now(tzinfo)
        x2 = x.astimezone(datetime.timezone.utc)
        res = msgspec.msgpack.encode(x)
        sol = msgspec.msgpack.encode(x2)
        assert res == sol


class TestDecoderMisc:
    def test_decoder_type_attribute(self):
        dec = msgspec.msgpack.Decoder()
        assert dec.type is Any

        dec = msgspec.msgpack.Decoder(int)
        assert dec.type is int

    def test_decoder_ext_hook_attribute(self):
        def ext_hook(code, buf):
            pass

        dec = msgspec.msgpack.Decoder()
        assert dec.ext_hook is None

        dec = msgspec.msgpack.Decoder(ext_hook=None)
        assert dec.ext_hook is None

        dec = msgspec.msgpack.Decoder(ext_hook=ext_hook)
        assert dec.ext_hook is ext_hook

    def test_decoder_bad_args(self):
        with pytest.raises(TypeError):
            msgspec.msgpack.Decoder(bad=1)

    def test_decoder_ext_hook_not_callable(self):
        with pytest.raises(TypeError):
            msgspec.msgpack.Decoder(ext_hook=1)

    def test_decoder_repr(self):
        typ = List[Dict[int, float]]
        dec = msgspec.msgpack.Decoder(typ)
        assert repr(dec) == f"msgspec.msgpack.Decoder({typ!r})"

        dec = msgspec.msgpack.Decoder()
        assert repr(dec) == f"msgspec.msgpack.Decoder({Any!r})"

    def test_decode_with_trailing_characters_errors(self):
        dec = msgspec.msgpack.Decoder()

        msg = msgspec.msgpack.encode([1, 2, 3]) + b"trailing"

        with pytest.raises(msgspec.DecodeError):
            dec.decode(msg)

    def test_decode_invalid_opcode_error(self):
        with pytest.raises(msgspec.DecodeError, match="invalid opcode '\\\\xc1'"):
            msgspec.msgpack.decode(b"\xc1abc")

    def test_decode_skip_invalid_submessage_raises(self):
        """Ensure errors in submessage skipping are raised"""

        class Test(msgspec.Struct):
            x: int

        msg = msgspec.msgpack.encode({"x": 1, "y": ["one", "two", "three"]})

        # Break the message
        msg = msg.replace(b"three", b"tree")

        with pytest.raises(msgspec.DecodeError, match="truncated"):
            msgspec.msgpack.decode(msg, type=Test)

    @pytest.mark.parametrize("length", [3, 31, 33])
    @pytest.mark.parametrize("typed", [False, True])
    @pytest.mark.skipif(
        hasattr(sys.flags, "gil") and not sys.flags.gil,
        reason="cache is disabled without GIL",
    )
    def test_decode_dict_string_cache(self, length, typed):
        key = "x" * length
        msg = [{key: 1}, {key: 2}, {key: 3}]
        if typed:
            dec = msgspec.msgpack.Decoder(List[Dict[str, int]])
        else:
            dec = msgspec.msgpack.Decoder()
        res = dec.decode(msgspec.msgpack.encode(msg))
        assert msg == res
        ids = {id(k) for d in res for k in d.keys()}
        if length > 32:
            assert len(ids) == 3
        else:
            assert len(ids) == 1

    def test_decode_dict_string_cache_ascii_only(self):
        """Short non-ascii strings aren't cached"""
        s = "123 á 456"
        msg = [{s: 1}, {s: 2}, {s: 3}]
        res = msgspec.msgpack.decode(msgspec.msgpack.encode(msg))
        ids = {id(k) for d in res for k in d.keys()}
        assert len(ids) == 3

    @pytest.mark.parametrize("type", [None, list, tuple, set])
    def test_decoding_large_arrays_doesnt_preallocate(self, type):
        # <maximum sized array, truncated>
        b = b"\xdd\xff\xff\xff\xff"
        with pytest.raises(msgspec.DecodeError, match="truncated"):
            if type is None:
                msgspec.msgpack.decode(b)
            else:
                msgspec.msgpack.decode(b, type=type)

    def test_decoding_large_arrays_as_keys_doesnt_preallocate(self):
        # {"x": <maximum sized array, truncated>}
        b = b"\x81\xa1x\xdd\xff\xff\xff\xff"
        with pytest.raises(msgspec.DecodeError, match="truncated"):
            msgspec.msgpack.decode(b)


class TestTypedDecoder:
    def check_unexpected_type(self, dec_type, val, msg):
        dec = msgspec.msgpack.Decoder(dec_type)
        s = msgspec.msgpack.Encoder().encode(val)
        with pytest.raises(msgspec.ValidationError, match=msg):
            dec.decode(s)

    def test_any(self):
        dec = msgspec.msgpack.Decoder(Any)
        assert dec.decode(msgspec.msgpack.encode([1, 2, 3])) == [1, 2, 3]

        # A union that includes `Any` is just `Any`
        dec = msgspec.msgpack.Decoder(Union[Any, float, int, None])
        assert dec.decode(msgspec.msgpack.encode([1, 2, 3])) == [1, 2, 3]

    def test_none(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(None)
        assert dec.decode(enc.encode(None)) is None
        with pytest.raises(msgspec.ValidationError, match="Expected `null`"):
            assert dec.decode(enc.encode(1))

    @pytest.mark.parametrize("x", [False, True])
    def test_bool(self, x):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(bool)
        assert dec.decode(enc.encode(x)) is x

    def test_bool_unexpected_type(self):
        self.check_unexpected_type(bool, "a", "Expected `bool`")

    @pytest.mark.parametrize("x", INTS)
    def test_int(self, x):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(int)
        assert dec.decode(enc.encode(x)) == x

    def test_int_unexpected_type(self):
        self.check_unexpected_type(int, "a", "Expected `int`")

    @pytest.mark.parametrize("x", FLOATS + INTS)
    def test_float(self, x):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(float)
        res = dec.decode(enc.encode(x))
        sol = float(x)
        if math.isnan(sol):
            assert math.isnan(res)
        else:
            assert res == sol

    def test_float_unexpected_type(self):
        self.check_unexpected_type(float, "a", "Expected `float`")

    def test_decode_float4(self):
        x = 1.2
        packed = struct.pack(">f", x)
        # Loss of resolution in float32 leads to some rounding error
        x4 = struct.unpack(">f", packed)[0]
        msg = b"\xca" + packed
        assert msgspec.msgpack.decode(msg) == x4
        assert msgspec.msgpack.decode(msg, type=float) == x4

    @pytest.mark.parametrize("size", SIZES)
    def test_str(self, size):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(str)
        x = "a" * size
        res = dec.decode(enc.encode(x))
        assert res == x

    def test_str_unexpected_type(self):
        self.check_unexpected_type(str, 1, "Expected `str`")

    @pytest.mark.parametrize("size", SIZES)
    @pytest.mark.parametrize("typ", [bytes, bytearray, memoryview])
    def test_binary(self, size, typ):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(typ)
        sol = b"a" * size
        res = dec.decode(enc.encode(typ(sol)))
        assert isinstance(res, typ)
        assert bytes(res) == sol

    @pytest.mark.parametrize("typ", [bytes, bytearray, memoryview])
    def test_binary_unexpected_type(self, typ):
        self.check_unexpected_type(typ, 1, "Expected `bytes`")

    @pytest.mark.parametrize("input_type", [bytes, bytearray, memoryview])
    def test_decode_memoryview_zerocopy(self, input_type):
        msg = msgspec.msgpack.encode(b"abcde")
        ref = msg if input_type is memoryview else None
        msg = input_type(msg)
        res = msgspec.msgpack.decode(msg, type=memoryview)
        assert isinstance(res, memoryview)
        assert bytes(res) == b"abcde"
        assert len(res) == 5
        if input_type is memoryview:
            assert sys.getrefcount(ref) <= 3
            del msg
            assert sys.getrefcount(ref) <= 3
            del res
            assert sys.getrefcount(ref) <= 2
        elif input_type is bytes:
            assert sys.getrefcount(msg) <= 3

    def test_datetime_aware_ext(self):
        dec = msgspec.msgpack.Decoder(datetime.datetime)
        x = datetime.datetime.now(UTC)
        res = dec.decode(msgspec.msgpack.encode(x))
        assert x == res

    @pytest.mark.parametrize(
        "s",
        [
            "1234-01-02T03:04:05Z",
            "1234-01-02T03:04:05.123Z",
            "1234-01-02T03:04:05.123+00:00",
        ],
    )
    def test_decode_datetime_aware_str(self, s):
        sol = datetime.datetime.fromisoformat(s.replace("Z", "+00:00"))
        msg = msgspec.msgpack.encode(s)
        res = msgspec.msgpack.decode(msg, type=datetime.datetime)
        assert sol == res

    @pytest.mark.parametrize(
        "s",
        [
            "1234-01-02T03:04:05",
            "1234-01-02T03:04:05.123",
            "1234-01-02T03:04:05.123456",
        ],
    )
    def test_decode_datetime_naive(self, s):
        sol = datetime.datetime.fromisoformat(s)
        msg = msgspec.msgpack.encode(s)
        res = msgspec.msgpack.decode(msg, type=datetime.datetime)
        assert sol == res

    def test_datetime_unexpected_type(self):
        self.check_unexpected_type(datetime.datetime, 1, "Expected `datetime`")
        self.check_unexpected_type(
            datetime.datetime, msgspec.msgpack.Ext(1, b"test"), "Expected `datetime`"
        )

    def test_datetime_invalid(self):
        msg = msgspec.msgpack.encode(msgspec.msgpack.Ext(-1, b"\x01\x02\x03"))
        with pytest.raises(
            msgspec.ValidationError, match="Invalid MessagePack timestamp"
        ):
            msgspec.msgpack.decode(msg, type=datetime.datetime)

    @pytest.mark.parametrize("size", SIZES)
    def test_list_lengths(self, size):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(list)
        x = list(range(size))
        res = dec.decode(enc.encode(x))
        assert res == x

    @pytest.mark.parametrize("typ", [list, List, List[Any]])
    def test_list_any(self, typ):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(typ)
        x = [1, "two", b"three"]
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(msgspec.ValidationError, match="Expected `array`"):
            dec.decode(enc.encode(1))

    def test_list_typed(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(List[int])
        x = [1, 2, 3]
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(
            msgspec.ValidationError,
            match=r"Expected `int`, got `str` - at `\$\[2\]`",
        ):
            dec.decode(enc.encode([1, 2, "three"]))

    @pytest.mark.parametrize("size", SIZES)
    @pytest.mark.parametrize("typ", [set, frozenset])
    def test_set_lengths(self, size, typ):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(typ)
        x = typ(range(size))
        res = dec.decode(enc.encode(x))
        assert res == x
        assert isinstance(res, typ)

    @pytest.mark.parametrize(
        "typ", [set, Set, Set[Any], frozenset, FrozenSet, FrozenSet[Any]]
    )
    def test_set_any(self, typ):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(typ)
        real_type = getattr(typ, "__origin__", typ)
        x = {1, "two", b"three"}
        res = dec.decode(enc.encode(x))
        assert res == x
        assert type(res) is real_type
        with pytest.raises(msgspec.ValidationError, match="Expected `array`"):
            dec.decode(enc.encode(1))

    @pytest.mark.parametrize("typ", [Set, FrozenSet])
    def test_set_typed(self, typ):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(typ[int])
        x = {1, 2, 3}
        res = dec.decode(enc.encode(x))
        assert res == x
        assert type(res) is typ.__origin__
        with pytest.raises(
            msgspec.ValidationError,
            match=r"Expected `int`, got `str` - at `\$\[2\]`",
        ):
            dec.decode(enc.encode([1, 2, "three"]))

    @pytest.mark.parametrize("size", SIZES)
    def test_vartuple_lengths(self, size):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(tuple)
        x = tuple(f"x{i}x" for i in range(size))
        res = dec.decode(enc.encode(x))
        assert res == x
        if res:
            assert sys.getrefcount(res[0]) <= 3  # 1 tuple, 1 index, 1 func call

    @pytest.mark.parametrize("typ", [tuple, Tuple, Tuple[Any, ...]])
    def test_vartuple_any(self, typ):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(typ)
        x = (1, "two", b"three")
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(
            msgspec.ValidationError, match="Expected `array`, got `int`"
        ):
            dec.decode(enc.encode(1))

    def test_vartuple_typed(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Tuple[int, ...])
        x = (1, 2, 3)
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(
            msgspec.ValidationError,
            match=r"Expected `int`, got `str` - at `\$\[2\]`",
        ):
            dec.decode(enc.encode((1, 2, "three")))

    def test_fixtuple_any(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Tuple[Any, Any, Any])
        x = (1, "two", b"three")
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(
            msgspec.ValidationError, match="Expected `array`, got `int`"
        ):
            dec.decode(enc.encode(1))
        with pytest.raises(
            msgspec.ValidationError, match="Expected `array` of length 3, got 2"
        ):
            dec.decode(enc.encode((1, 2)))

    def test_fixtuple_typed(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Tuple[int, str, bytes])
        x = (1, "two", b"three")
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(msgspec.ValidationError, match="Expected `bytes`"):
            dec.decode(enc.encode((1, "two", "three")))
        with pytest.raises(
            msgspec.ValidationError, match="Expected `array` of length 3, got 2"
        ):
            dec.decode(enc.encode((1, 2)))

    @pytest.mark.parametrize("size", SIZES)
    def test_dict_lengths(self, size):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(dict)
        x = {i: i for i in range(size)}
        res = dec.decode(enc.encode(x))
        assert res == x

    @pytest.mark.parametrize("typ", [dict, Dict, Dict[Any, Any]])
    def test_dict_any_any(self, typ):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(typ)
        x = {1: "one", "two": 2, b"three": 3.0}
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(
            msgspec.ValidationError, match=r"Expected `object`, got `int`"
        ):
            dec.decode(enc.encode(1))

    def test_dict_any_val(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Dict[str, Any])
        x = {"a": 1, "b": "two", "c": b"three"}
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(
            msgspec.ValidationError,
            match=r"Expected `str`, got `int` - at `key` in `\$`",
        ):
            dec.decode(enc.encode({1: 2}))

    def test_dict_any_key(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Dict[Any, str])
        x = {1: "a", "two": "b", b"three": "c"}
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(
            msgspec.ValidationError, match=r"Expected `str`, got `int` - at `\$\[...\]`"
        ):
            dec.decode(enc.encode({1: 2}))

    def test_dict_str_subclass_key(self):
        class mystr(str):
            pass

        msg1 = msgspec.msgpack.encode({mystr("test"): 1})
        msg2 = msgspec.msgpack.encode({"test": 1})
        assert msg1 == msg2

    def test_dict_typed(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Dict[str, int])
        x = {"a": 1, "b": 2}
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(
            msgspec.ValidationError,
            match=r"Expected `str`, got `int` - at `key` in `\$`",
        ):
            dec.decode(enc.encode({1: 2}))
        with pytest.raises(
            msgspec.ValidationError, match=r"Expected `int`, got `str` - at `\$\[...\]`"
        ):
            dec.decode(enc.encode({"a": "two"}))

    def test_dict_typed_non_str_key(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Dict[int, int])
        x = {0: 1, 2: 3}
        res = dec.decode(enc.encode(x))
        assert res == x
        with pytest.raises(
            msgspec.ValidationError,
            match=r"Expected `int`, got `str` - at `key` in `\$`",
        ):
            dec.decode(enc.encode({"bad": 2}))

    def test_enum(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(FruitStr)

        a = enc.encode(FruitStr.APPLE)
        assert enc.encode("apple") == a
        assert dec.decode(a) == FruitStr.APPLE

        with pytest.raises(msgspec.DecodeError, match="truncated"):
            dec.decode(a[:-2])

        with pytest.raises(
            msgspec.ValidationError, match="Invalid enum value 'MISSING'"
        ):
            dec.decode(enc.encode("MISSING"))

        with pytest.raises(
            msgspec.ValidationError,
            match=r"Invalid enum value 'MISSING' - at `\$\[0\]`",
        ):
            msgspec.msgpack.decode(enc.encode(["MISSING"]), type=List[FruitStr])

        with pytest.raises(msgspec.ValidationError):
            dec.decode(enc.encode(1))

    def test_int_enum(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(FruitInt)

        a = enc.encode(FruitInt.APPLE)
        assert enc.encode(1) == a
        assert dec.decode(a) == FruitInt.APPLE

        with pytest.raises(msgspec.DecodeError, match="truncated"):
            dec.decode(a[:-2])

        with pytest.raises(msgspec.ValidationError, match="Invalid enum value 1000"):
            dec.decode(enc.encode(1000))

        with pytest.raises(
            msgspec.ValidationError, match=r"Invalid enum value 1000 - at `\$\[0\]`"
        ):
            msgspec.msgpack.decode(enc.encode([1000]), type=List[FruitInt])

        with pytest.raises(msgspec.ValidationError):
            dec.decode(enc.encode("INVALID"))

    def test_str_literal(self):
        literal = Literal["one", "two"]
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(literal)

        assert dec.decode(enc.encode("one")) == "one"

        with pytest.raises(
            msgspec.ValidationError, match="Invalid enum value 'MISSING'"
        ):
            dec.decode(enc.encode("MISSING"))

        with pytest.raises(
            msgspec.ValidationError,
            match=r"Invalid enum value 'MISSING' - at `\$\[0\]`",
        ):
            msgspec.msgpack.decode(enc.encode(["MISSING"]), type=List[literal])

    def test_int_literal(self):
        literal = Literal[1, 2, 3]
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(literal)

        assert dec.decode(enc.encode(1)) == 1

        with pytest.raises(msgspec.ValidationError, match="Invalid enum value 1000"):
            dec.decode(enc.encode(1000))

        with pytest.raises(
            msgspec.ValidationError, match=r"Invalid enum value 1000 - at `\$\[0\]`"
        ):
            msgspec.msgpack.decode(enc.encode([1000]), type=List[literal])

    @pytest.mark.parametrize(
        "typ, value",
        [
            (bool, False),
            (bool, True),
            (int, 1),
            (float, 2.5),
            (str, "a"),
            (bytes, b"a"),
            (bytearray, bytearray(b"a")),
            (FruitInt, FruitInt.APPLE),
            (FruitStr, FruitStr.APPLE),
            (Person, Person("harry", "potter", 13)),
            (list, [1]),
            (set, {1}),
            (tuple, (1, 2)),
            (Tuple[int, int], (1, 2)),
            (dict, {1: 2}),
            (datetime.datetime, datetime.datetime.now(UTC)),
        ],
    )
    def test_optional(self, typ, value):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Optional[typ])

        s = enc.encode(value)
        s2 = enc.encode(None)
        assert dec.decode(s) == value
        assert dec.decode(s2) is None

        dec = msgspec.msgpack.Decoder(typ)
        with pytest.raises(msgspec.ValidationError):
            dec.decode(s2)

    @pytest.mark.parametrize(
        "typ, value",
        [
            (List[Optional[int]], [1, None]),
            (Tuple[Optional[int], int], (None, 1)),
            (Set[Optional[int]], {1, None}),
            (FrozenSet[Optional[int]], frozenset({1, None})),
            (Dict[str, Optional[int]], {"a": 1, "b": None}),
            (Dict[Optional[str], int], {"a": 1, None: 2}),
        ],
    )
    def test_optional_nested(self, typ, value):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(typ)

        s = enc.encode(value)
        assert dec.decode(s) == value

    @pytest.mark.parametrize(
        "types, vals",
        [
            ([int, float], [1, 2.5]),
            (
                [float, msgspec.msgpack.Ext, int, str],
                [1.5, msgspec.msgpack.Ext(1, b"two"), 1, "two"],
            ),
            ([bool, None, float, str], [True, None, 1.5, "test"]),
        ],
    )
    def test_union(self, types, vals):
        dec = msgspec.msgpack.Decoder(List[Union[tuple(types)]])
        s = msgspec.msgpack.encode(vals)
        res = dec.decode(s)
        assert res == vals
        for t, v in zip(types, res):
            if t is not None:
                t = getattr(t, "__origin__", t)
                assert type(v) == t

    @pytest.mark.parametrize(
        "types, vals",
        [
            (
                [PersonArray, FruitInt, FruitStr, Dict[int, str]],
                [PERSON_AA, FruitInt.APPLE, FruitStr.BANANA, {1: "two"}],
            ),
            (
                [Person, FruitInt, FruitStr, Tuple[int, ...]],
                [PERSON, FruitInt.APPLE, FruitStr.BANANA, (1, 2, 3)],
            ),
            (
                [Person, FruitInt, FruitStr, List[int]],
                [PERSON, FruitInt.APPLE, FruitStr.BANANA, [1, 2, 3]],
            ),
            (
                [Person, FruitInt, FruitStr, Set[int]],
                [PERSON, FruitInt.APPLE, FruitStr.BANANA, {1, 2, 3}],
            ),
            (
                [Person, FruitInt, FruitStr, Tuple[int, str, float]],
                [PERSON, FruitInt.APPLE, FruitStr.BANANA, (1, "two", 3.5)],
            ),
            (
                [Dict[int, str], FruitInt, FruitStr, Tuple[int, ...]],
                [{1: "two"}, FruitInt.APPLE, FruitStr.BANANA, (1, 2, 3)],
            ),
            (
                [Dict[int, str], FruitInt, FruitStr, List[int]],
                [{1: "two"}, FruitInt.APPLE, FruitStr.BANANA, [1, 2, 3]],
            ),
            (
                [Dict[int, str], FruitInt, FruitStr, Set[int]],
                [{1: "two"}, FruitInt.APPLE, FruitStr.BANANA, {1, 2, 3}],
            ),
            (
                [Dict[int, str], FruitInt, FruitStr, Tuple[int, str, float]],
                [{1: "two"}, FruitInt.APPLE, FruitStr.BANANA, (1, "two", 3.5)],
            ),
        ],
    )
    def test_compound_type_unions(self, types, vals):
        typ_vals = list(zip(types, vals))

        for N in range(2, len(typ_vals)):
            for typ_vals_subset in itertools.combinations(typ_vals, N):
                types, vals = zip(*typ_vals_subset)
                vals = list(vals)
                dec = msgspec.msgpack.Decoder(List[Union[types]])
                s = msgspec.msgpack.encode(vals)
                res = dec.decode(s)
                assert res == vals
                for t, v in zip(types, res):
                    t = getattr(t, "__origin__", t)
                    assert type(v) == t

    def test_union_error(self):
        msg = msgspec.msgpack.encode(1)
        with pytest.raises(
            msgspec.ValidationError, match="Expected `bool | string`, got `int`"
        ):
            msgspec.msgpack.decode(msg, type=Union[bool, str])

    def test_decoding_error_no_struct_toplevel(self):
        b = msgspec.msgpack.Encoder().encode([{"a": 1}])
        dec = msgspec.msgpack.Decoder(List[Dict[str, str]])
        with pytest.raises(
            msgspec.ValidationError,
            match=r"Expected `str`, got `int` - at `\$\[0\]\[...\]`",
        ):
            dec.decode(b)


class TestExt:
    @pytest.mark.parametrize("data", [b"test", bytearray(b"test"), memoryview(b"test")])
    def test_init(self, data):
        x = msgspec.msgpack.Ext(1, data)
        assert x.code == 1
        assert x.data == data

    def test_init_bad_arguments(self):
        with pytest.raises(TypeError, match="Ext takes no keyword arguments"):
            msgspec.msgpack.Ext(code=1, data=b"two")

        with pytest.raises(TypeError, match="expected 2 arguments, got 3"):
            msgspec.msgpack.Ext(1, b"two", 3)

        with pytest.raises(TypeError, match="expected 2 arguments, got 0"):
            msgspec.msgpack.Ext()

    def test_compare(self):
        x = msgspec.msgpack.Ext(1, b"two")
        x2 = msgspec.msgpack.Ext(1, b"two")
        x3 = msgspec.msgpack.Ext(1, b"three")
        x4 = msgspec.msgpack.Ext(2, b"two")
        assert x == x2
        assert not (x != x2)
        assert x != x3
        assert not (x == x3)
        assert x != x4
        assert not (x == x4)

        assert not (x == 1)
        assert x != 1
        with pytest.raises(TypeError):
            x >= x2

    @pytest.mark.parametrize("code", [-128, -2, 0, 2, 127])
    def test_code_roundtrip(self, code):
        ext = msgspec.msgpack.Ext(code, b"")
        assert ext.code == code
        ext2 = msgspec.msgpack.decode(msgspec.msgpack.encode(ext))
        assert ext2.code == code

    @pytest.mark.parametrize("code", [-129, 128, 2**65])
    def test_code_out_of_range(self, code):
        with pytest.raises(ValueError):
            msgspec.msgpack.Ext(code, b"bad")

    def test_data_wrong_type(self):
        with pytest.raises(TypeError):
            msgspec.msgpack.Ext(1, 2)

    def test_code_wrong_type(self):
        with pytest.raises(TypeError):
            msgspec.msgpack.Ext(b"bad", b"bad")

    def test_immutable(self):
        x = msgspec.msgpack.Ext(1, b"two")
        with pytest.raises(AttributeError):
            x.code = 2

    def test_pickleable(self):
        x = msgspec.msgpack.Ext(1, b"two")
        x2 = pickle.loads(pickle.dumps(x))
        assert x2.code == 1
        assert x2.data == b"two"

    @pytest.mark.parametrize("size", sorted({0, 1, 2, 4, 8, 16, *SIZES}))
    def test_serialize_compatibility(self, size):
        msgpack = pytest.importorskip("msgpack")
        data = b"x" * size
        code = 5

        msgspec_bytes = msgspec.msgpack.encode(msgspec.msgpack.Ext(code, data))
        msgpack_bytes = msgpack.dumps(msgpack.ExtType(code, data))
        assert msgspec_bytes == msgpack_bytes

    @pytest.mark.parametrize("typ", [bytearray, memoryview])
    def test_serialize_other_types(self, typ):
        buf = b"test"
        a = msgspec.msgpack.encode(msgspec.msgpack.Ext(1, buf))
        b = msgspec.msgpack.encode(msgspec.msgpack.Ext(1, typ(buf)))
        assert a == b

    @pytest.mark.parametrize("size", sorted({0, 1, 2, 4, 8, 16, *SIZES}))
    def test_roundtrip(self, size):
        data = b"x" * size
        code = 5

        buf = msgspec.msgpack.encode(msgspec.msgpack.Ext(code, data))
        out = msgspec.msgpack.decode(buf)
        assert out.code == code
        assert out.data == data

    @pytest.mark.parametrize("size", sorted({0, 1, 2, 4, 8, 16, *SIZES}))
    def test_roundtrip_typed_decoder(self, size):
        dec = msgspec.msgpack.Decoder(msgspec.msgpack.Ext)

        ext = msgspec.msgpack.Ext(5, b"x" * size)
        buf = msgspec.msgpack.encode(ext)
        out = dec.decode(buf)
        assert out == ext

    def test_typed_decoder_skips_ext_hook(self):
        def ext_hook(code, data):
            assert False, "shouldn't ever get called"

        msg = [None, msgspec.msgpack.Ext(1, b"test")]
        dec = msgspec.msgpack.Decoder(List[Optional[msgspec.msgpack.Ext]])
        buf = msgspec.msgpack.encode(msg)
        out = dec.decode(buf)
        assert out == msg

    def test_ext_typed_decoder_error(self):
        dec = msgspec.msgpack.Decoder(msgspec.msgpack.Ext)
        with pytest.raises(msgspec.ValidationError, match="Expected `ext`, got `int`"):
            assert dec.decode(msgspec.msgpack.encode(1))

    @pytest.mark.parametrize("use_function", [True, False])
    def test_decoder_ext_hook(self, use_function):
        obj = {"x": range(10)}
        exp_buf = pickle.dumps(range(10))

        def enc_hook(x):
            return msgspec.msgpack.Ext(5, pickle.dumps(x))

        def ext_hook(code, buf):
            assert isinstance(buf, memoryview)
            assert bytes(buf) == exp_buf
            assert len(buf) == len(exp_buf)
            assert code == 5
            return pickle.loads(buf)

        msg = msgspec.msgpack.encode(obj, enc_hook=enc_hook)
        if use_function:
            out = msgspec.msgpack.decode(msg, ext_hook=ext_hook)
        else:
            dec = msgspec.msgpack.Decoder(ext_hook=ext_hook)
            out = dec.decode(msg)
        assert out == obj

    def test_decoder_ext_hook_bad_signature(self):
        msg = msgspec.msgpack.encode(
            range(5), enc_hook=lambda x: msgspec.msgpack.Ext(1, b"test")
        )
        with pytest.raises(TypeError):
            msgspec.msgpack.decode(msg, ext_hook=lambda: None)

    def test_decoder_ext_hook_raises(self):
        class CustomError(Exception):
            pass

        def ext_hook(code, buf):
            raise CustomError

        msg = msgspec.msgpack.encode(
            range(5), enc_hook=lambda x: msgspec.msgpack.Ext(1, b"test")
        )
        with pytest.raises(CustomError):
            msgspec.msgpack.decode(msg, ext_hook=ext_hook)


class TestTimestampExt:
    def check(self, dt, msg):
        assert msgspec.msgpack.encode(dt) == msg
        assert msgspec.msgpack.decode(msg) == dt

    def test_timestamp32_lower(self):
        dt = datetime.datetime.fromtimestamp(0, UTC)
        msg = b"\xd6\xff\x00\x00\x00\x00"
        self.check(dt, msg)

    def test_timestamp32_upper(self):
        dt = datetime.datetime.fromtimestamp(2**32 - 1, UTC)
        msg = b"\xd6\xff\xff\xff\xff\xff"
        self.check(dt, msg)

    def test_timestamp64_lower(self):
        dt = datetime.datetime.fromtimestamp(1e-6, UTC)
        msg = b"\xd7\xff\x00\x00\x0f\xa0\x00\x00\x00\x00"
        self.check(dt, msg)

    def test_timestamp64_upper(self):
        dt = datetime.datetime.fromtimestamp(2**34, UTC) - datetime.timedelta(
            microseconds=1
        )
        msg = b"\xd7\xff\xeek\x18c\xff\xff\xff\xff"
        self.check(dt, msg)

    def test_timestamp96_lower(self):
        dt = datetime.datetime.fromtimestamp(-1e-6, UTC)
        msg = b"\xc7\x0c\xff;\x9a\xc6\x18\xff\xff\xff\xff\xff\xff\xff\xff"
        self.check(dt, msg)

    def test_timestamp96_upper(self):
        dt = datetime.datetime.fromtimestamp(2**34, UTC)
        msg = b"\xc7\x0c\xff\x00\x00\x00\x00\x00\x00\x00\x04\x00\x00\x00\x00"
        self.check(dt, msg)

    @pytest.mark.parametrize(
        "msg, secs, micros",
        [
            (b"\xd7\xff\x00\x00\x07\xd0\x00\x00\x00\x00", 0, 1),
            (b"\xd7\xff\x00\x00\x07\xcc\x00\x00\x00\x00", 0, 0),
            (b"\xd7\xff\xeek 0\x00\x00\x00\x00", 1, 0),
            (b"\xd7\xff\xeek ,\x00\x00\x00\x00", 0, 999999),
        ],
    )
    def test_timestamp_rounds_nanos(self, msg, secs, micros):
        res = msgspec.msgpack.decode(msg)
        assert res.second == secs
        assert res.microsecond == micros


class CommonTypeTestBase:
    """Test msgspec untyped encode/decode"""

    def test_none(self):
        self.check(None)

    @pytest.mark.parametrize("x", [False, True])
    def test_bool(self, x):
        self.check(x)

    @pytest.mark.parametrize("x", INTS)
    def test_int(self, x):
        self.check(x)

    @pytest.mark.parametrize("x", FLOATS)
    def test_float(self, x):
        self.check(x)

    @pytest.mark.parametrize("size", SIZES)
    def test_str(self, size):
        self.check(" " * size)

    @pytest.mark.parametrize("size", SIZES)
    def test_bytes(self, size):
        self.check(b" " * size)

    @pytest.mark.parametrize("size", SIZES)
    def test_dict(self, size):
        self.check({str(i): i for i in range(size)})

    @pytest.mark.parametrize("size", SIZES)
    def test_list(self, size):
        self.check(list(range(size)))


class TestDecodeArrayTypeUsesTupleIfHashableRequired:
    def test_decode_tuple_dict_keys_as_tuples(self):
        orig = {(1, 2): [1, 2, [3, 4]], (1, (2, 3)): [4, 5, 6], tuple(range(32)): []}
        data = msgspec.msgpack.encode(orig)
        out = msgspec.msgpack.decode(data)
        assert orig == out

    @pytest.mark.parametrize(
        "typ",
        [
            Dict[Tuple[int, Tuple[int, int]], List[int]],
            Dict[Tuple[int, Tuple[int, ...]], Any],
            Dict[Tuple, List[int]],
            Dict[Tuple[Any, ...], Any],
            Dict[Tuple[Any, Any], Any],
        ],
    )
    def test_decode_dict_key_status_forwarded_through_typed_tuples(self, typ):
        orig = {(1, (2, 3)): [1, 2, 3]}
        data = msgspec.msgpack.encode(orig)
        out = msgspec.msgpack.Decoder(typ).decode(data)
        assert orig == out

    def test_decode_tuple_set_keys_as_tuples(self):
        orig = {(1, 2), (3, (4, 5)), 6}
        data = msgspec.msgpack.encode(orig)
        out = msgspec.msgpack.decode(data, type=set)
        assert orig == out

    def test_decode_hashable_struct_in_key(self):
        class Test(msgspec.Struct):
            data: List[int]

            def __hash__(self):
                return hash(tuple(self.data))

        orig = {(1, Test([1, 2])): [1, 2]}
        data = msgspec.msgpack.encode(orig)
        out = msgspec.msgpack.Decoder(Dict[Tuple[int, Test], List[int]]).decode(data)
        assert orig == out


class TestUntypedDecoder(CommonTypeTestBase):
    """Check the untyped deserializer works for common types"""

    def check(self, x):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder()
        assert_eq(dec.decode(enc.encode(x)), x)


class TestCompatibility(CommonTypeTestBase):
    """Test compatibility with the existing python msgpack library"""

    def check(self, x):
        msgpack = pytest.importorskip("msgpack")

        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder()

        assert_eq(dec.decode(msgpack.dumps(x)), x)
        assert_eq(msgpack.loads(enc.encode(x)), x)


class TestStruct:
    @pytest.mark.parametrize("tag", [False, "Test", 123])
    def test_encode_empty_struct(self, tag):
        class Test(msgspec.Struct, tag=tag):
            pass

        if tag:
            msg = {"type": tag}
        else:
            msg = {}
        s = msgspec.msgpack.encode(Test())
        s2 = msgspec.msgpack.encode(msg)
        assert s == s2

    @pytest.mark.parametrize("tag", [False, "Test", 123])
    def test_encode_one_field_struct(self, tag):
        class Test(msgspec.Struct, tag=tag):
            a: int

        if tag:
            msg = {"type": tag, "a": 1}
        else:
            msg = {"a": 1}
        s = msgspec.msgpack.encode(Test(a=1))
        s2 = msgspec.msgpack.encode(msg)
        assert s == s2

    @pytest.mark.parametrize("tag", [False, "Test", 123])
    def test_encode_two_field_struct(self, tag):
        class Test(msgspec.Struct, tag=tag):
            a: int
            b: str

        if tag:
            msg = {"type": tag, "a": 1, "b": "two"}
        else:
            msg = {"a": 1, "b": "two"}
        s = msgspec.msgpack.encode(Test(a=1, b="two"))
        s2 = msgspec.msgpack.encode(msg)
        assert s == s2

    def test_decode_struct(self):
        dec = msgspec.msgpack.Decoder(Person)
        msg = msgspec.msgpack.encode(
            {"first": "harry", "last": "potter", "age": 13, "prefect": False}
        )
        x = dec.decode(msg)
        assert x == Person("harry", "potter", 13, False)

        with pytest.raises(
            msgspec.ValidationError, match="Expected `object`, got `int`"
        ):
            dec.decode(msgspec.msgpack.encode(1))

    def test_decode_struct_field_wrong_type(self):
        dec = msgspec.msgpack.Decoder(Person)

        msg = msgspec.msgpack.encode({"first": "harry", "last": "potter", "age": "bad"})
        with pytest.raises(
            msgspec.ValidationError, match=r"Expected `int`, got `str` - at `\$.age`"
        ):
            dec.decode(msg)

    def test_decode_struct_missing_fields(self):
        bad = msgspec.msgpack.encode({"first": "harry", "last": "potter"})
        with pytest.raises(
            msgspec.ValidationError, match="Object missing required field `age`"
        ):
            msgspec.msgpack.decode(bad, type=Person)

        bad = msgspec.msgpack.encode({})
        with pytest.raises(
            msgspec.ValidationError, match="Object missing required field `first`"
        ):
            msgspec.msgpack.decode(bad, type=Person)

        bad = msgspec.msgpack.encode([{"first": "harry", "last": "potter"}])
        with pytest.raises(
            msgspec.ValidationError,
            match=r"Object missing required field `age` - at `\$\[0\]`",
        ):
            msgspec.msgpack.decode(bad, type=List[Person])

    @pytest.mark.parametrize(
        "extra",
        [
            None,
            False,
            True,
            1,
            2.0,
            "three",
            b"four",
            [1, 2],
            {3: 4},
            msgspec.msgpack.Ext(1, b"12345"),
            msgspec.msgpack.Ext(1, b""),
        ],
    )
    def test_decode_struct_ignore_extra_fields(self, extra):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Person)

        a = enc.encode(
            {
                "extra1": extra,
                "first": "harry",
                "extra2": extra,
                "last": "potter",
                "age": 13,
                "extra3": extra,
            }
        )
        res = dec.decode(a)
        assert res == Person("harry", "potter", 13)

    def test_decode_struct_defaults_missing_fields(self):
        dec = msgspec.msgpack.Decoder(Person)

        a = msgspec.msgpack.encode({"first": "harry", "last": "potter", "age": 13})
        res = dec.decode(a)
        assert res == Person("harry", "potter", 13)
        assert res.prefect is False

    @pytest.mark.parametrize("array_like", [False, True])
    def test_struct_gc_maybe_untracked_on_decode(self, array_like):
        class Test(msgspec.Struct, array_like=array_like):
            x: Any
            y: Any
            z: Tuple = ()

        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(List[Test])

        ts = [
            Test(1, 2),
            Test(3, "hello"),
            Test([], []),
            Test({}, {}),
            Test(None, None, ()),
        ]
        a, b, c, d, e = dec.decode(enc.encode(ts))
        assert not gc.is_tracked(a)
        assert not gc.is_tracked(b)
        assert gc.is_tracked(c)
        assert gc.is_tracked(d)
        assert not gc.is_tracked(e)

    @pytest.mark.parametrize("array_like", [False, True])
    def test_struct_gc_false_always_untracked_on_decode(self, array_like):
        class Test(msgspec.Struct, array_like=array_like, gc=False):
            x: Any
            y: Any

        dec = msgspec.msgpack.Decoder(List[Test])

        ts = [
            Test(1, 2),
            Test([], []),
            Test({}, {}),
        ]
        for obj in dec.decode(msgspec.msgpack.encode(ts)):
            assert not gc.is_tracked(obj)

    def test_struct_recursive_definition(self):
        enc = msgspec.msgpack.Encoder()
        dec = msgspec.msgpack.Decoder(Node)

        x = Node(Node(Node(), Node(Node())))
        s = enc.encode(x)
        res = dec.decode(s)
        assert res == x

    @pytest.mark.parametrize("tag", ["Test", 123, -123])
    def test_decode_tagged_struct(self, tag):
        class Test(msgspec.Struct, tag=tag):
            a: int
            b: int

        dec = msgspec.msgpack.Decoder(Test)

        # Test decode with and without tag
        for msg in [
            {"a": 1, "b": 2},
            {"type": tag, "a": 1, "b": 2},
            {"a": 1, "type": tag, "b": 2},
        ]:
            res = dec.decode(msgspec.msgpack.encode(msg))
            assert res == Test(1, 2)

        # Tag incorrect type
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msgspec.msgpack.encode({"type": 123.456}))
        assert f"Expected `{type(tag).__name__}`" in str(rec.value)
        assert "`$.type`" in str(rec.value)

        # Tag incorrect value
        bad = -3 if isinstance(tag, int) else "bad"
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msgspec.msgpack.encode({"type": bad}))
        assert f"Invalid value {bad!r}" in str(rec.value)
        assert "`$.type`" in str(rec.value)

    @pytest.mark.parametrize("tag", [i for i in INTS if -(2**63) <= i < 2**63])
    def test_decode_tagged_struct_int_ranges(self, tag):
        class Test(msgspec.Struct, tag=tag):
            a: int
            b: int

        dec = msgspec.msgpack.Decoder(Test)
        t = Test(1, 2)
        assert dec.decode(msgspec.msgpack.encode(t))

    def test_decode_tagged_struct_int_tag_uint64_always_invalid(self):
        """Uint64 values aren't currently valid tag values, but we still want
        to raise a good error message."""

        class Test(msgspec.Struct, tag=123):
            pass

        with pytest.raises(msgspec.ValidationError) as rec:
            msgspec.msgpack.decode(
                msgspec.msgpack.encode({"type": 2**64 - 1}), type=Test
            )
        assert f"Invalid value {2**64 - 1}" in str(rec.value)
        assert "`$.type`" in str(rec.value)

    @pytest.mark.parametrize("tag", ["Test", 123, -123])
    def test_decode_tagged_empty_struct(self, tag):
        class Test(msgspec.Struct, tag=tag):
            pass

        dec = msgspec.msgpack.Decoder(Test)

        # Tag missing
        res = dec.decode(msgspec.msgpack.encode({}))
        assert res == Test()

        # Tag present
        res = dec.decode(msgspec.msgpack.encode({"type": tag}))
        assert res == Test()


class TestStructArray:
    @pytest.mark.parametrize("tag", [False, "Test", 123])
    def test_encode_empty_struct(self, tag):
        class Test(msgspec.Struct, array_like=True, tag=tag):
            pass

        s = msgspec.msgpack.encode(Test())
        if tag:
            msg = [tag]
        else:
            msg = []
        s2 = msgspec.msgpack.encode(msg)
        assert s == s2

    @pytest.mark.parametrize("tag", [False, "Test", 123])
    def test_encode_one_field_struct(self, tag):
        class Test(msgspec.Struct, array_like=True, tag=tag):
            a: int

        s = msgspec.msgpack.encode(Test(a=1))
        if tag:
            msg = [tag, 1]
        else:
            msg = [1]
        s2 = msgspec.msgpack.encode(msg)
        assert s == s2

    @pytest.mark.parametrize("tag", [False, "Test", 123])
    def test_encode_two_field_struct(self, tag):
        class Test(msgspec.Struct, array_like=True, tag=tag):
            a: int
            b: str

        s = msgspec.msgpack.encode(Test(a=1, b="two"))
        if tag:
            msg = [tag, 1, "two"]
        else:
            msg = [1, "two"]
        s2 = msgspec.msgpack.encode(msg)
        assert s == s2

    def test_struct_array_like(self):
        dec = msgspec.msgpack.Decoder(PersonArray)

        x = PersonArray(first="harry", last="potter", age=13)
        a = msgspec.msgpack.encode(x)
        assert msgspec.msgpack.encode(("harry", "potter", 13, False)) == a
        assert dec.decode(a) == x

        with pytest.raises(
            msgspec.ValidationError, match="Expected `array`, got `int`"
        ):
            dec.decode(b"1")

        # Wrong field type
        bad = msgspec.msgpack.encode(("harry", "potter", "thirteen"))
        with pytest.raises(
            msgspec.ValidationError, match=r"Expected `int`, got `str` - at `\$\[2\]`"
        ):
            dec.decode(bad)

        # Missing fields
        bad = msgspec.msgpack.encode(("harry", "potter"))
        with pytest.raises(
            msgspec.ValidationError,
            match="Expected `array` of at least length 3, got 2",
        ):
            dec.decode(bad)

        bad = msgspec.msgpack.encode(())
        with pytest.raises(
            msgspec.ValidationError,
            match="Expected `array` of at least length 3, got 0",
        ):
            dec.decode(bad)

        # Extra fields ignored
        dec2 = msgspec.msgpack.Decoder(List[PersonArray])
        msg = msgspec.msgpack.encode(
            [
                ("harry", "potter", 13, False, 1, 2, 3, 4),
                ("ron", "weasley", 13, False, 5, 6),
            ]
        )
        res = dec2.decode(msg)
        assert res == [
            PersonArray("harry", "potter", 13),
            PersonArray("ron", "weasley", 13),
        ]

        # Defaults applied
        res = dec.decode(msgspec.msgpack.encode(("harry", "potter", 13)))
        assert res == PersonArray("harry", "potter", 13)
        assert res.prefect is False

    def test_struct_map_and_array_like_messages_cant_mix(self):
        array_msg = msgspec.msgpack.encode(("harry", "potter", 13))
        map_msg = msgspec.msgpack.encode(
            {"first": "harry", "last": "potter", "age": 13}
        )
        sol = Person("harry", "potter", 13)
        array_sol = PersonArray("harry", "potter", 13)

        dec = msgspec.msgpack.Decoder(Person)
        array_dec = msgspec.msgpack.Decoder(PersonArray)

        assert array_dec.decode(array_msg) == array_sol
        assert dec.decode(map_msg) == sol
        with pytest.raises(
            msgspec.ValidationError, match="Expected `object`, got `array`"
        ):
            dec.decode(array_msg)
        with pytest.raises(
            msgspec.ValidationError, match="Expected `array`, got `object`"
        ):
            array_dec.decode(map_msg)

    @pytest.mark.parametrize("tag", ["Test", -123, 123])
    def test_decode_tagged_struct(self, tag):
        class Test(msgspec.Struct, tag=tag, array_like=True):
            a: int
            b: int
            c: int = 0

        dec = msgspec.msgpack.Decoder(Test)

        # Decode with tag
        res = dec.decode(msgspec.msgpack.encode([tag, 1, 2]))
        assert res == Test(1, 2)
        res = dec.decode(msgspec.msgpack.encode([tag, 1, 2, 3]))
        assert res == Test(1, 2, 3)

        # Trailing fields ignored
        res = dec.decode(msgspec.msgpack.encode([tag, 1, 2, 3, 4]))
        assert res == Test(1, 2, 3)

        # Missing required field errors
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msgspec.msgpack.encode([tag, 1]))
        assert "Expected `array` of at least length 3, got 2" in str(rec.value)

        # Tag missing
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msgspec.msgpack.encode([]))
        assert "Expected `array` of at least length 3, got 0" in str(rec.value)

        # Tag incorrect type
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msgspec.msgpack.encode([123.456, 2, 3]))
        assert f"Expected `{type(tag).__name__}`" in str(rec.value)
        assert "`$[0]`" in str(rec.value)

        # Tag incorrect value
        bad = -3 if isinstance(tag, int) else "bad"
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msgspec.msgpack.encode([bad, 1, 2]))
        assert f"Invalid value {bad!r}" in str(rec.value)
        assert "`$[0]`" in str(rec.value)

        # Field incorrect type correct index
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msgspec.msgpack.encode([tag, "a", 2]))
        assert "Expected `int`, got `str`" in str(rec.value)
        assert "`$[1]`" in str(rec.value)

    @pytest.mark.parametrize("tag", ["Test", 123, -123])
    def test_decode_tagged_empty_struct(self, tag):
        class Test(msgspec.Struct, tag=tag, array_like=True):
            pass

        dec = msgspec.msgpack.Decoder(Test)

        # Decode with tag
        res = dec.decode(msgspec.msgpack.encode([tag, 1, 2]))
        assert res == Test()

        # Tag missing
        with pytest.raises(msgspec.ValidationError) as rec:
            dec.decode(msgspec.msgpack.encode([]))
        assert "Expected `array` of at least length 1, got 0" in str(rec.value)


class TestRaw:
    def test_encode_raw(self):
        b = msgspec.msgpack.encode({"x": 1})
        r = msgspec.Raw(b)
        assert msgspec.msgpack.encode(r) == b
        assert msgspec.msgpack.encode({"y": r}) == msgspec.msgpack.encode(
            {"y": {"x": 1}}
        )

    def test_decode_raw_field(self):
        class Test(msgspec.Struct):
            x: int
            y: msgspec.Raw

        s = msgspec.msgpack.encode({"x": 1, "y": [1, 2, 3]})
        res = msgspec.msgpack.decode(s, type=Test)
        assert res.x == 1
        assert bytes(res.y) == msgspec.msgpack.encode([1, 2, 3])

    def test_decode_raw_optional_field(self):
        default = msgspec.Raw()

        class Test(msgspec.Struct):
            x: int
            y: msgspec.Raw = default

        s = msgspec.msgpack.encode({"x": 1, "y": [1, 2, 3]})
        res = msgspec.msgpack.decode(s, type=Test)
        assert res.x == 1
        assert bytes(res.y) == msgspec.msgpack.encode([1, 2, 3])

        s = msgspec.msgpack.encode({"x": 1})
        res = msgspec.msgpack.decode(s, type=Test)
        assert res.x == 1
        assert res.y is default

    def test_decode_raw_malformed_data(self):
        class Test(msgspec.Struct):
            x: int
            y: msgspec.Raw

        s = msgspec.msgpack.encode({"x": 1, "y": [1, 2]})[:3]
        with pytest.raises(msgspec.DecodeError):
            msgspec.msgpack.decode(s, type=Test)

    def test_decode_raw_is_view(self):
        s = msgspec.msgpack.encode({"x": 1})
        r = msgspec.msgpack.decode(s, type=msgspec.Raw)
        assert bytes(r) == s
        assert r.copy() is not r  # actual copy indicates a view

    def test_raw_in_union_works_but_doesnt_change_anything(self):
        class Test(msgspec.Struct):
            x: Union[int, str, msgspec.Raw]

        s = msgspec.msgpack.encode({"x": 1})
        r = msgspec.msgpack.decode(s, type=Test)
        assert r == Test(1)

    def test_raw_can_be_mixed_with_custom_type(self):
        class Test(msgspec.Struct):
            x: Union[Custom, msgspec.Raw]

        def dec_hook(typ, obj):
            assert typ is Custom
            return typ(*obj)

        s = msgspec.msgpack.encode({"x": [1, 2]})
        res = msgspec.msgpack.decode(s, type=Test, dec_hook=dec_hook)
        assert res == Test(Custom(1, 2))
