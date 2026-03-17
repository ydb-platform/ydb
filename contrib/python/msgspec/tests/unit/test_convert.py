import datetime
import decimal
import enum
import gc
import math
import sys
import uuid
from base64 import b64encode
from collections.abc import MutableMapping
from dataclasses import dataclass, field
from typing import (
    Annotated,
    Any,
    Dict,
    FrozenSet,
    Generic,
    List,
    Literal,
    NamedTuple,
    Set,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
)

import pytest

import msgspec
from msgspec import Meta, Struct, ValidationError, convert, to_builtins

from .utils import max_call_depth, temp_module

try:
    import attrs
except ImportError:
    attrs = None

PY310 = sys.version_info[:2] >= (3, 10)
PY311 = sys.version_info[:2] >= (3, 11)
PY312 = sys.version_info[:2] >= (3, 12)

UTC = datetime.timezone.utc

T = TypeVar("T")


class GetAttrObj:
    def __init__(self, _data=None, **kwargs):
        self._data = _data or {}
        self._data.update(kwargs)

    def __getattr__(self, key):
        try:
            return self._data[key]
        except KeyError:
            raise AttributeError(key) from None


class GetItemObj(MutableMapping):
    def __init__(self, _data=None, **kwargs):
        self._data = _data or {}
        self._data.update(kwargs)

    def __getitem__(self, key):
        return self._data[key]

    def __setitem__(self, key, val):
        self._data[key] = val

    def __delitem__(self, key):
        del self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)


class GetAttrOrItemObj:
    def __init__(self, _data=None, **kwargs):
        self._attrs = _data or {}
        self._attrs.update(kwargs)
        self._items = {}
        if self._attrs:
            k, v = self._attrs.popitem()
            self._items[k] = v

    def __getitem__(self, key):
        return self._items[key]

    def __getattr__(self, key):
        try:
            return self._attrs[key]
        except KeyError:
            raise AttributeError(key) from None


def KWList(**kwargs):
    return list(kwargs.values())


class SubList(list):
    pass


class SubTuple(tuple):
    pass


class SubSet(set):
    pass


class SubFrozenSet(frozenset):
    pass


class SubDict(dict):
    pass


mapcls_and_from_attributes = pytest.mark.parametrize(
    "mapcls, from_attributes",
    [
        (dict, False),
        (SubDict, False),
        (GetAttrObj, True),
        (GetAttrOrItemObj, True),
        (GetItemObj, False),
    ],
)

mapcls_from_attributes_and_array_like = pytest.mark.parametrize(
    "mapcls, from_attributes, array_like",
    [
        (dict, False, False),
        (SubDict, False, False),
        (KWList, False, True),
        (GetAttrObj, True, True),
        (GetAttrObj, True, False),
        (GetAttrOrItemObj, True, True),
        (GetAttrOrItemObj, True, False),
        (GetAttrOrItemObj, False, False),
        (GetItemObj, False, False),
    ],
)

seq_in_type = pytest.mark.parametrize(
    "in_type",
    [
        list,
        tuple,
        set,
        frozenset,
        SubList,
        SubTuple,
        SubSet,
        SubFrozenSet,
    ],
)


@pytest.fixture(params=["dict", "subclass", "mapping"])
def dictcls(request):
    if request.param == "dict":
        return dict
    elif request.param == "subclass":
        return SubDict
    else:
        return GetItemObj


def assert_eq(x, y):
    assert type(x) is type(y)
    if type(x) is float and math.isnan(x):
        return math.isnan(y)
    assert x == y


def roundtrip(obj, typ):
    return convert(to_builtins(obj), typ)


class TestConvert:
    def test_bad_calls(self):
        with pytest.raises(TypeError):
            convert()

        with pytest.raises(TypeError):
            convert(1)

        with pytest.raises(
            TypeError, match="builtin_types must be an iterable of types"
        ):
            convert(1, int, builtin_types=1)

        with pytest.raises(TypeError) as rec:
            convert(1, int, builtin_types=(int,))
        assert "Cannot treat" in str(rec.value)
        assert "int" in str(rec.value)

        with pytest.raises(TypeError, match="dec_hook must be callable"):
            convert(1, int, dec_hook=1)

    def test_dec_hook_explicit_none(self):
        assert convert(1, int, dec_hook=None) == 1

    def test_custom_input_type(self):
        class Custom:
            pass

        with pytest.raises(ValidationError, match="Expected `int`, got `Custom`"):
            convert(Custom(), int)

    def test_custom_input_type_works_with_any(self):
        class Custom:
            pass

        x = Custom()
        res = convert(x, Any)
        assert res is x
        assert sys.getrefcount(x) <= 3  # x + res + 1

    def test_custom_input_type_works_with_custom(self):
        class Custom:
            pass

        x = Custom()
        res = convert(x, Custom)
        assert res is x
        assert sys.getrefcount(x) <= 3  # x + res + 1

    def test_custom_input_type_works_with_dec_hook(self):
        class Custom:
            pass

        class Custom2:
            pass

        def dec_hook(typ, x):
            if typ is Custom2:
                assert isinstance(x, Custom)
                return Custom2()
            raise TypeError

        x = Custom()
        res = convert(x, Custom2, dec_hook=dec_hook)
        assert isinstance(res, Custom2)
        assert sys.getrefcount(res) <= 2  # res + 1
        assert sys.getrefcount(x) <= 2  # x + 1

    def test_unsupported_output_type(self):
        with pytest.raises(TypeError, match="more than one array-like"):
            convert({}, Union[List[int], Tuple[str, ...]])

    @pytest.mark.parametrize(
        "val, got",
        [
            (None, "null"),
            (True, "bool"),
            (1, "int"),
            (1.5, "float"),
            ("a", "str"),
            (b"b", "bytes"),
            (bytearray(b"c"), "bytes"),
            (datetime.datetime(2022, 1, 2), "datetime"),
            (datetime.time(12, 34), "time"),
            (datetime.date(2022, 1, 2), "date"),
            (uuid.uuid4(), "uuid"),
            (decimal.Decimal("1.5"), "decimal"),
            ([1], "array"),
            ((1,), "array"),
            ({"a": 1}, "object"),
        ],
    )
    def test_wrong_type(self, val, got):
        # An arbitrary wrong type,
        if isinstance(val, int):
            typ = str
            expected = "str"
        else:
            typ = int
            expected = "int"
        msg = f"Expected `{expected}`, got `{got}`"
        with pytest.raises(ValidationError, match=msg):
            convert(val, typ)


class TestAny:
    @pytest.mark.parametrize("msg", [(1, 2), {"a": 1}, object(), {1, 2}])
    def test_any_passthrough(self, msg):
        assert convert(msg, Any) is msg


class TestNone:
    def test_none(self):
        assert convert(None, Any) is None
        assert convert(None, None) is None
        with pytest.raises(ValidationError, match="Expected `null`, got `int`"):
            convert(1, None)


class TestBool:
    @pytest.mark.parametrize("val", [True, False])
    def test_bool(self, val):
        assert convert(val, Any) is val
        assert convert(val, bool) is val

    def test_bool_invalid(self):
        with pytest.raises(ValidationError, match="Expected `bool`, got `int`"):
            convert(1, bool)

        with pytest.raises(ValidationError, match="Expected `bool`, got `str`"):
            convert("true", bool)


class TestInt:
    def test_int(self):
        assert convert(1, Any) == 1
        assert convert(1, int) == 1
        with pytest.raises(ValidationError, match="Expected `int`, got `float`"):
            convert(1.5, int)

    @pytest.mark.parametrize("val", [2**64, -(2**63) - 1])
    def test_convert_big_ints(self, val):
        class myint(int):
            pass

        assert_eq(convert(val, int), val)
        assert_eq(convert(myint(val), int), val)

    @pytest.mark.parametrize(
        "name, bound, good, bad",
        [
            ("ge", -1, [-1, 2**63, 2**65], [-(2**64), -2]),
            ("gt", -1, [0, 2**63, 2**65], [-(2**64), -1]),
            ("le", -1, [-(2**64), -1], [0, 2**63, 2**65]),
            ("lt", -1, [-(2**64), -2], [-1, 2**63, 2**65]),
        ],
    )
    def test_int_constr_bounds(self, name, bound, good, bad):
        class Ex(Struct):
            x: Annotated[int, Meta(**{name: bound})]

        for x in good:
            assert convert({"x": x}, Ex).x == x

        op = ">=" if name.startswith("g") else "<="
        offset = {"lt": -1, "gt": 1}.get(name, 0)
        err_msg = rf"Expected `int` {op} {bound + offset} - at `\$.x`"
        for x in bad:
            with pytest.raises(ValidationError, match=err_msg):
                convert({"x": x}, Ex)

    def test_int_constr_multiple_of(self):
        class Ex(Struct):
            x: Annotated[int, Meta(multiple_of=2)]

        for x in [-(2**64), -2, 0, 2, 40, 2**63 + 2, 2**65]:
            assert convert({"x": x}, Ex).x == x

        err_msg = r"Expected `int` that's a multiple of 2 - at `\$.x`"
        for x in [1, -(2**64) + 1, -1, 2**63 + 1, 2**65 + 1]:
            with pytest.raises(ValidationError, match=err_msg):
                convert({"x": x}, Ex)

    @pytest.mark.parametrize(
        "meta, good, bad",
        [
            (Meta(ge=0, le=10, multiple_of=2), [0, 2, 10], [-1, 1, 11]),
            (Meta(ge=0, multiple_of=2), [0, 2**63 + 2], [-2, 2**63 + 1]),
            (Meta(le=0, multiple_of=2), [0, -(2**63)], [-1, 2, 2**63]),
            (Meta(ge=0, le=10), [0, 10], [-1, 11]),
        ],
    )
    def test_int_constrs(self, meta, good, bad):
        class Ex(Struct):
            x: Annotated[int, meta]

        for x in good:
            assert convert({"x": x}, Ex).x == x

        for x in bad:
            with pytest.raises(ValidationError):
                convert({"x": x}, Ex)

    def test_int_subclass(self):
        class MyInt(int):
            pass

        for val in [10, 0, -10]:
            sol = convert(MyInt(val), int)
            assert type(sol) is int
            assert sol == val

        x = MyInt(100)
        sol = convert(x, MyInt)
        assert sol is x
        assert sys.getrefcount(x) <= 3  # x + sol + 1


class TestFloat:
    def test_float(self):
        assert convert(1.5, Any) == 1.5
        assert convert(1.5, float) == 1.5
        res = convert(1, float)
        assert res == 1.0
        assert isinstance(res, float)
        with pytest.raises(ValidationError, match="Expected `float`, got `null`"):
            convert(None, float)

    @pytest.mark.parametrize(
        "meta, good, bad",
        [
            (Meta(ge=0.0, le=10.0, multiple_of=2.0), [0, 2.0, 10], [-2, 11, 3]),
            (Meta(ge=0.0, multiple_of=2.0), [0, 2, 10.0], [-2, 3]),
            (Meta(le=10.0, multiple_of=2.0), [-2.0, 10.0], [11.0, 3.0]),
            (Meta(ge=0.0, le=10.0), [0.0, 2.0, 10.0], [-1.0, 11.5, 11]),
        ],
    )
    def test_float_constrs(self, meta, good, bad):
        class Ex(Struct):
            x: Annotated[float, meta]

        for x in good:
            assert convert({"x": x}, Ex).x == x

        for x in bad:
            with pytest.raises(ValidationError):
                convert({"x": x}, Ex)

    def test_float_from_decimal(self):
        res = convert(decimal.Decimal("1.5"), float)
        assert res == 1.5
        assert type(res) is float

    def test_constr_float_from_decimal(self):
        typ = Annotated[float, Meta(ge=0)]
        res = convert(decimal.Decimal("1.5"), typ)
        assert res == 1.5
        assert type(res) is float

        with pytest.raises(ValidationError, match="Expected `float` >= 0.0"):
            convert(decimal.Decimal("-1.5"), typ)


class TestStr:
    def test_str(self):
        assert convert("test", Any) == "test"
        assert convert("test", str) == "test"
        with pytest.raises(ValidationError, match="Expected `str`, got `bytes`"):
            convert(b"test", str)

    @pytest.mark.parametrize(
        "meta, good, bad",
        [
            (
                Meta(min_length=2, max_length=3, pattern="x"),
                ["xy", "xyz"],
                ["x", "yy", "wxyz"],
            ),
            (Meta(min_length=2, max_length=4), ["xx", "xxxx"], ["x", "xxxxx"]),
            (Meta(min_length=2, pattern="x"), ["xy", "wxyz"], ["x", "bad"]),
            (Meta(max_length=3, pattern="x"), ["xy", "xyz"], ["y", "wxyz"]),
        ],
    )
    def test_str_constrs(self, meta, good, bad):
        class Ex(Struct):
            x: Annotated[str, meta]

        for x in good:
            assert convert({"x": x}, Ex).x == x

        for x in bad:
            with pytest.raises(ValidationError):
                convert({"x": x}, Ex)


class TestBinary:
    @pytest.mark.parametrize("out_type", [bytes, bytearray, memoryview])
    def test_binary_wrong_type(self, out_type):
        with pytest.raises(ValidationError, match="Expected `bytes`, got `int`"):
            convert(1, out_type)

    @pytest.mark.parametrize("in_type", [bytes, bytearray, memoryview])
    @pytest.mark.parametrize("out_type", [bytes, bytearray, memoryview])
    def test_binary_builtin(self, in_type, out_type):
        res = convert(in_type(b"test"), out_type)
        assert res == b"test"
        assert isinstance(res, out_type)

    @pytest.mark.parametrize("out_type", [bytes, bytearray, memoryview])
    def test_binary_base64(self, out_type):
        res = convert("AQI=", out_type)
        assert res == b"\x01\x02"
        assert isinstance(res, out_type)

    @pytest.mark.parametrize("out_type", [bytes, bytearray, memoryview])
    def test_binary_base64_disabled(self, out_type):
        with pytest.raises(ValidationError, match="Expected `bytes`, got `str`"):
            convert("AQI=", out_type, builtin_types=(bytes, bytearray, memoryview))

    @pytest.mark.parametrize("in_type", [bytes, bytearray, memoryview, str])
    @pytest.mark.parametrize("out_type", [bytes, bytearray, memoryview])
    def test_binary_constraints(self, in_type, out_type):
        class Ex(Struct):
            x: Annotated[out_type, Meta(min_length=2, max_length=4)]

        for x in [b"xx", b"xxx", b"xxxx"]:
            if in_type is str:
                msg = {"x": b64encode(x).decode("utf-8")}
            else:
                msg = {"x": in_type(x)}
            assert convert(msg, Ex).x == x

        for x in [b"x", b"xxxxx"]:
            if in_type is str:
                msg = {"x": b64encode(x).decode("utf-8")}
            else:
                msg = {"x": in_type(x)}
            with pytest.raises(ValidationError):
                convert(msg, Ex)

    def test_bytes_subclass(self):
        class MyBytes(bytes):
            pass

        msg = MyBytes(b"abc")

        for typ in [bytes, bytearray, memoryview]:
            sol = convert(msg, typ)
            assert type(sol) is typ
            assert sol == b"abc"

        del sol

        assert sys.getrefcount(msg) <= 2  # msg + 1
        sol = convert(msg, MyBytes)
        assert sol is msg
        assert sys.getrefcount(msg) <= 3  # msg + sol + 1


class TestDateTime:
    def test_datetime_wrong_type(self):
        with pytest.raises(ValidationError, match="Expected `datetime`, got `int`"):
            convert(1, datetime.datetime)

    @pytest.mark.parametrize("tz", [False, True])
    def test_datetime_builtin(self, tz):
        dt = datetime.datetime.now(UTC if tz else None)
        assert convert(dt, datetime.datetime) is dt

    @pytest.mark.parametrize("tz", [False, True])
    def test_datetime_str(self, tz):
        sol = datetime.datetime(1, 2, 3, 4, 5, 6, 7, UTC if tz else None)
        msg = "0001-02-03T04:05:06.000007" + ("Z" if tz else "")
        res = convert(msg, datetime.datetime)
        assert res == sol

    def test_datetime_str_disabled(self):
        with pytest.raises(ValidationError, match="Expected `datetime`, got `str`"):
            convert(
                "0001-02-03T04:05:06.000007Z",
                datetime.datetime,
                builtin_types=(datetime.datetime,),
            )

    @pytest.mark.parametrize("as_str", [False, True])
    def test_datetime_constrs(self, as_str):
        class Ex(Struct):
            x: Annotated[datetime.datetime, Meta(tz=True)]

        builtin_types = None if as_str else (datetime.datetime,)

        aware = Ex(datetime.datetime(1, 2, 3, 4, 5, 6, 7, UTC))
        aware_msg = to_builtins(aware, builtin_types=builtin_types)
        naive = Ex(datetime.datetime(1, 2, 3, 4, 5, 6, 7))
        naive_msg = to_builtins(naive, builtin_types=builtin_types)

        assert convert(aware_msg, Ex) == aware
        with pytest.raises(ValidationError):
            convert(naive_msg, Ex)


class TestTime:
    def test_time_wrong_type(self):
        with pytest.raises(ValidationError, match="Expected `time`, got `int`"):
            convert(1, datetime.time)

    @pytest.mark.parametrize("tz", [False, True])
    def test_time_builtin(self, tz):
        t = datetime.time(12, 34, tzinfo=(UTC if tz else None))
        assert convert(t, datetime.time) is t

    @pytest.mark.parametrize("tz", [False, True])
    def test_time_str(self, tz):
        sol = datetime.time(12, 34, tzinfo=(UTC if tz else None))
        msg = "12:34:00" + ("Z" if tz else "")
        res = convert(msg, datetime.time)
        assert res == sol

    def test_time_str_disabled(self):
        with pytest.raises(ValidationError, match="Expected `time`, got `str`"):
            convert("12:34:00Z", datetime.time, builtin_types=(datetime.time,))

    @pytest.mark.parametrize("as_str", [False, True])
    def test_time_constrs(self, as_str):
        class Ex(Struct):
            x: Annotated[datetime.time, Meta(tz=True)]

        builtin_types = None if as_str else (datetime.time,)

        aware = Ex(datetime.time(12, 34, tzinfo=UTC))
        aware_msg = to_builtins(aware, builtin_types=builtin_types)
        naive = Ex(datetime.time(12, 34))
        naive_msg = to_builtins(naive, builtin_types=builtin_types)

        assert convert(aware_msg, Ex) == aware
        with pytest.raises(ValidationError):
            convert(naive_msg, Ex)


class TestDate:
    def test_date_wrong_type(self):
        with pytest.raises(ValidationError, match="Expected `date`, got `int`"):
            convert(1, datetime.date)

    def test_date_builtin(self):
        dt = datetime.date.today()
        assert convert(dt, datetime.date) is dt

    def test_date_str(self):
        sol = datetime.date.today()
        res = convert(sol.isoformat(), datetime.date)
        assert res == sol

    def test_date_str_disabled(self):
        with pytest.raises(ValidationError, match="Expected `date`, got `str`"):
            convert("2022-01-02", datetime.date, builtin_types=(datetime.date,))


class TestTimeDelta:
    def test_timedelta_wrong_type(self):
        with pytest.raises(ValidationError, match="Expected `duration`, got `array`"):
            convert([], datetime.timedelta)

    def test_timedelta_builtin(self):
        td = datetime.timedelta(1)
        assert convert(td, datetime.timedelta) is td

    def test_timedelta_str(self):
        sol = datetime.timedelta(1, 2)
        res = convert("P1DT2S", datetime.timedelta)
        assert res == sol

    def test_timedelta_str_disabled(self):
        with pytest.raises(ValidationError, match="Expected `duration`, got `str`"):
            convert("P1DT2S", datetime.timedelta, builtin_types=(datetime.timedelta,))


class TestUUID:
    def test_uuid_wrong_type(self):
        with pytest.raises(ValidationError, match="Expected `uuid`, got `int`"):
            convert(1, uuid.UUID)

    def test_uuid_builtin(self):
        x = uuid.uuid4()
        assert convert(x, uuid.UUID) is x

    def test_uuid_str(self):
        sol = uuid.uuid4()
        res = convert(str(sol), uuid.UUID)
        assert res == sol

    @pytest.mark.parametrize("input_type", [bytes, bytearray, memoryview])
    def test_uuid_bytes(self, input_type):
        sol = uuid.uuid4()
        msg = input_type(sol.bytes)
        res = convert(msg, uuid.UUID)
        assert res == sol

        bad_msg = input_type(b"x" * 8)
        with pytest.raises(msgspec.ValidationError, match="Invalid UUID bytes"):
            convert(bad_msg, type=uuid.UUID)

    def test_uuid_disabled(self):
        u = uuid.uuid4()

        with pytest.raises(ValidationError, match="Expected `uuid`, got `str`"):
            convert(str(u), uuid.UUID, builtin_types=(uuid.UUID,))

        for typ in [bytes, bytearray]:
            with pytest.raises(ValidationError, match="Expected `uuid`, got `bytes`"):
                convert(typ(u.bytes), uuid.UUID, builtin_types=(uuid.UUID,))

    def test_convert_uuid_subclass(self):
        class UUID2(uuid.UUID): ...

        u1 = uuid.uuid4()
        u2 = UUID2(str(u1))
        assert convert(u2, uuid.UUID) is u2


class TestDecimal:
    def test_decimal_wrong_type(self):
        with pytest.raises(ValidationError, match="Expected `decimal`, got `array`"):
            convert([], decimal.Decimal)

    def test_decimal_builtin(self):
        x = decimal.Decimal("1.5")
        assert convert(x, decimal.Decimal) is x

    def test_decimal_str(self):
        sol = decimal.Decimal("1.5")
        res = convert("1.5", decimal.Decimal)
        assert res == sol
        assert type(res) is decimal.Decimal

    @pytest.mark.parametrize("val", [1.3, float("nan"), float("inf"), float("-inf")])
    def test_decimal_float(self, val):
        sol = decimal.Decimal(str(val))
        res = convert(val, decimal.Decimal)
        assert str(res) == str(sol)  # compare strs to support NaN
        assert type(res) is decimal.Decimal

    @pytest.mark.parametrize("val", [0, 1234, -1234])
    def test_decimal_int(self, val):
        sol = decimal.Decimal(val)
        res = convert(val, decimal.Decimal)
        assert res == sol
        assert type(res) is decimal.Decimal

    @pytest.mark.parametrize("val, typ", [("1.5", "str"), (123, "int"), (1.3, "float")])
    def test_decimal_conversion_disabled(self, val, typ):
        with pytest.raises(ValidationError, match=f"Expected `decimal`, got `{typ}`"):
            convert(val, decimal.Decimal, builtin_types=(decimal.Decimal,))


class TestExt:
    def test_ext(self):
        x = msgspec.msgpack.Ext(1, b"123")
        assert convert(x, msgspec.msgpack.Ext) is x

    def test_ext_errors(self):
        with pytest.raises(ValidationError, match="Expected `ext`, got `int`"):
            convert(1, msgspec.msgpack.Ext)

        with pytest.raises(ValidationError, match="Expected `int`, got `ext`"):
            convert(msgspec.msgpack.Ext(1, b"123"), int)


class TestEnum:
    def test_enum(self):
        class Ex(enum.Enum):
            x = "A"
            y = "B"

        class Ex2(enum.Enum):
            x = "A"

        assert convert(Ex.x, Ex) is Ex.x
        assert convert("A", Ex) is Ex.x
        assert convert("B", Ex) is Ex.y
        with pytest.raises(ValidationError, match="Invalid enum value 'C'"):
            convert("C", Ex)
        with pytest.raises(ValidationError, match="Expected `str`, got `int`"):
            convert(1, Ex)
        with pytest.raises(ValidationError, match="got `Ex2`"):
            convert(Ex2.x, Ex)

    def test_int_enum(self):
        class Ex(enum.IntEnum):
            x = 1
            y = 2

        class Ex2(enum.IntEnum):
            a = 1
            b = 3

        assert convert(Ex.x, Ex) is Ex.x
        assert convert(1, Ex) is Ex.x
        assert convert(2, Ex) is Ex.y
        assert convert(Ex2.a, Ex) is Ex.x

        with pytest.raises(ValidationError, match="Invalid enum value 3"):
            convert(3, Ex)

        with pytest.raises(ValidationError, match="Invalid enum value 3"):
            convert(Ex2.b, Ex)

        with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
            convert("A", Ex)

    def test_str_enum(self):
        if not hasattr(enum, "StrEnum"):
            pytest.skip(reason="StrEnum not available")

        class Ex(enum.StrEnum):
            x = "A"
            y = "B"

        class Ex2(enum.StrEnum):
            a = "A"
            b = "C"

        assert convert(Ex.x, Ex) is Ex.x
        assert convert("A", Ex) is Ex.x
        assert convert("B", Ex) is Ex.y
        assert convert(Ex2.a, Ex) is Ex.x

        with pytest.raises(ValidationError, match="Invalid enum value 'C'"):
            convert("C", Ex)

        with pytest.raises(ValidationError, match="Invalid enum value 'C'"):
            convert(Ex2.b, Ex)

        with pytest.raises(ValidationError, match="Expected `str`, got `int`"):
            convert(3, Ex)

    def test_int_enum_int_subclass(self):
        class MyInt(int):
            pass

        class Ex(enum.IntEnum):
            x = 1
            y = 2

        msg = MyInt(1)
        assert convert(msg, Ex) is Ex.x
        assert sys.getrefcount(msg) <= 2  # msg + 1
        assert convert(MyInt(2), Ex) is Ex.y

    def test_enum_missing(self):
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

        assert msgspec.convert("a", Ex) is Ex.A
        assert msgspec.convert("return-A", Ex) is Ex.A
        assert msgspec.convert("return-B", Ex) is Ex.B
        with pytest.raises(ValidationError, match="Invalid enum value 'error'"):
            msgspec.convert("error", Ex)
        with pytest.raises(ValidationError, match="Invalid enum value 'other'"):
            msgspec.convert("other", Ex)

    def test_intenum_missing(self):
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

        assert msgspec.convert(1, Ex) is Ex.A
        assert msgspec.convert(3, Ex) is Ex.A
        assert msgspec.convert(-4, Ex) is Ex.B
        with pytest.raises(ValidationError, match="Invalid enum value 5"):
            msgspec.convert(5, Ex)
        with pytest.raises(ValidationError, match="Invalid enum value 6"):
            msgspec.convert(6, Ex)


class TestLiteral:
    def test_str_literal(self):
        typ = Literal["A", "B"]
        assert convert("A", typ) == "A"
        assert convert("B", typ) == "B"
        with pytest.raises(ValidationError, match="Invalid enum value 'C'"):
            convert("C", typ)
        with pytest.raises(ValidationError, match="Expected `str`, got `int`"):
            convert(1, typ)

    def test_int_literal(self):
        typ = Literal[1, -2]
        assert convert(1, typ) == 1
        assert convert(-2, typ) == -2
        with pytest.raises(ValidationError, match="Invalid enum value 3"):
            convert(3, typ)
        with pytest.raises(ValidationError, match="Invalid enum value -3"):
            convert(-3, typ)
        with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
            convert("A", typ)


class TestSequences:
    def test_any_sequence(self):
        msg = (1, 2, 3)
        assert convert(msg, Any) is msg

    @seq_in_type
    @pytest.mark.parametrize("out_type", [list, tuple, set, frozenset])
    def test_empty_sequence(self, in_type, out_type):
        assert convert(in_type(), out_type) == out_type()

    @seq_in_type
    @pytest.mark.parametrize(
        "out_type_annot",
        [(list, List), (tuple, Tuple), (set, Set), (frozenset, FrozenSet)],
    )
    @pytest.mark.parametrize("item_annot", [None, int])
    def test_sequence(self, in_type, out_type_annot, item_annot):
        out_type, out_annot = out_type_annot
        if item_annot is not None:
            if out_annot is Tuple:
                out_annot = out_annot[item_annot, ...]
            else:
                out_annot = out_annot[item_annot]
        res = convert(in_type([1, 2]), out_annot)
        sol = out_type([1, 2])
        assert res == sol
        assert isinstance(res, out_type)

    @seq_in_type
    @pytest.mark.parametrize(
        "out_annot", [List[int], Tuple[int, ...], Set[int], FrozenSet[int]]
    )
    def test_sequence_wrong_item_type(self, in_type, out_annot):
        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$\[0\]`"
        ):
            assert convert(in_type(["bad"]), out_annot)

    @pytest.mark.parametrize("out_type", [list, tuple, set, frozenset])
    def test_sequence_wrong_type(self, out_type):
        with pytest.raises(ValidationError, match=r"Expected `array`, got `int`"):
            assert convert(1, out_type)

    @pytest.mark.parametrize("kind", ["list", "tuple", "fixtuple", "set"])
    @pytest.mark.skipif(
        PY312,
        reason="CPython 3.12 internal changes prevent testing for recursion issues this way",
    )
    def test_sequence_cyclic_recursion(self, kind):
        depth = 50
        if kind == "list":
            typ = List[int]
            for _ in range(depth):
                typ = List[typ]
        elif kind == "tuple":
            typ = Tuple[int, ...]
            for _ in range(depth):
                typ = Tuple[typ, ...]
        elif kind == "fixtuple":
            typ = Tuple[int]
            for _ in range(depth):
                typ = Tuple[typ]
        elif kind == "set":
            typ = FrozenSet[int]
            for _ in range(depth):
                typ = FrozenSet[typ]

        class Cache(Struct):
            value: typ

        msgspec.json.Decoder(Cache)

        arr = []
        arr.append(arr)
        msg = {"value": arr}
        with pytest.raises(RecursionError):
            with max_call_depth(5):
                convert(msg, Cache)

    @pytest.mark.parametrize("out_type", [list, tuple, set, frozenset])
    def test_sequence_constrs(self, out_type):
        class Ex(Struct):
            x: Annotated[out_type, Meta(min_length=2, max_length=4)]

        for n in [2, 4]:
            x = out_type(range(n))
            assert convert({"x": list(range(n))}, Ex).x == x

        for n in [1, 5]:
            x = out_type(range(n))
            with pytest.raises(ValidationError):
                convert({"x": list(range(n))}, Ex)

    def test_fixtuple_any(self):
        typ = Tuple[Any, Any, Any]
        sol = (1, "two", False)
        res = convert([1, "two", False], typ)
        assert res == sol

        with pytest.raises(ValidationError, match="Expected `array`, got `int`"):
            convert(1, typ)

        with pytest.raises(ValidationError, match="Expected `array`, got `set`"):
            convert({1, 2, 3}, typ)

        with pytest.raises(ValidationError, match="Expected `array` of length 3"):
            convert((1, "two"), typ)

    def test_fixtuple_typed(self):
        typ = Tuple[int, str, bool]
        sol = (1, "two", False)
        res = convert([1, "two", False], typ)
        assert res == sol

        with pytest.raises(ValidationError, match="Expected `bool`"):
            convert([1, "two", "three"], typ)

        with pytest.raises(ValidationError, match="Expected `array`, got `set`"):
            convert({1, 2, 3}, typ)

        with pytest.raises(ValidationError, match="Expected `array` of length 3"):
            convert((1, "two"), typ)


class TestNamedTuple:
    def test_namedtuple_no_defaults(self):
        class Example(NamedTuple):
            a: int
            b: int
            c: int

        msg = Example(1, 2, 3)
        res = convert([1, 2, 3], Example)
        assert res == msg

        with pytest.raises(ValidationError, match="length 3, got 1"):
            convert([1], Example)

        with pytest.raises(ValidationError, match="length 3, got 6"):
            convert([1, 2, 3, 4, 5, 6], Example)

    def test_namedtuple_with_defaults(self):
        class Example(NamedTuple):
            a: int
            b: int
            c: int = -3
            d: int = -4
            e: int = -5

        for args in [(1, 2), (1, 2, 3), (1, 2, 3, 4), (1, 2, 3, 4, 5)]:
            msg = Example(*args)
            res = convert(args, Example)
            assert res == msg

        with pytest.raises(ValidationError, match="length 2 to 5, got 1"):
            convert([1], Example)

        with pytest.raises(ValidationError, match="length 2 to 5, got 6"):
            convert([1, 2, 3, 4, 5, 6], Example)

    def test_namedtuple_field_wrong_type(self):
        class Example(NamedTuple):
            a: int
            b: str

        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$\[0\]`"
        ):
            convert(("bad", 1), Example)

    def test_namedtuple_not_array(self):
        class Example(NamedTuple):
            a: int
            b: str

        with pytest.raises(ValidationError, match="Expected `array`, got `object`"):
            convert({"a": 1, "b": "two"}, Example)

    def test_namedtuple_cyclic_recursion(self):
        source = """
        from __future__ import annotations
        from typing import NamedTuple, Union, Dict

        class Ex(NamedTuple):
            a: int
            b: Union[Ex, None]
        """
        with temp_module(source) as mod:
            msg = [1]
            msg.append(msg)
            with pytest.raises(RecursionError):
                assert convert(msg, mod.Ex)

    def test_namedtuple_to_namedtuple(self):
        class Ex1(NamedTuple):
            x: int

        class Ex2(NamedTuple):
            x: int

        class Ex3(NamedTuple):
            x: str

        msg = Ex1(1)
        assert convert(msg, Ex1) is msg
        assert convert(msg, Ex2) == Ex2(1)

        with pytest.raises(ValidationError, match="Expected `str`, got `int`"):
            convert(msg, Ex3)


class TestDict:
    def test_any_dict(self, dictcls):
        assert convert(dictcls({"one": 1, 2: "two"}), Any) == {"one": 1, 2: "two"}

    def test_empty_dict(self, dictcls):
        assert convert(dictcls({}), dict) == {}
        assert convert(dictcls({}), Dict[int, int]) == {}

    def test_typed_dict(self, dictcls):
        res = convert(dictcls({"x": 1, "y": 2}), Dict[str, float])
        assert res == {"x": 1.0, "y": 2.0}
        assert all(type(v) is float for v in res.values())

        with pytest.raises(
            ValidationError, match=r"Expected `str`, got `int` - at `\$\[\.\.\.\]`"
        ):
            convert(dictcls({"x": 1}), Dict[str, str])

        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `key` in `\$`"
        ):
            convert(dictcls({"x": 1}), Dict[int, str])

    def test_dict_wrong_type(self):
        with pytest.raises(ValidationError, match=r"Expected `object`, got `int`"):
            assert convert(1, dict)

    def test_str_formatted_keys(self):
        msg = {uuid.uuid4(): 1, uuid.uuid4(): 2}
        res = convert(to_builtins(msg), Dict[uuid.UUID, int])
        assert res == msg

    @pytest.mark.parametrize("key_type", ["int", "enum", "literal"])
    def test_int_keys(self, dictcls, key_type):
        msg = dictcls({1: "A", 2: "B"})
        if key_type == "enum":
            Key = enum.IntEnum("Key", ["one", "two"])
            sol = {Key.one: "A", Key.two: "B"}
        elif key_type == "literal":
            Key = Literal[1, 2]
            sol = msg
        else:
            Key = int
            sol = msg

        res = convert(msg, Dict[Key, str])
        assert res == sol

        res = convert(msg, Dict[Key, str], str_keys=True)
        assert res == sol

        str_msg = dictcls(to_builtins(dict(msg), str_keys=True))
        res = convert(str_msg, Dict[Key, str], str_keys=True)
        assert res == sol

        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `key` in `\$`"
        ):
            convert(str_msg, Dict[Key, str])

    def test_non_str_keys(self, dictcls):
        convert(dictcls({1.5: 1}), Dict[float, int]) == {1.5: 1}

        with pytest.raises(ValidationError):
            convert(dictcls({"x": 1}), Dict[Tuple[int, int], int], str_keys=True)

    @pytest.mark.skipif(
        PY312,
        reason="CPython 3.12 internal changes prevent testing for recursion issues this way",
    )
    def test_dict_cyclic_recursion(self, dictcls):
        depth = 50
        typ = Dict[str, int]
        for _ in range(depth):
            typ = Dict[str, typ]

        class Cache(Struct):
            value: typ

        msgspec.json.Decoder(Cache)

        map = dictcls()
        map["x"] = map
        msg = {"value": map}

        with pytest.raises(RecursionError):
            with max_call_depth(5):
                convert(msg, Cache)

    def test_dict_constrs(self, dictcls):
        class Ex(Struct):
            x: Annotated[dict, Meta(min_length=2, max_length=4)]

        for n in [2, 4]:
            x = dictcls({str(i): i for i in range(n)})
            assert convert(dictcls({"x": x}), Ex).x == x

        for n in [1, 5]:
            x = {str(i): i for i in range(n)}
            with pytest.raises(ValidationError):
                convert(dictcls({"x": x}), Ex)


class TestTypedDict:
    def test_typeddict_total_true(self):
        class Ex(TypedDict):
            a: int
            b: str

        x = {"a": 1, "b": "two"}
        assert convert(x, Ex) == x

        x2 = {"a": 1, "b": "two", "c": "extra"}
        assert convert(x2, Ex) == x

        with pytest.raises(ValidationError) as rec:
            convert({"b": "two"}, Ex)
        assert "Object missing required field `a`" == str(rec.value)

        with pytest.raises(ValidationError) as rec:
            convert({"a": 1, "b": 2}, Ex)
        assert "Expected `str`, got `int` - at `$.b`" == str(rec.value)

        with pytest.raises(ValidationError) as rec:
            convert({"a": 1, 1: 2}, Ex)
        assert "Expected `str` - at `key` in `$`" == str(rec.value)

    def test_typeddict_total_false(self):
        class Ex(TypedDict, total=False):
            a: int
            b: str

        x = {"a": 1, "b": "two"}
        assert convert(x, Ex) == x

        x2 = {"a": 1, "b": "two", "c": "extra"}
        assert convert(x2, Ex) == x

        x3 = {"b": "two"}
        assert convert(x3, Ex) == x3

        x4 = {}
        assert convert(x4, Ex) == x4

    def test_typeddict_total_partially_optional(self):
        class Base(TypedDict):
            a: int
            b: str

        class Ex(Base, total=False):
            c: str

        x = {"a": 1, "b": "two", "c": "extra"}
        assert convert(x, Ex) == x

        x2 = {"a": 1, "b": "two"}
        assert convert(x2, Ex) == x2

        with pytest.raises(ValidationError) as rec:
            convert({"b": "two"}, Ex)
        assert "Object missing required field `a`" == str(rec.value)


class TestDataclass:
    @pytest.mark.parametrize("slots", [False, True])
    @mapcls_and_from_attributes
    def test_dataclass(self, slots, mapcls, from_attributes):
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

        sol = Example(1, 2, 3)
        msg = mapcls(a=1, b=2, c=3)
        res = convert(msg, Example, from_attributes=from_attributes)
        assert res == sol

        # Extra fields ignored
        res = convert(
            mapcls({"x": -1, "a": 1, "y": -2, "b": 2, "z": -3, "c": 3, "": -4}),
            Example,
            from_attributes=from_attributes,
        )
        assert res == sol

        # Missing fields error
        with pytest.raises(ValidationError, match="missing required field `b`"):
            convert(mapcls(a=1), Example, from_attributes=from_attributes)

        # Incorrect field types error
        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$.a`"
        ):
            convert(mapcls(a="bad"), Example, from_attributes=from_attributes)

    def test_dict_to_dataclass_errors(self):
        @dataclass
        class Example:
            a: int

        with pytest.raises(ValidationError, match=r"Expected `str` - at `key` in `\$`"):
            convert({"a": 1, 1: 2}, Example)

    def test_from_attributes_option_disables_attribute_coercion(self):
        class Bad:
            def __init__(self):
                self.x = 1

        msg = Bad()

        @dataclass
        class Ex:
            x: int

        with pytest.raises(ValidationError, match="Expected `object`, got `Bad`"):
            convert(msg, Ex)

        assert convert(msg, Ex, from_attributes=True) == Ex(1)

    @pytest.mark.parametrize("frozen", [False, True])
    @pytest.mark.parametrize("slots", [False, True])
    @mapcls_and_from_attributes
    def test_dataclass_defaults(self, frozen, slots, mapcls, from_attributes):
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

        for args in [(1, 2), (1, 2, 3), (1, 2, 3, 4), (1, 2, 3, 4, 5)]:
            sol = Example(*args)
            msg = mapcls(dict(zip("abcde", args)))
            res = convert(msg, Example, from_attributes=from_attributes)
            assert res == sol

        # Missing fields error
        with pytest.raises(ValidationError, match="missing required field `a`"):
            convert(mapcls(c=1, d=2, e=3), Example, from_attributes=from_attributes)

    @mapcls_and_from_attributes
    def test_dataclass_default_factory_errors(self, mapcls, from_attributes):
        def bad():
            raise ValueError("Oh no!")

        @dataclass
        class Example:
            a: int = field(default_factory=bad)

        msg = mapcls()

        with pytest.raises(ValueError, match="Oh no!"):
            convert(msg, Example, from_attributes=from_attributes)

    @mapcls_and_from_attributes
    def test_dataclass_post_init(self, mapcls, from_attributes):
        called = False

        @dataclass
        class Example:
            a: int

            def __post_init__(self):
                nonlocal called
                called = True

        msg = mapcls(a=1)
        res = convert(msg, Example, from_attributes=from_attributes)
        assert res.a == 1
        assert called

    @mapcls_and_from_attributes
    def test_dataclass_post_init_errors(self, mapcls, from_attributes):
        @dataclass
        class Example:
            a: int

            def __post_init__(self):
                raise ValueError("Oh no!")

        msg = mapcls(a=1)

        with pytest.raises(ValidationError, match="Oh no!"):
            convert(msg, Example, from_attributes=from_attributes)

    @mapcls_and_from_attributes
    def test_dataclass_not_object(self, mapcls, from_attributes):
        @dataclass
        class Example:
            a: int
            b: int

        with pytest.raises(ValidationError, match="Expected `object`, got `array`"):
            convert([], Example, from_attributes=from_attributes)

    def test_dataclass_to_dataclass(self):
        @dataclass
        class Ex1:
            x: int

        @dataclass
        class Ex2:
            x: int

        msg = Ex1(1)
        assert convert(msg, Ex1) is msg

        with pytest.raises(ValidationError, match="got `Ex1`"):
            convert(msg, Ex2)

        assert convert(msg, Ex2, from_attributes=True) == Ex2(1)

    def test_struct_to_dataclass(self):
        @dataclass
        class Ex1:
            x: int

        class Ex2(Struct):
            x: int

        assert convert(Ex1(1), Ex2, from_attributes=True) == Ex2(1)


@pytest.mark.skipif(attrs is None, reason="attrs is not installed")
class TestAttrs:
    @pytest.mark.parametrize("slots", [False, True])
    @mapcls_and_from_attributes
    def test_attrs(self, slots, mapcls, from_attributes):
        @attrs.define(slots=slots)
        class Example:
            a: int
            b: int
            c: int

        sol = Example(1, 2, 3)
        msg = mapcls(a=1, b=2, c=3)
        res = convert(msg, Example, from_attributes=from_attributes)
        assert res == sol

        # Extra fields ignored
        res = convert(
            mapcls({"x": -1, "a": 1, "y": -2, "b": 2, "z": -3, "c": 3, "": -4}),
            Example,
            from_attributes=from_attributes,
        )
        assert res == sol

        # Missing fields error
        with pytest.raises(ValidationError, match="missing required field `b`"):
            convert(mapcls(a=1), Example, from_attributes=from_attributes)

        # Incorrect field types error
        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$.a`"
        ):
            convert({"a": "bad"}, Example, from_attributes=from_attributes)

    def test_dict_to_attrs_errors(self):
        @attrs.define
        class Example:
            a: int

        with pytest.raises(ValidationError, match=r"Expected `str` - at `key` in `\$`"):
            convert({"a": 1, 1: 2}, Example)

    def test_from_attributes_option_disables_attribute_coercion(self):
        class Bad:
            def __init__(self):
                self.x = 1

        msg = Bad()

        @attrs.define
        class Ex:
            x: int

        with pytest.raises(ValidationError, match="Expected `object`, got `Bad`"):
            convert(msg, Ex)

        assert convert(msg, Ex, from_attributes=True) == Ex(1)

    @pytest.mark.parametrize("frozen", [False, True])
    @pytest.mark.parametrize("slots", [False, True])
    @mapcls_and_from_attributes
    def test_attrs_defaults(self, frozen, slots, mapcls, from_attributes):
        @attrs.define(frozen=frozen, slots=slots)
        class Example:
            a: int
            b: int
            c: int = -3
            d: int = -4
            e: int = attrs.field(factory=lambda: -1000)

        for args in [(1, 2), (1, 2, 3), (1, 2, 3, 4), (1, 2, 3, 4, 5)]:
            sol = Example(*args)
            msg = mapcls(dict(zip("abcde", args)))
            res = convert(msg, Example, from_attributes=from_attributes)
            assert res == sol

        # Missing fields error
        with pytest.raises(ValidationError, match="missing required field `a`"):
            convert(mapcls(c=1, d=2, e=3), Example, from_attributes=from_attributes)

    @mapcls_and_from_attributes
    def test_attrs_frozen(self, mapcls, from_attributes):
        @attrs.define(frozen=True)
        class Example:
            x: int
            y: int

        sol = Example(1, 2)
        msg = mapcls(x=1, y=2)
        res = convert(msg, Example, from_attributes=from_attributes)
        assert res == sol

    @mapcls_and_from_attributes
    def test_attrs_pre_init(self, mapcls, from_attributes):
        called = False

        @attrs.define
        class Example:
            a: int

            def __attrs_pre_init__(self):
                nonlocal called
                called = True

        res = convert(mapcls(a=1), Example, from_attributes=from_attributes)
        assert res.a == 1
        assert called

    @mapcls_and_from_attributes
    def test_attrs_pre_init_errors(self, mapcls, from_attributes):
        @attrs.define
        class Example:
            a: int

            def __attrs_pre_init__(self):
                raise ValueError("Oh no!")

        with pytest.raises(ValueError, match="Oh no!"):
            convert(mapcls(a=1), Example, from_attributes=from_attributes)

    @mapcls_and_from_attributes
    def test_attrs_post_init(self, mapcls, from_attributes):
        called = False

        @attrs.define
        class Example:
            a: int

            def __attrs_post_init__(self):
                nonlocal called
                called = True

        res = convert(mapcls(a=1), Example, from_attributes=from_attributes)
        assert res.a == 1
        assert called

    @mapcls_and_from_attributes
    def test_attrs_post_init_errors(self, mapcls, from_attributes):
        @attrs.define
        class Example:
            a: int

            def __attrs_post_init__(self):
                raise ValueError("Oh no!")

        with pytest.raises(ValidationError, match="Oh no!"):
            convert(mapcls(a=1), Example, from_attributes=from_attributes)

    def test_attrs_to_attrs(self):
        @attrs.define
        class Ex1:
            x: int

        @attrs.define
        class Ex2:
            x: int

        msg = Ex1(1)
        assert convert(msg, Ex1) is msg

        with pytest.raises(ValidationError, match="got `Ex1`"):
            convert(msg, Ex2)

        assert convert(msg, Ex2, from_attributes=True) == Ex2(1)

    def test_struct_to_attrs(self):
        @attrs.define
        class Ex1:
            x: int

        class Ex2(Struct):
            x: int

        assert convert(Ex1(1), Ex2, from_attributes=True) == Ex2(1)


class TestStruct:
    class Account(Struct, kw_only=True):
        first: str
        last: str
        verified: bool = False
        age: int

    @mapcls_and_from_attributes
    def test_struct(self, mapcls, from_attributes):
        msg = mapcls(first="alice", last="munro", age=91, verified=True)
        sol = self.Account(first="alice", last="munro", verified=True, age=91)
        res = convert(msg, self.Account, from_attributes=from_attributes)
        assert res == sol

        with pytest.raises(ValidationError, match="Expected `object`, got `array`"):
            convert([], self.Account, from_attributes=from_attributes)

        with pytest.raises(
            ValidationError, match=r"Expected `str`, got `int` - at `\$.last`"
        ):
            convert(
                mapcls(first="alice", last=1),
                self.Account,
                from_attributes=from_attributes,
            )

        with pytest.raises(
            ValidationError, match="Object missing required field `age`"
        ):
            convert(
                mapcls(first="alice", last="munro"),
                self.Account,
                from_attributes=from_attributes,
            )

    def test_dict_to_struct_errors(self):
        with pytest.raises(ValidationError, match=r"Expected `str` - at `key` in `\$`"):
            convert({"age": 1, 1: 2}, self.Account)

    def test_from_attributes_option_disables_attribute_coercion(self):
        class Bad:
            def __init__(self):
                self.x = 1

        msg = Bad()

        class Ex(Struct):
            x: int

        with pytest.raises(ValidationError, match="Expected `object`, got `Bad`"):
            convert(msg, Ex)

        assert convert(msg, Ex, from_attributes=True) == Ex(1)

    @pytest.mark.parametrize("mapcls", [GetAttrObj, GetItemObj])
    def test_object_to_struct_with_renamed_fields(self, mapcls):
        class Ex(Struct, rename="camel"):
            fa: int
            f_b: int
            fc: int
            f_d: int

        sol = Ex(1, 2, 3, 4)

        # Use attribute names
        msg = mapcls(fa=1, f_b=2, fc=3, f_d=4)
        assert convert(msg, Ex, from_attributes=True) == sol

        # Use renamed names
        msg = mapcls(fa=1, fB=2, fc=3, fD=4)
        assert convert(msg, Ex, from_attributes=True) == sol

        # Priority to attribute names
        msg = mapcls(fa=1, f_b=2, fB=100, fc=3, f_d=4, fD=100)
        assert convert(msg, Ex, from_attributes=True) == sol

        # Don't allow renamed names if determined to be attributes
        msg = mapcls(fa=1, f_b=2, fc=3, fD=4)
        with pytest.raises(ValidationError, match="missing required field `f_d`"):
            convert(msg, Ex, from_attributes=True)

        # Don't allow attributes if determined to be renamed names
        msg = mapcls(fa=1, fB=2, fc=3, f_d=4)
        with pytest.raises(ValidationError, match="missing required field `fD`"):
            convert(msg, Ex, from_attributes=True)

        # Errors use attribute name if using attributes
        msg = mapcls(fa=1, f_b=2, fc=3, f_d="bad")
        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$.f_d`"
        ):
            convert(msg, Ex, from_attributes=True)

        # Errors use renamed name if using renamed names
        msg = mapcls(fa=1, fB=2, fc=3, fD="bad")
        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$.fD`"
        ):
            convert(msg, Ex, from_attributes=True)

        # Errors use attribute name if undecided
        msg = mapcls(fa="bad")
        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$.fa`"
        ):
            convert(msg, Ex, from_attributes=True)

    @pytest.mark.parametrize("forbid_unknown_fields", [False, True])
    @mapcls_and_from_attributes
    def test_struct_extra_fields(self, forbid_unknown_fields, mapcls, from_attributes):
        class Ex(Struct, forbid_unknown_fields=forbid_unknown_fields):
            a: int
            b: int

        msg = mapcls(x=1, a=2, y=3, b=4, z=5)
        if forbid_unknown_fields and issubclass(mapcls, dict):
            with pytest.raises(ValidationError, match="unknown field `x`"):
                convert(msg, Ex, from_attributes=from_attributes)
        else:
            res = convert(msg, Ex, from_attributes=from_attributes)
            assert res == Ex(2, 4)

    @mapcls_and_from_attributes
    def test_struct_defaults_missing_fields(self, mapcls, from_attributes):
        msg = mapcls(first="alice", last="munro", age=91)
        res = convert(msg, self.Account, from_attributes=from_attributes)
        assert res == self.Account(first="alice", last="munro", age=91)

    @mapcls_from_attributes_and_array_like
    def test_struct_gc_maybe_untracked_on_decode(
        self, mapcls, from_attributes, array_like
    ):
        class Test(Struct, array_like=array_like):
            x: Any
            y: Any
            z: Tuple = ()

        ts = [
            mapcls(x=1, y=2),
            mapcls(x=3, y="hello"),
            mapcls(x=[], y=[]),
            mapcls(x={}, y={}),
            mapcls(x=None, y=None, z=()),
        ]
        a, b, c, d, e = convert(ts, List[Test], from_attributes=from_attributes)
        assert not gc.is_tracked(a)
        assert not gc.is_tracked(b)
        assert gc.is_tracked(c)
        assert gc.is_tracked(d)
        assert not gc.is_tracked(e)

    @mapcls_from_attributes_and_array_like
    def test_struct_gc_false_always_untracked_on_decode(
        self, mapcls, from_attributes, array_like
    ):
        class Test(Struct, array_like=array_like, gc=False):
            x: Any
            y: Any

        ts = [
            mapcls(x=1, y=2),
            mapcls(x=[], y=[]),
            mapcls(x={}, y={}),
        ]
        for obj in convert(ts, List[Test], from_attributes=from_attributes):
            assert not gc.is_tracked(obj)

    @pytest.mark.parametrize("tag", ["Test", 123, -123])
    @mapcls_and_from_attributes
    def test_tagged_struct(self, tag, mapcls, from_attributes):
        class Test(Struct, tag=tag):
            a: int
            b: int

        # Test with and without tag
        for msg in [
            mapcls(a=1, b=2),
            mapcls(type=tag, a=1, b=2),
            mapcls(a=1, type=tag, b=2),
        ]:
            res = convert(msg, Test, from_attributes=from_attributes)
            assert res == Test(1, 2)

        # Tag incorrect type
        with pytest.raises(ValidationError) as rec:
            convert(mapcls(type=123.456), Test, from_attributes=from_attributes)
        assert f"Expected `{type(tag).__name__}`" in str(rec.value)
        assert "`$.type`" in str(rec.value)

        # Tag incorrect value
        bad = -3 if isinstance(tag, int) else "bad"
        with pytest.raises(ValidationError) as rec:
            convert(mapcls(type=bad), Test, from_attributes=from_attributes)
        assert f"Invalid value {bad!r}" in str(rec.value)
        assert "`$.type`" in str(rec.value)

    @pytest.mark.parametrize("tag_val", [2**64 - 1, 2**64, -(2**63) - 1])
    @mapcls_and_from_attributes
    def test_tagged_struct_int_tag_not_int64_always_invalid(
        self, tag_val, mapcls, from_attributes
    ):
        """Tag values that don't fit in an int64 are currently unsupported, but
        we still want to raise a good error message."""

        class Test(Struct, tag=123):
            pass

        with pytest.raises(ValidationError) as rec:
            convert(mapcls(type=tag_val), Test, from_attributes=from_attributes)

        assert f"Invalid value {tag_val}" in str(rec.value)
        assert "`$.type`" in str(rec.value)

    @pytest.mark.parametrize("tag", ["Test", 123, -123])
    @mapcls_and_from_attributes
    def test_tagged_empty_struct(self, tag, mapcls, from_attributes):
        class Test(Struct, tag=tag):
            pass

        # Tag missing
        res = convert(mapcls(), Test, from_attributes=from_attributes)
        assert res == Test()

        # Tag present
        res = convert(mapcls(type=tag), Test, from_attributes=from_attributes)
        assert res == Test()

    @pytest.mark.parametrize("array_like", [False, True])
    def test_struct_to_struct(self, array_like):
        class Ex1(Struct, array_like=array_like):
            x: int

        class Ex2(Struct, array_like=array_like):
            x: int

        msg = Ex1(1)
        assert convert(msg, Ex1) is msg
        with pytest.raises(ValidationError, match="got `Ex1`"):
            convert(msg, Ex2)

        assert convert(msg, Ex2, from_attributes=True) == Ex2(1)

    @pytest.mark.parametrize("array_like", [False, True])
    def test_dataclass_to_struct(self, array_like):
        @dataclass
        class Ex1:
            x: int

        class Ex2(Struct, array_like=array_like):
            x: int

        assert convert(Ex1(1), Ex2, from_attributes=True) == Ex2(1)


class TestStructArray:
    class Account(Struct, array_like=True):
        first: str
        last: str
        age: int
        verified: bool = False

    def test_struct_array_like(self):
        msg = self.Account("alice", "munro", 91, True)
        res = roundtrip(msg, self.Account)
        assert res == msg

        with pytest.raises(ValidationError, match="Expected `array`, got `int`"):
            roundtrip(1, self.Account)

        # Wrong field type
        with pytest.raises(
            ValidationError, match=r"Expected `int`, got `str` - at `\$\[2\]`"
        ):
            roundtrip(("alice", "munro", "bad"), self.Account)

        # Missing fields
        with pytest.raises(
            ValidationError,
            match="Expected `array` of at least length 3, got 2",
        ):
            roundtrip(("alice", "munro"), self.Account)

        with pytest.raises(
            ValidationError,
            match="Expected `array` of at least length 3, got 0",
        ):
            roundtrip((), self.Account)

    @pytest.mark.parametrize("forbid_unknown_fields", [False, True])
    def test_struct_extra_fields(self, forbid_unknown_fields):
        class Ex(Struct, array_like=True, forbid_unknown_fields=forbid_unknown_fields):
            a: int
            b: int

        msg = (1, 2, 3, 4)
        if forbid_unknown_fields:
            with pytest.raises(
                ValidationError, match="Expected `array` of at most length 2, got 4"
            ):
                roundtrip(msg, Ex)
        else:
            res = roundtrip(msg, Ex)
            assert res == Ex(1, 2)

    def test_struct_defaults_missing_fields(self):
        res = roundtrip(("alice", "munro", 91), self.Account)
        assert res == self.Account("alice", "munro", 91)

    @pytest.mark.parametrize("tag", ["Test", -123, 123])
    def test_tagged_struct(self, tag):
        class Test(Struct, tag=tag, array_like=True):
            a: int
            b: int
            c: int = 0

        # Decode with tag
        res = roundtrip((tag, 1, 2), Test)
        assert res == Test(1, 2)
        res = roundtrip((tag, 1, 2, 3), Test)
        assert res == Test(1, 2, 3)

        # Trailing fields ignored
        res = roundtrip((tag, 1, 2, 3, 4), Test)
        assert res == Test(1, 2, 3)

        # Missing required field errors
        with pytest.raises(ValidationError) as rec:
            roundtrip((tag, 1), Test)
        assert "Expected `array` of at least length 3, got 2" in str(rec.value)

        # Tag missing
        with pytest.raises(ValidationError) as rec:
            roundtrip((), Test)
        assert "Expected `array` of at least length 3, got 0" in str(rec.value)

        # Tag incorrect type
        with pytest.raises(ValidationError) as rec:
            roundtrip((123.456, 2, 3), Test)
        assert f"Expected `{type(tag).__name__}`" in str(rec.value)
        assert "`$[0]`" in str(rec.value)

        # Tag incorrect value
        bad = -3 if isinstance(tag, int) else "bad"
        with pytest.raises(ValidationError) as rec:
            roundtrip((bad, 1, 2), Test)
        assert f"Invalid value {bad!r}" in str(rec.value)
        assert "`$[0]`" in str(rec.value)

        # Field incorrect type correct index
        with pytest.raises(ValidationError) as rec:
            roundtrip((tag, "a", 2), Test)
        assert "Expected `int`, got `str`" in str(rec.value)
        assert "`$[1]`" in str(rec.value)

    @pytest.mark.parametrize("tag", ["Test", 123, -123])
    def test_tagged_empty_struct(self, tag):
        class Test(Struct, tag=tag, array_like=True):
            pass

        # Decode with tag
        res = roundtrip((tag, 1, 2), Test)
        assert res == Test()

        # Tag missing
        with pytest.raises(ValidationError) as rec:
            roundtrip((), Test)
        assert "Expected `array` of at least length 1, got 0" in str(rec.value)


class TestStructUnion:
    @pytest.mark.parametrize(
        "tag1, tag2, unknown",
        [
            ("Test1", "Test2", "Test3"),
            (0, 1, 2),
            (123, -123, 0),
        ],
    )
    @mapcls_and_from_attributes
    def test_struct_union(self, tag1, tag2, unknown, mapcls, from_attributes):
        def decode(msg):
            return convert(
                mapcls(msg), Union[Test1, Test2], from_attributes=from_attributes
            )

        class Test1(Struct, tag=tag1):
            a: int
            b: int
            c: int = 0

        class Test2(Struct, tag=tag2):
            x: int
            y: int

        # Tag can be in any position
        assert decode({"type": tag1, "a": 1, "b": 2}) == Test1(1, 2)
        assert decode({"a": 1, "type": tag1, "b": 2}) == Test1(1, 2)
        assert decode({"x": 1, "y": 2, "type": tag2}) == Test2(1, 2)

        # Optional fields still work
        assert decode({"type": tag1, "a": 1, "b": 2, "c": 3}) == Test1(1, 2, 3)
        assert decode({"a": 1, "b": 2, "c": 3, "type": tag1}) == Test1(1, 2, 3)

        # Extra fields still ignored
        assert decode({"a": 1, "b": 2, "d": 4, "type": tag1}) == Test1(1, 2)

        # Tag missing
        with pytest.raises(ValidationError) as rec:
            decode({"a": 1, "b": 2})
        assert "missing required field `type`" in str(rec.value)

        # Tag wrong type
        with pytest.raises(ValidationError) as rec:
            decode({"type": 123.456, "a": 1, "b": 2})
        assert f"Expected `{type(tag1).__name__}`" in str(rec.value)
        assert "`$.type`" in str(rec.value)

        # Tag unknown
        with pytest.raises(ValidationError) as rec:
            decode({"type": unknown, "a": 1, "b": 2})
        assert f"Invalid value {unknown!r} - at `$.type`" == str(rec.value)

    @pytest.mark.parametrize(
        "tag1, tag2, tag3, unknown",
        [
            ("Test1", "Test2", "Test3", "Test4"),
            (0, 1, 2, 3),
            (123, -123, 0, -1),
        ],
    )
    def test_struct_array_union(self, tag1, tag2, tag3, unknown):
        class Test1(Struct, tag=tag1, array_like=True):
            a: int
            b: int
            c: int = 0

        class Test2(Struct, tag=tag2, array_like=True):
            x: int
            y: int

        class Test3(Struct, tag=tag3, array_like=True):
            pass

        typ = Union[Test1, Test2, Test3]

        # Decoding works
        assert roundtrip([tag1, 1, 2], typ) == Test1(1, 2)
        assert roundtrip([tag2, 3, 4], typ) == Test2(3, 4)
        assert roundtrip([tag3], typ) == Test3()

        # Optional & Extra fields still respected
        assert roundtrip([tag1, 1, 2, 3], typ) == Test1(1, 2, 3)
        assert roundtrip([tag1, 1, 2, 3, 4], typ) == Test1(1, 2, 3)

        # Missing required field
        with pytest.raises(ValidationError) as rec:
            roundtrip([tag1, 1], typ)
        assert "Expected `array` of at least length 3, got 2" in str(rec.value)

        # Type error has correct field index
        with pytest.raises(ValidationError) as rec:
            roundtrip([tag1, 1, "bad", 2], typ)
        assert "Expected `int`, got `str` - at `$[2]`" == str(rec.value)

        # Tag missing
        with pytest.raises(ValidationError) as rec:
            roundtrip([], typ)
        assert "Expected `array` of at least length 1, got 0" == str(rec.value)

        # Tag wrong type
        with pytest.raises(ValidationError) as rec:
            roundtrip([123.456, 2, 3, 4], typ)
        assert f"Expected `{type(tag1).__name__}`" in str(rec.value)
        assert "`$[0]`" in str(rec.value)

        # Tag unknown
        with pytest.raises(ValidationError) as rec:
            roundtrip([unknown, 1, 2, 3], typ)
        assert f"Invalid value {unknown!r} - at `$[0]`" == str(rec.value)

    @pytest.mark.parametrize("tags", [(1, 2), (-10000, 10000), ("A", "B")])
    @pytest.mark.parametrize("array_like", [False, True])
    def test_struct_to_struct_union(self, tags, array_like):
        class Ex1(Struct, array_like=array_like, tag=tags[0]):
            x: int

        class Ex2(Struct, array_like=array_like, tag=tags[1]):
            x: int

        class Ex3(Struct, array_like=array_like):
            x: int

        typ = Union[Ex1, Ex2]

        msg = Ex1(1)
        assert convert(msg, typ) is msg

        with pytest.raises(ValidationError, match="got `Ex3`"):
            convert(Ex3(1), typ)


class TestGenericStruct:
    @mapcls_from_attributes_and_array_like
    def test_generic_struct(self, mapcls, from_attributes, array_like):
        class Ex(Struct, Generic[T], array_like=array_like):
            x: T
            y: List[T]

        sol = Ex(1, [1, 2])
        msg = mapcls(x=1, y=[1, 2])

        res = convert(msg, Ex, from_attributes=from_attributes)
        assert res == sol

        res = convert(msg, Ex[int], from_attributes=from_attributes)
        assert res == sol

        res = convert(msg, Ex[Union[int, str]], from_attributes=from_attributes)
        assert res == sol

        res = convert(msg, Ex[float], from_attributes=from_attributes)
        assert type(res.x) is float

        with pytest.raises(ValidationError, match="Expected `str`, got `int`"):
            convert(msg, Ex[str], from_attributes=from_attributes)

    @mapcls_from_attributes_and_array_like
    def test_generic_struct_union(self, mapcls, from_attributes, array_like):
        class Test1(Struct, Generic[T], tag=True, array_like=array_like):
            a: Union[T, None]
            b: int

        class Test2(Struct, Generic[T], tag=True, array_like=array_like):
            x: T
            y: int

        typ = Union[Test1[T], Test2[T]]

        msg1 = Test1(1, 2)
        s1 = mapcls(type="Test1", a=1, b=2)
        msg2 = Test2("three", 4)
        s2 = mapcls(type="Test2", x="three", y=4)
        msg3 = Test1(None, 4)
        s3 = mapcls(type="Test1", a=None, b=4)

        assert convert(s1, typ, from_attributes=from_attributes) == msg1
        assert convert(s2, typ, from_attributes=from_attributes) == msg2
        assert convert(s3, typ, from_attributes=from_attributes) == msg3

        assert convert(s1, typ[int], from_attributes=from_attributes) == msg1
        assert convert(s3, typ[int], from_attributes=from_attributes) == msg3
        assert convert(s2, typ[str], from_attributes=from_attributes) == msg2
        assert convert(s3, typ[str], from_attributes=from_attributes) == msg3

        with pytest.raises(ValidationError) as rec:
            convert(s1, typ[str], from_attributes=from_attributes)
        assert "Expected `str | null`, got `int`" in str(rec.value)
        loc = "$[1]" if array_like and not from_attributes else "$.a"
        assert loc in str(rec.value)

        with pytest.raises(ValidationError) as rec:
            convert(s2, typ[int], from_attributes=from_attributes)
        assert "Expected `int`, got `str`" in str(rec.value)
        loc = "$[1]" if array_like and not from_attributes else "$.x"
        assert loc in str(rec.value)


class TestStructPostInit:
    @pytest.mark.parametrize("union", [False, True])
    @mapcls_from_attributes_and_array_like
    def test_struct_post_init(self, union, mapcls, from_attributes, array_like):
        called = False
        singleton = object()

        class Ex(Struct, array_like=array_like, tag=union):
            x: int

            def __post_init__(self):
                nonlocal called
                called = True
                return singleton

        if union:

            class Ex2(Struct, array_like=array_like, tag=True):
                pass

            typ = Union[Ex, Ex2]
        else:
            typ = Ex

        msg = mapcls(type="Ex", x=1) if union else mapcls(x=1)
        res = convert(msg, type=typ, from_attributes=from_attributes)
        assert type(res) is Ex
        assert called
        assert sys.getrefcount(singleton) <= 2  # 1 for ref, 1 for call

    @pytest.mark.parametrize("union", [False, True])
    @pytest.mark.parametrize("exc_class", [ValueError, TypeError, OSError])
    @mapcls_from_attributes_and_array_like
    def test_struct_post_init_errors(
        self, union, exc_class, mapcls, from_attributes, array_like
    ):
        class Ex(Struct, array_like=array_like, tag=union):
            x: int

            def __post_init__(self):
                raise exc_class("Oh no!")

        if union:

            class Ex2(Struct, array_like=array_like, tag=True):
                pass

            typ = Union[Ex, Ex2]
        else:
            typ = Ex

        msg = [mapcls(type="Ex", x=1) if union else mapcls(x=1)]

        if exc_class in (ValueError, TypeError):
            expected = ValidationError
        else:
            expected = exc_class

        with pytest.raises(expected, match="Oh no!") as rec:
            convert(msg, type=List[typ], from_attributes=from_attributes)

        if expected is ValidationError:
            assert "- at `$[0]`" in str(rec.value)


class TestLax:
    def test_lax_none(self):
        for x in ["null", "Null", "nUll", "nuLl", "nulL"]:
            assert convert(x, None, strict=False) is None

        for x in ["xull", "nxll", "nuxl", "nulx"]:
            with pytest.raises(ValidationError, match="Expected `null`, got `str`"):
                convert(x, None, strict=False)

    def test_lax_bool_true(self):
        for x in [1, "1", "true", "True", "tRue", "trUe", "truE"]:
            assert convert(x, bool, strict=False) is True

    def test_lax_bool_false(self):
        for x in [0, "0", "false", "False", "fAlse", "faLse", "falSe", "falsE"]:
            assert convert(x, bool, strict=False) is False

    def test_lax_bool_true_invalid(self):
        for x in [-1, 3, "x", "xx", "xrue", "txue", "trxe", "trux"]:
            typ = type(x).__name__
            with pytest.raises(ValidationError, match=f"Expected `bool`, got `{typ}`"):
                assert convert(x, bool, strict=False)

    def test_lax_bool_false_invalid(self):
        for x in [-1, 3, "x", "xx", "xalse", "fxlse", "faxse", "falxe", "falsx"]:
            typ = type(x).__name__
            with pytest.raises(ValidationError, match=f"Expected `bool`, got `{typ}`"):
                assert convert(x, bool, strict=False)

    def test_lax_int_from_str(self):
        for x in ["1", "-1", "123456", "1.0"]:
            assert convert(x, int, strict=False) == int(float(x))

        for x in ["a", "1a", "1.5", "1..", "nan", "inf"]:
            with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
                convert(x, int, strict=False)

    def test_lax_int_from_float(self):
        bound = float(1 << 53)
        for x in [-bound, -1.0, -0.0, 0.0, 1.0, bound]:
            assert convert(x, int, strict=False) == int(x)

        for x in [-bound - 2, -1.5, 0.001, 1.5, bound + 2, float("inf"), float("nan")]:
            with pytest.raises(ValidationError, match="Expected `int`, got `float`"):
                convert(x, int, strict=False)

    def test_lax_int_constr(self):
        typ = Annotated[int, Meta(ge=0)]
        assert convert("1", typ, strict=False) == 1

        with pytest.raises(ValidationError):
            convert("-1", typ, strict=False)

    def test_lax_int_enum(self):
        class Ex(enum.IntEnum):
            x = 1
            y = -2

        assert convert("1", Ex, strict=False) is Ex.x
        assert convert("-2", Ex, strict=False) is Ex.y
        with pytest.raises(ValidationError, match="Invalid enum value 3"):
            convert("3", Ex, strict=False)
        with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
            convert("A", Ex, strict=False)

    def test_lax_int_literal(self):
        typ = Literal[1, -2]
        assert convert("1", typ, strict=False) == 1
        assert convert("-2", typ, strict=False) == -2
        with pytest.raises(ValidationError, match="Invalid enum value 3"):
            convert("3", typ, strict=False)
        with pytest.raises(ValidationError, match="Expected `int`, got `str`"):
            convert("A", typ, strict=False)

    def test_lax_float(self):
        for x in ["1", "-1", "123456", "1.5", "-1.5", "inf"]:
            assert convert(x, float, strict=False) == float(x)

        for x in ["a", "1a", "1.0.0", "1.."]:
            with pytest.raises(ValidationError, match="Expected `float`, got `str`"):
                convert(x, float, strict=False)

    @pytest.mark.parametrize("str_value", ["nan", "infinity"])
    @pytest.mark.parametrize("negative", [False, True])
    def test_lax_float_nonfinite(self, str_value, negative):
        prefix = "-" if negative else ""
        for i in range(len(str_value)):
            msg = prefix + str_value[:i] + str_value[i].upper() + str_value[i + 1 :]
            sol = float(msg)
            res = convert(msg, float, strict=False)
            assert_eq(res, sol)

    def test_lax_float_nonfinite_invalid(self):
        for bad in ["abcd", "-abcd", "inx", "-inx", "infinitx", "-infinitx", "nax"]:
            for msg in [bad, bad.upper()]:
                with pytest.raises(
                    ValidationError, match="Expected `float`, got `str`"
                ):
                    convert(msg, float, strict=False)

    def test_lax_float_constr(self):
        assert convert("1.5", Annotated[float, Meta(ge=0)], strict=False) == 1.5

        with pytest.raises(ValidationError):
            convert("-1.0", Annotated[float, Meta(ge=0)], strict=False)

    def test_lax_str(self):
        for x in ["1", "1.5", "false", "null"]:
            assert convert(x, str, strict=False) == x

    def test_lax_str_constr(self):
        typ = Annotated[str, Meta(max_length=10)]
        assert convert("xxx", typ, strict=False) == "xxx"

        with pytest.raises(ValidationError):
            convert("x" * 20, typ, strict=False)

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
    def test_lax_datetime(self, x, sign, transform):
        timestamp = x * sign
        msg = transform(timestamp) if transform else timestamp
        sol = datetime.datetime.fromtimestamp(timestamp, UTC)
        res = convert(msg, type=datetime.datetime, strict=False)
        assert res == sol

    def test_lax_datetime_nonfinite_values(self):
        for msg in ["nan", "-inf", "inf", float("nan"), float("inf"), float("-inf")]:
            with pytest.raises(ValidationError, match="Invalid epoch timestamp"):
                convert(msg, type=datetime.datetime, strict=False)

    @pytest.mark.parametrize("val", [-62135596801, 253402300801])
    @pytest.mark.parametrize("type", [int, float, str])
    def test_lax_datetime_out_of_range(self, val, type):
        msg = type(val)
        with pytest.raises(ValidationError, match="out of range"):
            convert(msg, type=datetime.datetime, strict=False)

    def test_lax_datetime_invalid_numeric_str(self):
        for msg in ["", "12e", "1234a", "1234-1", "1234.a"]:
            with pytest.raises(ValidationError, match="Invalid"):
                convert(msg, type=datetime.datetime, strict=False)

    @pytest.mark.parametrize("msg", [123, -123, 123.456, "123.456"])
    def test_lax_datetime_naive_required(self, msg):
        with pytest.raises(ValidationError, match="no timezone component"):
            convert(
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
    def test_lax_timedelta(self, x, sign, transform):
        timestamp = x * sign
        msg = transform(timestamp) if transform else timestamp
        sol = datetime.timedelta(seconds=timestamp)
        res = convert(msg, type=datetime.timedelta, strict=False)
        assert res == sol

    def test_lax_timedelta_nonfinite_values(self):
        for msg in ["nan", "-inf", "inf", float("nan"), float("inf"), float("-inf")]:
            with pytest.raises(ValidationError, match="Duration is out of range"):
                convert(msg, type=datetime.timedelta, strict=False)

    @pytest.mark.parametrize("val", [86400000000001, -86399999913601])
    @pytest.mark.parametrize("type", [int, float, str])
    def test_lax_timedelta_out_of_range(self, val, type):
        msg = type(val)
        with pytest.raises(ValidationError, match="out of range"):
            convert(msg, type=datetime.timedelta, strict=False)

    def test_lax_timedelta_invalid_numeric_str(self):
        for msg in ["", "12e", "1234a", "1234-1", "1234.a"]:
            with pytest.raises(ValidationError, match="Invalid"):
                convert(msg, type=datetime.timedelta, strict=False)

    @pytest.mark.parametrize(
        "msg, sol",
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
    def test_lax_union_valid(self, msg, sol):
        typ = Union[int, float, bool, None]
        assert_eq(convert(msg, typ, strict=False), sol)

    @pytest.mark.parametrize("msg", ["1a", "1.5a", "falsx", "trux", "nulx"])
    def test_lax_union_invalid(self, msg):
        typ = Union[int, float, bool, None]
        with pytest.raises(
            ValidationError, match="Expected `int | float | bool | null`"
        ):
            convert(msg, typ, strict=False)

    @pytest.mark.parametrize(
        "msg, err",
        [
            ("-1", "`int` >= 0"),
            ("2000", "`int` <= 1000"),
            ("18446744073709551616", "`int` <= 1000"),
            ("-9223372036854775809", "`int` >= 0"),
            ("100.5", "`float` <= 100.0"),
        ],
    )
    def test_lax_union_invalid_constr(self, msg, err):
        """Ensure that values that parse properly but don't meet the specified
        constraints error with a specific constraint error"""
        typ = Union[
            Annotated[int, Meta(ge=0), Meta(le=1000)],
            Annotated[float, Meta(le=100)],
        ]
        with pytest.raises(ValidationError, match=err):
            convert(msg, typ, strict=False)

    def test_lax_union_extended(self):
        typ = Union[int, float, bool, None, datetime.datetime]
        dt = datetime.datetime.now()
        assert_eq(convert("1", typ, strict=False), 1)
        assert_eq(convert("1.5", typ, strict=False), 1.5)
        assert_eq(convert("false", typ, strict=False), False)
        assert_eq(convert("null", typ, strict=False), None)
        assert_eq(convert(dt.isoformat(), typ, strict=False), dt)

    def test_lax_implies_str_keys(self):
        res = convert({"1": False}, Dict[int, bool], strict=False)
        assert res == {1: False}

    def test_lax_implies_no_builtin_types(self):
        sol = uuid.uuid4()
        msg = str(sol)
        res = convert(msg, uuid.UUID, strict=False, builtin_types=(uuid.UUID,))
        assert res == sol


class TestCustom:
    def test_custom(self):
        def dec_hook(typ, x):
            assert typ is complex
            return complex(*x)

        msg = {"x": (1, 2)}
        sol = {"x": complex(1, 2)}
        res = convert(msg, Dict[str, complex], dec_hook=dec_hook)
        assert res == sol

    def test_custom_no_dec_hook(self):
        with pytest.raises(ValidationError, match="Expected `complex`, got `str`"):
            convert({"x": "oh no"}, Dict[str, complex])

    def test_custom_dec_hook_errors(self):
        def dec_hook(typ, x):
            raise TypeError("Oops!")

        with pytest.raises(ValidationError, match="Oops!") as rec:
            convert({"x": (1, 2)}, Dict[str, complex], dec_hook=dec_hook)

        assert rec.value.__cause__ is rec.value.__context__
        assert type(rec.value.__cause__) is TypeError


class TestRaw:
    def test_raw(self):
        raw = msgspec.Raw(b"123")

        class Ex(Struct):
            x: msgspec.Raw

        sol = Ex(x=raw)
        assert convert({"x": raw}, type=Ex) == sol
