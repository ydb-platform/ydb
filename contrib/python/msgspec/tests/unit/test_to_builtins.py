import base64
import datetime
import decimal
import enum
import os
import sys
import uuid
import weakref
from dataclasses import dataclass, make_dataclass
from typing import Any, NamedTuple, Union

import pytest

from msgspec import UNSET, Struct, UnsetType, defstruct, to_builtins

PY310 = sys.version_info[:2] >= (3, 10)
PY311 = sys.version_info[:2] >= (3, 11)

py310_plus = pytest.mark.skipif(not PY310, reason="3.10+ only")
py311_plus = pytest.mark.skipif(not PY311, reason="3.11+ only")

slots_params = [False, pytest.param(True, marks=[py310_plus])]


class FruitInt(enum.IntEnum):
    APPLE = -1
    BANANA = 2

    def __eq__(self, other):
        assert type(other) is type(self)
        return super().__eq__(other)

    def __hash__(self):
        return super().__hash__()


class FruitStr(enum.Enum):
    APPLE = "apple"
    BANANA = "banana"

    def __eq__(self, other):
        assert type(other) is type(self)
        return super().__eq__(other)

    def __hash__(self):
        return super().__hash__()


class Bad:
    """A type that msgspec doesn't natively handle"""


class TestToBuiltins:
    def test_to_builtins_bad_calls(self):
        with pytest.raises(TypeError):
            to_builtins()

        with pytest.raises(
            TypeError, match="builtin_types must be an iterable of types"
        ):
            to_builtins([1], builtin_types=1)

        with pytest.raises(
            TypeError, match="builtin_types must be an iterable of types"
        ):
            to_builtins([1], builtin_types=(1,))

        with pytest.raises(TypeError, match="enc_hook must be callable"):
            to_builtins([1], enc_hook=1)

    def test_to_builtins_builtin_types_explicit_none(self):
        assert to_builtins(1, builtin_types=None) == 1

    def test_to_builtins_enc_hook_explicit_none(self):
        assert to_builtins(1, enc_hook=None) == 1

    @pytest.mark.parametrize("case", [1, 2, 3, 4, 5])
    def test_to_builtins_recursive(self, case):
        if case == 1:
            o = []
            o.append(o)
        elif case == 2:
            o = ([],)
            o[0].append(o)
        elif case == 3:
            o = {}
            o["a"] = o
        elif case == 4:

            class Box(Struct):
                a: "Box"

            o = Box(None)
            o.a = o
        elif case == 5:

            @dataclass
            class Box:
                a: "Box"

            o = Box(None)
            o.a = o

        with pytest.raises(RecursionError):
            to_builtins(o)

    def test_none(self):
        assert to_builtins(None) is None

    def test_bool(self):
        assert to_builtins(False) is False
        assert to_builtins(True) is True

    def test_int(self):
        assert to_builtins(1) == 1

    def test_float(self):
        assert to_builtins(1.5) == 1.5

    def test_str(self):
        assert to_builtins("abc") == "abc"

    @pytest.mark.parametrize("typ", [bytes, bytearray, memoryview])
    @pytest.mark.parametrize("size", range(5))
    def test_binary(self, typ, size):
        msg = typ(os.urandom(size))
        res = to_builtins(msg)
        sol = base64.b64encode(msg).decode("utf-8")
        assert res == sol

    @pytest.mark.parametrize("typ", [bytes, bytearray, memoryview])
    def test_binary_builtin_types(self, typ):
        msg = typ(b"\x01\x02\x03")
        res = to_builtins(msg, builtin_types=(typ,))
        assert res is msg

    @pytest.mark.parametrize("tzinfo", [None, datetime.timezone.utc])
    @pytest.mark.parametrize("microsecond", [123456, 123, 0])
    def test_datetime(self, tzinfo, microsecond):
        msg = datetime.datetime.now(tzinfo).replace(microsecond=microsecond)
        res = to_builtins(msg)
        sol = msg.isoformat().replace("+00:00", "Z")
        assert res == sol

    def test_datetime_builtin_types(self):
        msg = datetime.datetime.now()
        res = to_builtins(msg, builtin_types=(datetime.datetime,))
        assert res is msg

    def test_date(self):
        msg = datetime.date.today()
        res = to_builtins(msg)
        sol = msg.isoformat()
        assert res == sol

    def test_date_builtin_types(self):
        msg = datetime.date.today()
        res = to_builtins(msg, builtin_types=(datetime.date,))
        assert res is msg

    @pytest.mark.parametrize("tzinfo", [None, datetime.timezone.utc])
    @pytest.mark.parametrize("microsecond", [123456, 123, 0])
    def test_time(self, tzinfo, microsecond):
        msg = datetime.datetime.now(tzinfo).replace(microsecond=microsecond).timetz()
        res = to_builtins(msg)
        sol = msg.isoformat().replace("+00:00", "Z")
        assert res == sol

    def test_time_builtin_types(self):
        msg = datetime.datetime.now().time()
        res = to_builtins(msg, builtin_types=(datetime.time,))
        assert res is msg

    def test_timedelta(self):
        msg = datetime.timedelta(1, 2, 300)
        res = to_builtins(msg)
        assert res == "P1DT2.0003S"

    def test_timedelta_builtin_types(self):
        msg = datetime.timedelta(1, 2, 300)
        res = to_builtins(msg, builtin_types=(datetime.timedelta,))
        assert res is msg

    def test_uuid(self):
        msg = uuid.uuid4()
        assert to_builtins(msg) == str(msg)

    def test_uuid_subclass(self):
        class Ex(uuid.UUID):
            pass

        s = "4184defa-4d1a-4497-a140-fd1ec0b22383"
        assert to_builtins(Ex(s)) == s

    def test_uuid_builtin_types(self):
        msg = uuid.uuid4()
        res = to_builtins(msg, builtin_types=(uuid.UUID,))
        assert res is msg

    def test_decimal(self):
        msg = decimal.Decimal("1.5")
        assert to_builtins(msg) == str(msg)

    def test_decimal_builtin_types(self):
        msg = decimal.Decimal("1.5")
        res = to_builtins(msg, builtin_types=(decimal.Decimal,))
        assert res is msg

    def test_intenum(self):
        res = to_builtins(FruitInt.APPLE)
        assert res == -1
        assert type(res) is int

    def test_enum(self):
        res = to_builtins(FruitStr.APPLE)
        assert res == "apple"
        assert type(res) is str

    def test_enum_complex(self):
        class Complex(enum.Enum):
            x = (1, 2)

        res = to_builtins(Complex.x)
        assert res is Complex.x.value

    @pytest.mark.parametrize(
        "in_type, out_type",
        [(list, list), (tuple, tuple), (set, list), (frozenset, list)],
    )
    @pytest.mark.parametrize("subclass", [False, True])
    def test_sequence(self, in_type, out_type, subclass):
        if subclass:

            class in_type(in_type):
                pass

        msg = in_type([1, FruitInt.APPLE])
        res = to_builtins(msg)
        assert res == out_type([1, -1])
        assert res is not msg

        res = to_builtins(in_type())
        assert res == out_type()

    @pytest.mark.parametrize("in_type", [list, tuple, set, frozenset])
    def test_sequence_unsupported_item(self, in_type):
        msg = in_type([1, Bad(), 3])
        with pytest.raises(TypeError, match="Encoding objects of type Bad"):
            to_builtins(msg)

    def test_namedtuple(self):
        class Point(NamedTuple):
            x: int
            y: FruitInt

        assert to_builtins(Point(1, FruitInt.APPLE)) == (1, -1)

    @pytest.mark.parametrize("subclass", [False, True])
    def test_dict(self, subclass):
        if subclass:

            class in_type(dict):
                pass

        else:
            in_type = dict

        msg = in_type({FruitStr.BANANA: 1, "b": [FruitInt.APPLE], 3: "three"})
        sol = {"banana": 1, "b": [-1], 3: "three"}

        res = to_builtins(msg)
        assert res == sol
        assert res is not msg

        res = to_builtins(in_type())
        assert res == {}

    def test_dict_str_subclass_key(self):
        class mystr(str):
            pass

        msg = to_builtins({mystr("test"): 1})
        assert msg == {"test": 1}
        assert type(list(msg.keys())[0]) is str

    def test_dict_unsupported_key(self):
        msg = {Bad(): 1}
        with pytest.raises(TypeError, match="Encoding objects of type Bad"):
            to_builtins(msg)

    def test_dict_unsupported_value(self):
        msg = {"x": Bad()}
        with pytest.raises(TypeError, match="Encoding objects of type Bad"):
            to_builtins(msg)

    def test_dict_str_keys(self):
        assert to_builtins({FruitStr.BANANA: 1}, str_keys=True) == {"banana": 1}
        assert to_builtins({"banana": 1}, str_keys=True) == {"banana": 1}
        assert to_builtins({FruitInt.BANANA: 1}, str_keys=True) == {"2": 1}
        assert to_builtins({2: 1}, str_keys=True) == {"2": 1}

    def test_dict_sequence_keys(self):
        msg = {frozenset([1, 2]): 1}
        assert to_builtins(msg) == {(1, 2): 1}

        with pytest.raises(
            TypeError,
            match="Only dicts with str-like or number-like keys are supported",
        ):
            to_builtins(msg, str_keys=True)

    @pytest.mark.parametrize("tagged", [False, True])
    def test_struct_object(self, tagged):
        class Ex(Struct, tag=tagged):
            x: int
            y: FruitInt

        sol = {"type": "Ex", "x": 1, "y": -1} if tagged else {"x": 1, "y": -1}
        assert to_builtins(Ex(1, FruitInt.APPLE)) == sol

    def test_struct_object_omit_defaults(self):
        class Ex(Struct, omit_defaults=True):
            x: int
            a: list = []
            b: FruitStr = FruitStr.BANANA
            c: FruitInt = FruitInt.APPLE

        assert to_builtins(Ex(1)) == {"x": 1}
        assert to_builtins(Ex(1, a=[2])) == {"x": 1, "a": [2]}
        assert to_builtins(Ex(1, b=FruitStr.APPLE)) == {"x": 1, "b": "apple"}

    @pytest.mark.parametrize("tagged", [False, True])
    def test_struct_array(self, tagged):
        class Ex(Struct, array_like=True, tag=tagged):
            x: int
            y: FruitInt

        sol = ["Ex", 1, -1] if tagged else [1, -1]
        assert to_builtins(Ex(1, FruitInt.APPLE)) == sol

    @pytest.mark.parametrize("tagged", [False, True])
    def test_struct_array_keys(self, tagged):
        class Ex(Struct, array_like=True, tag=tagged, frozen=True):
            x: int
            y: FruitInt

        msg = {Ex(1, FruitInt.APPLE): "abc"}
        sol = {("Ex", 1, -1) if tagged else (1, -1): "abc"}
        assert to_builtins(msg) == sol

    @pytest.mark.parametrize("array_like", [False, True])
    def test_struct_unsupported_value(self, array_like):
        class Ex(Struct):
            a: Any
            b: Any

        msg = Ex(1, Bad())
        with pytest.raises(TypeError, match="Encoding objects of type Bad"):
            to_builtins(msg)

    @pytest.mark.parametrize("slots", slots_params)
    def test_dataclass(self, slots):
        @dataclass(**({"slots": True} if slots else {}))
        class Ex:
            x: int
            y: FruitInt

        msg = Ex(1, FruitInt.APPLE)
        assert to_builtins(msg) == {"x": 1, "y": -1}

    @pytest.mark.parametrize("slots", slots_params)
    def test_dataclass_missing_fields(self, slots):
        @dataclass(**({"slots": True} if slots else {}))
        class Ex:
            x: int
            y: int
            z: int

        x = Ex(1, 2, 3)
        sol = {"x": 1, "y": 2, "z": 3}
        for key in "xyz":
            delattr(x, key)
            del sol[key]
            assert to_builtins(x) == sol

    @pytest.mark.parametrize("slots_base", slots_params)
    @pytest.mark.parametrize("slots", slots_params)
    def test_dataclass_subclasses(self, slots_base, slots):
        @dataclass(**({"slots": True} if slots_base else {}))
        class Base:
            x: int
            y: int

        @dataclass(**({"slots": True} if slots else {}))
        class Ex(Base):
            y: int
            z: int

        x = Ex(1, 2, 3)
        res = to_builtins(x)
        assert res == {"x": 1, "y": 2, "z": 3}

        # Missing attribute ignored
        del x.y
        res = to_builtins(x)
        assert res == {"x": 1, "z": 3}

    @py311_plus
    def test_dataclass_weakref_slot(self):
        @dataclass(slots=True, weakref_slot=True)
        class Ex:
            x: int
            y: int

        x = Ex(1, 2)
        ref = weakref.ref(x)  # noqa
        res = to_builtins(x)
        assert res == {"x": 1, "y": 2}

    @pytest.mark.parametrize("slots", slots_params)
    def test_dataclass_unsupported_value(self, slots):
        @dataclass(**({"slots": True} if slots else {}))
        class Ex:
            x: Any
            y: Any

        msg = Ex(1, Bad())
        with pytest.raises(TypeError, match="Encoding objects of type Bad"):
            to_builtins(msg)

    def test_dataclass_class_errors(self):
        @dataclass
        class Ex:
            x: int

        with pytest.raises(TypeError, match="Encoding objects of type type"):
            to_builtins(Ex)

    @pytest.mark.parametrize("slots", [True, False])
    def test_attrs(self, slots):
        attrs = pytest.importorskip("attrs")

        @attrs.define(slots=slots)
        class Ex:
            x: int
            y: FruitInt

        msg = Ex(1, FruitInt.APPLE)
        assert to_builtins(msg) == {"x": 1, "y": -1}

    @pytest.mark.parametrize("slots", [True, False])
    def test_attrs_skip_leading_underscore(self, slots):
        attrs = pytest.importorskip("attrs")

        @attrs.define(slots=slots)
        class Ex:
            x: int
            y: int
            _z: int

        x = Ex(1, 2, 3)
        res = to_builtins(x)
        assert res == {"x": 1, "y": 2}

    @pytest.mark.parametrize("kind", ["struct", "dataclass", "attrs"])
    def test_unset_fields(self, kind):
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

        res = to_builtins(Ex(1, UNSET))
        assert res == {"x": 1}

        res = to_builtins(Ex(UNSET, 2))
        assert res == {"y": 2}

        res = to_builtins(Ex(UNSET, UNSET))
        assert res == {}

    def test_unset_errors_in_other_contexts(self):
        with pytest.raises(TypeError):
            to_builtins(UNSET)

    def test_custom(self):
        with pytest.raises(TypeError, match="Encoding objects of type Bad"):
            to_builtins(Bad())

        assert to_builtins(Bad(), enc_hook=lambda x: "bad") == "bad"

    @pytest.mark.parametrize("col_type", [tuple, list, set])
    def test_custom_builtin_types(self, col_type):
        class C1:
            pass

        class C2:
            pass

        builtins = col_type([C1, bytes, C2])
        count = sys.getrefcount(builtins)

        for msg in [C1(), C2(), b"test"]:
            assert to_builtins(msg, builtin_types=builtins) is msg

        with pytest.raises(TypeError, match="Encoding objects of type Bad"):
            to_builtins(Bad(), builtin_types=builtins)

        assert sys.getrefcount(builtins) == count


class TestOrder:
    def test_order_invalid(self):
        with pytest.raises(ValueError, match="`order` must be one of"):
            to_builtins(1, order="bad")

    @staticmethod
    def assert_eq(left, right):
        assert left == right
        if isinstance(left, dict):
            assert list(left) == list(right)

    @pytest.mark.parametrize("msg", [{}, {"y": 1, "x": 2, "z": 3}])
    @pytest.mark.parametrize("order", [None, "deterministic", "sorted"])
    def test_order_dict(self, msg, order):
        res = to_builtins(msg, order=order)
        sol = dict(sorted(msg.items())) if order else msg
        self.assert_eq(res, sol)

    def test_order_dict_non_str_errors(self):
        with pytest.raises(TypeError):
            to_builtins({"b": 2, 1: "a"}, order="deterministic")

    def test_order_dict_unsortable(self):
        with pytest.raises(TypeError):
            to_builtins({"x": 1, 1: 2}, order="deterministic")

    @pytest.mark.parametrize("typ", [set, frozenset])
    @pytest.mark.parametrize("order", ["deterministic", "sorted"])
    def test_order_set(self, typ, rand, order):
        assert to_builtins(typ(), order=order) == []

        msg = typ(rand.str(10) for _ in range(20))

        res = to_builtins(msg, order=order)
        self.assert_eq(res, sorted(msg))

        res = to_builtins(msg)
        self.assert_eq(res, list(msg))

    def test_order_set_unsortable(self):
        with pytest.raises(TypeError):
            to_builtins({"x", 1}, order="deterministic")

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
    def test_order_object(self, kind, n):
        fields = [f"x{i}" for i in range(n)]
        fields.reverse()
        if kind == "struct":
            cls = defstruct("Test", fields)
        elif kind == "dataclass":
            cls = make_dataclass("Test", fields)
        else:
            attrs = pytest.importorskip("attrs")
            cls = attrs.make_class("Test", fields, slots=(kind == "attrs"))
        msg = cls(*range(n))

        if kind in ("struct", "dataclass"):
            # we currently don't guarantee field order with attrs types
            res = to_builtins(msg)
            sol = dict(zip(fields, range(n)))
            self.assert_eq(res, sol)

            res = to_builtins(msg, order="deterministic")
            self.assert_eq(res, sol)

        res = to_builtins(msg, order="sorted")
        sol = dict(sorted(zip(fields, range(n))))
        self.assert_eq(res, sol)

    @pytest.mark.parametrize("kind", ["struct", "dataclass", "attrs", "attrs-dict"])
    def test_order_unset(self, kind):
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

        res = to_builtins(Ex(), order="sorted")
        self.assert_eq(res, {})

        res = to_builtins(Ex(z=10), order="sorted")
        self.assert_eq(res, {"z": 10})

        res = to_builtins(Ex(z=10, x=-1), order="sorted")
        self.assert_eq(res, {"x": -1, "z": 10})

    def test_order_struct_omit_defaults(self):
        class Ex(Struct, omit_defaults=True):
            z: int = 0
            x: int = 1
            y: int = 2

        res = to_builtins(Ex(), order="sorted")
        self.assert_eq(res, {})

        res = to_builtins(Ex(z=10), order="sorted")
        self.assert_eq(res, {"z": 10})

        res = to_builtins(Ex(z=10, x=-1), order="sorted")
        self.assert_eq(res, {"x": -1, "z": 10})

    def test_order_struct_tag(self):
        class Ex(Struct, tag_field="y", tag=2):
            z: int
            x: int

        res = to_builtins(Ex(0, 1), order="sorted")
        self.assert_eq(res, {"x": 1, "y": 2, "z": 0})
