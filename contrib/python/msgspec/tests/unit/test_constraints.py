import datetime
import math
import re
from typing import Annotated, Dict, List, Union

import pytest

import msgspec
from msgspec import Meta


@pytest.fixture(params=["json", "msgpack"])
def proto(request):
    if request.param == "json":
        return msgspec.json
    elif request.param == "msgpack":
        return msgspec.msgpack


FIELDS = {
    "gt": 0,
    "ge": 0,
    "lt": 10,
    "le": 10,
    "multiple_of": 1,
    "pattern": "^foo$",
    "min_length": 0,
    "max_length": 10,
    "tz": True,
    "title": "example title",
    "description": "example description",
    "examples": ["example 1", "example 2"],
    "extra_json_schema": {"foo": "bar"},
    "extra": {"fizz": "buzz"},
}


def assert_eq(a, b):
    assert a == b
    assert not a != b


def assert_ne(a, b):
    assert a != b
    assert not a == b


class TestMetaObject:
    def test_init_nokwargs(self):
        c = Meta()
        for f in FIELDS:
            assert getattr(c, f) is None

    @pytest.mark.parametrize("field", FIELDS)
    def test_init_explicit_none(self, field):
        c = Meta(**{field: None})
        for f in FIELDS:
            assert getattr(c, f) is None

    @pytest.mark.parametrize("field", FIELDS)
    def test_init(self, field):
        c = Meta(**{field: FIELDS[field]})
        for f in FIELDS:
            sol = FIELDS[field] if f == field else None
            assert getattr(c, f) == sol

    def test_repr_empty(self):
        assert repr(Meta()) == "msgspec.Meta()"
        for field in FIELDS:
            c = Meta(**{field: None})
            assert repr(c) == "msgspec.Meta()"

    def test_repr_error(self):
        class Oops:
            def __repr__(self):
                raise ValueError("Oh no!")

        m = Meta(extra_json_schema={"oops": Oops()})
        with pytest.raises(ValueError, match="Oh no!"):
            repr(m)

    @pytest.mark.parametrize("field", FIELDS)
    def test_repr_one_field(self, field):
        c = Meta(**{field: FIELDS[field]})
        assert repr(c) == f"msgspec.Meta({field}={FIELDS[field]!r})"

    def test_repr_multiple_fields(self):
        c = Meta(gt=0, lt=1)
        assert repr(c) == "msgspec.Meta(gt=0, lt=1)"

    def test_rich_repr_empty(self):
        assert Meta().__rich_repr__() == []

    @pytest.mark.parametrize("field", FIELDS)
    def test_rich_repr_one_field(self, field):
        m = Meta(**{field: FIELDS[field]})
        assert m.__rich_repr__() == [(field, FIELDS[field])]

    def test_rich_repr_multiple_fields(self):
        m = Meta(gt=0, lt=1)
        assert m.__rich_repr__() == [("gt", 0), ("lt", 1)]

    def test_equality(self):
        assert_eq(Meta(), Meta())
        assert_ne(Meta(), None)

        with pytest.raises(TypeError):
            Meta() > Meta()
        with pytest.raises(TypeError):
            Meta() > None

    def test_hash(self):
        def samples():
            return [
                Meta(),
                Meta(ge=0),
                Meta(ge=1, le=2),
                Meta(ge=1, le=2, examples=["stuff"]),
            ]

        lk = {k: k for k in samples()}

        for key in samples():
            assert lk[key] == key

    @pytest.mark.parametrize("field", FIELDS)
    def test_field_equality(self, field):
        val = FIELDS[field]
        if isinstance(val, dict):
            val2 = {}
        elif isinstance(val, list):
            val2 = []
        elif isinstance(val, bool):
            val2 = not val
        elif isinstance(val, int):
            val2 = val + 25
        else:
            val2 = "foobar"

        c = Meta(**{field: val})
        c2 = Meta(**{field: val})
        c3 = Meta(**{field: val2})
        c4 = Meta()
        assert_eq(c, c)
        assert_eq(c, c2)
        assert_ne(c, c3)
        assert_ne(c, c4)
        assert_ne(c4, c)

    @pytest.mark.parametrize("field", ["gt", "ge", "lt", "le", "multiple_of"])
    def test_numeric_fields(self, field):
        Meta(**{field: 1})
        Meta(**{field: 2.5})
        with pytest.raises(
            TypeError, match=f"`{field}` must be an int or float, got str"
        ):
            Meta(**{field: "bad"})

        with pytest.raises(ValueError, match=f"`{field}` must be finite"):
            Meta(**{field: float("inf")})

    @pytest.mark.parametrize("val", [0, 0.0])
    def test_multiple_of_bounds(self, val):
        with pytest.raises(ValueError, match=r"`multiple_of` must be > 0"):
            Meta(multiple_of=val)

    @pytest.mark.parametrize("field", ["min_length", "max_length"])
    def test_nonnegative_integer_fields(self, field):
        Meta(**{field: 0})
        Meta(**{field: 10})
        with pytest.raises(TypeError, match=f"`{field}` must be an int, got float"):
            Meta(**{field: 1.5})
        with pytest.raises(ValueError, match=f"{field}` must be >= 0, got -10"):
            Meta(**{field: -10})

    @pytest.mark.parametrize("field", ["pattern", "title", "description"])
    def test_string_fields(self, field):
        Meta(**{field: "good"})
        with pytest.raises(TypeError, match=f"`{field}` must be a str, got bytes"):
            Meta(**{field: b"bad"})

    @pytest.mark.parametrize("field", ["tz"])
    def test_bool_fields(self, field):
        Meta(**{field: True})
        Meta(**{field: False})
        with pytest.raises(TypeError, match=f"`{field}` must be a bool, got float"):
            Meta(**{field: 1.5})

    @pytest.mark.parametrize("field", ["examples"])
    def test_list_fields(self, field):
        Meta(**{field: ["good", "stuff"]})
        with pytest.raises(TypeError, match=f"`{field}` must be a list, got str"):
            Meta(**{field: "bad"})

    @pytest.mark.parametrize("field", ["extra_json_schema", "extra"])
    def test_dict_fields(self, field):
        Meta(**{field: {"good": "stuff"}})
        with pytest.raises(TypeError, match=f"`{field}` must be a dict, got str"):
            Meta(**{field: "bad"})

    def test_invalid_pattern_errors(self):
        with pytest.raises(re.error):
            Meta(pattern="[abc")

    def test_conflicting_bounds_errors(self):
        with pytest.raises(ValueError, match="both `gt` and `ge`"):
            Meta(gt=0, ge=1)

        with pytest.raises(ValueError, match="both `lt` and `le`"):
            Meta(lt=0, le=1)

    def test_mixing_numeric_and_nonnumeric_constraints_errors(self):
        with pytest.raises(ValueError, match="Cannot mix numeric constraints"):
            Meta(gt=0, pattern="foo")


class TestInvalidConstraintAnnotations:
    """Constraint validity is applied in two places:

    - Type checks on constraint values in the `Meta` constructor
    - Type checks on type & constraint annotations in Decoder constructors

    The tests here check the latter.
    """

    @pytest.mark.parametrize("name", ["ge", "gt", "le", "lt", "multiple_of"])
    def test_invalid_numeric_constraints(self, name):
        with pytest.raises(TypeError, match=f"Can only set `{name}` on a numeric type"):
            msgspec.json.Decoder(Annotated[str, Meta(**{name: 1})])

    def test_invalid_pattern_constraint(self):
        with pytest.raises(TypeError, match="Can only set `pattern` on a str type"):
            msgspec.json.Decoder(Annotated[int, Meta(pattern="ok")])

    @pytest.mark.parametrize("name", ["min_length", "max_length"])
    def test_invalid_length_constraint(self, name):
        with pytest.raises(
            TypeError,
            match=f"Can only set `{name}` on a str, bytes, or collection type",
        ):
            msgspec.json.Decoder(Annotated[int, Meta(**{name: 1})])

    def test_invalid_tz_constraint(self):
        with pytest.raises(
            TypeError,
            match="Can only set `tz` on a datetime or time type",
        ):
            msgspec.json.Decoder(Annotated[int, Meta(tz=True)])

    @pytest.mark.parametrize(
        "name, val",
        [("ge", 2**63), ("gt", 2**63 - 1), ("le", 2**63), ("lt", -(2**63))],
    )
    def test_invalid_integer_bounds(self, name, val):
        with pytest.raises(ValueError) as rec:
            msgspec.json.Decoder(Annotated[int, Meta(**{name: val})])
        assert name in str(rec.value)
        assert "not supported" in str(rec.value)

    def test_invalid_multiple_meta_annotations_conflict(self):
        with pytest.raises(TypeError, match="Multiple `Meta` annotations"):
            msgspec.json.Decoder(Annotated[int, Meta(ge=1), Meta(ge=2)])

    def test_invalid_gt_and_ge_conflict(self):
        with pytest.raises(TypeError, match="Cannot set both `gt` and `ge`"):
            msgspec.json.Decoder(Annotated[int, Meta(gt=1), Meta(ge=2)])

    def test_invalid_lt_and_le_conflict(self):
        with pytest.raises(TypeError, match="Cannot set both `lt` and `le`"):
            msgspec.json.Decoder(Annotated[int, Meta(lt=2), Meta(le=1)])


class TestIntConstraints:
    @pytest.mark.parametrize(
        "name, bound, good, bad",
        [
            ("ge", -1, [-1, 2**63, 2**65], [-(2**64), -2]),
            ("gt", -1, [0, 2**63, 2**65], [-(2**64), -1]),
            ("le", -1, [-(2**64), -1], [0, 2**63, 2**65]),
            ("lt", -1, [-(2**64), -2], [-1, 2**63, 2**65]),
        ],
    )
    def test_bounds(self, proto, name, bound, good, bad):
        if proto is msgspec.msgpack:
            # msgpack only supports int64/uint64 values
            good = [i for i in good if -(2**63) - 1 <= i <= 2**64]
            bad = [i for i in bad if -(2**63) - 1 <= i <= 2**64]

        class Ex(msgspec.Struct):
            x: Annotated[int, Meta(**{name: bound})]

        dec = proto.Decoder(Ex)

        for x in good:
            assert dec.decode(proto.encode(Ex(x))).x == x

        op = ">=" if name.startswith("g") else "<="
        offset = {"lt": -1, "gt": 1}.get(name, 0)
        err_msg = rf"Expected `int` {op} {bound + offset} - at `\$.x`"
        for x in bad:
            with pytest.raises(msgspec.ValidationError, match=err_msg):
                dec.decode(proto.encode(Ex(x)))

    def test_multiple_of(self, proto):
        good = [-(2**64), -2, 0, 2, 40, 2**63 + 2, 2**65]
        bad = [1, -1, 2**63 + 1, 2**65 + 1]
        if proto is msgspec.msgpack:
            # msgpack only supports int64/uint64 values
            good = [i for i in good if -(2**63) - 1 <= i <= 2**64]
            bad = [i for i in bad if -(2**63) - 1 <= i <= 2**64]

        class Ex(msgspec.Struct):
            x: Annotated[int, Meta(multiple_of=2)]

        dec = proto.Decoder(Ex)

        for x in good:
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = r"Expected `int` that's a multiple of 2 - at `\$.x`"
        for x in bad:
            with pytest.raises(msgspec.ValidationError, match=err_msg):
                dec.decode(proto.encode(Ex(x)))

    @pytest.mark.parametrize(
        "meta, good, bad",
        [
            (Meta(ge=0, le=10, multiple_of=2), [0, 2, 10], [-1, 1, 11]),
            (Meta(ge=0, multiple_of=2), [0, 2**63 + 2], [-2, 2**63 + 1]),
            (Meta(le=0, multiple_of=2), [0, -(2**63)], [-1, 2, 2**63]),
            (Meta(ge=0, le=10), [0, 10], [-1, 11]),
            (Meta(gt=0, lt=10), [1, 2, 9], [-1, 0, 10]),
        ],
    )
    def test_combinations(self, proto, meta, good, bad):
        class Ex(msgspec.Struct):
            x: Annotated[int, meta]

        dec = proto.Decoder(Ex)

        for x in good:
            assert dec.decode(proto.encode(Ex(x))).x == x

        for x in bad:
            with pytest.raises(msgspec.ValidationError):
                dec.decode(proto.encode(Ex(x)))


class TestFloatConstraints:
    @pytest.mark.parametrize("name", ["ge", "gt", "le", "lt"])
    def test_bound_constraint_uint64_valid_for_floats(self, name):
        typ = Annotated[float, Meta(**{name: 2**63})]
        msgspec.json.Decoder(typ)

    def get_bounds_cases(self, name, bound):
        def ceilp1(x):
            return int(math.ceil(x + 1))

        def floorm1(x):
            return int(math.floor(x - 1))

        if name.startswith("g"):
            good_dir = math.inf
            good_round = ceilp1
            bad_round = floorm1
        else:
            good_dir = -math.inf
            good_round = floorm1
            bad_round = ceilp1

        if name.endswith("e"):
            good = bound
            bad = math.nextafter(bound, -good_dir)
        else:
            good = math.nextafter(bound, good_dir)
            bad = bound
        good_cases = [good, good_round(good), float(good_round(good))]
        bad_cases = [bad, bad_round(bad), float(bad_round(bad))]

        op = ">" if name.startswith("g") else "<"
        if name.endswith("e"):
            op += "="

        return good_cases, bad_cases, op

    @pytest.mark.parametrize("name", ["ge", "gt", "le", "lt"])
    @pytest.mark.parametrize("bound", [1.5, -1.5, 10.0])
    def test_bounds(self, proto, name, bound):
        class Ex(msgspec.Struct):
            x: Annotated[float, Meta(**{name: bound})]

        dec = proto.Decoder(Ex)

        good, bad, op = self.get_bounds_cases(name, bound)

        for x in good:
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = rf"Expected `float` {op} {bound} - at `\$.x`"
        for x in bad:
            with pytest.raises(msgspec.ValidationError, match=err_msg):
                dec.decode(proto.encode(Ex(x)))

    def test_multiple_of(self, proto):
        """multipleOf for floats will always have precisions issues. This check
        just ensures that _some_ cases work. See
        https://github.com/json-schema-org/json-schema-spec/issues/312 for more
        info."""

        class Ex(msgspec.Struct):
            x: Annotated[float, Meta(multiple_of=0.1)]

        dec = proto.Decoder(Ex)

        for x in [0, 0.0, 0.1, -0.1, 0.2, -0.2]:
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = r"Expected `float` that's a multiple of 0.1 - at `\$.x`"
        for x in [0.01, -0.15]:
            with pytest.raises(msgspec.ValidationError, match=err_msg):
                dec.decode(proto.encode(Ex(x)))

    @pytest.mark.parametrize(
        "meta, good, bad",
        [
            (Meta(ge=0.0, le=10.0, multiple_of=2.0), [0, 2.0, 10], [-2, 11, 3]),
            (Meta(ge=0.0, multiple_of=2.0), [0, 2, 10.0], [-2, 3]),
            (Meta(le=10.0, multiple_of=2.0), [-2.0, 10.0], [11.0, 3.0]),
            (Meta(ge=0.0, le=10.0), [0.0, 2.0, 10.0], [-1.0, 11.5, 11]),
        ],
    )
    def test_combinations(self, proto, meta, good, bad):
        class Ex(msgspec.Struct):
            x: Annotated[float, meta]

        dec = proto.Decoder(Ex)

        for x in good:
            assert dec.decode(proto.encode(Ex(x))).x == x

        for x in bad:
            with pytest.raises(msgspec.ValidationError):
                assert dec.decode(proto.encode(Ex(x)))


class TestStrConstraints:
    def test_min_length(self, proto):
        class Ex(msgspec.Struct):
            x: Annotated[str, Meta(min_length=2)]

        dec = proto.Decoder(Ex)

        for x in ["xx", "xxx", "𝄞x"]:
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = r"Expected `str` of length >= 2 - at `\$.x`"
        for x in ["x", "𝄞", ""]:
            with pytest.raises(msgspec.ValidationError, match=err_msg):
                dec.decode(proto.encode(Ex(x)))

    def test_max_length(self, proto):
        class Ex(msgspec.Struct):
            x: Annotated[str, Meta(max_length=2)]

        dec = proto.Decoder(Ex)

        for x in ["", "xx", "𝄞x"]:
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = r"Expected `str` of length <= 2 - at `\$.x`"
        for x in ["xxx", "𝄞xx"]:
            with pytest.raises(msgspec.ValidationError, match=err_msg):
                dec.decode(proto.encode(Ex(x)))

    @pytest.mark.parametrize(
        "pattern, good, bad",
        [
            ("", ["", "test"], []),
            ("as", ["as", "ease", "ast", "pass"], ["", "nope"]),
            ("^pre[123]*$", ["pre1", "pre123"], ["apre1", "pre1two"]),
        ],
    )
    def test_pattern(self, proto, pattern, good, bad):
        class Ex(msgspec.Struct):
            x: Annotated[str, Meta(pattern=pattern)]

        dec = proto.Decoder(Ex)

        for x in good:
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = f"Expected `str` matching regex {pattern!r} - at `$.x`"
        for x in bad:
            with pytest.raises(msgspec.ValidationError) as rec:
                dec.decode(proto.encode(Ex(x)))
            assert str(rec.value) == err_msg

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
    def test_combinations(self, proto, meta, good, bad):
        class Ex(msgspec.Struct):
            x: Annotated[str, meta]

        dec = proto.Decoder(Ex)

        for x in good:
            assert dec.decode(proto.encode(Ex(x))).x == x

        for x in bad:
            with pytest.raises(msgspec.ValidationError):
                dec.decode(proto.encode(Ex(x)))

    @pytest.mark.parametrize(
        "meta, good, bad",
        [
            (Meta(min_length=2), ["xy", "𝄞xy"], ["", "𝄞"]),
            (Meta(pattern="as"), ["as", "pass", "𝄞as"], ["", "nope", "𝄞"]),
        ],
    )
    def test_str_constraints_on_dict_keys(self, proto, meta, good, bad):
        dec = proto.Decoder(Dict[Annotated[str, meta], int])

        for x in good:
            assert dec.decode(proto.encode({x: 1})) == {x: 1}

        for x in bad:
            with pytest.raises(msgspec.ValidationError):
                dec.decode(proto.encode({x: 1}))


class TestDateTimeConstraints:
    @staticmethod
    def roundtrip(proto, cls, aware, as_str):
        dt = datetime.datetime.now(datetime.timezone.utc if aware else None)

        if as_str:
            s = proto.encode(cls(dt.isoformat()))
        else:
            s = proto.encode(cls(dt))

        res = proto.decode(s, type=cls)
        assert res.x == dt

    @pytest.mark.parametrize("as_str", [True, False])
    def test_tz_none(self, proto, as_str):
        class Ex(msgspec.Struct):
            x: Annotated[datetime.datetime, Meta(tz=None)]

        self.roundtrip(proto, Ex, True, as_str)
        self.roundtrip(proto, Ex, False, as_str)

    @pytest.mark.parametrize("as_str", [True, False])
    def test_tz_false(self, proto, as_str):
        class Ex(msgspec.Struct):
            x: Annotated[datetime.datetime, Meta(tz=False)]

        self.roundtrip(proto, Ex, False, as_str)

        err_msg = r"Expected `datetime` with no timezone component - at `\$.x`"

        with pytest.raises(msgspec.ValidationError, match=err_msg):
            self.roundtrip(proto, Ex, True, as_str)

    @pytest.mark.parametrize("as_str", [True, False])
    def test_tz_true(self, proto, as_str):
        class Ex(msgspec.Struct):
            x: Annotated[datetime.datetime, Meta(tz=True)]

        self.roundtrip(proto, Ex, True, as_str)

        err_msg = r"Expected `datetime` with a timezone component - at `\$.x`"

        with pytest.raises(msgspec.ValidationError, match=err_msg):
            self.roundtrip(proto, Ex, False, as_str)


class TestTimeConstraints:
    @staticmethod
    def roundtrip(proto, cls, aware, as_str):
        dt = datetime.datetime.now(datetime.timezone.utc if aware else None).timetz()

        if as_str:
            s = proto.encode(cls(dt.isoformat()))
        else:
            s = proto.encode(cls(dt))

        res = proto.decode(s, type=cls)
        assert res.x == dt

    @pytest.mark.parametrize("as_str", [True, False])
    def test_tz_none(self, proto, as_str):
        class Ex(msgspec.Struct):
            x: Annotated[datetime.time, Meta(tz=None)]

        self.roundtrip(proto, Ex, True, as_str)
        self.roundtrip(proto, Ex, False, as_str)

    @pytest.mark.parametrize("as_str", [True, False])
    def test_tz_false(self, proto, as_str):
        class Ex(msgspec.Struct):
            x: Annotated[datetime.time, Meta(tz=False)]

        self.roundtrip(proto, Ex, False, as_str)

        err_msg = r"Expected `time` with no timezone component - at `\$.x`"

        with pytest.raises(msgspec.ValidationError, match=err_msg):
            self.roundtrip(proto, Ex, True, as_str)

    @pytest.mark.parametrize("as_str", [True, False])
    def test_tz_true(self, proto, as_str):
        class Ex(msgspec.Struct):
            x: Annotated[datetime.time, Meta(tz=True)]

        self.roundtrip(proto, Ex, True, as_str)

        err_msg = r"Expected `time` with a timezone component - at `\$.x`"

        with pytest.raises(msgspec.ValidationError, match=err_msg):
            self.roundtrip(proto, Ex, False, as_str)


class TestBytesConstraints:
    @pytest.mark.parametrize("typ", [bytes, bytearray, memoryview])
    def test_min_length(self, proto, typ):
        class Ex(msgspec.Struct):
            x: Annotated[typ, Meta(min_length=2)]

        dec = proto.Decoder(Ex)

        for x in [b"xx", b"xxx"]:
            assert bytes(dec.decode(proto.encode(Ex(x))).x) == x

        err_msg = r"Expected `bytes` of length >= 2 - at `\$.x`"
        for x in [b"", b"x"]:
            with pytest.raises(msgspec.ValidationError, match=err_msg):
                dec.decode(proto.encode(Ex(x)))

    @pytest.mark.parametrize("typ", [bytes, bytearray, memoryview])
    def test_max_length(self, proto, typ):
        class Ex(msgspec.Struct):
            x: Annotated[typ, Meta(max_length=2)]

        dec = proto.Decoder(Ex)

        for x in [b"", b"xx"]:
            assert bytes(dec.decode(proto.encode(Ex(x))).x) == x

        err_msg = r"Expected `bytes` of length <= 2 - at `\$.x`"
        with pytest.raises(msgspec.ValidationError, match=err_msg):
            dec.decode(proto.encode(Ex(b"xxx")))

    @pytest.mark.parametrize("typ", [bytes, bytearray, memoryview])
    def test_combinations(self, proto, typ):
        class Ex(msgspec.Struct):
            x: Annotated[typ, Meta(min_length=2, max_length=4)]

        dec = proto.Decoder(Ex)

        for x in [b"xx", b"xxx", b"xxxx"]:
            assert bytes(dec.decode(proto.encode(Ex(x))).x) == x

        for x in [b"x", b"xxxxx"]:
            with pytest.raises(msgspec.ValidationError):
                dec.decode(proto.encode(Ex(x)))


class TestArrayConstraints:
    @pytest.mark.parametrize("typ", [list, tuple, set, frozenset])
    def test_min_length(self, proto, typ):
        class Ex(msgspec.Struct):
            x: Annotated[typ, Meta(min_length=2)]

        dec = proto.Decoder(Ex)

        for n in [2, 3]:
            x = typ(range(n))
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = r"Expected `array` of length >= 2 - at `\$.x`"
        for n in [0, 1]:
            x = typ(range(n))
            with pytest.raises(msgspec.ValidationError, match=err_msg):
                dec.decode(proto.encode(Ex(x)))

    @pytest.mark.parametrize("typ", [list, tuple, set, frozenset])
    def test_max_length(self, proto, typ):
        class Ex(msgspec.Struct):
            x: Annotated[typ, Meta(max_length=2)]

        dec = proto.Decoder(Ex)

        for n in [0, 2]:
            x = typ(range(n))
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = r"Expected `array` of length <= 2 - at `\$.x`"
        with pytest.raises(msgspec.ValidationError, match=err_msg):
            dec.decode(proto.encode(Ex(typ(range(3)))))

    @pytest.mark.parametrize("typ", [list, tuple, set, frozenset])
    def test_combinations(self, proto, typ):
        class Ex(msgspec.Struct):
            x: Annotated[typ, Meta(min_length=2, max_length=4)]

        dec = proto.Decoder(Ex)

        for n in [2, 3, 4]:
            x = typ(range(n))
            assert dec.decode(proto.encode(Ex(x))).x == x

        for n in [1, 5]:
            x = typ(range(n))
            with pytest.raises(msgspec.ValidationError):
                dec.decode(proto.encode(Ex(x)))


class TestMapConstraints:
    def test_min_length(self, proto):
        class Ex(msgspec.Struct):
            x: Annotated[Dict[str, int], Meta(min_length=2)]

        dec = proto.Decoder(Ex)

        for n in [2, 3]:
            x = {str(i): i for i in range(n)}
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = r"Expected `object` of length >= 2 - at `\$.x`"
        for n in [0, 1]:
            x = {str(i): i for i in range(n)}
            with pytest.raises(msgspec.ValidationError, match=err_msg):
                dec.decode(proto.encode(Ex(x)))

    def test_max_length(self, proto):
        class Ex(msgspec.Struct):
            x: Annotated[Dict[str, int], Meta(max_length=2)]

        dec = proto.Decoder(Ex)

        for n in [0, 2]:
            x = {str(i): i for i in range(n)}
            assert dec.decode(proto.encode(Ex(x))).x == x

        err_msg = r"Expected `object` of length <= 2 - at `\$.x`"
        x = {"1": 1, "2": 2, "3": 3}
        with pytest.raises(msgspec.ValidationError, match=err_msg):
            dec.decode(proto.encode(Ex(x)))

    def test_combinations(self, proto):
        class Ex(msgspec.Struct):
            x: Annotated[Dict[str, int], Meta(min_length=2, max_length=4)]

        dec = proto.Decoder(Ex)

        for n in [2, 3, 4]:
            x = {str(i): i for i in range(n)}
            assert dec.decode(proto.encode(Ex(x))).x == x

        for n in [1, 5]:
            x = {str(i): i for i in range(n)}
            with pytest.raises(msgspec.ValidationError):
                dec.decode(proto.encode(Ex(x)))


class TestUnionConstraints:
    def test_mix_float_and_int(self, proto):
        class Ex(msgspec.Struct):
            x: Union[
                Annotated[int, Meta(ge=0, le=10)],
                Annotated[float, Meta(ge=1000, le=2000)],
            ]

        dec = proto.Decoder(Ex)

        for x in [0, 5, 10, 1000.0, 1234.5, 2000.0]:
            assert dec.decode(proto.encode(Ex(x))).x == x

        for x in [0.0, 10.0, 1000, 2000]:
            with pytest.raises(msgspec.ValidationError):
                dec.decode(proto.encode(Ex(x)))

    def test_mix_length_constraints(self, proto):
        class Ex(msgspec.Struct):
            x: Union[
                Annotated[Dict[str, int], Meta(min_length=1, max_length=2)],
                Annotated[List[int], Meta(min_length=3, max_length=4)],
                Annotated[str, Meta(min_length=5, max_length=6)],
            ]

        dec = proto.Decoder(Ex)

        for x in [{"x": 1}, [1, 2, 3], "xxxxx"]:
            assert dec.decode(proto.encode(Ex(x))).x == x

        for x in [{}, [1], "x"]:
            with pytest.raises(msgspec.ValidationError):
                dec.decode(proto.encode(Ex(x)))
