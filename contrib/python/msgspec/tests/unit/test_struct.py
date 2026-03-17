import copy
import datetime
import enum
import gc
import operator
import pickle
import sys
import weakref
from contextlib import contextmanager
from inspect import Parameter, Signature
from typing import Any, Generic, List, Optional, TypeVar

import pytest

import msgspec
from msgspec import NODEFAULT, UNSET, Struct, defstruct, field
from msgspec.structs import StructConfig

from .utils import temp_module

if hasattr(copy, "replace"):
    # Added in Python 3.13
    copy_replace = copy.replace
else:

    def copy_replace(s, **changes):
        return s.__replace__(**changes)


@contextmanager
def nogc():
    """Temporarily disable GC"""
    try:
        gc.disable()
        yield
    finally:
        gc.enable()


class Fruit(enum.IntEnum):
    APPLE = 1
    BANANA = 2


def as_tuple(x):
    return tuple(getattr(x, f) for f in x.__struct_fields__)


@pytest.mark.parametrize("obj, str_obj", [(UNSET, "UNSET"), (NODEFAULT, "NODEFAULT")])
def test_singletons(obj, str_obj):
    assert str(obj) == str_obj
    assert pickle.loads(pickle.dumps(obj)) is obj

    cls = type(obj)
    assert cls() is obj
    with pytest.raises(TypeError):
        cls(1)
    with pytest.raises(TypeError):
        cls(foo=1)
    if obj is UNSET:
        assert bool(obj) is False
    else:
        assert bool(obj) is True


def test_field():
    f1 = msgspec.field()
    assert f1.default is NODEFAULT
    assert f1.default_factory is NODEFAULT
    assert f1.name is None

    f2 = msgspec.field(default=1)
    assert f2.default == 1
    assert f2.default_factory is NODEFAULT
    assert f2.name is None

    f3 = msgspec.field(default_factory=int)
    assert f3.default is NODEFAULT
    assert f3.default_factory is int
    assert f3.name is None

    f4 = msgspec.field(name="foo")
    assert f4.name == "foo"

    f5 = msgspec.field(name=None)
    assert f5.name is None

    with pytest.raises(TypeError, match="Cannot set both"):
        msgspec.field(default=1, default_factory=int)

    with pytest.raises(TypeError, match="must be callable"):
        msgspec.field(default_factory=1)

    with pytest.raises(TypeError, match="must be a str or None"):
        msgspec.field(name=b"bad")


def test_struct_class_attributes():
    assert Struct.__struct_fields__ == ()
    assert Struct.__struct_encode_fields__ == ()
    assert Struct.__struct_defaults__ == ()
    assert Struct.__match_args__ == ()
    assert Struct.__slots__ == ()
    assert Struct.__module__ == "msgspec"
    assert isinstance(Struct.__struct_config__, StructConfig)


def test_struct_class_and_instance_dir():
    expected = {"__struct_fields__", "__struct_config__"}
    assert expected.issubset(dir(Struct))
    assert expected.issubset(dir(Struct()))


def test_struct_instance_attributes():
    class Test(Struct):
        c: int
        b: float
        a: str = "hello"

    x = Test(1, 2.0, a="goodbye")

    assert x.__struct_fields__ == ("c", "b", "a")
    assert x.__struct_encode_fields__ == ("c", "b", "a")
    assert x.__struct_fields__ is x.__struct_encode_fields__
    assert x.__struct_defaults__ == ("hello",)
    assert x.__slots__ == ("a", "b", "c")
    assert isinstance(x.__struct_config__, StructConfig)

    assert x.c == 1
    assert x.b == 2.0
    assert x.a == "goodbye"


def test_struct_subclass_forbids_init_new_slots():
    with pytest.raises(TypeError, match="__init__"):

        class Test1(Struct):
            a: int

            def __init__(self, a):
                pass

    with pytest.raises(TypeError, match="__new__"):

        class Test2(Struct):
            a: int

            def __new__(self, a):
                pass

    with pytest.raises(TypeError, match="__slots__"):

        class Test3(Struct):
            __slots__ = ("a",)
            a: int


def test_struct_subclass_forbidden_field_names():
    with pytest.raises(
        TypeError, match="Cannot have a struct field named '__weakref__'"
    ):

        class Test1(Struct):
            __weakref__: int

    with pytest.raises(TypeError, match="Cannot have a struct field named '__dict__'"):

        class Test2(Struct):
            __dict__: int

    with pytest.raises(
        TypeError, match="Cannot have a struct field named '__msgspec_cached_hash__'"
    ):

        class Test3(Struct):
            __msgspec_cached_hash__: int


class TestMixins:
    def test_mixin_no_slots(self):
        class Mixin(object):
            def method(self):
                pass

        class Test1(Struct, Mixin):
            pass

        assert issubclass(Test1, Mixin)
        assert Test1.__dictoffset__ != 0
        assert Test1.__weakrefoffset__ != 0

        class Test2(Struct, Mixin, dict=True, weakref=True):
            pass

        assert Test2.__dictoffset__ != 0
        assert Test2.__weakrefoffset__ != 0

    def test_mixin_slots(self):
        class Mixin(object):
            __slots__ = ()

            def method(self):
                pass

        class Test1(Struct, Mixin):
            pass

        assert issubclass(Test1, Mixin)
        assert Test1.__dictoffset__ == 0
        assert Test1.__weakrefoffset__ == 0

        class Test2(Struct, Mixin, dict=True, weakref=True):
            pass

        assert Test2.__dictoffset__ != 0
        assert Test2.__weakrefoffset__ != 0

    def test_mixin_nonempty_slots(self):
        class Mixin(object):
            __slots__ = "_state"

            def method(self):
                try:
                    return self._state
                except AttributeError:
                    self._state = self.x + 1
                    return self._state

        class Test(Struct, Mixin):
            x: int

        assert Test.__dictoffset__ == 0

        t = Test(1)
        assert t.method() == 2
        assert t.method() == 2

    def test_mixin_forbids_init(self):
        class Mixin(object):
            def __init__(self):
                pass

        with pytest.raises(TypeError, match="cannot define __init__"):

            class Test(Struct, Mixin):
                pass

    def test_mixin_forbids_new(self):
        class Mixin(object):
            def __new__(self):
                pass

        with pytest.raises(TypeError, match="cannot define __new__"):

            class Test(Struct, Mixin):
                pass

    def test_mixin_builtin_type_errors(self):
        with pytest.raises(TypeError):

            class Test(Struct, Exception):
                pass


def test_struct_subclass_forbids_non_types():
    # Currently this failcase is handled by CPython's internals, but it's good
    # to make sure this user error actually errors.
    class Foo:
        pass

    with pytest.raises(TypeError):

        class Test(msgspec.Struct, Foo()):
            pass


def test_struct_subclass_forbids_mixed_layouts():
    class A(Struct):
        a: int
        b: int

    class B(Struct):
        c: int
        d: int

    # This error is raised by cpython
    with pytest.raises(TypeError, match="lay-out conflict"):

        class C(A, B):
            pass


def test_struct_errors_nicely_if_used_in_init_subclass():
    ran = False

    class Test(Struct):
        def __init_subclass__(cls):
            # Class attributes aren't yet defined, error nicely
            for attr in [
                "__struct_fields__",
                "__struct_encode_fields__",
                "__match_args__",
                "__struct_defaults__",
            ]:
                with pytest.raises(AttributeError):
                    getattr(cls, attr)

            # Init doesn't work
            with pytest.raises(Exception):
                cls()

            # Decoder/decode doesn't work
            for proto in [msgspec.json, msgspec.msgpack]:
                with pytest.raises(ValueError, match="isn't fully defined"):
                    proto.Decoder(cls)

                with pytest.raises(ValueError, match="isn't fully defined"):
                    proto.decode(b"", type=cls)

            nonlocal ran
            ran = True

    class Subclass(Test):
        x: int

    assert ran


class TestStructParameterOrdering:
    """Tests for parsing parameter types & defaults from one or more class
    definitions."""

    def test_no_args(self):
        class Test(Struct):
            pass

        assert Test.__struct_fields__ == ()
        assert Test.__struct_defaults__ == ()
        assert Test.__match_args__ == ()
        assert Test.__slots__ == ()

    def test_all_positional(self):
        class Test(Struct):
            y: float
            x: int

        assert Test.__struct_fields__ == ("y", "x")
        assert Test.__struct_defaults__ == ()
        assert Test.__match_args__ == ("y", "x")
        assert Test.__slots__ == ("x", "y")

    def test_all_positional_with_defaults(self):
        class Test(Struct):
            y: int = 1
            x: float = 2.0

        assert Test.__struct_fields__ == ("y", "x")
        assert Test.__struct_defaults__ == (1, 2.0)
        assert Test.__match_args__ == ("y", "x")
        assert Test.__slots__ == ("x", "y")

    def test_subclass_no_change(self):
        class Test(Struct):
            y: float
            x: int

        class Test2(Test):
            pass

        assert Test2.__struct_fields__ == ("y", "x")
        assert Test2.__struct_defaults__ == ()
        assert Test2.__match_args__ == ("y", "x")
        assert Test2.__slots__ == ()

    def test_subclass_extends(self):
        class Test(Struct):
            c: int
            b: float
            d: int = 1
            a: float = 2.0

        class Test2(Test):
            e: str = "3.0"
            f: float = 4.0

        assert Test2.__struct_fields__ == ("c", "b", "d", "a", "e", "f")
        assert Test2.__struct_defaults__ == (1, 2.0, "3.0", 4.0)
        assert Test2.__match_args__ == ("c", "b", "d", "a", "e", "f")
        assert Test2.__slots__ == ("e", "f")

    def test_subclass_overrides(self):
        class Test(Struct):
            c: int
            b: int
            d: int = 1
            a: float = 2.0

        class Test2(Test):
            b: float = 3  # switch to keyword, change type
            d: int = 4  # change default
            e: float = 5.0  # new

        assert Test2.__struct_fields__ == ("c", "b", "d", "a", "e")
        assert Test2.__struct_defaults__ == (3, 4, 2.0, 5.0)
        assert Test2.__match_args__ == ("c", "b", "d", "a", "e")
        assert Test2.__slots__ == ("e",)

    def test_subclass_with_mixin(self):
        class A(Struct):
            b: int
            a: float = 1.0

        class Mixin(Struct):
            pass

        class B(A, Mixin):
            a: float = 2.0

        assert B.__struct_fields__ == ("b", "a")
        assert B.__struct_defaults__ == (2.0,)
        assert B.__match_args__ == ("b", "a")
        assert B.__slots__ == ()

    def test_positional_after_keyword_errors(self):
        with pytest.raises(TypeError) as rec:

            class Test(Struct):
                a: int
                b: int = 1
                c: float

        assert "Required field 'c' cannot follow optional fields" in str(rec.value)

    def test_positional_after_keyword_subclass_errors(self):
        class Base(Struct):
            a: int
            b: int = 1

        with pytest.raises(TypeError) as rec:

            class Test(Base):
                c: float

        assert "Required field 'c' cannot follow optional fields" in str(rec.value)

    def test_kw_only_positional(self):
        class Test(Struct, kw_only=True):
            b: int
            a: int

        assert Test.__struct_fields__ == ("b", "a")
        assert Test.__struct_defaults__ == ()
        assert Test.__match_args__ == ()
        assert Test.__slots__ == ("a", "b")

    def test_kw_only_mixed(self):
        class Test(Struct, kw_only=True):
            b: int
            a: int = 0
            c: int
            d: int = 1

        assert Test.__struct_fields__ == ("b", "a", "c", "d")
        assert Test.__struct_defaults__ == (0, NODEFAULT, 1)
        assert Test.__match_args__ == ()
        assert Test.__slots__ == ("a", "b", "c", "d")

    def test_kw_only_positional_base_class(self):
        class Base(Struct, kw_only=True):
            b: int
            a: int

        class S1(Base):
            d: int
            c: int

        class S2(Base):
            d: int
            c: int = 1

        assert S1.__struct_fields__ == ("d", "c", "b", "a")
        assert S1.__struct_defaults__ == ()
        assert S1.__match_args__ == ("d", "c")
        assert S1.__slots__ == ("c", "d")

        assert S2.__struct_fields__ == ("d", "c", "b", "a")
        assert S2.__struct_defaults__ == (1, NODEFAULT, NODEFAULT)
        assert S2.__match_args__ == ("d", "c")
        assert S2.__slots__ == ("c", "d")

    def test_kw_only_base_class(self):
        class Base(Struct, kw_only=True):
            b: int = 1
            a: int

        class S1(Base):
            d: int
            c: int = 2

        assert S1.__struct_fields__ == ("d", "c", "b", "a")
        assert S1.__struct_defaults__ == (2, 1, NODEFAULT)
        assert S1.__match_args__ == ("d", "c")
        assert S1.__slots__ == ("c", "d")

    def test_kw_only_subclass(self):
        class Base(Struct):
            b: int
            a: int

        class S1(Base, kw_only=True):
            d: int
            c: int

        assert S1.__struct_fields__ == ("b", "a", "d", "c")
        assert S1.__struct_defaults__ == ()
        assert S1.__match_args__ == ("b", "a")
        assert S1.__slots__ == ("c", "d")

    def test_kw_only_defaults_subclass(self):
        class Base(Struct):
            b: int
            a: int = 0

        class S1(Base, kw_only=True):
            d: int
            c: int = 1

        assert S1.__struct_fields__ == ("b", "a", "d", "c")
        assert S1.__struct_defaults__ == (0, NODEFAULT, 1)
        assert S1.__match_args__ == ("b", "a")
        assert S1.__slots__ == ("c", "d")

    def test_kw_only_overrides(self):
        class Base(Struct):
            b: int
            a: int = 2

        class S1(Base, kw_only=True):
            b: int
            c: int = 3

        assert S1.__struct_fields__ == ("a", "b", "c")
        assert S1.__struct_defaults__ == (2, NODEFAULT, 3)
        assert S1.__match_args__ == ("a",)
        assert S1.__slots__ == ("c",)

    def test_kw_only_overridden(self):
        class Base(Struct, kw_only=True):
            b: int
            a: int = 2

        class S1(Base):
            b: int
            c: int = 3

        assert S1.__struct_fields__ == ("b", "c", "a")
        assert S1.__struct_defaults__ == (3, 2)
        assert S1.__match_args__ == ("b", "c")
        assert S1.__slots__ == ("c",)


class TestStructInit:
    def test_init_positional(self):
        class Test(Struct):
            a: int
            b: float
            c: int = 3
            d: float = 4.0

        assert as_tuple(Test(1, 2.0)) == (1, 2.0, 3, 4.0)
        assert as_tuple(Test(1, b=2.0)) == (1, 2.0, 3, 4.0)
        assert as_tuple(Test(a=1, b=2.0)) == (1, 2.0, 3, 4.0)
        assert as_tuple(Test(1, b=2.0, c=5)) == (1, 2.0, 5, 4.0)
        assert as_tuple(Test(1, b=2.0, d=5.0)) == (1, 2.0, 3, 5.0)
        assert as_tuple(Test(1, 2.0, 5)) == (1, 2.0, 5, 4.0)
        assert as_tuple(Test(1, 2.0, 5, 6.0)) == (1, 2.0, 5, 6.0)

        with pytest.raises(TypeError, match="Missing required argument 'a'"):
            Test()

        with pytest.raises(TypeError, match="Missing required argument 'b'"):
            Test(1)

        with pytest.raises(TypeError, match="Extra positional arguments provided"):
            Test(1, 2, 3, 4, 5)

        with pytest.raises(TypeError, match="Argument 'a' given by name and position"):
            Test(1, 2, a=3)

        with pytest.raises(TypeError, match="Unexpected keyword argument 'e'"):
            Test(1, 2, e=5)

    def test_init_kw_only(self):
        class Test(Struct, kw_only=True):
            a: int
            b: float = 2.0
            c: int = 3

        assert as_tuple(Test(a=1)) == (1, 2.0, 3)
        assert as_tuple(Test(a=1, b=4.0)) == (1, 4.0, 3)
        assert as_tuple(Test(a=1, c=4)) == (1, 2.0, 4)
        assert as_tuple(Test(a=1, b=4.0, c=5)) == (1, 4.0, 5)

        with pytest.raises(TypeError, match="Missing required argument 'a'"):
            Test()

        with pytest.raises(TypeError, match="Extra positional arguments provided"):
            Test(1)

        with pytest.raises(TypeError, match="Unexpected keyword argument 'e'"):
            Test(a=1, e=5)

    def test_init_kw_only_mixed(self):
        class Base(Struct, kw_only=True):
            c: int = 3
            d: float = 4.0

        class Test(Base):
            a: int
            b: float = 2.0

        assert as_tuple(Test(1)) == (1, 2.0, 3, 4.0)
        assert as_tuple(Test(1, 5.0)) == (1, 5.0, 3, 4.0)
        assert as_tuple(Test(a=1)) == (1, 2.0, 3, 4.0)
        assert as_tuple(Test(a=1, b=5.0)) == (1, 5.0, 3, 4.0)
        assert as_tuple(Test(1, c=5)) == (1, 2.0, 5, 4.0)

        with pytest.raises(TypeError, match="Missing required argument 'a'"):
            Test()

        with pytest.raises(TypeError, match="Argument 'a' given by name and position"):
            Test(1, b=3.0, c=4, a=3)

        with pytest.raises(TypeError, match="Extra positional arguments provided"):
            Test(1, 5.0, 3)

        with pytest.raises(TypeError, match="Unexpected keyword argument 'e'"):
            Test(1, e=5)


class TestSignature:
    def test_signature_no_args(self):
        class Test(Struct):
            pass

        sig = Signature(parameters=[])
        assert Test.__signature__ == sig

    def test_signature_positional(self):
        class Test(Struct):
            b: float
            a: int = 1

        sig = Signature(
            parameters=[
                Parameter("b", Parameter.POSITIONAL_OR_KEYWORD, annotation=float),
                Parameter(
                    "a",
                    Parameter.POSITIONAL_OR_KEYWORD,
                    default=1,
                    annotation=int,
                ),
            ]
        )
        assert Test.__signature__ == sig

    def test_signature_kw_only(self):
        class Base(Struct, kw_only=True):
            c: float
            d: int = 2

        class Test(Base):
            b: float
            a: int = 1

        sig = Signature(
            parameters=[
                Parameter("b", Parameter.POSITIONAL_OR_KEYWORD, annotation=float),
                Parameter(
                    "a",
                    Parameter.POSITIONAL_OR_KEYWORD,
                    default=1,
                    annotation=int,
                ),
                Parameter("c", Parameter.KEYWORD_ONLY, annotation=float),
                Parameter("d", Parameter.KEYWORD_ONLY, default=2, annotation=int),
            ]
        )
        assert Test.__signature__ == sig


class TestRepr:
    def test_repr_base(self):
        x = Struct()
        assert repr(x) == "Struct()"
        assert x.__rich_repr__() == []

    def test_repr_empty(self):
        class Test(Struct):
            pass

        x = Test()
        assert repr(x) == "Test()"
        assert x.__rich_repr__() == []

    def test_repr_one_field(self):
        class Test(Struct):
            a: int

        x = Test(1)
        assert repr(x) == "Test(a=1)"
        assert x.__rich_repr__() == [("a", 1)]

    def test_repr_two_fields(self):
        class Test(Struct):
            a: int
            b: str

        x = Test(1, "y")
        assert repr(x) == "Test(a=1, b='y')"
        assert x.__rich_repr__() == [("a", 1), ("b", "y")]

    def test_repr_omit_defaults_empty(self):
        class Test(Struct, repr_omit_defaults=True):
            pass

        x = Test()
        assert repr(x) == "Test()"
        assert x.__rich_repr__() == []

    def test_repr_omit_defaults_one_field(self):
        class Test(Struct, repr_omit_defaults=True):
            a: int = 0

        x = Test(0)
        assert repr(x) == "Test()"
        assert x.__rich_repr__() == []

        x = Test(1)
        assert repr(x) == "Test(a=1)"
        assert x.__rich_repr__() == [("a", 1)]

    def test_repr_omit_defaults_multiple_fields(self):
        class Test(Struct, repr_omit_defaults=True):
            a: int
            b: int = 0
            c: str = ""

        x = Test(0)
        assert repr(x) == "Test(a=0)"
        assert x.__rich_repr__() == [("a", 0)]

        x = Test(0, b=1)
        assert repr(x) == "Test(a=0, b=1)"
        assert x.__rich_repr__() == [("a", 0), ("b", 1)]

        x = Test(0, c="two")
        assert repr(x) == "Test(a=0, c='two')"
        assert x.__rich_repr__() == [("a", 0), ("c", "two")]

        x = Test(0, b=1, c="two")
        assert repr(x) == "Test(a=0, b=1, c='two')"
        assert x.__rich_repr__() == [("a", 0), ("b", 1), ("c", "two")]

    def test_repr_recursive(self):
        class Test(Struct):
            a: int
            b: Any

        t = Test(1, Test(2, None))
        t.b.b = t
        assert repr(t) == "Test(a=1, b=Test(a=2, b=...))"

    def test_repr_missing_attr_errors(self):
        class Test(Struct):
            a: int
            b: str

        t = Test(1, "hello")
        del t.b

        with pytest.raises(AttributeError):
            repr(t)

        with pytest.raises(AttributeError):
            t.__rich_repr__()

    def test_repr_errors(self):
        msg = "Oh no!"

        class Bad:
            def __repr__(self):
                raise ValueError(msg)

        class Test(Struct):
            a: object
            b: object

        t = Test(1, Bad())

        with pytest.raises(ValueError, match=msg):
            repr(t)


def test_struct_copy():
    x = copy.copy(Struct())
    assert type(x) is Struct

    class Test(Struct):
        b: int
        a: int

    x = copy.copy(Test(1, 2))
    assert type(x) is Test
    assert x.b == 1
    assert x.a == 2


class FrozenPoint(Struct, frozen=True):
    x: int
    y: int


@pytest.mark.parametrize(
    "default",
    [
        None,
        False,
        True,
        1,
        2.0,
        1.5 + 2.32j,
        b"test",
        "test",
        (),
        frozenset(),
        frozenset((1, (2, 3, 4), 5)),
        Fruit.APPLE,
        datetime.time(1),
        datetime.date.today(),
        datetime.timedelta(seconds=2),
        datetime.datetime.now(),
        FrozenPoint(1, 2),
    ],
)
def test_struct_immutable_defaults_use_instance(default):
    class Test(Struct):
        value: object = default

    t = Test()
    assert t.value is default


@pytest.mark.parametrize("default", [[], {}, set()])
def test_struct_empty_mutable_defaults_fast_copy(default):
    class Test(Struct):
        value: object = default

    t = Test()
    assert t.value == default
    assert t.value is not default


class Point(Struct):
    x: int
    y: int


class PointKWOnly(Struct, kw_only=True):
    x: int
    y: int


@pytest.mark.parametrize("default", [[], {}, set(), bytearray()])
def test_struct_empty_mutable_defaults_work(default):
    class Test(Struct):
        value: object = default

    x = Test().value
    x == default
    assert x is not default


@pytest.mark.parametrize(
    "default",
    [Point(1, 2), [1], {"a": "b"}, {1, 2}, bytearray(b"test")],
)
def test_struct_nonempty_mutable_defaults_error(default):
    with pytest.raises(TypeError) as rec:

        class Test(Struct):
            value: object = default

    assert "as a default value is unsafe" in str(rec.value)
    assert repr(default) in str(rec.value)


def test_struct_defaults_from_field():
    default = []

    class Test(Struct):
        req: int = field()
        x: int = field(default=1)
        y: int = field(default_factory=lambda: 2)
        z: List[int] = field(default=default)

    t = Test(100)
    assert t.req == 100
    assert t.x == 1
    assert t.y == 2
    assert t.z == []
    assert t.z is not default


def test_struct_default_factory_errors():
    def bad():
        raise ValueError("Oh no")

    class Test(Struct):
        x: int = field(default_factory=bad)

    with pytest.raises(ValueError):
        Test()


def test_struct_reference_counting():
    """Test that struct operations that access fields properly decref"""

    class Test(Struct):
        value: list

    data = [1, 2, 3]

    t = Test(data)
    assert sys.getrefcount(data) <= 3

    repr(t)
    assert sys.getrefcount(data) <= 3

    t2 = t.__copy__()
    assert sys.getrefcount(data) <= 4

    assert t == t2
    assert sys.getrefcount(data) <= 4


def test_struct_gc_not_added_if_not_needed():
    """Structs aren't tracked by GC until/unless they reference a container type"""

    class Test(Struct):
        x: object
        y: object

    assert not gc.is_tracked(Test(1, 2))
    assert not gc.is_tracked(Test("hello", "world"))
    assert gc.is_tracked(Test([1, 2, 3], 1))
    assert gc.is_tracked(Test(1, [1, 2, 3]))
    # Tuples are all tracked on creation, but through GC passes eventually
    # become untracked if they don't contain tracked types
    untracked_tuple = (1, 2, 3)
    for i in range(5):
        gc.collect()
        if not gc.is_tracked(untracked_tuple):
            break
    else:
        assert False, "something has changed with Python's GC, investigate"
    assert not gc.is_tracked(Test(1, untracked_tuple))
    tracked_tuple = ([],)
    assert gc.is_tracked(Test(1, tracked_tuple))

    # On mutation, if a tracked objected is stored on a struct, an untracked
    # struct will become tracked
    t = Test(1, 2)
    assert not gc.is_tracked(t)
    t.x = 3
    assert not gc.is_tracked(t)
    t.x = untracked_tuple
    assert not gc.is_tracked(t)
    t.x = []
    assert gc.is_tracked(t)

    # An error in setattr doesn't change tracked status
    t = Test(1, 2)
    assert not gc.is_tracked(t)
    with pytest.raises(AttributeError):
        t.z = []
    assert not gc.is_tracked(t)


class TestStructGC:
    @pytest.mark.skipif(
        hasattr(sys.flags, "gil") and not sys.flags.gil,
        reason="object layout is different on free-threading builds",
    )
    def test_memory_layout(self):
        sizes = {}
        for has_gc in [False, True]:

            class Test(Struct, gc=has_gc):
                x: object
                y: object

            sizes[has_gc] = sys.getsizeof(Test(1, 2))

        # Currently gc=False structs are 16 bytes smaller than gc=True structs,
        # but that's a cpython implementation detail. This test is mainly to
        # check that the smaller layout is being actually used.
        assert sizes[False] < sizes[True]

    def test_init(self):
        class Test(Struct, gc=False):
            x: object
            y: object

        assert not gc.is_tracked(Test(1, 2))
        assert not gc.is_tracked(Test([1, 2, 3], 1))
        assert not gc.is_tracked(Test(1, [1, 2, 3]))

    def test_setattr(self):
        class Test(Struct, gc=False):
            x: object
            y: object

        # Tracked status doesn't change on mutation
        t = Test(1, 2)
        assert not gc.is_tracked(t)
        t.x = []
        assert not gc.is_tracked(t)

    def test_gc_false_inherit_from_gc_true(self):
        class HasGC(Struct):
            x: object

        class NoGC(HasGC, gc=False):
            y: object

        assert gc.is_tracked(HasGC([]))
        assert not gc.is_tracked(NoGC(1, 2))
        assert not gc.is_tracked(NoGC(1, []))
        x = NoGC([], 2)
        assert not gc.is_tracked(x)
        x.y = []
        assert not gc.is_tracked(x)

    def test_gc_true_inherit_from_gc_false(self):
        class NoGC(Struct, gc=False):
            y: object

        class HasGC(NoGC, gc=True):
            x: object

        assert gc.is_tracked(HasGC(1, []))
        assert gc.is_tracked(HasGC([], 1))
        x = HasGC(1, 2)
        assert not gc.is_tracked(x)
        x.x = []
        assert gc.is_tracked(x)

    @pytest.mark.parametrize("has_gc", [False, True])
    def test_struct_gc_set_on_copy(self, has_gc):
        """Copying doesn't go through the struct constructor"""

        class Test(Struct, gc=has_gc):
            x: object
            y: object

        assert not gc.is_tracked(copy.copy(Test(1, 2)))
        assert not gc.is_tracked(copy.copy(Test(1, ())))
        assert gc.is_tracked(copy.copy(Test(1, []))) == has_gc

    def test_struct_gc_false_cannot_inherit_from_non_slots_classes(self):
        class Base:
            pass

        with pytest.raises(
            ValueError,
            match="Cannot set gc=False when inheriting from non-struct types with a __dict__",
        ):

            class Test(Struct, Base, gc=False):
                pass

    def test_struct_gc_false_can_inherit_from_slots_class_mixin(self):
        class Base:
            __slots__ = ()

        class Test(Struct, Base, gc=False):
            x: int

        t = Test(1)
        assert not gc.is_tracked(t)

    @pytest.mark.parametrize("case", ["base-dict", "base-nogc", "nobase"])
    def test_struct_gc_false_forbids_dict_true(self, case):
        if case == "base-dict":

            class Base(Struct, dict=True):
                pass

            opts = {"gc": False}
        elif case == "base-nogc":

            class Base(Struct, gc=False):
                pass

            opts = {"dict": True}
        elif case == "nobase":
            Base = Struct
            opts = {"gc": False, "dict": True}

        with pytest.raises(ValueError, match="Cannot set gc=False and dict=True"):

            class Test(Base, **opts):
                pass


class TestStructDealloc:
    @pytest.mark.parametrize("has_gc", [False, True])
    def test_struct_dealloc_decrefs_type(self, has_gc):
        class Test1(Struct, gc=has_gc):
            x: int
            y: int

        class Test2(Struct, gc=has_gc):
            x: int
            y: int

        with nogc():
            orig_1 = sys.getrefcount(Test1)
            orig_2 = sys.getrefcount(Test2)
            t = Test1(1, 2)
            assert sys.getrefcount(Test1) <= orig_1 + 1
            del t
            assert sys.getrefcount(Test1) <= orig_1
            t = Test2(1, 2)
            assert sys.getrefcount(Test1) <= orig_1
            assert sys.getrefcount(Test2) <= orig_2 + 1
            del t
            assert sys.getrefcount(Test1) <= orig_1
            assert sys.getrefcount(Test2) <= orig_2
            gc.collect()
            assert sys.getrefcount(Test1) == orig_1
            assert sys.getrefcount(Test2) == orig_2

    @pytest.mark.parametrize("has_gc", [False, True])
    def test_struct_dealloc_calls_finalizer(self, has_gc):
        for _ in range(3):
            called = False

            class Test(Struct, gc=has_gc):
                x: int
                y: int

                def __del__(self):
                    nonlocal called
                    called = True

            t = Test(1, 2)
            if hasattr(gc, "is_finalized"):
                assert not gc.is_finalized(t)
            del t

            assert called

    @pytest.mark.parametrize("has_gc", [False, True])
    def test_struct_dealloc_supports_finalizer_resurrection(self, has_gc):
        for _ in range(3):
            called = False
            new_ref = None

            class Test(Struct, gc=has_gc):
                x: int
                y: int

                def __del__(self):
                    # XXX: Python will only run `__del__` once, even if it's
                    # resurrected FOR GC TYPES ONLY. If gc=False, cpython will
                    # happily run `__del__` every time the refcount drops to 0
                    nonlocal called
                    nonlocal new_ref
                    if not called:
                        called = True
                        new_ref = self

            t = Test(1, 2)

            del t
            assert called
            assert new_ref is not None
            del new_ref

    @pytest.mark.parametrize("has_gc", [False, True])
    def test_struct_dealloc_trashcan(self, has_gc):
        N = 100
        called = set()

        class Node(Struct, gc=has_gc):
            child: "Optional[Node]" = None

            def __del__(self):
                called.add(id(self))

        node = None
        for _ in range(N):
            node = Node(node)

        del node
        assert len(called) == N

    @pytest.mark.parametrize("has_gc", [False, True])
    def test_struct_dealloc_decrefs_fields(self, has_gc):
        class Test(Struct, gc=has_gc):
            x: Any

        x = object()
        t = Test(x)
        count = sys.getrefcount(x)
        del t
        assert sys.getrefcount(x) == count - 1

    @pytest.mark.parametrize("has_gc", [False, True])
    def test_struct_dealloc_works_with_missing_fields(self, has_gc):
        class Test(Struct, gc=has_gc):
            x: Any
            y: Any

        x = object()
        t = Test(x, None)
        del t.y
        count = sys.getrefcount(x)
        del t
        assert sys.getrefcount(x) == count - 1

    def test_struct_dealloc_dict(self):
        class Test(Struct, dict=True):
            x: int

        called = False

        class Flag:
            def __del__(self):
                nonlocal called
                called = True

        t = Test(1)
        t.flag = Flag()
        del t
        assert called

    def test_struct_dealloc_weakref(self):
        class Test(Struct, weakref=True):
            x: int

        t = Test(1)
        # smoketest dealloc weakrefable struct doesn't crash
        del t

        t = Test(1)
        ref = weakref.ref(t)
        assert ref() is not None
        del t
        assert ref() is None

    def test_struct_dealloc_in_gc_properly_handles_type_decref(self):
        def inner():
            class Box(msgspec.Struct):
                a: Any

            gc.collect()

            o = Box(None)
            o.a = o

        for _ in range(5):
            inner()
            gc.collect()


@pytest.mark.parametrize("kw_only", [False, True])
def test_struct_pickle(kw_only):
    cls = PointKWOnly if kw_only else Point
    a = cls(x=1, y=2)
    b = cls(x=3, y=4)

    assert pickle.loads(pickle.dumps(a)) == a
    assert pickle.loads(pickle.dumps(b)) == b

    del a.x
    with pytest.raises(AttributeError, match="Struct field 'x' is unset"):
        pickle.dumps(a)


def test_struct_handles_missing_attributes():
    """If an attribute is unset, raise an AttributeError appropriately"""

    class MyStruct(Struct):
        x: int
        y: int
        z: str = "default"

    t = MyStruct(1, 2)
    del t.y
    t2 = MyStruct(1, 2)

    match = "Struct field 'y' is unset"

    with pytest.raises(AttributeError, match=match):
        repr(t)

    with pytest.raises(AttributeError, match=match):
        copy.copy(t)

    with pytest.raises(AttributeError, match=match):
        t == t2

    with pytest.raises(AttributeError, match=match):
        t2 == t


@pytest.mark.parametrize(
    "option, default",
    [
        ("frozen", False),
        ("order", False),
        ("eq", True),
        ("repr_omit_defaults", False),
        ("array_like", False),
        ("gc", True),
        ("omit_defaults", False),
        ("forbid_unknown_fields", False),
    ],
)
def test_struct_option_precedence(option, default):
    def get(cls):
        return getattr(cls.__struct_config__, option)

    class Default(Struct):
        pass

    assert get(Default) is default

    class Enabled(Struct, **{option: True}):
        pass

    assert get(Enabled) is True

    class Disabled(Struct, **{option: False}):
        pass

    assert get(Disabled) is False

    class T(Enabled):
        pass

    assert get(T) is True

    class T(Enabled, **{option: False}):
        pass

    assert get(T) is False

    class T(Enabled, Default):
        pass

    assert get(T) is True

    class T(Default, Enabled):
        pass

    assert get(T) is True

    class T(Default, Disabled, Enabled):
        pass

    assert get(T) is False


def test_weakref_option():
    class Default(Struct):
        pass

    assert Default.__weakrefoffset__ == 0

    class Enabled(Struct, weakref=True):
        pass

    assert Enabled.__weakrefoffset__ != 0
    assert Enabled.__struct_config__.weakref

    class Disabled(Struct, weakref=False):
        pass

    assert Disabled.__weakrefoffset__ == 0
    assert not Disabled.__struct_config__.weakref

    class T(Enabled):
        pass

    assert T.__weakrefoffset__ != 0
    assert T.__struct_config__.weakref

    class T(Enabled, Default):
        pass

    assert T.__weakrefoffset__ != 0
    assert T.__struct_config__.weakref

    class T(Default, Disabled, Enabled):
        pass

    assert T.__weakrefoffset__ != 0
    assert T.__struct_config__.weakref

    with pytest.raises(ValueError, match="Cannot set `weakref=False`"):

        class T(Enabled, weakref=False):
            pass


def test_dict_option():
    class Default(Struct):
        pass

    assert Default.__dictoffset__ == 0

    class Enabled(Struct, dict=True):
        pass

    assert Enabled.__dictoffset__ != 0
    assert Enabled.__struct_config__.dict

    class Disabled(Struct, dict=False):
        pass

    assert Disabled.__dictoffset__ == 0
    assert not Disabled.__struct_config__.dict

    class T(Enabled):
        pass

    assert T.__dictoffset__ != 0
    assert T.__struct_config__.dict

    class T(Enabled, Default):
        pass

    assert T.__dictoffset__ != 0
    assert T.__struct_config__.dict

    class T(Default, Disabled, Enabled):
        pass

    assert T.__dictoffset__ != 0
    assert T.__struct_config__.dict

    with pytest.raises(ValueError, match="Cannot set `dict=False`"):

        class T(Enabled, dict=False):
            pass


def test_cache_hash_option():
    with pytest.raises(
        ValueError, match="Cannot set cache_hash=True without frozen=True"
    ):

        class Invalid(Struct, cache_hash=True):
            pass

    class Default(Struct, frozen=True):
        pass

    assert "__msgspec_cached_hash__" not in Default.__slots__
    assert not Default.__struct_config__.cache_hash

    class Enabled(Struct, cache_hash=True, frozen=True):
        pass

    assert "__msgspec_cached_hash__" in Enabled.__slots__
    assert Enabled.__struct_config__.cache_hash

    class Disabled(Struct, cache_hash=False, frozen=True):
        pass

    assert "__msgspec_cached_hash__" not in Disabled.__slots__
    assert not Disabled.__struct_config__.cache_hash

    class T(Enabled):
        pass

    assert "__msgspec_cached_hash__" not in T.__slots__
    assert T.__struct_config__.cache_hash

    class T(Enabled, Default):
        pass

    assert "__msgspec_cached_hash__" not in T.__slots__
    assert T.__struct_config__.cache_hash

    class T(Default, Disabled, Enabled):
        pass

    assert "__msgspec_cached_hash__" not in T.__slots__
    assert T.__struct_config__.cache_hash

    with pytest.raises(ValueError, match="Cannot set `cache_hash=False`"):

        class T(Enabled, cache_hash=False):
            pass


def test_invalid_option_raises():
    with pytest.raises(TypeError):

        class Foo(Struct, invalid=True):
            pass


class FrozenPoint(Struct, frozen=True):
    x: int
    y: int


class TestHash:
    def test_frozen_objects_hashable(self):
        p1 = FrozenPoint(1, 2)
        p2 = FrozenPoint(1, 2)
        p3 = FrozenPoint(1, 3)
        assert hash(p1) == hash(p2)
        assert hash(p1) != hash(p3)
        assert p1 == p2
        assert p1 != p3

    def test_frozen_objects_hash_errors_if_field_unhashable(self):
        p = FrozenPoint(1, [2])
        with pytest.raises(TypeError):
            hash(p)

    def test_frozen_hash_mutable_objects_hash_errors(self):
        p = Point(1, 2)
        with pytest.raises(TypeError, match="unhashable type"):
            hash(p)

    def test_hash_includes_type(self):
        Ex1 = defstruct("Ex1", ["x"], frozen=True)
        Ex2 = defstruct("Ex2", ["x"], frozen=True)
        Ex3 = defstruct("Ex3", [], frozen=True)
        Ex4 = defstruct("Ex4", [], frozen=True)
        assert hash(Ex1(1)) == hash(Ex1(1))
        assert hash(Ex1(1)) != hash(Ex2(1))
        assert hash(Ex3()) == hash(Ex3())
        assert hash(Ex3()) != hash(Ex4())

    def test_cache_hash(self):
        class Inner:
            def __init__(self):
                self.hash_calls = 0

            def __hash__(self):
                self.hash_calls += 1
                return 123

        class Cached(Struct, frozen=True, cache_hash=True):
            x: int
            y: Inner

        assert "__msgspec_cached_hash__" in Cached.__slots__
        obj = Cached(1, Inner())
        assert not hasattr(obj, "__msgspec_cached_hash__")
        assert hash(obj) == hash(obj)
        assert obj.__msgspec_cached_hash__ == hash(obj)
        assert obj.y.hash_calls == 1


class TestSetAttr:
    def test_frozen_objects_no_setattr(self):
        p = FrozenPoint(1, 2)
        with pytest.raises(AttributeError, match="immutable type: 'FrozenPoint'"):
            p.x = 3

    @pytest.mark.parametrize("base_gc", [True, None, False])
    @pytest.mark.parametrize("base_frozen", [True, False])
    @pytest.mark.parametrize("has_gc", [True, None, False])
    def test_override_setattr(self, has_gc, base_gc, base_frozen):
        called = False

        class Base(Struct, gc=base_gc, frozen=base_frozen):
            pass

        class Test(Struct, gc=has_gc, frozen=False):
            x: Any

            def __setattr__(self, name, value):
                nonlocal called
                called = True
                super().__setattr__(name, value)

        t = Test(1)
        assert not called
        t.x = 2
        assert called
        if has_gc:
            assert not gc.is_tracked(t)
            t.x = [1]
            assert gc.is_tracked(t)

    @pytest.mark.parametrize("base_gc", [True, None, False])
    @pytest.mark.parametrize("has_gc", [True, None, False])
    def test_override_setattr_inherit(self, base_gc, has_gc):
        called = False

        class Base(Struct, gc=base_gc):
            x: Any

            def __setattr__(self, name, value):
                nonlocal called
                called = True
                super().__setattr__(name, value)

        class Test(Base, gc=has_gc):
            pass

        t = Test(1)
        assert not called
        t.x = 2
        assert called
        if has_gc:
            assert not gc.is_tracked(t)
            t.x = [1]
            assert gc.is_tracked(t)

    def test_force_setattr(self):
        class Ex(Struct, frozen=True):
            x: Any

        obj = Ex(1)

        res = msgspec.structs.force_setattr(obj, "x", 2)
        assert res is None
        assert obj.x == 2

        with pytest.raises(AttributeError):
            msgspec.structs.force_setattr(obj, "oops", 3)

        with pytest.raises(TypeError):
            msgspec.structs.force_setattr(1, "oops", 3)


class TestOrderAndEq:
    @staticmethod
    def assert_eq(a, b):
        assert a == b
        assert not a != b

    @staticmethod
    def assert_neq(a, b):
        assert a != b
        assert not a == b

    def test_order_no_eq_errors(self):
        with pytest.raises(ValueError, match="Cannot set eq=False and order=True"):

            class Test(Struct, order=True, eq=False):
                pass

    def test_struct_eq_false(self):
        class Point(Struct, eq=False):
            x: int
            y: int

        p = Point(1, 2)
        # identity based equality
        self.assert_eq(p, p)
        self.assert_neq(p, Point(1, 2))
        # identity based hash
        assert hash(p) == hash(p)
        assert hash(p) != hash(Point(1, 2))

    def test_struct_eq(self):
        class Test(Struct):
            a: int
            b: int

        class Test2(Test):
            pass

        x = Struct()

        self.assert_eq(x, Struct())
        self.assert_neq(x, None)

        x = Test(1, 2)
        self.assert_eq(x, Test(1, 2))
        self.assert_neq(x, None)
        self.assert_neq(x, Test(1, 3))
        self.assert_neq(x, Test(2, 2))
        self.assert_neq(x, Test2(1, 2))

    def test_struct_override_eq(self):
        class Ex(Struct):
            a: int
            b: int

            def __eq__(self, other):
                return self.a == other.a

        x = Ex(1, 2)
        y = Ex(1, 3)
        z = Ex(2, 3)

        self.assert_eq(x, y)
        self.assert_neq(x, z)

    def test_struct_eq_identity_fastpath(self):
        class Bad:
            def __eq__(self, other):
                raise ValueError("Oh no!")

        class Test(Struct):
            a: int
            b: Bad

        t = Test(1, Bad())
        self.assert_eq(t, t)

    @pytest.mark.parametrize("op", ["le", "lt", "ge", "gt"])
    def test_struct_order(self, op):
        func = getattr(operator, op)

        class Point(Struct, order=True):
            x: int
            y: int

        origin = Point(0, 0)
        for x in [-1, 0, 1]:
            for y in [-1, 0, 1]:
                sol = func((0, 0), (x, y))
                res = func(origin, Point(x, y))
                assert res == sol

        assert func(origin, origin) == func(1, 1)

    @pytest.mark.parametrize("eq, order", [(False, False), (True, False), (True, True)])
    def test_struct_compare_returns_notimplemented(self, eq, order):
        class Test(Struct, eq=eq, order=order):
            x: int

        t1 = Test(1)
        t2 = Test(2)
        assert t1.__eq__(t2) is (False if eq else NotImplemented)
        assert t1.__lt__(t2) is (True if order else NotImplemented)
        assert t1.__eq__(None) is NotImplemented
        assert t1.__lt__(None) is NotImplemented

    @pytest.mark.parametrize("op", ["eq", "ne", "le", "lt", "ge", "gt"])
    def test_struct_compare_errors(self, op):
        func = getattr(operator, op)

        class Bad:
            def __eq__(self, other):
                raise ValueError("Oh no!")

        class Test(Struct, order=True):
            a: object
            b: object

        t = Test(1, Bad())
        t2 = Test(1, 2)

        with pytest.raises(ValueError, match="Oh no!"):
            func(t, t2)
        with pytest.raises(ValueError, match="Oh no!"):
            func(t2, t)


class TestTagAndTagField:
    @pytest.mark.parametrize(
        "opts, tag_field, tag",
        [
            # Default & explicit NULL
            ({}, None, None),
            ({"tag": None, "tag_field": None}, None, None),
            # tag=True
            ({"tag": True}, "type", "Test"),
            ({"tag": True, "tag_field": "test"}, "test", "Test"),
            # tag=False
            ({"tag": False}, None, None),
            ({"tag": False, "tag_field": "kind"}, None, None),
            # tag str
            ({"tag": "test"}, "type", "test"),
            (dict(tag="test", tag_field="kind"), "kind", "test"),
            # tag int
            ({"tag": 1}, "type", 1),
            (dict(tag=1, tag_field="kind"), "kind", 1),
            # tag callable
            (dict(tag=lambda n: n.lower()), "type", "test"),
            (dict(tag=lambda n: n.lower(), tag_field="kind"), "kind", "test"),
            # tag_field alone
            (dict(tag_field="kind"), "kind", "Test"),
        ],
    )
    def test_config(self, opts, tag_field, tag):
        class Test(Struct, **opts):
            x: int
            y: int

        assert Test.__struct_config__.tag_field == tag_field
        assert Test.__struct_config__.tag == tag

    @pytest.mark.parametrize(
        "opts1, opts2, tag_field, tag",
        [
            # tag=True
            ({"tag": True}, {}, "type", "S2"),
            ({"tag": True}, {"tag": None}, "type", "S2"),
            ({"tag": True}, {"tag": False}, None, None),
            ({"tag": True}, {"tag_field": "foo"}, "foo", "S2"),
            # tag str
            ({"tag": "test"}, {}, "type", "test"),
            ({"tag": "test"}, {"tag": "test2"}, "type", "test2"),
            ({"tag": "test"}, {"tag": None}, "type", "test"),
            ({"tag": "test"}, {"tag_field": "foo"}, "foo", "test"),
            # tag int
            ({"tag": 1}, {}, "type", 1),
            ({"tag": 1}, {"tag": "test2"}, "type", "test2"),
            ({"tag": 1}, {"tag": None}, "type", 1),
            ({"tag": 1}, {"tag_field": "foo"}, "foo", 1),
            # tag callable
            ({"tag": lambda n: n.lower()}, {}, "type", "s2"),
            ({"tag": lambda n: n.lower()}, {"tag": False}, None, None),
            ({"tag": lambda n: n.lower()}, {"tag": None}, "type", "s2"),
            ({"tag": lambda n: n.lower()}, {"tag_field": "foo"}, "foo", "s2"),
        ],
    )
    def test_inheritance(self, opts1, opts2, tag_field, tag):
        class S1(Struct, **opts1):
            pass

        class S2(S1, **opts2):
            pass

        assert S2.__struct_config__.tag_field == tag_field
        assert S2.__struct_config__.tag == tag

    def test_tag_uses_simple_qualname(self):
        class S1(Struct, tag=True):
            class S2(Struct, tag=True):
                pass

        assert S1.__struct_config__.tag == "S1"
        assert S1.S2.__struct_config__.tag == "S1.S2"

        class S1(Struct, tag=str.lower):
            class S2(Struct, tag=str.lower):
                pass

        assert S1.__struct_config__.tag == "s1"
        assert S1.S2.__struct_config__.tag == "s1.s2"

    @pytest.mark.parametrize("tag", [b"bad", lambda n: b"bad"])
    def test_tag_wrong_type(self, tag):
        with pytest.raises(TypeError, match="`tag` must be a `str` or an `int`"):

            class Test(Struct, tag=tag):
                pass

    @pytest.mark.parametrize("tag", [-(2**63) - 1, 2**63])
    def test_tag_integer_out_of_range(self, tag):
        with pytest.raises(ValueError, match="Integer `tag` values must be"):

            class Test(Struct, tag=tag):
                pass

    def test_tag_field_wrong_type(self):
        with pytest.raises(TypeError, match="`tag_field` must be a `str`"):

            class Test(Struct, tag_field=b"bad"):
                pass

    def test_tag_field_collision(self):
        with pytest.raises(ValueError, match="tag_field='y'"):

            class Test(Struct, tag_field="y"):
                x: int
                y: int

    def test_tag_field_inheritance_collision(self):
        # Inherit the tag field
        class Base(Struct, tag_field="y"):
            pass

        with pytest.raises(ValueError, match="tag_field='y'"):

            class Test(Base):
                x: int
                y: int

        # Inherit the field
        class Base(Struct):
            x: int
            y: int

        with pytest.raises(ValueError, match="tag_field='y'"):

            class Test(Base, tag_field="y"):  # noqa
                pass


class TestRename:
    def test_field_name(self):
        class Test(Struct):
            x: int = field(name="field_x")

        assert Test.__struct_encode_fields__ == ("field_x",)

    def test_rename_mixed_with_field_name(self):
        class Test(Struct, rename="upper"):
            x: int = field(name="field_x")
            y: int

        assert Test.__struct_encode_fields__ == ("field_x", "Y")

    def test_rename_no_change(self):
        class Test(Struct, rename="lower"):
            x: int

        assert Test.__struct_fields__ is Test.__struct_encode_fields__

    def test_field_name_no_change(self):
        class Test(Struct):
            x: int = field(name="x")

        assert Test.__struct_fields__ is Test.__struct_encode_fields__

    def test_field_name_none(self):
        class Test(Struct):
            x: int = field(name=None)

        assert Test.__struct_fields__ is Test.__struct_encode_fields__

        class Test(Struct, rename="upper"):
            x: int = field(name=None)

        assert Test.__struct_encode_fields__ == ("X",)

    def test_rename_explicit_none(self):
        class Test(Struct, rename=None):
            field_one: int
            field_two: str

        assert Test.__struct_encode_fields__ == ("field_one", "field_two")
        assert Test.__struct_fields__ is Test.__struct_encode_fields__

    def test_rename_lower(self):
        class Test(Struct, rename="lower"):
            field_One: int
            field_Two: str

        assert Test.__struct_encode_fields__ == ("field_one", "field_two")

    def test_rename_upper(self):
        class Test(Struct, rename="upper"):
            field_one: int
            field_two: str

        assert Test.__struct_encode_fields__ == ("FIELD_ONE", "FIELD_TWO")

    def test_rename_kebab(self):
        class Test(Struct, rename="kebab"):
            field_one: int
            field_two_with_suffix: str
            __field_three__: bool
            field4: float
            _field_five: int

        assert Test.__struct_encode_fields__ == (
            "field-one",
            "field-two-with-suffix",
            "field-three",
            "field4",
            "field-five",
        )

    def test_rename_camel(self):
        class Test(Struct, rename="camel"):
            field_one: int
            field_two_with_suffix: str
            __field__three__: bool
            field4: float
            _field_five: int

        assert Test.__struct_encode_fields__ == (
            "fieldOne",
            "fieldTwoWithSuffix",
            "__fieldThree",
            "field4",
            "_fieldFive",
        )

    def test_rename_pascal(self):
        class Test(Struct, rename="pascal"):
            field_one: int
            field_two_with_suffix: str
            __field__three__: bool
            field4: float
            _field_five: int

        assert Test.__struct_encode_fields__ == (
            "FieldOne",
            "FieldTwoWithSuffix",
            "__FieldThree",
            "Field4",
            "_FieldFive",
        )

    def test_rename_callable(self):
        class Test(Struct, rename=str.title):
            field_one: int
            field_two: str

        assert Test.__struct_encode_fields__ == ("Field_One", "Field_Two")

    def test_rename_callable_returns_none(self):
        class Test(Struct, rename={"from_": "from"}.get):
            from_: str
            to: str

        assert Test.__struct_encode_fields__ == ("from", "to")

    def test_rename_callable_returns_non_string(self):
        with pytest.raises(
            TypeError,
            match="Expected calling `rename` to return a `str` or `None`, got `int`",
        ):

            class Test(Struct, rename=lambda x: 1):
                aa1: int
                aa2: int
                ab1: int

    def test_rename_mapping(self):
        class Test(Struct, rename={"from_": "from"}):
            from_: str
            to: str

        assert Test.__struct_encode_fields__ == ("from", "to")

    def test_rename_bad_value(self):
        with pytest.raises(ValueError, match="rename='invalid' is unsupported"):

            class Test(Struct, rename="invalid"):
                x: int

    def test_rename_bad_type(self):
        with pytest.raises(TypeError, match="str, callable, or mapping"):

            class Test(Struct, rename=1):
                x: int

    def test_rename_fields_collide(self):
        with pytest.raises(ValueError, match="Multiple fields rename to the same name"):

            class Test(Struct, rename=lambda x: x[:2]):
                aa1: int
                aa2: int
                ab1: int

    @pytest.mark.parametrize("field", ["foo\\bar", 'foo"bar', "foo\tbar"])
    def test_rename_field_invalid_characters(self, field):
        with pytest.raises(ValueError) as rec:

            class Test(Struct, rename=lambda x: field):
                x: int

        assert field in str(rec.value)
        assert "must not contain" in str(rec.value)

    def test_rename_inherit(self):
        class Base(Struct, rename="upper"):
            pass

        class Test1(Base):
            x: int

        assert Test1.__struct_encode_fields__ == ("X",)

        class Test2(Base, rename="camel"):
            my_field: int

        assert Test2.__struct_encode_fields__ == ("myField",)

        class Test3(Test2, rename="kebab"):
            my_other_field: int

        assert Test3.__struct_encode_fields__ == ("myField", "my-other-field")

        class Test4(Base, rename=None):
            my_field: int

        assert Test4.__struct_encode_fields__ == ("my_field",)

    def test_rename_fields_only_used_for_encode_and_decode(self):
        """Check that the renamed fields don't show up elsewhere"""

        class Test(Struct, rename="upper"):
            one: int
            two: str

        t = Test(one=1, two="test")
        assert t.one == 1
        assert t.two == "test"
        assert repr(t) == "Test(one=1, two='test')"
        with pytest.raises(TypeError, match="Missing required argument 'two'"):
            Test(one=1)


class TestDefStruct:
    def test_defstruct_simple(self):
        Point = defstruct("Point", ["x", "y"])
        assert issubclass(Point, Struct)
        assert as_tuple(Point(1, 2)) == (1, 2)
        assert Point.__module__ == "__tests__.unit.test_struct"

    def test_defstruct_empty(self):
        Empty = defstruct("Empty", [])
        assert as_tuple(Empty()) == ()

    def test_defstruct_fields(self):
        Test = defstruct("Point", ["x", ("y", int), ("z", int, 0)])
        assert Test.__struct_fields__ == ("x", "y", "z")
        assert Test.__struct_defaults__ == (0,)
        assert Test.__annotations__ == {"x": Any, "y": int, "z": int}
        assert as_tuple(Test(1, 2)) == (1, 2, 0)
        assert as_tuple(Test(1, 2, 3)) == (1, 2, 3)

    def test_defstruct_fields_iterable(self):
        Test = defstruct("Point", ((n, int) for n in "xyz"))
        assert Test.__struct_fields__ == ("x", "y", "z")
        assert Test.__annotations__ == {"x": int, "y": int, "z": int}

    def test_defstruct_errors(self):
        with pytest.raises(TypeError, match="must be str, not int"):
            defstruct(1)

        with pytest.raises(TypeError, match="`fields` must be an iterable"):
            defstruct("Test", 1)

        with pytest.raises(TypeError, match="items in `fields` must be one of"):
            defstruct("Test", ["x", 1])

        with pytest.raises(TypeError, match="items in `fields` must be one of"):
            defstruct("Test", ["x", (1, 2)])

        with pytest.raises(TypeError, match="must be a tuple or None"):
            defstruct("Test", [], bases=[])

        with pytest.raises(TypeError, match="must be a str or None"):
            defstruct("Test", [], module=1)

        with pytest.raises(TypeError, match="must be a dict or None"):
            defstruct("Test", [], namespace=1)

    def test_defstruct_bases(self):
        class Base(Struct):
            z: int

        Point = defstruct("Point", ["x", "y"], bases=(Base,))
        assert issubclass(Point, Base)
        assert issubclass(Point, Struct)
        assert as_tuple(Point(1, 2, 0)) == (1, 2, 0)
        assert as_tuple(Point(1, 2, 3)) == (1, 2, 3)

    def test_defstruct_bases_none(self):
        Point = defstruct("Point", ["x", "y"], bases=None)
        assert Point.mro() == [Point, *Struct.mro()]
        assert Point(1, 2) == Point(1, 2)

    def test_defstruct_module(self):
        Test = defstruct("Test", [], module="testmod")
        assert Test.__module__ == "testmod"

    def test_defstruct_module_none(self):
        Test = defstruct("Test", [], module=None)
        assert Test.__module__ == "__tests__.unit.test_struct"

    def test_defstruct_namespace(self):
        Test = defstruct(
            "Test", ["x", "y"], namespace={"add": lambda self: self.x + self.y}
        )
        t = Test(1, 2)
        assert t.add() == 3

    def test_defstruct_namespace_none(self):
        Test = defstruct("Test", [], namespace=None)
        assert Test() == Test()  # smoketest

    def test_defstruct_kw_only(self):
        Test = defstruct("Test", ["x", "y"], kw_only=True)
        t = Test(x=1, y=2)
        assert t.x == 1 and t.y == 2
        with pytest.raises(TypeError, match="Extra positional arguments"):
            Test(1, 2)

    @pytest.mark.parametrize(
        "option, default",
        [
            ("repr_omit_defaults", False),
            ("omit_defaults", False),
            ("forbid_unknown_fields", False),
            ("frozen", False),
            ("order", False),
            ("eq", True),
            ("array_like", False),
            ("gc", True),
        ],
    )
    def test_defstruct_bool_options(self, option, default):
        Test = defstruct("Test", [], **{option: True})
        assert getattr(Test.__struct_config__, option) is True

        Test = defstruct("Test", [], **{option: False})
        assert getattr(Test.__struct_config__, option) is False

        Test = defstruct("Test", [])
        assert getattr(Test.__struct_config__, option) is default

    def test_defstruct_weakref(self):
        Test = defstruct("Test", [])
        assert Test.__weakrefoffset__ == 0
        assert not Test.__struct_config__.weakref

        Test = defstruct("Test", [], weakref=False)
        assert Test.__weakrefoffset__ == 0
        assert not Test.__struct_config__.weakref

        Test = defstruct("Test", [], weakref=True)
        assert Test.__weakrefoffset__ != 0
        assert Test.__struct_config__.weakref

    def test_defstruct_dict(self):
        Test = defstruct("Test", [])
        assert not hasattr(Test(), "__dict__")
        assert not Test.__struct_config__.dict

        Test = defstruct("Test", [], dict=False)
        assert not hasattr(Test(), "__dict__")
        assert not Test.__struct_config__.dict

        Test = defstruct("Test", [], dict=True)
        assert hasattr(Test(), "__dict__")
        assert Test.__struct_config__.dict

    def test_defstruct_cache_hash(self):
        Test = defstruct("Test", [], frozen=True)
        assert not Test.__struct_config__.cache_hash

        Test = defstruct("Test", [], frozen=True, cache_hash=False)
        assert not Test.__struct_config__.cache_hash

        Test = defstruct("Test", [], frozen=True, cache_hash=True)
        assert Test.__struct_config__.cache_hash

    def test_defstruct_tag_and_tag_field(self):
        Test = defstruct("Test", [], tag=True)
        assert Test.__struct_config__.tag == "Test"

        Test = defstruct("Test", [], namespace={"__qualname__": "Foo.Test"}, tag=True)
        assert Test.__struct_config__.tag == "Foo.Test"

        Test = defstruct("Test", [], tag="mytag", tag_field="mytagfield")
        config = Test.__struct_config__
        assert config.tag_field == "mytagfield"
        assert config.tag == "mytag"

    def test_defstruct_rename(self):
        Test = defstruct("Test", ["my_field"], rename="camel")
        assert Test.__struct_fields__ == ("my_field",)
        assert Test.__struct_encode_fields__ == ("myField",)


@pytest.fixture(params=["structs.replace", "copy.replace"])
def replace(request):
    if request.param == "structs.replace":
        return msgspec.structs.replace
    else:
        return copy_replace


class TestReplace:
    def test_replace_not_a_struct(self):
        with pytest.raises(TypeError, match="`struct` must be a `msgspec.Struct`"):
            msgspec.structs.replace(1, x=2)

    def test_replace_no_kwargs(self, replace):
        p = Point(1, 2)
        assert replace(p) == p

    def test_replace_kwargs(self, replace):
        p = Point(1, 2)
        assert replace(p, x=3) == Point(3, 2)
        assert replace(p, y=4) == Point(1, 4)
        assert replace(p, x=3, y=4) == Point(3, 4)

    def test_replace_unknown_field(self, replace):
        p = Point(1, 2)
        with pytest.raises(TypeError, match="`Point` has no field 'oops'"):
            replace(p, oops=3)

    def test_replace_errors_unset_fields(self, replace):
        p = Point(1, 2)
        del p.x

        with pytest.raises(AttributeError, match="Struct field 'x' is unset"):
            replace(p)

        with pytest.raises(AttributeError, match="Struct field 'x' is unset"):
            replace(p, y=1)

        assert replace(p, x=3) == Point(3, 2)

    def test_replace_frozen(self, replace):
        class Test(msgspec.Struct, frozen=True):
            x: int
            y: int

        assert replace(Test(1, 2), x=3) == Test(3, 2)

    def test_replace_gc_delayed_tracking(self, replace):
        class Test(msgspec.Struct):
            x: int
            y: Optional[List[int]]

        obj = Test(1, None)
        assert not gc.is_tracked(replace(obj))
        assert not gc.is_tracked(replace(obj, x=10))
        assert not gc.is_tracked(replace(obj, y=None))
        assert gc.is_tracked(replace(obj, y=[1, 2, 3]))

        obj = Test(1, [1, 2, 3])
        assert gc.is_tracked(replace(obj))
        assert gc.is_tracked(replace(obj, x=1))
        assert not gc.is_tracked(replace(obj, y=None))

    def test_replace_gc_false(self, replace):
        class Test(msgspec.Struct, gc=False):
            x: int
            y: List[int]

        res = replace(Test(1, [1, 2]), x=3)
        assert res == Test(3, [1, 2])
        assert not gc.is_tracked(res)

    def test_replace_reference_counts(self, replace):
        class Test(msgspec.Struct):
            x: Any
            y: int

        x = object()
        t = Test(x, 1)

        x_count = sys.getrefcount(x)

        t2 = replace(t)
        assert sys.getrefcount(x) == x_count + 1
        del t2

        t2 = replace(t, x=None)
        assert sys.getrefcount(x) == x_count
        del t2

        t2 = replace(t, y=2)
        assert sys.getrefcount(x) == x_count + 1
        del t2

        x2 = object()
        x2_count = sys.getrefcount(x2)
        t2 = replace(t, x=x2)
        assert sys.getrefcount(x) == x_count
        assert sys.getrefcount(x2) == x2_count + 1
        del t2


class TestAsDictAndAsTuple:
    def test_asdict(self):
        x = Point(1, 2)
        assert msgspec.structs.asdict(x) == {"x": 1, "y": 2}

    def test_astuple(self):
        x = Point(1, 2)
        assert msgspec.structs.astuple(x) == (1, 2)

    @pytest.mark.parametrize("func", [msgspec.structs.asdict, msgspec.structs.astuple])
    def test_errors(self, func):
        with pytest.raises(TypeError):
            func(1)

        x = Point(1, 2)
        del x.y

        with pytest.raises(AttributeError):
            func(x)


class TestInspectFields:
    def test_fields_bad_arg(self):
        T = TypeVar("T")

        class Bad(Generic[T]):
            x: T

        for val in [1, int, Bad, Bad[int]]:
            with pytest.raises(TypeError, match="struct type or instance"):
                msgspec.structs.fields(val)

    def test_fields_struct_meta(self):
        class CustomMeta(msgspec.StructMeta):
            pass

        class Base(metaclass=CustomMeta):
            pass

        class Model(Base):
            pass

        assert msgspec.structs.fields(Model) == ()

    def test_fields_struct_meta_instance(self):
        class CustomMeta(msgspec.StructMeta):
            pass

        class Base(metaclass=CustomMeta):
            pass

        class Model(Base):
            pass

        assert msgspec.structs.fields(Model()) == ()

    def test_fields_no_fields(self):
        assert msgspec.structs.fields(msgspec.Struct) == ()

    @pytest.mark.parametrize("instance", [False, True])
    def test_fields(self, instance):
        def factory():
            return 1

        class Example(msgspec.Struct):
            x: int
            y: int = 0
            z: int = msgspec.field(default_factory=factory)

        arg = Example(1, 2, 3) if instance else Example
        fields = msgspec.structs.fields(arg)
        x_field, y_field, z_field = fields

        assert x_field.required
        assert x_field.default is NODEFAULT
        assert x_field.default_factory is NODEFAULT

        assert not y_field.required
        assert y_field.default == 0
        assert y_field.default_factory is NODEFAULT

        assert not z_field.required
        assert z_field.default is NODEFAULT
        assert z_field.default_factory is factory

    def test_fields_keyword_only(self):
        class Example(msgspec.Struct, kw_only=True):
            a: int
            b: int = 1
            c: int
            d: int = 2

        sol = (
            msgspec.structs.FieldInfo("a", "a", int),
            msgspec.structs.FieldInfo("b", "b", int, default=1),
            msgspec.structs.FieldInfo("c", "c", int),
            msgspec.structs.FieldInfo("d", "d", int, default=2),
        )
        assert msgspec.structs.fields(Example) == sol

    def test_fields_encode_name(self):
        class Example(msgspec.Struct, rename="camel"):
            field_one: int
            field_two: int

        sol = (
            msgspec.structs.FieldInfo("field_one", "fieldOne", int),
            msgspec.structs.FieldInfo("field_two", "fieldTwo", int),
        )

        assert msgspec.structs.fields(Example) == sol

    def test_fields_generic(self):
        T = TypeVar("T")

        class Example(msgspec.Struct, Generic[T]):
            x: T
            y: int

        sol = (
            msgspec.structs.FieldInfo("x", "x", T),
            msgspec.structs.FieldInfo("y", "y", int),
        )
        assert msgspec.structs.fields(Example) == sol
        assert msgspec.structs.fields(Example(1, 2)) == sol

        sol = (
            msgspec.structs.FieldInfo("x", "x", str),
            msgspec.structs.FieldInfo("y", "y", int),
        )
        assert msgspec.structs.fields(Example[str])


class TestClassVar:
    def case1(self):
        return """
        from typing import ClassVar
        from msgspec import Struct

        class Ex(Struct):
            a: int
            cv1: ClassVar
            b: int
            cv2: ClassVar[int] = 1
        """

    def case2(self):
        return """
        import typing
        from msgspec import Struct

        class Ex(Struct):
            a: int
            cv1: typing.ClassVar
            b: int
            cv2: typing.ClassVar[int] = 1
        """

    def case3(self):
        return """
        import typing
        from msgspec import Struct

        ClassVar = typing.List

        class Ex(Struct):
            a: ClassVar
            b: ClassVar[int]
            cv2: typing.ClassVar[int] = 1
        """

    def case4(self):
        return """
        from typing import ClassVar, List
        from msgspec import Struct

        class typing:
            ClassVar = List

        class Ex(Struct):
            a: typing.ClassVar
            b: typing.ClassVar[int]
            cv2: ClassVar[int] = 1
        """

    def case5(self):
        """Annotations that start with `ClassVar`/`typing.ClassVar` but don't
        end there aren't treated as false-positives"""
        return """
        from typing import ClassVar, List
        from msgspec import Struct

        ClassVariable = List

        class typing:
            ClassVariable = List

        class Ex(Struct):
            a: typing.ClassVariable
            b: ClassVariable[int]
            cv2: ClassVar[int] = 1
        """

    @pytest.mark.parametrize("case", [1, 2, 3, 4, 5])
    @pytest.mark.parametrize("future_annotations", [True, False])
    def test_classvar(self, case, future_annotations):
        source = getattr(self, f"case{case}")()
        if future_annotations:
            source = "        from __future__ import annotations\n" + source
        with temp_module(source) as mod:
            assert mod.Ex.__struct_fields__ == ("a", "b")
            assert mod.Ex.cv2 == 1


class TestPostInit:
    def test_post_init(self):
        called = False
        singleton = object()

        class Ex(Struct):
            x: int

            def __post_init__(self):
                nonlocal called
                called = True
                return singleton

        Ex(1)
        assert called
        # Return value is decref'd
        assert sys.getrefcount(singleton) <= 2  # 1 for ref, 1 for call

    def test_post_init_errors(self):
        class Ex(Struct):
            x: int

            def __post_init__(self):
                raise ValueError("Oh no!")

        with pytest.raises(ValueError, match="Oh no!"):
            Ex(1)

    def test_post_init_invalid(self):
        class Bad1(Struct):
            __post_init__ = 1

        class Bad2(Struct):
            def __post_init__(self, other):
                pass

        with pytest.raises(TypeError):
            Bad1()

        with pytest.raises(TypeError):
            Bad2()

    def test_post_init_inheritance(self):
        called = False

        class Base:
            def __post_init__(self):
                nonlocal called
                called = True

        class Ex(Struct, Base):
            x: int

        Ex(1)
        assert called

    def test_post_init_not_called_on_copy(self):
        count = 0

        class Ex(Struct):
            def __post_init__(self):
                nonlocal count
                count += 1

        x1 = Ex()
        assert count == 1
        x2 = x1.__copy__()
        assert x1 == x2
        assert count == 1

    def test_post_init_not_called_on_replace(self):
        count = 0

        class Ex(Struct):
            def __post_init__(self):
                nonlocal count
                count += 1

        x1 = Ex()
        assert count == 1
        x2 = msgspec.structs.replace(x1)
        assert x1 == x2
        assert count == 1
