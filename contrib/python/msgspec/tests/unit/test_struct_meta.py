"""Tests for the exposed StructMeta metaclass."""

import gc
import re
import secrets
from abc import ABCMeta, _abc_init, abstractmethod

import pytest

import msgspec
from msgspec import Struct, StructMeta
from msgspec.structs import asdict, astuple, force_setattr, replace


def test_struct_meta_exists():
    """Test that StructMeta is properly exposed."""
    assert hasattr(msgspec, "StructMeta")
    assert isinstance(Struct, StructMeta)
    assert issubclass(StructMeta, type)


def test_struct_meta_direct_usage():
    """Test that StructMeta can be used directly as a metaclass."""

    class CustomStruct(metaclass=StructMeta):
        x: int
        y: str

    # Verify the struct works as expected
    instance = CustomStruct(x=1, y="test")
    assert instance.x == 1
    assert instance.y == "test"
    assert isinstance(instance, CustomStruct)
    assert isinstance(CustomStruct, StructMeta)


def test_struct_meta_options():
    """Test that StructMeta properly handles struct options."""

    class CustomStruct(metaclass=StructMeta, frozen=True):
        x: int

    # Verify options were applied
    instance = CustomStruct(x=1)
    with pytest.raises(AttributeError):
        instance.x = 2  # Should be frozen


def test_struct_meta_field_processing():
    """Test that StructMeta properly processes fields."""

    class CustomStruct(metaclass=StructMeta):
        x: int
        y: str = "default"

    # Verify struct functionality
    instance = CustomStruct(x=1)
    assert instance.x == 1
    assert instance.y == "default"

    # Check struct metadata
    assert hasattr(CustomStruct, "__struct_fields__")
    assert "x" in CustomStruct.__struct_fields__
    assert "y" in CustomStruct.__struct_fields__


def test_struct_meta_with_struct_base():
    """Test using StructMeta with Struct as a base class."""

    class CustomStruct(Struct):
        x: int
        y: str

    # Verify the struct works as expected
    instance = CustomStruct(x=1, y="test")
    assert instance.x == 1
    assert instance.y == "test"
    assert isinstance(instance, CustomStruct)
    assert isinstance(CustomStruct, StructMeta)


def test_struct_meta_validation():
    """Test that StructMeta validation works."""
    # Should raise TypeError for invalid field name
    with pytest.raises(TypeError):

        class InvalidStruct(metaclass=StructMeta):
            __dict__: int  # __dict__ is a reserved name


def test_struct_meta_with_options():
    """Test StructMeta with various options."""

    class Point(metaclass=StructMeta, frozen=True, eq=True, order=True):
        x: int
        y: int

    p1 = Point(x=1, y=2)
    p2 = Point(x=1, y=3)

    # Test frozen
    with pytest.raises(AttributeError):
        p1.x = 10

    # Test eq - note that we need to compare fields manually
    # since equality is based on identity by default
    assert p1.x == Point(x=1, y=2).x and p1.y == Point(x=1, y=2).y
    assert p1.x == p2.x and p1.y != p2.y

    # Test order - we can't directly compare instances
    # but we can compare their field values
    assert (p1.x, p1.y) < (p2.x, p2.y)


def test_struct_meta_inheritance():
    """Test that StructMeta can be inherited in Python code."""

    class CustomMeta(StructMeta):
        """A custom metaclass that inherits from StructMeta.

        This metaclass adds a kw_only_default parameter that can be used to
        set the default kw_only value for all subclasses.

        When a class is created with this metaclass:
        1. If kw_only is explicitly specified, use that value
        2. If kw_only is not specified but kw_only_default is, use kw_only_default
        3. If neither is specified but a parent class has kw_only_default defined,
           use the parent's kw_only_default
        4. Otherwise, default to False
        """

        # Class attribute to store kw_only_default settings for each class
        _kw_only_default_settings = {}

        def __new__(mcls, name, bases, namespace, **kwargs):
            # Check if kw_only is explicitly specified
            kw_only_specified = "kw_only" in kwargs

            # Process kw_only_default parameter
            kw_only_default = kwargs.pop("kw_only_default", None)

            # If kw_only_default is specified, store it
            if kw_only_default is not None:
                # Remember this setting for future subclasses
                mcls._kw_only_default_settings[name] = kw_only_default
            else:
                # Check if any parent class has kw_only_default defined
                for base in bases:
                    base_name = base.__name__
                    if base_name in mcls._kw_only_default_settings:
                        # Use parent's kw_only_default
                        kw_only_default = mcls._kw_only_default_settings[base_name]
                        break

            # If kw_only is not specified but kw_only_default is available, use it
            if not kw_only_specified and kw_only_default is not None:
                kwargs["kw_only"] = kw_only_default

            # Create the class
            return super().__new__(mcls, name, bases, namespace, **kwargs)

    # Test basic functionality - without kw_only_default
    class SimpleModel(metaclass=CustomMeta):
        x: int
        y: str

    # Verify the class was created correctly
    assert isinstance(SimpleModel, CustomMeta)
    assert issubclass(CustomMeta, StructMeta)

    # Test creating an instance with positional arguments (should work)
    instance = SimpleModel(1, "test")
    assert instance.x == 1
    assert instance.y == "test"

    # Test setting kw_only_default=True
    class KwOnlyBase(metaclass=CustomMeta, kw_only_default=True):
        """Base class that sets kw_only_default=True"""

    # Test a simple child class, should inherit kw_only_default
    class SimpleChild(KwOnlyBase):
        x: int

    # Should only allow keyword arguments
    with pytest.raises(TypeError):
        SimpleChild(1)

    class BadFieldOrder(KwOnlyBase):
        x: int = 0
        y: int

    BadFieldOrder(y=10)

    # Create instance with keyword arguments
    child = SimpleChild(x=1)
    assert child.x == 1

    # Test overriding inherited kw_only_default
    class NonKwOnlyChild(KwOnlyBase, kw_only=False):
        x: int

    # Should allow positional arguments
    non_kw_child = NonKwOnlyChild(1)
    assert non_kw_child.x == 1

    # Test independent class, not inheriting kw_only_default
    class IndependentModel(metaclass=CustomMeta):
        x: int
        y: str

    # Should allow positional arguments
    independent = IndependentModel(1, "test")
    assert independent.x == 1
    assert independent.y == "test"

    # Print debug information
    print(
        f"KwOnlyBase in _kw_only_default_settings: {'KwOnlyBase' in CustomMeta._kw_only_default_settings}"
    )
    print(
        f"KwOnlyBase default: {CustomMeta._kw_only_default_settings.get('KwOnlyBase')}"
    )
    print(
        f"SimpleChild in _kw_only_default_settings: {'SimpleChild' in CustomMeta._kw_only_default_settings}"
    )

    # Test that kw_only_default values are correctly passed
    assert "KwOnlyBase" in CustomMeta._kw_only_default_settings
    assert CustomMeta._kw_only_default_settings["KwOnlyBase"] is True

    # Test asdict
    d = asdict(independent)
    assert d["x"] == 1
    assert d["y"] == "test"


def test_struct_meta_subclass_functions():
    """Test if structs created by StructMeta subclasses support various function operations."""

    # Define a custom metaclass
    class CustomMeta(StructMeta):
        """Custom metaclass that inherits from StructMeta"""

    # Use the custom metaclass to create a struct class
    class CustomStruct(metaclass=CustomMeta):
        x: int
        y: str
        z: float = 3.14

    # Create an instance
    obj = CustomStruct(x=1, y="test")
    assert obj.x == 1
    assert obj.y == "test"
    assert obj.z == 3.14

    # Test asdict function
    d = asdict(obj)
    assert isinstance(d, dict)
    assert d["x"] == 1
    assert d["y"] == "test"
    assert d["z"] == 3.14

    # Test astuple function
    t = astuple(obj)
    assert isinstance(t, tuple)
    assert t == (1, "test", 3.14)

    # Test replace function
    obj2 = replace(obj, y="replaced")
    assert obj2.x == 1
    assert obj2.y == "replaced"
    assert obj2.z == 3.14

    # Test force_setattr function
    force_setattr(obj, "x", 100)
    assert obj.x == 100

    # Test nested structs
    class NestedStruct(metaclass=CustomMeta):
        inner: CustomStruct
        name: str

    nested = NestedStruct(inner=obj, name="nested")
    assert nested.inner.x == 100
    assert nested.inner.y == "test"
    assert nested.name == "nested"

    # Test asdict with nested structs
    nested_dict = asdict(nested)
    assert isinstance(nested_dict, dict)
    # Note: asdict doesn't recursively convert nested struct objects, so inner remains a CustomStruct object
    assert isinstance(nested_dict["inner"], CustomStruct)
    assert nested_dict["inner"].x == 100
    assert nested_dict["inner"].y == "test"
    assert nested_dict["name"] == "nested"


def test_struct_meta_subclass_inheritance():
    """Test multi-level inheritance of StructMeta subclasses."""

    # Define the first level custom metaclass
    class BaseMeta(StructMeta):
        """Base custom metaclass"""

    # Define the second level custom metaclass
    class DerivedMeta(BaseMeta):
        """Derived custom metaclass"""

    # Use the second level custom metaclass to create a struct class
    class DerivedStruct(metaclass=DerivedMeta):
        a: int
        b: str

    # Create an instance
    obj = DerivedStruct(a=42, b="derived")
    assert obj.a == 42
    assert obj.b == "derived"

    # Test various functions
    # asdict
    d = asdict(obj)
    assert d["a"] == 42
    assert d["b"] == "derived"

    # astuple
    t = astuple(obj)
    assert t == (42, "derived")

    # replace
    obj2 = replace(obj, a=99)
    assert obj2.a == 99
    assert obj2.b == "derived"


def test_struct_meta_subclass_with_encoder():
    """Test compatibility of structs created by StructMeta subclasses with encoders."""

    # Define a custom metaclass
    class EncoderMeta(StructMeta):
        """Custom metaclass for testing encoders"""

    # Use the custom metaclass to create a struct class
    class EncoderStruct(metaclass=EncoderMeta):
        id: int
        name: str
        tags: list[str] = []

    # Create an instance
    obj = EncoderStruct(id=123, name="test")

    # Test JSON encoding and decoding
    json_bytes = msgspec.json.encode(obj)
    decoded = msgspec.json.decode(json_bytes, type=EncoderStruct)

    assert decoded.id == 123
    assert decoded.name == "test"
    assert decoded.tags == []

    # Test encoding and decoding with nested structs
    class Container(metaclass=EncoderMeta):
        item: EncoderStruct
        count: int

    container = Container(item=obj, count=1)
    json_bytes = msgspec.json.encode(container)
    decoded = msgspec.json.decode(json_bytes, type=Container)

    assert decoded.count == 1
    assert decoded.item.id == 123
    assert decoded.item.name == "test"


def test_structmeta_abcmeta_mixed_behaves_like_abc():
    class IntegerStructMeta(StructMeta, ABCMeta):
        pass

    class IntegerStructBase(Struct, metaclass=IntegerStructMeta):
        @abstractmethod
        def to_integer(self) -> int: ...

        @classmethod
        @abstractmethod
        def from_integer(cls, val: int) -> "IntegerStructBase": ...

    class ConcreteIntStruct(IntegerStructBase):
        val: int

        def to_integer(self) -> int:
            return self.val << 2

        @classmethod
        def from_integer(cls, val: int) -> "ConcreteIntStruct":
            return cls(val)

    # Abstract base cannot be instantiated when there are abstract methods
    with pytest.raises(
        TypeError,
        match=(
            r"^Can't instantiate abstract class IntegerStructBase without an "
            r"implementation for abstract methods 'from_integer', 'to_integer'$"
        ),
    ):
        IntegerStructBase()

    # Concrete subclass is fine when all abstract methods are implemented
    obj = ConcreteIntStruct(1)
    assert obj.to_integer() == 4

    # ABC semantics: issubclass / isinstance must work and not raise
    assert issubclass(ConcreteIntStruct, IntegerStructBase)
    assert isinstance(obj, IntegerStructBase)

    # msgspec roundtrip still works
    encoded = msgspec.json.encode(obj)
    decoded = msgspec.json.decode(encoded, type=ConcreteIntStruct)
    assert decoded == obj

    # Repeated checks must continue working (no latent _abc_impl issues)
    for _ in range(5):
        assert issubclass(ConcreteIntStruct, IntegerStructBase)
        assert isinstance(obj, IntegerStructBase)


def test_structmeta_abcmeta_intermediate_still_abstract_and_message():
    class IntegerStructMeta(StructMeta, ABCMeta):
        pass

    class IntegerStructBase(Struct, metaclass=IntegerStructMeta):
        @abstractmethod
        def to_integer(self) -> int: ...

        @classmethod
        @abstractmethod
        def from_integer(cls, val: int) -> "IntegerStructBase": ...

    class Intermediate(IntegerStructBase):
        # Implement only one of the abstract methods
        @classmethod
        def from_integer(cls, val: int) -> "Intermediate":
            return cls()

    # Intermediate remains abstract: only to_integer is missing
    with pytest.raises(
        TypeError,
        match=(
            r"^Can't instantiate abstract class Intermediate without an "
            r"implementation for abstract method 'to_integer'$"
        ),
    ):
        Intermediate()


def test_structmeta_abcmeta_single_abstract_method_message():
    class IntegerStructMeta(StructMeta, ABCMeta):
        pass

    class SingleAbstract(Struct, metaclass=IntegerStructMeta):
        @abstractmethod
        def only(self) -> int: ...

    with pytest.raises(
        TypeError,
        match=(
            r"^Can't instantiate abstract class SingleAbstract without an "
            r"implementation for abstract method 'only'$"
        ),
    ):
        SingleAbstract()


def test_structmeta_abcmeta_mixed_reverse_order():
    class IntegerStructMeta(ABCMeta, StructMeta):
        pass

    class IntegerStructBase(Struct, metaclass=IntegerStructMeta):
        @abstractmethod
        def to_integer(self) -> int: ...

        @classmethod
        @abstractmethod
        def from_integer(cls, val: int) -> "IntegerStructBase": ...

    class ConcreteIntStruct(IntegerStructBase):
        val: int

        def to_integer(self) -> int:
            return self.val + 1

        @classmethod
        def from_integer(cls, val: int) -> "ConcreteIntStruct":
            return cls(val)

    obj = ConcreteIntStruct(10)

    assert issubclass(ConcreteIntStruct, IntegerStructBase)
    assert isinstance(obj, IntegerStructBase)

    encoded = msgspec.json.encode(obj)
    decoded = msgspec.json.decode(encoded, type=ConcreteIntStruct)
    assert decoded == obj


def test_structmeta_abcmeta_mixed_supports_register():
    class IntegerStructMeta(StructMeta, ABCMeta):
        pass

    class IntegerStructBase(Struct, metaclass=IntegerStructMeta):
        @abstractmethod
        def to_integer(self) -> int: ...

        @classmethod
        @abstractmethod
        def from_integer(cls, val: int) -> "IntegerStructBase": ...

    class OtherStruct(Struct):
        val: int

    # Register a non-subclass as a virtual subclass
    IntegerStructBase.register(OtherStruct)

    other = OtherStruct(5)

    # Virtual subclassing should work
    assert issubclass(OtherStruct, IntegerStructBase)
    assert isinstance(other, IntegerStructBase)

    # msgspec usage should still be fine
    encoded = msgspec.json.encode(other)
    decoded = msgspec.json.decode(encoded, type=OtherStruct)
    assert decoded == other


def test_plain_struct_not_treated_as_abc():
    class Plain(Struct):
        x: int

    obj = Plain(1)

    # Normal msgspec behaviour works
    encoded = msgspec.json.encode(obj)
    decoded = msgspec.json.decode(encoded, type=Plain)
    assert decoded == obj

    # Sanity: Plain should not suddenly be an ABC
    # (we don't rely on _abc_impl directly, but this is a cheap guard)
    assert not any(
        base.__module__ == "abc" and base.__name__ == "ABC" for base in Plain.__mro__
    )


def test_structmeta_abcmeta_mixed_nested_subclass():
    class IntegerStructMeta(StructMeta, ABCMeta):
        pass

    class IntegerStructBase(Struct, metaclass=IntegerStructMeta):
        @abstractmethod
        def to_integer(self) -> int: ...

        @classmethod
        @abstractmethod
        def from_integer(cls, val: int) -> "IntegerStructBase": ...

    class Intermediate(IntegerStructBase):
        @classmethod
        def from_integer(cls, val: int) -> "Intermediate":
            return cls()

    class Concrete(Intermediate):
        val: int

        def to_integer(self) -> int:
            return self.val

        @classmethod
        def from_integer(cls, val: int) -> "Concrete":
            return cls(val)

    obj = Concrete(7)

    assert issubclass(Concrete, IntegerStructBase)
    assert isinstance(obj, IntegerStructBase)
    assert isinstance(obj, Intermediate)


def test_structmeta_abcmeta_with_no_abstract_methods_is_concrete():
    class IntegerStructMeta(StructMeta, ABCMeta):
        pass

    class ConcreteBase(Struct, metaclass=IntegerStructMeta):
        # no @abstractmethod
        def foo(self) -> int:
            return 1

    # Should be instantiable (no TypeError)
    obj = ConcreteBase()
    assert obj.foo() == 1

    # And should not be considered abstract
    assert getattr(ConcreteBase, "__abstractmethods__", frozenset()) in (
        frozenset(),
        set(),
    )


def test_struct_abc_via_init_subclass_and__abc_init():
    class ABCStruct(Struct):
        def __init_subclass__(cls, **kwargs):
            super().__init_subclass__(**kwargs)
            _abc_init(cls)

    class Base(ABCStruct):
        @abstractmethod
        def foo(self) -> int: ...

    # Base is abstract; instantiation should fail
    with pytest.raises(
        TypeError,
        match=r"Can't instantiate abstract class Base without an implementation for abstract method 'foo'",
    ):
        Base()

    class Concrete(Base):
        x: int

        def foo(self) -> int:
            return self.x

    c = Concrete(5)
    assert c.foo() == 5


def test_struct_meta_pattern_ref_leak():
    # ensure that we're not keeping around references to re.Pattern longer than necessary
    # see https://github.com/jcrist/msgspec/pull/899 for details

    # clear cache to get a baseline
    re.purge()

    # use a random string to create a pattern, to ensure there can never be an overlap
    # with any cached pattern
    pattern_string = secrets.token_hex()
    msgspec.Meta(pattern=pattern_string)
    # purge cache and gc again
    re.purge()
    gc.collect()
    # there shouldn't be an re.Pattern with our pattern any more. if there is, it's
    # being kept alive by some reference
    assert not any(
        o
        for o in gc.get_objects()
        if isinstance(o, re.Pattern) and o.pattern == pattern_string
    )
