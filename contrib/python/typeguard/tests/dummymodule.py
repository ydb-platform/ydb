"""Module docstring."""

import sys
from contextlib import contextmanager
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Callable,
    Dict,
    Generator,
    List,
    Literal,
    Sequence,
    Tuple,
    Type,
    TypedDict,
    TypeVar,
    Union,
    no_type_check,
    no_type_check_decorator,
    overload,
)

from typeguard import (
    CollectionCheckStrategy,
    ForwardRefPolicy,
    typechecked,
    typeguard_ignore,
)

if sys.version_info >= (3, 10):
    from typing import ParamSpec
else:
    from typing_extensions import ParamSpec

if TYPE_CHECKING:
    from nonexistent import Imaginary

T = TypeVar("T", bound="DummyClass")
P = ParamSpec("P")


if sys.version_info <= (3, 13):

    @no_type_check_decorator
    def dummy_decorator(func):
        return func

    @dummy_decorator
    def non_type_checked_decorated_func(x: int, y: str) -> 6:
        # This is to ensure that we avoid using a local variable that's already in use
        _call_memo = "foo"  # noqa: F841
        return "foo"


@typechecked
def type_checked_func(x: int, y: int) -> int:
    return x * y


@no_type_check
def non_type_checked_func(x: int, y: str) -> 6:
    return "foo"


@typeguard_ignore
def non_typeguard_checked_func(x: int, y: str) -> 6:
    return "foo"


class Metaclass(type):
    pass


@typechecked
class DummyClass(metaclass=Metaclass):
    bar: str
    baz: int

    def type_checked_method(self, x: int, y: int) -> int:
        return x * y

    @classmethod
    def type_checked_classmethod(cls, x: int, y: int) -> int:
        return x * y

    @staticmethod
    def type_checked_staticmethod(x: int, y: int) -> int:
        return x * y

    @classmethod
    def undocumented_classmethod(cls, x, y):
        pass

    @staticmethod
    def undocumented_staticmethod(x, y):
        pass

    @property
    def unannotated_property(self):
        return None


def outer():
    @typechecked
    class Inner:
        def get_self(self) -> "Inner":
            return self

    def create_inner() -> "Inner":
        return Inner()

    return create_inner


@typechecked
class Outer:
    class Inner:
        pass

    def create_inner(self) -> "Inner":
        return Outer.Inner()

    @classmethod
    def create_inner_classmethod(cls) -> "Inner":
        return Outer.Inner()

    @staticmethod
    def create_inner_staticmethod() -> "Inner":
        return Outer.Inner()


@contextmanager
@typechecked
def dummy_context_manager() -> Generator[int, None, None]:
    yield 1


@overload
def overloaded_func(a: int) -> int: ...


@overload
def overloaded_func(a: str) -> str: ...


@typechecked
def overloaded_func(a: Union[str, int]) -> Union[str, int]:
    return a


@typechecked
def missing_return() -> int:
    pass


def get_inner_class() -> type:
    @typechecked
    class InnerClass:
        def get_self(self) -> "InnerClass":
            return self

    return InnerClass


def create_local_class_instance() -> object:
    class Inner:
        pass

    @typechecked
    def get_instance() -> "Inner":
        return instance

    instance = Inner()
    return get_instance()


@typechecked
async def async_func(a: int) -> str:
    return str(a)


@typechecked
def generator_func(yield_value: Any, return_value: Any) -> Generator[int, Any, str]:
    yield yield_value
    return return_value


@typechecked
async def asyncgen_func(yield_value: Any) -> AsyncGenerator[int, Any]:
    yield yield_value


@typechecked
def pep_604_union_args(
    x: "Callable[[], Literal[-1]] | Callable[..., Union[int, str]]",
) -> None:
    pass


@typechecked
def pep_604_union_retval(x: Any) -> "str | int":
    return x


@typechecked
def builtin_generic_collections(x: "list[set[int]]") -> Any:
    return x


@typechecked
def paramspec_function(func: P, args: P.args, kwargs: P.kwargs) -> None:
    pass


@typechecked
def aug_assign() -> int:
    x: int = 1
    x += 1
    return x


@typechecked
def multi_assign_single_value() -> Tuple[int, float, complex]:
    x: int
    y: float
    z: complex
    x = y = z = 6
    return x, y, z


@typechecked
def multi_assign_iterable() -> Tuple[Sequence[int], Sequence[float], Sequence[complex]]:
    x: Sequence[int]
    y: Sequence[float]
    z: Sequence[complex]
    x = y = z = [6, 7]
    return x, y, z


@typechecked
def unpacking_assign() -> Tuple[int, str]:
    x: int
    x, y = (1, "foo")
    return x, y


@typechecked
def unpacking_assign_single_item_tuple() -> str:
    x: str
    (x,) = ("foo",)
    return x


@typechecked
def unpacking_assign_generator() -> Tuple[int, str]:
    def genfunc():
        yield 1
        yield "foo"

    x: int
    x, y = genfunc()
    return x, y


@typechecked
def unpacking_assign_star_with_annotation() -> Tuple[int, List[bytes], str]:
    x: int
    z: str
    x, *y, z = (1, b"abc", b"bah", "foo")
    return x, y, z


@typechecked
def unpacking_assign_star_no_annotation(value: Any) -> Tuple[int, List[bytes], str]:
    x: int
    y: List[bytes]
    z: str
    x, *y, z = value
    return x, y, z


@typechecked
def attribute_assign_unpacking(obj: DummyClass) -> None:
    obj.bar, obj.baz = "foo", 123123


@typechecked(forward_ref_policy=ForwardRefPolicy.ERROR)
def override_forward_ref_policy(value: "NonexistentType") -> None:  # noqa: F821
    pass


@typechecked(typecheck_fail_callback=lambda exc, memo: print(exc))
def override_typecheck_fail_callback(value: int) -> None:
    pass


@typechecked(collection_check_strategy=CollectionCheckStrategy.ALL_ITEMS)
def override_collection_check_strategy(value: List[int]) -> None:
    pass


@typechecked(typecheck_fail_callback=lambda exc, memo: print(exc))
class OverrideClass:
    def override_typecheck_fail_callback(self, value: int) -> None:
        pass

    class Inner:
        @typechecked
        def override_typecheck_fail_callback(self, value: int) -> None:
            pass


@typechecked
def typed_variable_args(
    *args: str, **kwargs: int
) -> Tuple[Tuple[str, ...], Dict[str, int]]:
    return args, kwargs


@typechecked
def guarded_type_hint_plain(x: "Imaginary") -> "Imaginary":
    y: Imaginary = x
    return y


@typechecked
def guarded_type_hint_subscript_toplevel(x: "Imaginary[int]") -> "Imaginary[int]":
    y: Imaginary[int] = x
    return y


@typechecked
def guarded_type_hint_subscript_nested(
    x: List["Imaginary[int]"],
) -> List["Imaginary[int]"]:
    y: List[Imaginary[int]] = x
    return y


@typechecked
def literal(x: Literal["foo"]) -> Literal["foo"]:
    y: Literal["foo"] = x
    return y


@typechecked
def literal_in_union(x: Union[Literal["foo"],]) -> Literal["foo"]:
    y: Literal["foo"] = x
    return y


@typechecked
def typevar_forwardref(x: Type[T]) -> T:
    return x()


def never_called(x: List["NonExistentType"]) -> List["NonExistentType"]:  # noqa: F821
    """Regression test for #335."""
    return x


# Regression test for #536 - forward reference evaluation on Python 3.14
class ModuleLocalClass:
    """A class only available in this module's namespace, not the caller's."""


class TypedDictWithForwardRef(TypedDict):
    x: "ModuleLocalClass"
