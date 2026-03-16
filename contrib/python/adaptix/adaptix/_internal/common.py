from typing import TYPE_CHECKING, Any, Callable, TypeVar, Union

K_contra = TypeVar("K_contra", contravariant=True)
V_co = TypeVar("V_co", covariant=True)
T = TypeVar("T")

Loader = Callable[[Any], V_co]
Dumper = Callable[[K_contra], Any]
Converter = Callable[..., Any]
Coercer = Callable[[Any, Any], Any]
OneArgCoercer = Callable[[Any], Any]

TypeHint = Any

VarTuple = tuple[T, ...]

Catchable = Union[type[BaseException], VarTuple[type[BaseException]]]

# https://github.com/python/typing/issues/684#issuecomment-548203158
if TYPE_CHECKING:
    EllipsisType = ellipsis  # noqa: F821
else:
    EllipsisType = type(Ellipsis)
