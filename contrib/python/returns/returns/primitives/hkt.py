from collections.abc import Callable
from typing import TYPE_CHECKING, Any, Generic, Protocol, TypeVar

from typing_extensions import Never, TypeVarTuple, Unpack

_InstanceType_co = TypeVar('_InstanceType_co', covariant=True)
_TypeArgType1_co = TypeVar('_TypeArgType1_co', covariant=True)
_TypeArgType2_co = TypeVar('_TypeArgType2_co', covariant=True)
_TypeArgType3_co = TypeVar('_TypeArgType3_co', covariant=True)

_FunctionDefType_co = TypeVar(
    '_FunctionDefType_co',
    bound=Callable,
    covariant=True,  # This is a must! Otherwise it would not work.
)
_FunctionType = TypeVar(
    '_FunctionType',
    bound=Callable,
)

_UpdatedType = TypeVar('_UpdatedType')
_TypeVars = TypeVarTuple('_TypeVars')


class KindN(Generic[_InstanceType_co, Unpack[_TypeVars]]):
    """
    Emulation support for Higher Kinded Types.

    Consider ``KindN`` to be an alias of ``Generic`` type.
    But with some extra goodies.

    ``KindN`` is the top-most type for other ``Kind`` types
    like ``Kind1``, ``Kind2``, ``Kind3``, etc.

    The only difference between them is how many type arguments they can hold.
    ``Kind1`` can hold just two type arguments: ``Kind1[IO, int]``
    which is almost equals to ``IO[int]``.
    ``Kind2`` can hold just two type arguments: ``Kind2[IOResult, int, str]``
    which is almost equals to ``IOResult[int, str]``.
    And so on.

    The idea behind ``KindN`` is that one cannot write this code:

    .. code:: python

      from typing import TypeVar

      T = TypeVar('T')
      V = TypeVar('V')

      def impossible(generic: T, value: V) -> T[V]:
          return generic(value)

    But, with ``KindN`` this becomes possible in a form of ``Kind1[T, V]``.

    .. note::
        To make sure it works correctly,
        your type has to be a subtype of ``KindN``.

    We use a custom ``mypy`` plugin to make sure types are correct.
    Otherwise, it is currently impossible to properly type this.

    We use "emulated Higher Kinded Types" concept.
    Read the whitepaper: https://bit.ly/2ABACx2

    ``KindN`` does not exist in runtime. It is used just for typing.
    There are (and must be) no instances of this type directly.

    .. rubric:: Implementation details

    We didn't use ``ABCMeta`` to disallow its creation,
    because we don't want to have
    a possible metaclass conflict with other metaclasses.
    Current API allows you to mix ``KindN`` anywhere.

    We allow ``_InstanceType_co`` of ``KindN``
    to be ``Instance`` type or ``TypeVarType`` with ``bound=...``.

    See also:
        - https://arrow-kt.io/docs/0.10/patterns/glossary/#higher-kinds
        - https://github.com/gcanti/fp-ts/blob/master/docs/guides/HKT.md
        - https://bow-swift.io/docs/fp-concepts/higher-kinded-types
        - https://github.com/pelotom/hkts

    """

    __slots__ = ()

    if TYPE_CHECKING:  # noqa: WPS604 # pragma: no cover

        def __getattr__(self, attrname: str):
            """
            This function is required for ``get_attribute_hook`` in mypy plugin.

            It is never called in real-life, because ``KindN`` is abstract.
            It only exists during the type-checking phase.
            """


#: Type alias for kinds with one type argument.
Kind1 = KindN[_InstanceType_co, _TypeArgType1_co, Any, Any]

#: Type alias for kinds with two type arguments.
Kind2 = KindN[_InstanceType_co, _TypeArgType1_co, _TypeArgType2_co, Any]

#: Type alias for kinds with three type arguments.
Kind3 = KindN[
    _InstanceType_co, _TypeArgType1_co, _TypeArgType2_co, _TypeArgType3_co
]


class SupportsKindN(KindN[_InstanceType_co, Unpack[_TypeVars]]):
    """
    Base class for your containers.

    Notice, that we use ``KindN`` / ``Kind1`` to annotate values,
    but we use ``SupportsKindN`` / ``SupportsKind1`` to inherit from.

    .. rubric:: Implementation details

    The only thing this class does is: making sure that the resulting classes
    won't have ``__getattr__`` available during the typechecking phase.

    Needless to say, that ``__getattr__`` during runtime - never exists at all.
    """

    __slots__ = ()

    __getattr__: None  # type: ignore


#: Type alias used for inheritance with one type argument.
SupportsKind1 = SupportsKindN[
    _InstanceType_co,
    _TypeArgType1_co,
    Never,
    Never,
]

#: Type alias used for inheritance with two type arguments.
SupportsKind2 = SupportsKindN[
    _InstanceType_co,
    _TypeArgType1_co,
    _TypeArgType2_co,
    Never,
]

#: Type alias used for inheritance with three type arguments.
SupportsKind3 = SupportsKindN[
    _InstanceType_co,
    _TypeArgType1_co,
    _TypeArgType2_co,
    _TypeArgType3_co,
]


def dekind(
    kind: KindN[
        _InstanceType_co, _TypeArgType1_co, _TypeArgType2_co, _TypeArgType3_co
    ],
) -> _InstanceType_co:
    """
    Turns ``Kind1[IO, int]`` type into real ``IO[int]`` type.

    Should be used when you are left with accidental ``KindN`` instance
    when you really want to have the real type.

    Works with type arguments of any length.

    We use a custom ``mypy`` plugin to make sure types are correct.
    Otherwise, it is currently impossible to properly type this.

    In runtime it just returns the passed argument, nothing really happens:

    .. code:: python

      >>> from returns.io import IO
      >>> from returns.primitives.hkt import Kind1

      >>> container: Kind1[IO, int] = IO(1)
      >>> assert dekind(container) is container

    However, please, do not use this function
    unless you know exactly what you are doing and why do you need it.
    """
    return kind  # type: ignore


# Utils to define kinded functions
# ================================


# TODO: in the future we would be able to write a custom plugin
# with `transform_kind(T) -> T'` support.
# It would visit all the possible `KindN[]` types in any type and run `dekind`
# on them, so this will be how it works:
# in:  => Callable[[KindN[IO[Any], int]], KindN[IO[Any], str]]
# out: => Callable[[IO[int]], IO[str]]
# This will allow to have better support for callable protocols and similar.
# Blocked by: https://github.com/python/mypy/issues/9001
class Kinded(Protocol[_FunctionDefType_co]):  # type: ignore
    """
    Protocol that tracks kinded functions calls.

    We use a custom ``mypy`` plugin to make sure types are correct.
    Otherwise, it is currently impossible to properly type this.
    """

    __slots__ = ()

    #: Used to translate `KindN` into real types.
    __call__: _FunctionDefType_co

    def __get__(
        self,
        instance: _UpdatedType,
        type_,
    ) -> Callable[..., _UpdatedType]:
        """Used to decorate and properly analyze method calls."""


def kinded(function: _FunctionType) -> Kinded[_FunctionType]:
    """
    Decorator to be used when you want to dekind the function's return type.

    Does nothing in runtime, just returns its argument.

    We use a custom ``mypy`` plugin to make sure types are correct.
    Otherwise, it is currently impossible to properly type this.

    Here's an example of how it should be used:

    .. code:: python

      >>> from typing import TypeVar
      >>> from returns.primitives.hkt import KindN, kinded
      >>> from returns.interfaces.bindable import BindableN

      >>> _Binds = TypeVar('_Binds', bound=BindableN)  # just an example
      >>> _Type1 = TypeVar('_Type1')
      >>> _Type2 = TypeVar('_Type2')
      >>> _Type3 = TypeVar('_Type3')

      >>> @kinded
      ... def bindable_identity(
      ...    container: KindN[_Binds, _Type1, _Type2, _Type3],
      ... ) -> KindN[_Binds, _Type1, _Type2, _Type3]:
      ...     return container  # just do nothing

    As you can see, here we annotate our return type as
    ``-> KindN[_Binds, _Type1, _Type2, _Type3]``,
    it would be true without ``@kinded`` decorator.

    But, ``@kinded`` decorator dekinds the return type and infers
    the real type behind it:

    .. code:: python

      >>> from returns.io import IO, IOResult

      >>> assert bindable_identity(IO(1)) == IO(1)
      >>> # => Revealed type: 'IO[int]'

      >>> iores: IOResult[int, str] = IOResult.from_value(1)
      >>> assert bindable_identity(iores) == iores
      >>> # => Revealed type: 'IOResult[int, str]'

    The difference is very clear in ``methods`` modules, like:

    - Raw :func:`returns.methods.bind.internal_bind`
      that returns ``KindN`` instance
    - User-facing :func:`returns.methods.bind.bind`
      that returns the container type

    You must use this decorator for your own kinded functions as well.
    """
    return function  # type: ignore
