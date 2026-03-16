from typing import Callable, Iterable

from mypy.plugin import MethodContext
from mypy.type_visitor import TypeQuery
from mypy.types import AnyType, Instance, TupleType
from mypy.types import Type as MypyType
from mypy.types import TypeVarType, UnboundType, get_proper_type


def has_concrete_type(
    type_: MypyType,
    is_delegate: bool,
    ctx: MethodContext,
    *,
    forbid_explicit_any: bool,
) -> bool:
    """
    Queries if your instance has any concrete types.

    What do we call "concrete types"? Some examples:

    ``List[X]`` is generic, ``List[int]`` is concrete.
    ``List[Union[int, str]]`` is also concrete.
    ``Dict[str, X]`` is also concrete.

    ``Tuple[X, ...]`` is generic.
    While ``Tuple[X, X]`` and ``Tuple[int, ...]`` are concrete.

    So, this helps to write code like this:

    .. code:: python

      @some.instance(list)
      def _some_list(instance: List[X]): ...

    And not like:

    .. code:: python

      @some.instance(list)
      def _some_list(instance: List[int]): ...

    """
    def factory(typ) -> bool:
        return not is_delegate

    type_ = get_proper_type(type_)
    # TODO: support `Literal`?
    if isinstance(type_, TupleType):
        # This allows to have types like:
        #
        #   @some.instance(delegate=Coords)
        #   def _some_coords(instance: Tuple[int, int]) -> str:
        #       ...
        return not is_delegate
    if isinstance(type_, Instance):
        return any(
            type_arg.accept(_HasNoConcreteTypes(
                factory,
                forbid_explicit_any=forbid_explicit_any,
            ))
            for type_arg in type_.args
        )
    return False


def has_unbound_type(type_: MypyType, ctx: MethodContext) -> bool:
    """
    Queries if your instance has any unbound types.

    Note, that you need to understand
    how semantic and type analyzers work in ``mypy``
    to understand what "unbound type" is.

    Long story short, this helps to write code like this:

    .. code:: python

      @some.instance(list)
      def _some_list(instance: List[X]): ...

    And not like:

    .. code:: python

      @some.instance(List[X])
      def _some_list(instance: List[X]): ...

    """
    type_ = get_proper_type(type_)
    if isinstance(type_, Instance):
        return any(
            type_arg.accept(_HasUnboundTypes(lambda _: False))
            for type_arg in type_.args
        )
    return False


class _HasNoConcreteTypes(TypeQuery[bool]):
    def __init__(
        self,
        strategy: Callable[[Iterable[bool]], bool],
        *,
        forbid_explicit_any: bool,
    ) -> None:
        super().__init__(strategy)
        self._forbid_explicit_any = forbid_explicit_any

    def visit_type_var(self, type_: TypeVarType) -> bool:
        return False

    def visit_unbound_type(self, type_: UnboundType) -> bool:
        return False

    def visit_any(self, type_: AnyType) -> bool:
        return self._forbid_explicit_any


class _HasUnboundTypes(TypeQuery[bool]):
    def visit_unbound_type(self, type_: UnboundType) -> bool:
        return True
