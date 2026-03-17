from collections.abc import Iterator
from itertools import groupby, product
from operator import itemgetter
from typing import cast, final

from mypy.nodes import ARG_STAR, ARG_STAR2
from mypy.plugin import FunctionContext
from mypy.types import (
    AnyType,
    CallableType,
    FunctionLike,
    Overloaded,
    TypeOfAny,
    get_proper_type,
)
from mypy.types import Type as MypyType

from returns.contrib.mypy._structures.args import FuncArg
from returns.contrib.mypy._typeops.transform_callable import (
    Intermediate,
    proper_type,
)

#: Raw material to build `_ArgTree`.
_RawArgTree = list[list[list[FuncArg]]]


def analyze(ctx: FunctionContext) -> MypyType:
    """Returns proper type for curried functions."""
    default_return = get_proper_type(ctx.default_return_type)
    arg_type = get_proper_type(ctx.arg_types[0][0])
    if not isinstance(arg_type, CallableType):
        return default_return
    if not isinstance(default_return, CallableType):
        return default_return

    return _CurryFunctionOverloads(arg_type, ctx).build_overloads()


@final
class _ArgTree:
    """Represents a node in tree of arguments."""

    def __init__(self, case: CallableType | None) -> None:
        self.case = case
        self.children: list[_ArgTree] = []


@final
class _CurryFunctionOverloads:
    """
    Implementation of ``@curry`` decorator typings.

    Basically does just two things:

    1. Creates all possible ordered combitations of arguments
    2. Creates ``Overload`` instances for functions' return types

    """

    def __init__(self, original: CallableType, ctx: FunctionContext) -> None:
        """
        Saving the things we need.

        Args:
            original: original function that was passed to ``@curry``.
            ctx: function context.

        """
        self._original = original
        self._ctx = ctx
        self._overloads: list[CallableType] = []
        self._args = FuncArg.from_callable(self._original)

        # We need to get rid of generics here.
        # Because, otherwise `detach_callable` with add
        # unused variables to intermediate callables.
        self._default = cast(
            CallableType,
            self._ctx.default_return_type,
        ).copy_modified(
            ret_type=AnyType(TypeOfAny.implementation_artifact),
        )

    def build_overloads(self) -> MypyType:
        """
        Builds lots of possible overloads for a given function.

        Inside we try to repsent all functions as sequence of arguments,
        grouped by the similar ones and returning one more overload instance.
        """
        if not self._args:  # There's nothing to do, function has 0 args.
            return self._original

        if any(arg.kind in {ARG_STAR, ARG_STAR2} for arg in self._args):
            # We don't support `*args` and `**kwargs`.
            # Because it is very complex. It might be fixes in the future.
            return self._default.ret_type  # Any

        argtree = self._build_argtree(
            _ArgTree(None),  # starting from root node
            list(self._slices(self._args)),
        )
        self._build_overloads_from_argtree(argtree)
        return proper_type(self._overloads)

    def _build_argtree(
        self,
        node: _ArgTree,
        source: _RawArgTree,
    ) -> '_ArgTree':
        """
        Builds argument tree.

        Each argument can point to zero, one, or more other nodes.
        Arguments that have zero children are treated as bottom (last) ones.
        Arguments that have just one child are meant to be regular functions.
        Arguments that have more than one child are treated as overloads.

        """

        def factory(
            args: _RawArgTree,
        ) -> Iterator[tuple[list[FuncArg], _RawArgTree]]:
            if not args or not args[0]:
                return  # we have reached an end of arguments
            yield from (
                (case, [group[1:] for group in grouped])
                for case, grouped in groupby(args, itemgetter(0))
            )

        for case, rest in factory(source):
            new_node = _ArgTree(
                Intermediate(self._default).with_signature(case),
            )
            node.children.append(new_node)
            self._build_argtree(source=rest, node=new_node)
        return node

    def _build_overloads_from_argtree(self, argtree: _ArgTree) -> None:
        """Generates functions from argument tree."""
        for child in argtree.children:
            self._build_overloads_from_argtree(child)
            assert child.case  # mypy is not happy

            if not child.children:
                child.case = Intermediate(child.case).with_ret_type(
                    self._original.ret_type,
                )

            if argtree.case is None:
                # Root is reached, we need to save the result:
                self._overloads.append(child.case)
            else:
                # We need to go backwards and to replace the return types
                # of the previous functions. Like so:
                # 1.  `def x -> A`
                # 2.  `def y -> A`
                # Will take `2` and apply its type to the previous function `1`.
                # Will result in `def x -> y -> A`
                # We also overloadify existing return types.
                ret_type = get_proper_type(argtree.case.ret_type)
                temp_any = (
                    isinstance(
                        ret_type,
                        AnyType,
                    )
                    and ret_type.type_of_any
                    == TypeOfAny.implementation_artifact
                )
                argtree.case = Intermediate(argtree.case).with_ret_type(
                    child.case
                    if temp_any
                    else Overloaded(
                        [child.case, *cast(FunctionLike, ret_type).items],
                    ),
                )

    def _slices(self, source: list[FuncArg]) -> Iterator[list[list[FuncArg]]]:
        """
        Generate all possible slices of a source list.

        Example::

          _slices("AB") ->
            "AB"
            "A" "B"

          _slices("ABC") ->
            "ABC"
            "AB" "C"
            "A" "BC"
            "A" "B" "C"

        """
        for doslice in product([True, False], repeat=len(source) - 1):
            slices = []
            start = 0
            for index, slicehere in enumerate(doslice, 1):
                if slicehere:
                    slices.append(source[start:index])
                    start = index
            slices.append(source[start:])
            yield slices
