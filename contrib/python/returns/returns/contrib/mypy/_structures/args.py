from itertools import starmap
from typing import NamedTuple, final

from mypy.nodes import ArgKind, Context, TempNode
from mypy.types import CallableType
from mypy.types import Type as MypyType


class _FuncArgStruct(NamedTuple):
    """Basic struct to represent function arguments."""

    name: str | None
    type: MypyType  # noqa: WPS125
    kind: ArgKind


@final
class FuncArg(_FuncArgStruct):
    """Representation of function arg with all required fields and methods."""

    def expression(self, context: Context) -> TempNode:
        """Hack to pass unexisting `Expression` to typechecker."""
        return TempNode(self.type, context=context)

    @classmethod
    def from_callable(cls, function_def: CallableType) -> list['FuncArg']:
        """Public constructor to create FuncArg lists from callables."""
        parts = zip(
            function_def.arg_names,
            function_def.arg_types,
            function_def.arg_kinds,
            strict=False,
        )
        return list(starmap(cls, parts))
