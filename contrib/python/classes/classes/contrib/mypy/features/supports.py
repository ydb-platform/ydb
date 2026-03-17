from mypy.nodes import SymbolTableNode
from mypy.plugin import AnalyzeTypeContext
from mypy.types import Instance
from mypy.types import Type as MypyType
from mypy.types import UnionType
from typing_extensions import final

from classes.contrib.mypy.semanal.variadic_generic import (
    analize_variadic_generic,
)
from classes.contrib.mypy.validation import validate_supports


@final
class VariadicGeneric(object):
    """
    Variadic generic support for ``Supports`` type.

    We also need to validate that
    all type args of ``Supports`` are subtypes of ``AssociatedType``.
    """

    __slots__ = ('_associated_type_node',)

    def __init__(self, associated_type_node: SymbolTableNode) -> None:
        """We need ``AssociatedType`` fullname here."""
        self._associated_type_node = associated_type_node

    def __call__(self, ctx: AnalyzeTypeContext) -> MypyType:
        """Main entry point."""
        analyzed_type = analize_variadic_generic(
            validate_callback=self._validate,
            ctx=ctx,
        )
        if isinstance(analyzed_type, Instance):
            return analyzed_type.copy_modified(
                args=[UnionType.make_union(analyzed_type.args)],
            )
        return analyzed_type

    def _validate(self, instance: Instance, ctx: AnalyzeTypeContext) -> bool:
        return validate_supports.check_type(
            instance,
            self._associated_type_node,
            ctx,
        )
