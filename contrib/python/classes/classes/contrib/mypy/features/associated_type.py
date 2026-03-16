from mypy.plugin import AnalyzeTypeContext
from mypy.types import Type as MypyType

from classes.contrib.mypy.semanal.variadic_generic import (
    analize_variadic_generic,
)


def variadic_generic(ctx: AnalyzeTypeContext) -> MypyType:
    """Variadic generic support for ``AssociatedType`` type."""
    return analize_variadic_generic(ctx)
