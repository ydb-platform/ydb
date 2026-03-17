import sys

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

from .vendor.antlr4.ParserRuleContext import ParserRuleContext

# The antlr4 class `ParserRulerContext` is not a valid type for `mypy` for a number of
# reasons, including lack of strict typing in antlr4-python3-runtime and dynamically
# generated attributes that can only be checked at runtime.
# To be able to use the class for type hinting without raising `valid-type` mypy errors,
# we define a type alias that we can use throughout the code.
# Note that type aliases cannot shadow the name of the class they are aliasing,
# so we need to name the aliased class something different from `ParserRuleContext`

Antlr4ParserRuleContext: TypeAlias = ParserRuleContext  # type: ignore[valid-type]
