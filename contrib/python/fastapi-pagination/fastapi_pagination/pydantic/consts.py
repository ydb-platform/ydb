import re

from pydantic import VERSION

IS_PYDANTIC_V2 = VERSION.startswith("2.")
IS_PYDANTIC_V2_12_5_OR_HIGHER = IS_PYDANTIC_V2 and tuple(map(int, re.findall(r"\d+", VERSION))) >= (2, 12, 5)

__all__ = [
    "IS_PYDANTIC_V2",
    "IS_PYDANTIC_V2_12_5_OR_HIGHER",
]
