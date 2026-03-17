# Given the whole thing is a utility package, this is really meta.
from typing import Any, Dict, Optional

Categories = Dict[str, Optional[str]]
Encoding = str


def is_text(data: Any) -> bool:
    return isinstance(data, str)
