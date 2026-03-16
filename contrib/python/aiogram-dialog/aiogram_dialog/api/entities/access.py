from dataclasses import dataclass
from typing import Any


@dataclass
class AccessSettings:
    user_ids: list[int]
    custom: Any = None
