from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel


class Item(BaseModel):
    bar: Optional[Any] = None
    foo: str
