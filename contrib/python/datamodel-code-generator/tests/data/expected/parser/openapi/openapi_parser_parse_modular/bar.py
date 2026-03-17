from __future__ import annotations

from pydantic import BaseModel


class Field(BaseModel):
    __root__: str
