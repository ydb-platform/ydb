from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field

from .. import Id


class TeA(BaseModel):
    flavour_name: Optional[str] = Field(None, alias='flavour-name')
    id: Optional[Id] = None


class CocoA(BaseModel):
    quality: Optional[int] = None
