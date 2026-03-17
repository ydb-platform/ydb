from __future__ import annotations

from typing import Optional

from pydantic import BaseModel

from .. import Id


class Tea(BaseModel):
    flavour: Optional[str] = None
    id: Optional[Id] = None


class Cocoa(BaseModel):
    quality: Optional[int] = None
