from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class M(BaseModel):
    name: Optional[str] = None


class R(M):
    pass
