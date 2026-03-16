from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class Thing(BaseModel):
    attributes: Optional[Dict[str, Any]] = None


class Thang(BaseModel):
    attributes: Optional[List[Dict[str, Any]]] = None


class Others(BaseModel):
    name: Optional[str] = None


class Clone(Thing):
    others: Optional[Others] = None
