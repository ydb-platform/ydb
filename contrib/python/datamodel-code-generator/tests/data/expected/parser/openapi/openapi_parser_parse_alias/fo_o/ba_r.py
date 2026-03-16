from __future__ import annotations

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class ThinG(BaseModel):
    attribute_s: Optional[Dict[str, Any]] = Field(None, alias='attribute-s')


class ThanG(BaseModel):
    attributes: Optional[List[Dict[str, Any]]] = None


class ClonE(ThinG):
    pass
