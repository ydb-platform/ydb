from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel

from .. import Id, OptionalModel


class Tea(BaseModel):
    flavour: Optional[str] = None
    id: Optional[Id] = None
    self: Optional[Tea] = None
    optional: Optional[List[OptionalModel]] = None


class TeaClone(BaseModel):
    flavour: Optional[str] = None
    id: Optional[Id] = None
    self: Optional[Tea] = None
    optional: Optional[List[OptionalModel]] = None


class ListModel(BaseModel):
    __root__: List[Tea]
