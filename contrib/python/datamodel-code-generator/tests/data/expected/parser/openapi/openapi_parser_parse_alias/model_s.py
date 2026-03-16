from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


class SpecieS(Enum):
    dog = 'dog'
    cat = 'cat'
    snake = 'snake'


class PeT(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None
    species: Optional[SpecieS] = None


class UseR(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None


class EvenT(BaseModel):
    name: Optional[Union[str, float, int, bool, Dict[str, Any], List[str]]] = None
