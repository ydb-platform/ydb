from __future__ import annotations

from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel


class Species(Enum):
    dog = 'dog'
    cat = 'cat'
    snake = 'snake'


class Pet(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None
    species: Optional[Species] = None


class User(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None


class Event(BaseModel):
    name: Optional[Union[str, float, int, bool, Dict[str, Any], List[str]]] = None
