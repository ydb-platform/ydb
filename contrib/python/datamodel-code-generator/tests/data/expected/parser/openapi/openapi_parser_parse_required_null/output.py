from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class Type(Enum):
    my_first_object = 'my_first_object'
    my_second_object = 'my_second_object'
    my_third_object = 'my_third_object'


class ObjectBase(BaseModel):
    name: Optional[str] = Field(None, description='Name of the object')
    type: Optional[Type] = Field(None, description='Object type')
