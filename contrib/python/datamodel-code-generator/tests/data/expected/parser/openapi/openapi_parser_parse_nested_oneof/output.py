from __future__ import annotations

from typing import List, Optional, Union

from pydantic import BaseModel


class Type1(BaseModel):
    prop: Optional[str] = None


class Type2(BaseModel):
    prop: Optional[str] = None


class Container(BaseModel):
    contents: List[Union[Type1, Type2]]
