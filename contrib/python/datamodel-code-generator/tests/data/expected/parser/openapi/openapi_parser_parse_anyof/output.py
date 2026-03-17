from __future__ import annotations

from datetime import date
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, constr


class Pet(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None


class Car(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None


class AnyOfItem1(BaseModel):
    name: Optional[str] = None


class AnyOfItem(BaseModel):
    __root__: Union[Pet, Car, AnyOfItem1, constr(max_length=5000)]


class Item(BaseModel):
    name: Optional[str] = None


class AnyOfobj(BaseModel):
    item: Optional[Union[Pet, Car, Item, constr(max_length=5000)]] = None


class AnyOfArray1(BaseModel):
    name: Optional[str] = None
    birthday: Optional[date] = None


class AnyOfArray(BaseModel):
    __root__: List[Union[Pet, Car, AnyOfArray1, constr(max_length=5000)]]


class Error(BaseModel):
    code: int
    message: str


class Config(BaseModel):
    setting: Optional[Dict[str, Union[str, List[str]]]] = None
