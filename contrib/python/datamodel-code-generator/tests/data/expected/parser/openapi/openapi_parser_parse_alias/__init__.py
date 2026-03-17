from __future__ import annotations

from datetime import date, datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, conint

from . import model_s


class Pet(Enum):
    ca_t = 'ca-t'
    dog_ = 'dog*'


class Error(BaseModel):
    code: int
    message: str


class HomeAddress(BaseModel):
    address_1: Optional[str] = Field(None, alias='address-1')


class TeamMembers(BaseModel):
    __root__: List[str]


class AllOfObj(BaseModel):
    name: Optional[str] = None
    number: Optional[str] = None


class Id(BaseModel):
    __root__: str


class Result(BaseModel):
    event: Optional[model_s.EvenT] = None


class Source(BaseModel):
    country_name: Optional[str] = Field(None, alias='country-name')


class UserName(BaseModel):
    first_name: Optional[str] = Field(None, alias='first-name')
    home_address: Optional[HomeAddress] = Field(None, alias='home-address')


class AllOfRef(UserName, HomeAddress):
    pass


class AllOfCombine(UserName):
    birth_date: Optional[date] = Field(None, alias='birth-date')
    size: Optional[conint(ge=1)] = None


class AnyOfCombine(HomeAddress, UserName):
    age: Optional[str] = None


class Item(HomeAddress, UserName):
    age: Optional[str] = None


class AnyOfCombineInObject(BaseModel):
    item: Optional[Item] = None


class AnyOfCombineInArrayItem(HomeAddress, UserName):
    age: Optional[str] = None


class AnyOfCombineInArray(BaseModel):
    __root__: List[AnyOfCombineInArrayItem]


class AnyOfCombineInRoot(HomeAddress, UserName):
    age: Optional[str] = None
    birth_date: Optional[datetime] = Field(None, alias='birth-date')
