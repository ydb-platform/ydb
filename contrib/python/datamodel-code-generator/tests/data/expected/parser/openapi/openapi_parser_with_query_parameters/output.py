from __future__ import annotations

from enum import Enum
from typing import List, Optional, Union

from pydantic import BaseModel


class Pet(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None


class Error(BaseModel):
    code: int
    message: str


class PetForm(BaseModel):
    name: Optional[str] = None
    age: Optional[int] = None


class PetsPetIdGetParametersQuery(BaseModel):
    include: Optional[str] = None


class Filter(BaseModel):
    type: Optional[str] = None
    color: Optional[str] = None


class MediaType(Enum):
    xml = 'xml'
    json = 'json'


class MultipleMediaFilter(BaseModel):
    type: Optional[str] = None
    media_type: Optional[MediaType] = 'xml'


class MultipleMediaFilter1(BaseModel):
    type: Optional[str] = None
    media_type: Optional[MediaType] = 'json'


class PetsGetParametersQuery(BaseModel):
    limit: Optional[int] = 0
    HomeAddress: Optional[str] = 'Unknown'
    kind: Optional[str] = 'dog'
    filter: Optional[Filter] = None
    multipleMediaFilter: Optional[
        Union[MultipleMediaFilter, MultipleMediaFilter1]
    ] = None


class PetsGetResponse(BaseModel):
    __root__: List[Pet]


class PetsPostRequest(BaseModel):
    name: Optional[str] = None
    age: Optional[int] = None
