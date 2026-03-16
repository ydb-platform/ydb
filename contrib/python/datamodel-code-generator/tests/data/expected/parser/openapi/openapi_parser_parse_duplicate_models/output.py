from __future__ import annotations

from typing import List, Optional

from pydantic import BaseModel


class Pet(BaseModel):
    id: int
    name: str
    tag: Optional[str] = None


class Pets(BaseModel):
    __root__: List[Pet]


class Error(BaseModel):
    code: int
    message: str


class Event(BaseModel):
    name: Optional[str] = None


class Result(BaseModel):
    event: Optional[Event] = None


class Events(BaseModel):
    __root__: List[Event]


class EventRoot(BaseModel):
    __root__: Event


class EventObject(BaseModel):
    event: Optional[Event] = None


class DuplicateObject1(BaseModel):
    event: Optional[List[Event]] = None


class Event1(BaseModel):
    event: Optional[Event] = None


class DuplicateObject2(BaseModel):
    event: Optional[Event1] = None


class DuplicateObject3(BaseModel):
    __root__: Event
