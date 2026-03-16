from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import Extra

from custom_module import Base


class Pet(Base):
    id: int
    name: str
    tag: Optional[str] = None


class Pets(Base):
    __root__: List[Pet]


class User(Base):
    class Config:
        extra = Extra.allow

    id: int
    name: str
    tag: Optional[str] = None


class Users(Base):
    __root__: List[User]


class Id(Base):
    __root__: str


class Rules(Base):
    __root__: List[str]


class Error(Base):
    class Config:
        extra = Extra.forbid

    code: int
    message: str


class Event(Base):
    name: Optional[str] = None


class Result(Base):
    event: Optional[Event] = None


class Broken(Base):
    foo: Optional[str] = None
    bar: Optional[int] = None


class BrokenArray(Base):
    broken: Optional[Dict[str, List[Broken]]] = None


class FileSetUpload(Base):
    task_id: Optional[str] = None
    tags: Dict[str, List[str]]


class Test(Base):
    broken: Optional[Dict[str, Broken]] = None
    failing: Optional[Dict[str, str]] = {}
