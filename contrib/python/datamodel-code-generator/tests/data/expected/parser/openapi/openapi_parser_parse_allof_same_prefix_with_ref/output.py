from __future__ import annotations

from typing import Optional

from pydantic import BaseModel


class FooBar(BaseModel):
    id: Optional[int] = None


class FooBarBaz(BaseModel):
    id: Optional[int] = None


class Foo(BaseModel):
    foo_bar: Optional[FooBarBaz] = None
