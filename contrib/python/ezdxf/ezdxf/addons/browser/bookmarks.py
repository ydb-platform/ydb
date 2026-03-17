#  Copyright (c) 2021, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import NamedTuple, Optional


class Bookmark(NamedTuple):
    name: str
    handle: str
    offset: int


class Bookmarks:
    def __init__(self) -> None:
        self.bookmarks: dict[str, Bookmark] = dict()

    def add(self, name: str, handle: str, offset: int):
        self.bookmarks[name] = Bookmark(name, handle, offset)

    def get(self, name: str) -> Optional[Bookmark]:
        return self.bookmarks.get(name)

    def names(self) -> list[str]:
        return list(self.bookmarks.keys())

    def discard(self, name: str):
        try:
            del self.bookmarks[name]
        except KeyError:
            pass

    def clear(self):
        self.bookmarks.clear()
