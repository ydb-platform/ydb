# Copyright (c) 2019-2023 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Sequence, Optional, Iterator
from ezdxf.lldxf.types import dxftag, uniform_appid
from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.const import DXFKeyError, DXFStructureError
from ezdxf.lldxf.const import (
    ACAD_REACTORS,
    REACTOR_HANDLE_CODE,
    APP_DATA_MARKER,
)

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["AppData", "Reactors"]

ERR_INVALID_DXF_ATTRIB = "Invalid DXF attribute for entity {}"
ERR_DXF_ATTRIB_NOT_EXITS = "DXF attribute {} does not exist"


class AppData:
    def __init__(self) -> None:
        self.data: dict[str, Tags] = dict()

    def __contains__(self, appid: str) -> bool:
        """Returns ``True`` if application-defined data exist for `appid`."""
        return uniform_appid(appid) in self.data

    def __len__(self) -> int:
        """Returns the count of AppData."""
        return len(self.data)

    def tags(self) -> Iterable[Tags]:
        return self.data.values()

    def get(self, appid: str) -> Tags:
        """Get application-defined data for `appid` as
        :class:`~ezdxf.lldxf.tags.Tags` container.
        The first tag is always (102, "{APPID").
        The last tag is always (102, "}").
        """
        try:
            return self.data[uniform_appid(appid)]
        except KeyError:
            raise DXFKeyError(appid)

    def set(self, tags: Tags) -> None:
        """Store raw application-defined data tags.
        The first tag has to be (102, "{APPID").
        The last tag has to be (102, "}").
        """
        if len(tags):
            appid = tags[0].value
            self.data[appid] = tags

    def add(self, appid: str, data: Iterable[Sequence]) -> None:
        """Add application-defined tags for `appid`.
        Adds first tag (102, "{APPID") if not exist.
        Adds last tag (102, "}" if not exist.
        """
        data = Tags(dxftag(code, value) for code, value in data)
        appid = uniform_appid(appid)
        if data[0] != (APP_DATA_MARKER, appid):
            data.insert(0, dxftag(APP_DATA_MARKER, appid))
        if data[-1] != (APP_DATA_MARKER, "}"):
            data.append(dxftag(APP_DATA_MARKER, "}"))
        self.set(data)

    def discard(self, appid: str):
        """Delete application-defined data for `appid` without raising and error
        if `appid` doesn't exist.
        """
        _appid = uniform_appid(appid)
        if _appid in self.data:
            del self.data[_appid]

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        for data in self.data.values():
            tagwriter.write_tags(data)


class Reactors:
    """Handle storage for related reactors.

    Reactors are other objects related to the object that contains this
    Reactor() instance.

    """

    def __init__(self, handles: Optional[Iterable[str]] = None):
        self.reactors: set[str] = set(handles or [])

    def __len__(self) -> int:
        """Returns count of registered handles."""
        return len(self.reactors)

    def __contains__(self, handle: str) -> bool:
        """Returns ``True`` if `handle` is registered."""
        return handle in self.reactors

    def __iter__(self) -> Iterator[str]:
        """Returns an iterator for all registered handles."""
        return iter(self.get())

    def copy(self) -> Reactors:
        """Returns a copy."""
        return Reactors(self.reactors)

    @classmethod
    def from_tags(cls, tags: Optional[Tags] = None) -> Reactors:
        """Create Reactors() instance from tags.

        Expected DXF structure:
        [(102, '{ACAD_REACTORS'), (330, handle), ..., (102, '}')]

        Args:
            tags: list of DXFTags()

        """
        if tags is None:
            return cls(None)

        if len(tags) < 2:  # no reactors are valid
            raise DXFStructureError("ACAD_REACTORS error")
        return cls((handle.value for handle in tags[1:-1]))

    def get(self) -> list[str]:
        """Returns all registered handles as sorted list."""
        return sorted(self.reactors, key=lambda x: int(x, base=16))

    def set(self, handles: Optional[Iterable[str]]) -> None:
        """Reset all handles."""
        self.reactors = set(handles or [])

    def add(self, handle: str) -> None:
        """Add a single `handle`."""
        self.reactors.add(handle)

    def discard(self, handle: str):
        """Discard a single `handle`."""
        self.reactors.discard(handle)

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        tagwriter.write_tag2(APP_DATA_MARKER, ACAD_REACTORS)
        for handle in self.get():
            tagwriter.write_tag2(REACTOR_HANDLE_CODE, handle)
        tagwriter.write_tag2(APP_DATA_MARKER, "}")
