# Copyright (c) 2019-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Any,
    MutableSequence,
    MutableMapping,
    Iterator,
    Union,
    Optional,
)
from contextlib import contextmanager
import logging

from ezdxf.math import Vec3, Matrix44
from ezdxf.lldxf.types import dxftag, VALID_XDATA_GROUP_CODES, DXFTag
from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.const import XDATA_MARKER, DXFValueError, DXFTypeError
from ezdxf.lldxf.tags import (
    xdata_list,
    remove_named_list_from_xdata,
    get_named_list_from_xdata,
    NotFoundException,
)
from ezdxf.tools import take2
from ezdxf import options
from ezdxf.lldxf.repair import filter_invalid_xdata_group_codes

if TYPE_CHECKING:
    from ezdxf.entities import DXFEntity
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["XData", "XDataUserList", "XDataUserDict"]
logger = logging.getLogger("ezdxf")


def has_valid_xdata_group_codes(tags: Tags) -> bool:
    return all(tag.code in VALID_XDATA_GROUP_CODES for tag in tags)


class XData:
    def __init__(self, xdata: Optional[Iterable[Tags]] = None):
        self.data: dict[str, Tags] = dict()
        if xdata is not None:
            for data in xdata:
                self._add(data)

    @classmethod
    def safe_init(cls, xdata: Iterable[Tags]):
        return cls(
            [Tags(filter_invalid_xdata_group_codes(tags)) for tags in xdata]
        )

    def __len__(self):
        return len(self.data)

    def __contains__(self, appid: str) -> bool:
        """Returns ``True`` if  DXF tags for `appid` exist."""
        return appid in self.data

    def update_keys(self):
        """Update APPID keys. (internal API)"""
        self.data = {tags[0].value: tags for tags in self.data.values()}

    def _add(self, tags: Tags) -> None:
        tags = Tags(tags)
        if len(tags):
            appid = tags[0].value
            if appid in self.data:
                logger.info(f"Duplicate XDATA appid {appid} in one entity")
            if has_valid_xdata_group_codes(tags):
                self.data[appid] = tags
            else:
                raise DXFValueError(f"found invalid XDATA group code in {tags}")

    def add(
        self, appid: str, tags: Iterable[Union[tuple[int, Any], DXFTag]]
    ) -> None:
        """Add a list of DXF tags for `appid`. The `tags` argument is an
        iterable of (group code, value) tuples, where the group code has to be
        an integer value. The mandatory XDATA marker (1001, appid) is added
        automatically if front of the tags if missing.

        Each entity can contain only one list of tags for each `appid`.
        Adding a second list of tags for the same `appid` replaces the
        existing list of tags.

        The valid XDATA group codes are restricted to some specific values in
        the range from 1000 to 1071, for more information see also the
        internals about :ref:`xdata_internals`.

        """
        data = Tags(dxftag(code, value) for code, value in tags)
        if len(data) == 0 or data[0] != (XDATA_MARKER, appid):
            data.insert(0, dxftag(XDATA_MARKER, appid))
        self._add(data)

    def get(self, appid: str) -> Tags:
        """Returns the DXF tags as :class:`~ezdxf.lldxf.tags.Tags` list
        stored by `appid`.

        Raises:
             DXFValueError: no data for `appid` exist

        """
        if appid in self.data:
            return self.data[appid]
        else:
            raise DXFValueError(appid)

    def discard(self, appid):
        """Delete DXF tags for `appid`. None existing appids are silently
        ignored.
        """
        if appid in self.data:
            del self.data[appid]

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        for appid, tags in self.data.items():
            if options.filter_invalid_xdata_group_codes:
                tags = Tags(filter_invalid_xdata_group_codes(tags))
            tagwriter.write_tags(tags)

    def has_xlist(self, appid: str, name: str) -> bool:
        """Returns ``True`` if list `name` from XDATA `appid` exists.

        Args:
            appid: APPID
            name: list name

        """
        try:
            self.get_xlist(appid, name)
        except DXFValueError:
            return False
        else:
            return True

    def get_xlist(self, appid: str, name: str) -> list[tuple]:
        """Get list `name` from XDATA `appid`.

        Args:
            appid: APPID
            name: list name

        Returns: list of DXFTags including list name and curly braces '{' '}' tags

        Raises:
            DXFKeyError: XDATA `appid` does not exist
            DXFValueError: list `name` does not exist

        """
        xdata = self.get(appid)
        try:
            return get_named_list_from_xdata(name, xdata)
        except NotFoundException:
            raise DXFValueError(
                f'No data list "{name}" not found for APPID "{appid}"'
            )

    def set_xlist(self, appid: str, name: str, tags: Iterable) -> None:
        """Create new list `name` of XDATA `appid` with `xdata_tags` and
        replaces list `name` if already exists.

        Args:
            appid: APPID
            name: list name
            tags: list content as DXFTags or (code, value) tuples, list name and
                curly braces '{' '}' tags will be added
        """
        if appid not in self.data:
            data = [(XDATA_MARKER, appid)]
            data.extend(xdata_list(name, tags))
            self.add(appid, data)
        else:
            self.replace_xlist(appid, name, tags)

    def discard_xlist(self, appid: str, name: str) -> None:
        """Deletes list `name` from XDATA `appid`. Ignores silently if XDATA
        `appid` or list `name` not exist.

        Args:
            appid: APPID
            name: list name

        """
        try:
            xdata = self.get(appid)
        except DXFValueError:
            pass
        else:
            try:
                tags = remove_named_list_from_xdata(name, xdata)
            except NotFoundException:
                pass
            else:
                self.add(appid, tags)

    def replace_xlist(self, appid: str, name: str, tags: Iterable) -> None:
        """Replaces list `name` of existing XDATA `appid` by `tags`. Appends
        new list if list `name` do not exist, but raises `DXFValueError` if
        XDATA `appid` do not exist.

        Low level interface, if not sure use `set_xdata_list()` instead.

        Args:
            appid: APPID
            name: list name
            tags: list content as DXFTags or (code, value) tuples, list name and
                curly braces '{' '}' tags will be added
        Raises:
            DXFValueError: XDATA `appid` do not exist

        """
        xdata = self.get(appid)
        try:
            data = remove_named_list_from_xdata(name, xdata)
        except NotFoundException:
            data = xdata
        xlist = xdata_list(name, tags)
        data.extend(xlist)
        self.add(appid, data)

    def transform(self, m: Matrix44) -> None:
        """Transform XDATA tags with group codes 1011, 1012, 1013, 1041 and
        1042 inplace. For more information see :ref:`xdata_internals` Internals.

        """
        transformed_data = dict()
        for key, tags in self.data.items():
            transformed_data[key] = Tags(transform_xdata_tags(tags, m))
        self.data = transformed_data


def transform_xdata_tags(tags: Tags, m: Matrix44) -> Iterator[DXFTag]:
    for tag in tags:
        code, value = tag
        if code == 1011:
            # move, scale, rotate and mirror
            yield dxftag(code, m.transform(Vec3(value)))
        elif code == 1012:
            # scale, rotate and mirror
            yield dxftag(code, m.transform_direction(Vec3(value)))
        elif code == 1013:
            # rotate and mirror
            vec = Vec3(value)
            length = vec.magnitude
            if length > 1e-12:
                vec = m.transform_direction(vec).normalize(length)
                yield dxftag(code, vec)
            else:
                yield tag
        elif code == 1041 or code == 1042:
            # scale distance and factor, works only for uniform scaling
            vec = m.transform_direction(Vec3(value, 0, 0))
            yield dxftag(code, vec.magnitude)
        else:
            yield tag


class XDataUserList(MutableSequence):
    """Manage named XDATA lists as a list-like object.

    Stores just a few data types with fixed group codes:

        1000 str
        1010 Vec3
        1040 float
        1071 32bit int

    """

    converter = {
        1000: str,
        1010: Vec3,
        1040: float,
        1071: int,
    }
    group_codes = {
        str: 1000,
        Vec3: 1010,
        float: 1040,
        int: 1071,
    }

    def __init__(
        self, xdata: Optional[XData] = None, name="DefaultList", appid="EZDXF"
    ):
        """Setup a XDATA user list `name` for the given `appid`.

        The data is stored in the given `xdata` object, or in a new created
        :class:`XData` instance if ``None``.
        Changes of the content has to be committed at the end to be stored in
        the underlying `xdata` object.

        Args:
            xdata (XData): underlying :class:`XData` instance, if ``None`` a
                new one will be created
            name (str): name of the user list
            appid (str): application specific AppID

        """
        if xdata is None:
            xdata = XData()
        self.xdata = xdata
        self._appid = str(appid)
        self._name = str(name)
        try:
            data = xdata.get_xlist(self._appid, self._name)
        except DXFValueError:
            data = []
        self._data: list = self._parse_list(data)

    @classmethod
    @contextmanager
    def entity(
        cls, entity: DXFEntity, name="DefaultList", appid="EZDXF"
    ) -> Iterator[XDataUserList]:
        """Context manager to manage a XDATA list `name` for a given DXF
        `entity`. Appends the user list to the existing :class:`XData` instance
        or creates new :class:`XData` instance.

        Args:
            entity (DXFEntity): target DXF entity for the XDATA
            name (str): name of the user list
            appid (str): application specific AppID

        """
        xdata = entity.xdata
        if xdata is None:
            xdata = XData()
            entity.xdata = xdata
        xlist = cls(xdata, name, appid)
        yield xlist
        xlist.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()

    def __str__(self):
        """Return str(self)."""
        return str(self._data)

    def insert(self, index: int, value) -> None:
        self._data.insert(index, value)

    def __getitem__(self, item):
        """Get self[item]."""
        return self._data[item]

    def __setitem__(self, item, value):
        """Set self[item] to value."""
        self._data.__setitem__(item, value)

    def __delitem__(self, item):
        """Delete self[item]."""
        self._data.__delitem__(item)

    def _parse_list(self, tags: Iterable[tuple]) -> list:
        data = list(tags)
        content = []
        for code, value in data[2:-1]:
            factory = self.converter.get(code)
            if factory:
                content.append(factory(value))
            else:
                raise DXFValueError(f"unsupported group code: {code}")
        return content

    def __len__(self) -> int:
        """Returns len(self)."""
        return len(self._data)

    def commit(self) -> None:
        """Store all changes to the underlying :class:`XData` instance.
        This call is not required if using the :meth:`entity` context manager.

        Raises:
            DXFValueError: invalid chars ``"\\n"`` or ``"\\r"`` in a string
            DXFTypeError: invalid data type

        """
        data = []
        for value in self._data:
            if isinstance(value, str):
                if len(value) > 255:  # XDATA limit for group code 1000
                    raise DXFValueError("string too long, max. 255 characters")
                if "\n" in value or "\r" in value:
                    raise DXFValueError(
                        "found invalid line break '\\n' or '\\r'"
                    )
            code = self.group_codes.get(type(value))
            if code:
                data.append(dxftag(code, value))
            else:
                raise DXFTypeError(f"invalid type: {type(value)}")
        self.xdata.set_xlist(self._appid, self._name, data)


class XDataUserDict(MutableMapping):
    """Manage named XDATA lists as a dict-like object.

    Uses XDataUserList to store key, value pairs in XDATA.

    This class does not create the required AppID table entry, only the
    default AppID "EZDXF" exist by default.

    Implements the :class:`MutableMapping` interface.

    """

    def __init__(
        self, xdata: Optional[XData] = None, name="DefaultDict", appid="EZDXF"
    ):
        """Setup a XDATA user dict `name` for the given `appid`.

        The data is stored in the given `xdata` object, or in a new created
        :class:`XData` instance if ``None``.
        Changes of the content has to be committed at the end to be stored in
        the underlying `xdata` object.

        Args:
            xdata (XData): underlying :class:`XData` instance, if ``None`` a
                new one will be created
            name (str): name of the user list
            appid (str): application specific AppID

        """
        self._xlist = XDataUserList(xdata, name, appid)
        self._user_dict: dict[str, Any] = self._parse_xlist()

    def _parse_xlist(self) -> dict:
        if self._xlist:
            return dict(take2(self._xlist))
        else:
            return dict()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()

    def __str__(self):
        """Return str(self)."""
        return str(self._user_dict)

    @classmethod
    @contextmanager
    def entity(
        cls, entity: DXFEntity, name="DefaultDict", appid="EZDXF"
    ) -> Iterator[XDataUserDict]:
        """Context manager to manage a XDATA dict `name` for a given DXF
        `entity`. Appends the user dict to the existing :class:`XData` instance
        or creates new :class:`XData` instance.

        Args:
            entity (DXFEntity): target DXF entity for the XDATA
            name (str): name of the user list
            appid (str): application specific AppID

        """
        xdata = entity.xdata
        if xdata is None:
            xdata = XData()
            entity.xdata = xdata
        xdict = cls(xdata, name, appid)
        yield xdict
        xdict.commit()

    @property
    def xdata(self):
        return self._xlist.xdata

    def __len__(self):
        """Returns len(self)."""
        return len(self._user_dict)

    def __getitem__(self, key):
        """Get self[key]."""
        return self._user_dict[key]

    def __setitem__(self, key, item):
        """Set self[key] to value, key has to be a string.

        Raises:
            DXFTypeError: key is not a string

        """
        if not isinstance(key, str):
            raise DXFTypeError("key is not a string")
        self._user_dict[key] = item

    def __delitem__(self, key):
        """Delete self[key]."""
        del self._user_dict[key]

    def __iter__(self):
        """Implement iter(self)."""
        return iter(self._user_dict)

    def discard(self, key):
        """Delete self[key], without raising a :class:`KeyError` if `key` does
        not exist.

        """
        try:
            del self._user_dict[key]
        except KeyError:
            pass

    def commit(self) -> None:
        """Store all changes to the underlying :class:`XData` instance.
        This call is not required if using the :meth:`entity` context manager.

        Raises:
            DXFValueError: invalid chars ``"\\n"`` or ``"\\r"`` in a string
            DXFTypeError: invalid data type

        """
        xlist = self._xlist
        xlist.clear()
        for key, value in self._user_dict.items():
            xlist.append(key)
            xlist.append(value)
        xlist.commit()
