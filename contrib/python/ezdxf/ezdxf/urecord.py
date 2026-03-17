#  Copyright (c) 2021-2022, Manfred Moitzi
#  License: MIT License
"""
UserRecord(): store user data in a XRECORD entity.

The group code 302 is used as a structure tag.

All supported data types have a fixed group code:
    - str: 1
    - int: 90 - 32-bit values
    - float: 40 - doubles
    - Vec3, Vec2: 10, 20, 30 - Vec2 is stored as Vec3
    - list, tuple: starts with tag (302, "[") and ends with tag (302, "]")
    - dict: starts with tag (302, "{") and ends with tag (302, "}")

The str type can have a max. length of 2049 characters and cannot contain "\n"
or "\r".

This is an advanced feature for experienced programmers, handle with care!
The attribute UserRecord.data is a simple Python list with read/write access.

The UserRecord can store nested list and dict objects.

BinaryData(): store arbitrary binary data in a XRECORD entity

"""
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Optional,
    Iterable,
    Sequence,
    Mapping,
    MutableSequence,
    cast,
)
from ezdxf.lldxf import const
from ezdxf.entities import XRecord
from ezdxf.lldxf.tags import Tags, binary_data_to_dxf_tags
from ezdxf.lldxf.types import dxftag
from ezdxf.math import Vec3, Vec2
from ezdxf.tools import take2
from ezdxf.tools.binarydata import bytes_to_hexstr

if TYPE_CHECKING:
    from ezdxf.document import Drawing

TYPE_GROUP_CODE = 2
STR_GROUP_CODE = 1
INT_GROUP_CODE = 90
FLOAT_GROUP_CODE = 40
VEC3_GROUP_CODE = 10
COLLECTION_GROUP_CODE = 302
START_LIST = "["
END_LIST = "]"
START_DICT = "{"
END_DICT = "}"
DEFAULT_NAME = "UserRecord"

__all__ = ["UserRecord", "BinaryRecord"]


class UserRecord:
    def __init__(
        self,
        xrecord: Optional[XRecord] = None,
        *,
        name: str = DEFAULT_NAME,
        doc: Optional[Drawing] = None,
    ):
        """Setup a :class:`UserRecord` with the given `name`.

        The data is stored in the given `xrecord` object, or in a new created
        :class:`~ezdxf.entities.XRecord` instance if ``None``. If `doc` is not
        ``None`` the new xrecord is added to the OBJECTS section of the DXF
        document.

        Changes of the content has to be committed at the end to be stored in
        the underlying :attr:`xrecord` object.

        Args:
            xrecord (XRecord): underlying :class:`~ezdxf.entities.XRecord` instance,
                if ``None`` a new one will be created
            name (str): name of the user list
            doc (Drawing): DXF document or ``None``

        """
        if xrecord is None:
            if doc is None:
                xrecord = XRecord.new()
            else:
                xrecord = cast(XRecord, doc.objects.new_entity("XRECORD", {}))

        self.xrecord = xrecord
        self.name = str(name)
        self.data: MutableSequence = parse_xrecord(self.xrecord, self.name)

    @property
    def handle(self) -> Optional[str]:
        """DXF handle of the underlying :class:`~ezdxf.entities.XRecord` instance."""
        return self.xrecord.dxf.get("handle")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()

    def __str__(self):
        """Return str(self)."""
        return str(self.data)

    def commit(self) -> XRecord:
        """Store :attr:`data` in the underlying :class:`~ezdxf.entities.XRecord`
        instance. This call is not required if using the class by the ``with``
        statement.

        Raises:
            DXFValueError: invalid chars ``"\\n"`` or ``"\\r"`` in a string
            DXFTypeError: invalid data type

        """
        self.xrecord.tags = compile_user_record(self.name, self.data)
        return self.xrecord


def parse_xrecord(xrecord: XRecord, name: str) -> list:
    data: list = []
    tags = xrecord.tags
    if tags:
        code, value = tags[0]
        if code != TYPE_GROUP_CODE and value != name:
            raise const.DXFTypeError(
                f"{str(xrecord)} is not an user record of type {name}"
            )
        data.extend(item for item in parse_items(tags[1:]))
    return data


def parse_items(tags: Tags) -> list:
    stack: list = []
    items: list = []
    for tag in tags:
        code, value = tag
        if code == STR_GROUP_CODE:
            items.append(str(value))
        elif code == INT_GROUP_CODE:
            items.append(int(value))
        elif code == FLOAT_GROUP_CODE:
            items.append(float(value))
        elif code == VEC3_GROUP_CODE:
            items.append(Vec3(value))
        elif code == COLLECTION_GROUP_CODE and (
            value == START_LIST or value == START_DICT
        ):
            stack.append(items)
            items = []
        elif code == COLLECTION_GROUP_CODE and (
            value == END_LIST or value == END_DICT
        ):
            try:
                prev_level = stack.pop()
            except IndexError:
                raise const.DXFStructureError(
                    f"invalid nested structure, mismatch of structure tags"
                    f" ({COLLECTION_GROUP_CODE}, ...)"
                )
            if value == END_DICT:
                prev_level.append(dict(take2(items)))
            else:
                prev_level.append(items)
            items = prev_level
        else:
            raise const.DXFValueError(
                f"invalid group code in tag: ({code}, {value})"
            )
    if stack:
        raise const.DXFStructureError(
            f"invalid nested structure, mismatch of structure tags"
            f"({COLLECTION_GROUP_CODE}, ...)"
        )
    return items


def compile_user_record(name: str, data: Iterable) -> Tags:
    tags = Tags()
    tags.append(dxftag(TYPE_GROUP_CODE, name))
    tags.extend(tags_from_list(data))
    return tags


def tags_from_list(items: Iterable) -> Tags:
    tags = Tags()
    for item in items:
        if isinstance(item, str):
            if len(item) > 2049:  # DXF R2000 limit for group codes 0-9
                raise const.DXFValueError(
                    "string too long, max. 2049 characters"
                )
            if "\n" in item or "\r" in item:
                raise const.DXFValueError(
                    "found invalid line break '\\n' or '\\r'"
                )
            tags.append(dxftag(STR_GROUP_CODE, item))
        elif isinstance(item, int):
            tags.append(dxftag(INT_GROUP_CODE, item))
        elif isinstance(item, float):
            tags.append(dxftag(FLOAT_GROUP_CODE, item))
        elif isinstance(item, Vec3):
            tags.append(dxftag(VEC3_GROUP_CODE, item))
        elif isinstance(item, Vec2):
            tags.append(dxftag(VEC3_GROUP_CODE, Vec3(item)))
        elif isinstance(item, Sequence):
            tags.append(dxftag(COLLECTION_GROUP_CODE, START_LIST))
            tags.extend(tags_from_list(item))
            tags.append(dxftag(COLLECTION_GROUP_CODE, END_LIST))
        elif isinstance(item, Mapping):
            tags.append(dxftag(COLLECTION_GROUP_CODE, START_DICT))
            tags.extend(tags_from_list(key_value_list(item)))
            tags.append(dxftag(COLLECTION_GROUP_CODE, END_DICT))
        else:
            raise const.DXFTypeError(f"unsupported type: {type(item)}")
    return tags


def key_value_list(data: Mapping) -> Iterable:
    for k, v in data.items():
        yield k
        yield v


class BinaryRecord:
    def __init__(
        self,
        xrecord: Optional[XRecord] = None,
        *,
        doc: Optional[Drawing] = None,
    ):
        """Setup a :class:`BinaryRecord`.

        The data is stored in the given `xrecord` object, or in a new created
        :class:`~ezdxf.entities.XRecord` instance if ``None``. If `doc` is not
        ``None`` the new xrecord is added to the OBJECTS section of the DXF
        document.

        Changes of the content has to be committed at the end to be stored in
        the underlying :attr:`xrecord` object.

        Args:
            xrecord (XRecord): underlying :class:`~ezdxf.entities.XRecord` instance,
                if ``None`` a new one will be created
            doc (Drawing): DXF document or ``None``

        """
        if xrecord is None:
            if doc is None:
                xrecord = XRecord.new()
            else:
                xrecord = cast(XRecord, doc.objects.new_entity("XRECORD", {}))

        self.xrecord = xrecord
        self.data: bytes = parse_binary_data(self.xrecord.tags)

    @property
    def handle(self) -> Optional[str]:
        """DXF handle of the underlying :class:`~ezdxf.entities.XRecord` instance."""
        return self.xrecord.dxf.get("handle")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()

    def __str__(self) -> str:
        """Return str(self)."""
        return bytes_to_hexstr(self.data)

    def commit(self) -> XRecord:
        """Store binary :attr:`data` in the underlying :class:`~ezdxf.entities.XRecord`
        instance. This call is not required if using the class by the ``with``
        statement.

        """

        assert isinstance(
            self.data, (bytes, bytearray, memoryview)
        ), "expected binary data"
        self.xrecord.tags = binary_data_to_dxf_tags(
            self.data,
            length_group_code=160,
            value_group_code=310,
            value_size=127,
        )
        return self.xrecord


def parse_binary_data(tags: Tags) -> bytes:
    if tags and tags[0].code == 160:
        return b"".join(t.value for t in tags if t.code == 310)
    else:
        return b""
