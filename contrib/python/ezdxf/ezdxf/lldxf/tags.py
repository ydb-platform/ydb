# Copyright (c) 2011-2022, Manfred Moitzi
# License: MIT License
"""
Tags
----

A list of :class:`~ezdxf.lldxf.types.DXFTag`, inherits from Python standard list.
Unlike the statement in the DXF Reference "Do not write programs that rely on
the order given here", tag order is sometimes essential and some group codes
may appear multiples times in one entity. At the worst case
(:class:`~ezdxf.entities.material.Material`: normal map shares group codes with
diffuse map) using same group codes with different meanings.

"""
from __future__ import annotations
from typing import Iterable, Iterator, Any, Optional

from .const import DXFStructureError, DXFValueError, STRUCTURE_MARKER
from .types import DXFTag, EMBEDDED_OBJ_MARKER, EMBEDDED_OBJ_STR, dxftag
from .tagger import internal_tag_compiler
from . import types

COMMENT_CODE = 999


class Tags(list):
    """Collection of :class:`~ezdxf.lldxf.types.DXFTag` as flat list.
    Low level tag container, only required for advanced stuff.

    """

    @classmethod
    def from_text(cls, text: str) -> Tags:
        """Constructor from DXF string."""
        return cls(internal_tag_compiler(text))

    @classmethod
    def from_tuples(cls, tags: Iterable[tuple[int, Any]]) -> Tags:
        return cls(DXFTag(code, value) for code, value in tags)

    def __copy__(self) -> Tags:
        return self.__class__(tag.clone() for tag in self)

    clone = __copy__

    def get_handle(self) -> str:
        """Get DXF handle. Raises :class:`DXFValueError` if handle not exist.

        Returns:
            handle as plain hex string like ``'FF00'``

        Raises:
            DXFValueError: no handle found

        """
        try:
            code, handle = self[1]  # fast path  for most common cases
        except IndexError:
            raise DXFValueError("No handle found.")

        if code == 5 or code == 105:
            return handle

        for code, handle in self:
            if code == 5 or code == 105:
                return handle
        raise DXFValueError("No handle found.")

    def replace_handle(self, new_handle: str) -> None:
        """Replace existing handle.

        Args:
            new_handle: new handle as plain hex string e.g. ``'FF00'``

        """
        for index, tag in enumerate(self):
            if tag.code in (5, 105):
                self[index] = DXFTag(tag.code, new_handle)
                return

    def dxftype(self) -> str:
        """Returns DXF type of entity, e.g. ``'LINE'``."""
        return self[0].value

    def has_tag(self, code: int) -> bool:
        """Returns ``True`` if a :class:`~ezdxf.lldxf.types.DXFTag` with given
        group `code` is present.

        Args:
            code: group code as int

        """
        return any(tag.code == code for tag in self)

    def get_first_value(self, code: int, default: Any=DXFValueError) -> Any:
        """Returns value of first :class:`~ezdxf.lldxf.types.DXFTag` with given
        group code or default if `default` != :class:`DXFValueError`, else
        raises :class:`DXFValueError`.

        Args:
            code: group code as int
            default: return value for default case or raises :class:`DXFValueError`

        """
        for tag in self:
            if tag.code == code:
                return tag.value
        if default is DXFValueError:
            raise DXFValueError(code)
        else:
            return default

    def get_first_tag(self, code: int, default=DXFValueError) -> DXFTag:
        """Returns first :class:`~ezdxf.lldxf.types.DXFTag` with given group
        code or `default`, if `default` != :class:`DXFValueError`, else raises
        :class:`DXFValueError`.

        Args:
            code: group code as int
            default: return value for default case or raises :class:`DXFValueError`

        """
        for tag in self:
            if tag.code == code:
                return tag
        if default is DXFValueError:
            raise DXFValueError(code)
        else:
            return default

    def find_all(self, code: int) -> Tags:
        """Returns a list of :class:`~ezdxf.lldxf.types.DXFTag` with given
        group code.

        Args:
            code: group code as int

        """
        return self.__class__(tag for tag in self if tag.code == code)

    def tag_index(self, code: int, start: int = 0, end: Optional[int] = None) -> int:
        """Return index of first :class:`~ezdxf.lldxf.types.DXFTag` with given
        group code.

        Args:
            code: group code as int
            start: start index as int
            end: end index as int, ``None`` for end index = ``len(self)``

        """
        if end is None:
            end = len(self)
        index = start
        while index < end:
            if self[index].code == code:
                return index
            index += 1
        raise DXFValueError(code)

    def update(self, tag: DXFTag) -> None:
        """Update first existing tag with same group code as `tag`, raises
        :class:`DXFValueError` if tag not exist.

        """
        index = self.tag_index(tag.code)
        self[index] = tag

    def set_first(self, tag: DXFTag) -> None:
        """Update first existing tag with group code ``tag.code`` or append tag."""
        try:
            self.update(tag)
        except DXFValueError:
            self.append(tag)

    def remove_tags(self, codes: Iterable[int]) -> None:
        """Remove all tags inplace with group codes specified in `codes`.

        Args:
            codes: iterable of group codes as int

        """
        self[:] = [tag for tag in self if tag.code not in set(codes)]

    def pop_tags(self, codes: Iterable[int]) -> Iterator[DXFTag]:
        """Pop tags with group codes specified in `codes`.

        Args:
            codes: iterable of group codes

        """
        remaining = []
        codes = set(codes)
        for tag in self:
            if tag.code in codes:
                yield tag
            else:
                remaining.append(tag)
        self[:] = remaining

    def remove_tags_except(self, codes: Iterable[int]) -> None:
        """Remove all tags inplace except those with group codes specified in
        `codes`.

        Args:
            codes: iterable of group codes

        """
        self[:] = [tag for tag in self if tag.code in set(codes)]

    def filter(self, codes: Iterable[int]) -> Iterator[DXFTag]:
        """Iterate and filter tags by group `codes`.

        Args:
            codes: group codes to filter

        """
        return (tag for tag in self if tag.code not in set(codes))

    def collect_consecutive_tags(
        self, codes: Iterable[int], start: int = 0, end: Optional[int] = None
    ) -> Tags:
        """Collect all consecutive tags with group code in `codes`, `start` and
        `end` delimits the search range. A tag code not in codes ends the
        process.

        Args:
            codes: iterable of group codes
            start: start index as int
            end: end index as int, ``None`` for end index = ``len(self)``

        Returns:
            collected tags as :class:`Tags`

        """
        codes = frozenset(codes)
        index = int(start)
        if end is None:
            end = len(self)
        bag = self.__class__()

        while index < end:
            tag = self[index]
            if tag.code in codes:
                bag.append(tag)
                index += 1
            else:
                break
        return bag

    def has_embedded_objects(self) -> bool:
        for tag in self:
            if tag.code == EMBEDDED_OBJ_MARKER and tag.value == EMBEDDED_OBJ_STR:
                return True
        return False

    @classmethod
    def strip(cls, tags: Tags, codes: Iterable[int]) -> Tags:
        """Constructor from `tags`, strips all tags with group codes in `codes`
        from tags.

        Args:
            tags: iterable of :class:`~ezdxf.lldxf.types.DXFTag`
            codes: iterable of group codes as int

        """
        return cls((tag for tag in tags if tag.code not in frozenset(codes)))

    def get_soft_pointers(self) -> Tags:
        """Returns all soft-pointer handles in group code range 330-339."""
        return Tags(tag for tag in self if types.is_soft_pointer(tag))

    def get_hard_pointers(self) -> Tags:
        """Returns all hard-pointer handles in group code range 340-349, 390-399 and
        480-481. Hard pointers protect an object from being purged.
        """
        return Tags(tag for tag in self if types.is_hard_pointer(tag))

    def get_soft_owner_handles(self) -> Tags:
        """Returns all soft-owner handles in group code range 350-359."""
        return Tags(tag for tag in self if types.is_soft_owner(tag))

    def get_hard_owner_handles(self) -> Tags:
        """Returns all hard-owner handles in group code range 360-369."""
        return Tags(tag for tag in self if types.is_hard_owner(tag))

    def has_translatable_pointers(self) -> bool:
        """Returns ``True`` if any pointer handle has to be translated during INSERT
        and XREF operations.
        """
        return any(types.is_translatable_pointer(tag) for tag in self)

    def get_translatable_pointers(self) -> Tags:
        """Returns all pointer handles which should be translated during INSERT and XREF
        operations.
        """
        return Tags(tag for tag in self if types.is_translatable_pointer(tag))


def text2tags(text: str) -> Tags:
    return Tags.from_text(text)


def group_tags(
    tags: Iterable[DXFTag], splitcode: int = STRUCTURE_MARKER
) -> Iterable[Tags]:
    """Group of tags starts with a SplitTag and ends before the next SplitTag.
    A SplitTag is a tag with code == splitcode, like (0, 'SECTION') for
    splitcode == 0.

    Args:
        tags: iterable of :class:`DXFTag`
        splitcode: group code of split tag

    """

    # first do nothing, skip tags in front of the first split tag
    def append(tag):
        pass

    group = None
    for tag in tags:
        if tag.code == splitcode:
            if group is not None:
                yield group
            group = Tags([tag])
            append = group.append  # redefine append: add tags to this group
        else:
            append(tag)
    if group is not None:
        yield group


def text_to_multi_tags(
    text: str, code: int = 303, size: int = 255, line_ending: str = "^J"
) -> Tags:
    text = "".join(text).replace("\n", line_ending)

    def chop():
        start = 0
        end = size
        while start < len(text):
            yield text[start:end]
            start = end
            end += size

    return Tags(DXFTag(code, part) for part in chop())


def multi_tags_to_text(tags: Tags, line_ending: str = "^J") -> str:
    return "".join(tag.value for tag in tags).replace(line_ending, "\n")


OPEN_LIST = (1002, "{")
CLOSE_LIST = (1002, "}")


def xdata_list(name: str, xdata_tags: Iterable) -> Tags:
    tags = Tags()
    if name:
        tags.append((1000, name))
    tags.append(OPEN_LIST)
    tags.extend(xdata_tags)
    tags.append(CLOSE_LIST)
    return tags


def remove_named_list_from_xdata(name: str, tags: Tags) -> Tags:
    start, end = get_start_and_end_of_named_list_in_xdata(name, tags)
    del tags[start:end]
    return tags


def get_named_list_from_xdata(name: str, tags: Tags) -> Tags:
    start, end = get_start_and_end_of_named_list_in_xdata(name, tags)
    return Tags(tags[start:end])


class NotFoundException(Exception):
    pass


def get_start_and_end_of_named_list_in_xdata(name: str, tags: Tags) -> tuple[int, int]:
    start = None
    end = None
    level = 0
    for index in range(len(tags)):
        tag = tags[index]

        if start is None and tag == (1000, name):
            next_tag = tags[index + 1]
            if next_tag == OPEN_LIST:
                start = index
                continue
        if start is not None:
            if tag == OPEN_LIST:
                level += 1
            elif tag == CLOSE_LIST:
                level -= 1
            if level == 0:
                end = index
                break

    if start is None:
        raise NotFoundException
    if end is None:
        raise DXFStructureError('Invalid XDATA structure: missing  (1002, "}").')
    return start, end + 1


def find_begin_and_end_of_encoded_xdata_tags(name: str, tags: Tags) -> tuple[int, int]:
    """Find encoded XDATA tags, surrounded by group code 1000 tags
    name_BEGIN and name_END (e.g. MTEXT column specification).

    Raises:
        NotFoundError: tag group not found
        DXFStructureError: missing begin- or end tag

    """
    begin_name = name + "_BEGIN"
    end_name = name + "_END"
    start = None
    end = None
    for index, (code, value) in enumerate(tags):
        if code == 1000:
            if value == begin_name:
                start = index
            elif value == end_name:
                end = index + 1
                break
    if start is None:
        if end is not None:  # end tag without begin tag!
            raise DXFStructureError(
                f"Invalid XDATA structure: missing begin tag (1000, {begin_name})."
            )
        raise NotFoundException
    if end is None:
        raise DXFStructureError(
            f"Invalid XDATA structure: missing end tag (1000, {end_name})."
        )
    return start, end


def binary_data_to_dxf_tags(
    data: bytes,
    length_group_code: int = 160,
    value_group_code: int = 310,
    value_size=127,
) -> Tags:
    """Convert binary data to DXF tags."""
    tags = Tags()
    length = len(data)
    tags.append(dxftag(length_group_code, length))
    index = 0
    while index < length:
        chunk = data[index : index + value_size]
        tags.append(dxftag(value_group_code, chunk))
        index += value_size
    return tags
