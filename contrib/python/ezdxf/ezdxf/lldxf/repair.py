# Copyright (c) 2016-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Iterable,
    Optional,
    TYPE_CHECKING,
    Sequence,
    Any,
    Iterator,
)
from functools import partial
import logging
from .tags import DXFTag
from .types import POINT_CODES, NONE_TAG, VALID_XDATA_GROUP_CODES

if TYPE_CHECKING:
    from ezdxf.eztypes import Tags

logger = logging.getLogger("ezdxf")


def tag_reorder_layer(tagger: Iterable[DXFTag]) -> Iterator[DXFTag]:
    """Reorder coordinates of legacy DXF Entities, for now only LINE.

    Input Raw tag filter.

    Args:
        tagger: low level tagger

    """

    collector: Optional[list] = None
    for tag in tagger:
        if tag.code == 0:
            if collector is not None:
                # stop collecting if inside a supported entity
                entity = _s(collector[0].value)
                yield from COORDINATE_FIXING_TOOLBOX[entity](collector)  # type: ignore
                collector = None

            if _s(tag.value) in COORDINATE_FIXING_TOOLBOX:
                collector = [tag]
                # do not yield collected tag yet
                tag = NONE_TAG
        else:  # tag.code != 0
            if collector is not None:
                collector.append(tag)
                # do not yield collected tag yet
                tag = NONE_TAG
        if tag is not NONE_TAG:
            yield tag


# invalid point codes if not part of a point started with 1010, 1011, 1012, 1013
INVALID_Y_CODES = {code + 10 for code in POINT_CODES}
INVALID_Z_CODES = {code + 20 for code in POINT_CODES}
# A single group code 38 is an elevation tag (e.g. LWPOLYLINE)
# Is (18, 28, 38?) is a valid point code?
INVALID_Z_CODES.remove(38)
INVALID_CODES = INVALID_Y_CODES | INVALID_Z_CODES
X_CODES = POINT_CODES


def filter_invalid_point_codes(tagger: Iterable[DXFTag]) -> Iterable[DXFTag]:
    """Filter invalid and misplaced point group codes.

    - removes x-axis without following y-axis
    - removes y- and z-axis without leading x-axis

    Args:
        tagger: low level tagger

    """

    def entity() -> str:
        if handle_tag:
            return f"in entity #{_s(handle_tag[1])}"
        else:
            return ""

    expected_code = -1
    z_code = 0
    point: list[Any] = []
    handle_tag = None
    for tag in tagger:
        code = tag[0]
        if code == 5:  # ignore DIMSTYLE entity
            handle_tag = tag
        if point and code != expected_code:
            # at least x, y axis is required else ignore point
            if len(point) > 1:
                yield from point
            else:
                logger.info(
                    f"remove misplaced x-axis tag: {str(point[0])}" + entity()
                )
            point.clear()

        if code in X_CODES:
            expected_code = code + 10
            z_code = code + 20
            point.append(tag)
        elif code == expected_code:
            point.append(tag)
            expected_code += 10
            if expected_code > z_code:
                expected_code = -1
        else:
            # ignore point group codes without leading x-axis
            if code not in INVALID_CODES:
                yield tag
            else:
                axis = "y-axis" if code in INVALID_Y_CODES else "z-axis"
                logger.info(
                    f"remove misplaced {axis} tag: {str(tag)}" + entity()
                )

    if len(point) == 1:
        logger.info(f"remove misplaced x-axis tag: {str(point[0])}" + entity())
    elif len(point) > 1:
        yield from point


def fix_coordinate_order(tags: Tags, codes: Sequence[int] = (10, 11)):
    def extend_codes():
        for code in codes:
            yield code  # x tag
            yield code + 10  # y tag
            yield code + 20  # z tag

    def get_coords(code: int):
        # if x or y coordinate is missing, it is a DXFStructureError
        # but here is not the location to validate the DXF structure
        try:
            yield coordinates[code]
        except KeyError:
            pass
        try:
            yield coordinates[code + 10]
        except KeyError:
            pass
        try:
            yield coordinates[code + 20]
        except KeyError:
            pass

    coordinate_codes = frozenset(extend_codes())
    coordinates = {}
    remaining_tags = []
    insert_pos = None
    for tag in tags:
        # separate tags
        if tag.code in coordinate_codes:
            coordinates[tag.code] = tag
            if insert_pos is None:
                insert_pos = tags.index(tag)
        else:
            remaining_tags.append(tag)

    if len(coordinates) == 0:
        # no coordinates found, this is probably a DXFStructureError,
        # but here is not the location to validate the DXF structure,
        # just do nothing.
        return tags

    ordered_coords = []
    for code in codes:
        ordered_coords.extend(get_coords(code))
    remaining_tags[insert_pos:insert_pos] = ordered_coords
    return remaining_tags


COORDINATE_FIXING_TOOLBOX = {
    "LINE": partial(fix_coordinate_order, codes=(10, 11)),
}


def filter_invalid_xdata_group_codes(
    tags: Iterable[DXFTag],
) -> Iterator[DXFTag]:
    return (tag for tag in tags if tag.code in VALID_XDATA_GROUP_CODES)


def filter_invalid_handles(tags: Iterable[DXFTag]) -> Iterator[DXFTag]:
    line = -1
    handle_code = 5
    structure_tag = ""
    for tag in tags:
        line += 2
        if tag.code == 0:
            structure_tag = tag.value
            if _s(tag.value) == "DIMSTYLE":
                handle_code = 105
            else:
                handle_code = 5
        elif tag.code == handle_code:
            try:
                int(tag.value, 16)
            except ValueError:
                logger.warning(
                    f'skipped invalid handle "{_s(tag.value)}" in '
                    f'DXF entity "{_s(structure_tag)}" near line {line}'
                )
                continue
        yield tag


def _s(b) -> str:
    if isinstance(b, bytes):
        return b.decode(encoding="ascii", errors="ignore")
    return b
