#  Copyright (c) 2021-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Union
import enum

from ezdxf.entities import Text, Attrib, Hatch, DXFGraphic
from ezdxf.lldxf import const
from ezdxf.enums import TextEntityAlignment, MAP_TEXT_ENUM_TO_ALIGN_FLAGS
from ezdxf.math import Matrix44, BoundingBox
from ezdxf import path
from ezdxf.path import Path
from ezdxf.fonts import fonts
from ezdxf.query import EntityQuery

__all__ = [
    "make_path_from_str",
    "make_paths_from_str",
    "make_hatches_from_str",
    "make_path_from_entity",
    "make_paths_from_entity",
    "make_hatches_from_entity",
    "virtual_entities",
    "explode",
    "Kind",
]

AnyText = Union[Text, Attrib]
VALID_TYPES = ("TEXT", "ATTRIB")


def make_path_from_str(
    s: str,
    font: fonts.FontFace,
    size: float = 1.0,
    align=TextEntityAlignment.LEFT,
    length: float = 0,
    m: Matrix44 = None,
) -> Path:
    """Convert a single line string `s` into a :term:`Multi-Path` object.
    The text `size` is the height of the uppercase letter "X" (cap height).
    The paths are aligned about the insertion point at (0, 0).
    BASELINE means the bottom of the letter "X".

    Args:
         s: text to convert
         font: font face definition as :class:`~ezdxf.tools.fonts.FontFace` object
         size: text size (cap height) in drawing units
         align: alignment as :class:`ezdxf.enums.TextEntityAlignment`,
            default is :attr:`LEFT`
         length: target length for the :attr:`ALIGNED` and :attr:`FIT` alignments
         m: transformation :class:`~ezdxf.math.Matrix44`

    """
    if len(s) == 0:
        return Path()
    abstract_font = get_font(font)
    # scale font rendering units to drawing units:
    p = _str_to_path(s, abstract_font, size)
    bbox = path.bbox([p], fast=True)

    # Text is rendered in drawing units,
    # therefore do alignment in drawing units:
    draw_units_fm = abstract_font.measurements.scale_from_baseline(size)
    matrix = alignment_transformation(draw_units_fm, bbox, align, length)
    if m is not None:
        matrix *= m
    return p.transform(matrix)


def make_paths_from_str(
    s: str,
    font: fonts.FontFace,
    size: float = 1.0,
    align=TextEntityAlignment.LEFT,
    length: float = 0,
    m: Matrix44 = None,
) -> list[Path]:
    """Convert a single line string `s` into a list of
    :class:`~ezdxf.path.Path` objects. All paths are returned as a list of
    :term:`Single-Path` objects.
    The text `size` is the height of the uppercase letter "X" (cap height).
    The paths are aligned about the insertion point at (0, 0).
    BASELINE means the bottom of the letter "X".

    Args:
         s: text to convert
         font: font face definition as :class:`~ezdxf.tools.fonts.FontFace` object
         size: text size (cap height) in drawing units
         align: alignment as :class:`ezdxf.enums.TextEntityAlignment`,
            default is :attr:`LEFT`
         length: target length for the :attr:`ALIGNED` and :attr:`FIT` alignments
         m: transformation :class:`~ezdxf.math.Matrix44`

    """
    if len(s) == 0:
        return []
    p = make_path_from_str(s, font, size, align, length, m)
    return list(p.sub_paths())


def get_font(font: fonts.FontFace) -> fonts.AbstractFont:
    font_name = fonts.font_manager.find_font_name(font)
    return fonts.make_font(font_name, cap_height=1.0)


def _str_to_path(s: str, render_engine: fonts.AbstractFont, size: float = 1.0) -> Path:
    return render_engine.text_path_ex(s, cap_height=size).to_path()


def alignment_transformation(
    fm: fonts.FontMeasurements,
    bbox: BoundingBox,
    align: TextEntityAlignment,
    length: float,
) -> Matrix44:
    """Returns the alignment transformation matrix to transform a basic
    text path at location (0, 0) and alignment :attr:`LEFT` into the final text
    path of the given alignment.
    For the alignments :attr:`FIT` and :attr:`ALIGNED` defines the argument
    `length` the  total length of the final text path. The given bounding box
    defines the rendering borders of the basic text path.

    """
    halign, valign = MAP_TEXT_ENUM_TO_ALIGN_FLAGS[align]
    matrix = basic_alignment_transformation(fm, bbox, halign, valign)

    stretch_x = 1.0
    stretch_y = 1.0
    if align == TextEntityAlignment.ALIGNED:
        stretch_x = length / bbox.size.x
        stretch_y = stretch_x
    elif align == TextEntityAlignment.FIT:
        stretch_x = length / bbox.size.x
    if stretch_x != 1.0:
        matrix *= Matrix44.scale(stretch_x, stretch_y, 1.0)
    return matrix


def basic_alignment_transformation(
    fm: fonts.FontMeasurements, bbox: BoundingBox, halign: int, valign: int
) -> Matrix44:
    if halign == const.LEFT:
        shift_x = 0.0
    elif halign == const.RIGHT:
        assert bbox.extmax is not None, "invalid empty bounding box"
        shift_x = -bbox.extmax.x
    elif halign == const.CENTER or halign > 2:  # ALIGNED, MIDDLE, FIT
        assert bbox.center is not None, "invalid empty bounding box"
        shift_x = -bbox.center.x
    else:
        raise ValueError(f"invalid halign argument: {halign}")
    cap_height = fm.cap_height
    descender_height = fm.descender_height
    if valign == const.BASELINE:
        shift_y = 0.0
    elif valign == const.TOP:
        shift_y = -cap_height
    elif valign == const.MIDDLE:
        shift_y = -cap_height / 2
    elif valign == const.BOTTOM:
        shift_y = descender_height
    else:
        raise ValueError(f"invalid valign argument: {valign}")
    if halign == 4:  # MIDDLE
        shift_y = -cap_height + fm.total_height / 2.0
    return Matrix44.translate(shift_x, shift_y, 0)


def make_hatches_from_str(
    s: str,
    font: fonts.FontFace,
    size: float = 1.0,
    align=TextEntityAlignment.LEFT,
    length: float = 0,
    dxfattribs=None,
    m: Matrix44 = None,
) -> list[Hatch]:
    """Convert a single line string `s` into a list of virtual
    :class:`~ezdxf.entities.Hatch` entities.
    The text `size` is the height of the uppercase letter "X" (cap height).
    The paths are aligned about the insertion point at (0, 0).
    The HATCH entities are aligned to this insertion point. BASELINE means the
    bottom of the letter "X".

    .. important::

        Returns an empty list for .shx, .shp and .lff fonts a.k.a. stroke fonts.

    Args:
         s: text to convert
         font: font face definition as :class:`~ezdxf.tools.fonts.FontFace` object
         size: text size (cap height) in drawing units
         align: alignment as :class:`ezdxf.enums.TextEntityAlignment`,
            default is :attr:`LEFT`
         length: target length for the :attr:`ALIGNED` and :attr:`FIT` alignments
         dxfattribs: additional DXF attributes
         m: transformation :class:`~ezdxf.math.Matrix44`

    """
    font_ = get_font(font)
    if font_.font_render_type is fonts.FontRenderType.STROKE:
        return []

    # HATCH is an OCS entity, transforming just the polyline paths
    # is not correct! The Hatch has to be created in the xy-plane!
    paths = make_paths_from_str(s, font, size, align, length)
    dxfattribs = dict(dxfattribs or {})
    dxfattribs.setdefault("solid_fill", 1)
    dxfattribs.setdefault("pattern_name", "SOLID")
    dxfattribs.setdefault("color", const.BYLAYER)
    hatches = path.to_hatches(paths, edge_path=True, dxfattribs=dxfattribs)
    if m is not None:
        # Transform HATCH entities as a unit:
        return [hatch.transform(m) for hatch in hatches]  # type: ignore
    else:
        return list(hatches)


def check_entity_type(entity):
    if entity is None:
        raise TypeError("entity is None")
    elif not entity.dxftype() in VALID_TYPES:
        raise TypeError(f"unsupported entity type: {entity.dxftype()}")


def make_path_from_entity(entity: AnyText) -> Path:
    """Convert text content from DXF entities TEXT and ATTRIB into a
    :term:`Multi-Path` object.
    The paths are located at the location of the source entity.
    """

    check_entity_type(entity)
    text = entity.plain_text()
    p = make_path_from_str(
        text,
        fonts.get_font_face(entity.font_name()),
        size=entity.dxf.height,  # cap height in drawing units
        align=entity.get_align_enum(),
        length=entity.fit_length(),
    )
    m = entity.wcs_transformation_matrix()
    return p.transform(m)


def make_paths_from_entity(entity: AnyText) -> list[Path]:
    """Convert text content from DXF entities TEXT and ATTRIB into a
    list of :class:`~ezdxf.path.Path` objects. All paths are returned as a
    list of :term:`Single-Path` objects.
    The paths are located at the location of the source entity.

    """
    return list(make_path_from_entity(entity).sub_paths())


def make_hatches_from_entity(entity: AnyText) -> list[Hatch]:
    """Convert text content from DXF entities TEXT and ATTRIB into a
    list of virtual :class:`~ezdxf.entities.Hatch` entities.
    The hatches are placed at the same location as the source entity and have
    the same DXF attributes as the source entity.

    """
    check_entity_type(entity)
    extrusion = entity.dxf.extrusion
    attribs = entity.graphic_properties()
    paths = make_paths_from_entity(entity)
    return list(
        path.to_hatches(
            paths,
            edge_path=True,
            extrusion=extrusion,
            dxfattribs=attribs,
        )
    )


@enum.unique
class Kind(enum.IntEnum):
    """The :class:`Kind` enum defines the DXF types to create as bit flags,
    e.g. 1+2 to get HATCHES as filling and SPLINES and POLYLINES as outline:

    === =========== ==============================
    Int Enum        Description
    === =========== ==============================
    1   HATCHES     :class:`~ezdxf.entities.Hatch` entities as filling
    2   SPLINES     :class:`~ezdxf.entities.Spline` and 3D :class:`~ezdxf.entities.Polyline`
                    entities as outline
    4   LWPOLYLINES :class:`~ezdxf.entities.LWPolyline` entities as approximated
                    (flattened) outline
    === =========== ==============================

    """

    HATCHES = 1
    SPLINES = 2
    LWPOLYLINES = 4


def virtual_entities(entity: AnyText, kind: int = Kind.HATCHES) -> EntityQuery:
    """Convert the text content of DXF entities TEXT and ATTRIB into virtual
    SPLINE and 3D POLYLINE entities or approximated LWPOLYLINE entities
    as outlines, or as HATCH entities as fillings.

    Returns the virtual DXF entities as an :class:`~ezdxf.query.EntityQuery`
    object.

    Args:
        entity: TEXT or ATTRIB entity
        kind: kind of entities to create as bit flags, see enum :class:`Kind`

    """
    check_entity_type(entity)
    extrusion = entity.dxf.extrusion
    attribs = entity.graphic_properties()
    entities: list[DXFGraphic] = []

    if kind & Kind.HATCHES:
        entities.extend(make_hatches_from_entity(entity))
    if kind & (Kind.SPLINES + Kind.LWPOLYLINES):
        paths = make_paths_from_entity(entity)
        if kind & Kind.SPLINES:
            entities.extend(path.to_splines_and_polylines(paths, dxfattribs=attribs))
        if kind & Kind.LWPOLYLINES:
            entities.extend(
                path.to_lwpolylines(paths, extrusion=extrusion, dxfattribs=attribs)
            )

    return EntityQuery(entities)


def explode(entity: AnyText, kind: int = Kind.HATCHES, target=None) -> EntityQuery:
    """Explode the text `entity` into virtual entities,
    see :func:`virtual_entities`. The source entity will be destroyed.

    The target layout is given by the `target` argument, if `target` is ``None``,
    the target layout is the source layout of the text entity.

    Returns the created DXF entities as an :class:`~ezdxf.query.EntityQuery`
    object.

    Args:
        entity: TEXT or ATTRIB entity to explode
        kind: kind of entities to create as bit flags, see enum :class:`Kind`
        target: target layout for new created DXF entities, ``None`` for the
            same layout as the source entity.

    """
    entities = virtual_entities(entity, kind)

    # Explicit check for None is required, because empty layouts are also False
    if target is None:
        target = entity.get_layout()
    entity.destroy()

    if target is not None:
        for e in entities:
            target.add_entity(e)
    return EntityQuery(entities)
