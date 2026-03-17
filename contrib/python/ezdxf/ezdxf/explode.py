# Copyright (c) 2020-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
import logging
from typing import (
    TYPE_CHECKING,
    Iterable,
    Callable,
    Optional,
    cast,
)
from ezdxf.lldxf import const
from ezdxf.entities import factory
from ezdxf.entities.boundary_paths import (
    PolylinePath,
    EdgePath,
    LineEdge,
    ArcEdge,
    EllipseEdge,
    SplineEdge,
)
from ezdxf.math import OCS, Vec3, ABS_TOL
from ezdxf.math.transformtools import (
    NonUniformScalingError,
    InsertTransformationError,
)
from ezdxf.query import EntityQuery
from ezdxf.entities.copy import default_copy, CopyNotSupported

if TYPE_CHECKING:
    from ezdxf.entities import (
        DXFGraphic,
        Insert,
        Attrib,
        Text,
        Dimension,
    )
    from ezdxf.entities.polygon import DXFPolygon
    from ezdxf.layouts import BaseLayout

logger = logging.getLogger("ezdxf")

__all__ = [
    "virtual_block_reference_entities",
    "virtual_boundary_path_entities",
    "explode_block_reference",
    "explode_entity",
    "attrib_to_text",
]


def default_logging_callback(entity, reason):
    logger.debug(
        f'(Virtual Block Reference Entities) Ignoring {str(entity)}: "{reason}"'
    )


def explode_block_reference(
    block_ref: Insert,
    target_layout: BaseLayout,
    *,
    redraw_order=False,
    copy_strategy=default_copy,
) -> EntityQuery:
    """Explode a block reference into DXF primitives.

    Transforms the block entities into the required WCS location by applying the
    block reference attributes `insert`, `extrusion`, `rotation` and the scaling
    values `xscale`, `yscale` and `zscale`.

    Returns an EntityQuery() container with all exploded DXF entities.

    Attached ATTRIB entities are converted to TEXT entities, this is the
    behavior of the BURST command of the AutoCAD Express Tools.

    This method does not apply the clipping path created by the XCLIP command. 
    The method returns all entities and ignores the clipping path polygon and no 
    entity is clipped.

    Args:
        block_ref: Block reference entity (INSERT)
        target_layout: explicit target layout for exploded DXF entities
        redraw_order: create entities in ascending redraw order if ``True``
        copy_strategy: customizable copy strategy

    .. warning::

        **Non uniform scaling** may lead to incorrect results for text entities
        (TEXT, MTEXT, ATTRIB) and maybe some other entities.

    (internal API)

    """
    if target_layout is None:
        raise const.DXFStructureError("Target layout is None.")

    if block_ref.doc is None:
        raise const.DXFStructureError(
            "Block reference has to be assigned to a DXF document."
        )

    def _explode_single_block_ref(block_ref):
        for entity in virtual_block_reference_entities(
            block_ref, redraw_order=redraw_order, copy_strategy=copy_strategy
        ):
            dxftype = entity.dxftype()
            target_layout.add_entity(entity)
            if dxftype == "DIMENSION":
                # Render a graphical representation for each exploded DIMENSION
                # entity as anonymous block.
                cast("Dimension", entity).render()
            entities.append(entity)

        # Convert attached ATTRIB entities to TEXT entities:
        # This is the behavior of the BURST command of the AutoCAD Express Tools
        for attrib in block_ref.attribs:
            # Attached ATTRIB entities are already located in the WCS
            text = attrib_to_text(attrib)
            target_layout.add_entity(text)
            entities.append(text)

    entitydb = block_ref.doc.entitydb
    assert (
        entitydb is not None
    ), "Exploding a block reference requires an entity database."

    entities: list[DXFGraphic] = []
    if block_ref.mcount > 1:
        for virtual_insert in block_ref.multi_insert():
            _explode_single_block_ref(virtual_insert)
    else:
        _explode_single_block_ref(block_ref)

    source_layout = block_ref.get_layout()
    if source_layout is not None:
        # Remove and destroy exploded INSERT if assigned to a layout
        source_layout.delete_entity(block_ref)
    else:
        entitydb.delete_entity(block_ref)
    return EntityQuery(entities)


IGNORE_FROM_ATTRIB = {
    "version",
    "prompt",
    "tag",
    "flags",
    "field_length",
    "lock_position",
    "attribute_type",
}


def attrib_to_text(attrib: Attrib) -> Text:
    dxfattribs = attrib.dxfattribs(drop=IGNORE_FROM_ATTRIB)
    # ATTRIB has same owner as INSERT but does not reside in any EntitySpace()
    # and must not deleted from any layout.
    # New TEXT entity has same handle as the replaced ATTRIB entity and replaces
    # the ATTRIB entity in the database.
    text = factory.new("TEXT", dxfattribs=dxfattribs)
    if attrib.doc:
        factory.bind(text, attrib.doc)
    return cast("Text", text)


def virtual_block_reference_entities(
    block_ref: Insert,
    *,
    skipped_entity_callback: Optional[Callable[[DXFGraphic, str], None]] = None,
    redraw_order=False,
    copy_strategy=default_copy,
) -> Iterable[DXFGraphic]:
    """Yields 'virtual' parts of block reference `block_ref`.

    This method is meant to examine the block reference entities without the need to
    explode the block reference. The `skipped_entity_callback()` will be called for all
    entities which are not processed, signature:
    :code:`skipped_entity_callback(entity: DXFGraphic, reason: str)`,
    `entity` is the original (untransformed) DXF entity of the block definition,
    the `reason` string is an explanation why the entity was skipped.

    These entities are located at the 'exploded' positions, but are not stored in
    the entity database, have no handle and are not assigned to any layout.

    This method does not apply the clipping path created by the XCLIP command. 
    The method returns all entities and ignores the clipping path polygon and no 
    entity is clipped.

    Args:
        block_ref: Block reference entity (INSERT)
        skipped_entity_callback: called whenever the transformation of an entity
            is not supported and so was skipped.
        redraw_order: yield entities in ascending redraw order if ``True``
        copy_strategy: customizable copy strategy

    .. warning::

        **Non uniform scaling** may lead to incorrect results for text entities
        (TEXT, MTEXT, ATTRIB) and maybe some other entities.

    (internal API)

    """
    assert block_ref.dxftype() == "INSERT"
    from ezdxf.entities import Ellipse

    skipped_entity_callback = skipped_entity_callback or default_logging_callback

    def disassemble(layout: BaseLayout) -> Iterable[DXFGraphic]:
        for entity in layout.entities_in_redraw_order() if redraw_order else layout:
            # Do not explode ATTDEF entities. Already available in Insert.attribs
            if entity.dxftype() == "ATTDEF":
                continue
            try:
                copy = entity.copy(copy_strategy=copy_strategy)
            except CopyNotSupported:
                if hasattr(entity, "virtual_entities"):
                    yield from entity.virtual_entities()
                else:
                    skipped_entity_callback(entity, "non copyable")
            else:
                if hasattr(copy, "remove_association"):
                    copy.remove_association()
                yield copy

    def transform(entities):
        for entity in entities:
            try:
                entity.transform(m)
            except NotImplementedError:
                skipped_entity_callback(entity, "non transformable")
            except NonUniformScalingError:
                dxftype = entity.dxftype()
                if dxftype in {"ARC", "CIRCLE"}:
                    if abs(entity.dxf.radius) > ABS_TOL:
                        yield Ellipse.from_arc(entity).transform(m)
                    else:
                        skipped_entity_callback(
                            entity, f"Invalid radius in entity {str(entity)}."
                        )
                elif dxftype in {"LWPOLYLINE", "POLYLINE"}:  # has arcs
                    yield from transform(entity.virtual_entities())
                else:
                    skipped_entity_callback(entity, "unsupported non-uniform scaling")
            except InsertTransformationError:
                # INSERT entity can not be represented in the target coordinate
                # system defined by transformation matrix `m`.
                # Yield transformed sub-entities of the INSERT entity:
                yield from transform(
                    virtual_block_reference_entities(
                        entity, skipped_entity_callback=skipped_entity_callback
                    )
                )
            else:
                yield entity

    m = block_ref.matrix44()
    block_layout = block_ref.block()
    if block_layout is None:
        raise const.DXFStructureError(
            f'Required block definition for "{block_ref.dxf.name}" does not exist.'
        )

    yield from transform(disassemble(block_layout))


EXCLUDE_FROM_EXPLODE = {"POINT"}


def explode_entity(
    entity: DXFGraphic, target_layout: Optional[BaseLayout] = None
) -> EntityQuery:
    """Explode parts of an entity as primitives into target layout, if target
    layout is ``None``, the target layout is the layout of the source entity.

    Returns an :class:`~ezdxf.query.EntityQuery` container with all DXF parts.

    Args:
        entity: DXF entity to explode, has to have a :meth:`virtual_entities()`
            method
        target_layout: target layout for DXF parts, ``None`` for same layout as
            source entity

    (internal API)

    """
    dxftype = entity.dxftype()
    virtual_entities = getattr(entity, "virtual_entities")
    if virtual_entities is None or dxftype in EXCLUDE_FROM_EXPLODE:
        raise const.DXFTypeError(f"Can not explode entity {dxftype}.")

    if entity.doc is None:
        raise const.DXFStructureError(
            f"{dxftype} has to be assigned to a DXF document."
        )

    entitydb = entity.doc.entitydb
    if entitydb is None:
        raise const.DXFStructureError(
            f"Exploding {dxftype} requires an entity database."
        )

    if target_layout is None:
        target_layout = entity.get_layout()
        if target_layout is None:
            raise const.DXFStructureError(
                f"{dxftype} without layout assignment, specify target layout."
            )

    entities = []
    for e in virtual_entities():
        target_layout.add_entity(e)
        entities.append(e)

    source_layout = entity.get_layout()
    if source_layout is not None:
        source_layout.delete_entity(entity)
    else:
        entitydb.delete_entity(entity)
    return EntityQuery(entities)


def virtual_boundary_path_entities(
    polygon: DXFPolygon,
) -> list[list[DXFGraphic]]:
    from ezdxf.entities import LWPolyline

    def polyline():
        p = LWPolyline.new(dxfattribs=dict(graphic_attribs))
        p.append_formatted_vertices(path.vertices, format="xyb")
        p.dxf.extrusion = ocs.uz
        p.dxf.elevation = elevation
        p.closed = path.is_closed
        return p

    graphic_attribs = polygon.graphic_properties()
    elevation = float(polygon.dxf.elevation.z)
    ocs = polygon.ocs()
    entities = []
    for path in polygon.paths:
        if isinstance(path, PolylinePath):
            entities.append([polyline()])
        elif isinstance(path, EdgePath):
            entities.append(
                _virtual_edge_path(path, dict(graphic_attribs), ocs, elevation)
            )
    return entities


def _virtual_edge_path(
    path: EdgePath, dxfattribs, ocs: OCS, elevation: float
) -> list[DXFGraphic]:
    from ezdxf.entities import Line, Arc, Ellipse, Spline

    def pnt_to_wcs(v):
        return ocs.to_wcs(Vec3(v).replace(z=elevation))

    def dir_to_wcs(v):
        return ocs.to_wcs(v)

    edges: list[DXFGraphic] = []
    for edge in path.edges:
        attribs = dict(dxfattribs)
        if isinstance(edge, LineEdge):
            attribs["start"] = pnt_to_wcs(edge.start)
            attribs["end"] = pnt_to_wcs(edge.end)
            edges.append(Line.new(dxfattribs=attribs))
        elif isinstance(edge, ArcEdge):
            attribs["center"] = edge.center
            attribs["radius"] = edge.radius
            attribs["elevation"] = elevation
            # Arcs angles are always stored in counter-clockwise orientation
            # around the extrusion vector!
            attribs["start_angle"] = edge.start_angle
            attribs["end_angle"] = edge.end_angle
            attribs["extrusion"] = ocs.uz
            edges.append(Arc.new(dxfattribs=attribs))
        elif isinstance(edge, EllipseEdge):
            attribs["center"] = pnt_to_wcs(edge.center)
            attribs["major_axis"] = dir_to_wcs(edge.major_axis)
            attribs["ratio"] = edge.ratio
            # Ellipse angles are always stored in counter-clockwise orientation
            # around the extrusion vector!
            attribs["start_param"] = edge.start_param
            attribs["end_param"] = edge.end_param
            attribs["extrusion"] = ocs.uz
            edges.append(Ellipse.new(dxfattribs=attribs))
        elif isinstance(edge, SplineEdge):
            spline = Spline.new(dxfattribs=attribs)
            spline.dxf.degree = edge.degree
            spline.knots = edge.knot_values
            spline.control_points = [pnt_to_wcs(v) for v in edge.control_points]
            if edge.weights:
                spline.weights = edge.weights
            if edge.fit_points:
                spline.fit_points = [pnt_to_wcs(v) for v in edge.fit_points]
            if edge.start_tangent is not None:
                spline.dxf.start_tangent = dir_to_wcs(edge.start_tangent)
            if edge.end_tangent is not None:
                spline.dxf.end_tangent = dir_to_wcs(edge.end_tangent)
            edges.append(spline)
    return edges
