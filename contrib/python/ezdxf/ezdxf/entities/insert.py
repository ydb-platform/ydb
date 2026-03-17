# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Iterator,
    cast,
    Union,
    Optional,
    Callable,
)
from typing_extensions import Self
import math
import logging

from ezdxf.lldxf import validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
    merge_group_code_mappings,
)
from ezdxf.lldxf.const import (
    DXF12,
    SUBCLASS_MARKER,
    DXFValueError,
    DXFKeyError,
    DXFStructureError,
)
from ezdxf.math import (
    Vec3,
    UVec,
    X_AXIS,
    Y_AXIS,
    Z_AXIS,
    Matrix44,
    UCS,
    NULLVEC,
)
from ezdxf.math.transformtools import (
    InsertTransformationError,
    InsertCoordinateSystem,
)
from ezdxf.explode import (
    explode_block_reference,
    virtual_block_reference_entities,
)
from ezdxf.entities import factory
from ezdxf.query import EntityQuery
from ezdxf.audit import AuditError
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import (
    DXFGraphic,
    acdb_entity,
    elevation_to_z_axis,
    acdb_entity_group_codes,
)
from .subentity import LinkedEntities
from .attrib import Attrib

if TYPE_CHECKING:
    from ezdxf.audit import Auditor
    from ezdxf.entities import DXFNamespace, AttDef, DXFEntity
    from ezdxf.layouts import BaseLayout, BlockLayout
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf import xref

__all__ = ["Insert"]

logger = logging.getLogger("ezdxf")
ABS_TOL = 1e-9

# DXF files as XREF:
# The INSERT entity is used to attach XREFS.
# The model space is the block content, if the whole document is used as
# BLOCK (XREF), but this is only supported for the DWG format.
# AutoCAD does not support DXF files as XREFS, they are ignored, but the DXF
# file is valid! BricsCAD shows DXF files as XREFS, but does not allow to attach
# DXF files as XREFS by the application itself.

# Multi-INSERT has subclass id AcDbMInsertBlock
acdb_block_reference = DefSubclass(
    "AcDbBlockReference",
    {
        "attribs_follow": DXFAttr(66, default=0, optional=True),
        "name": DXFAttr(2, validator=validator.is_valid_block_name),
        "insert": DXFAttr(10, xtype=XType.any_point),
        # Elevation is a legacy feature from R11 and prior, do not use this
        # attribute, store the entity elevation in the z-axis of the vertices.
        # ezdxf does not export the elevation attribute!
        "elevation": DXFAttr(38, default=0, optional=True),
        "xscale": DXFAttr(
            41,
            default=1,
            optional=True,
            validator=validator.is_not_zero,
            fixer=RETURN_DEFAULT,
        ),
        "yscale": DXFAttr(
            42,
            default=1,
            optional=True,
            validator=validator.is_not_zero,
            fixer=RETURN_DEFAULT,
        ),
        "zscale": DXFAttr(
            43,
            default=1,
            optional=True,
            validator=validator.is_not_zero,
            fixer=RETURN_DEFAULT,
        ),
        "rotation": DXFAttr(50, default=0, optional=True),
        "column_count": DXFAttr(
            70,
            default=1,
            optional=True,
            validator=validator.is_greater_zero,
            fixer=RETURN_DEFAULT,
        ),
        "row_count": DXFAttr(
            71,
            default=1,
            optional=True,
            validator=validator.is_greater_zero,
            fixer=RETURN_DEFAULT,
        ),
        "column_spacing": DXFAttr(44, default=0, optional=True),
        "row_spacing": DXFAttr(45, default=0, optional=True),
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            optional=True,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
    },
)
acdb_block_reference_group_codes = group_code_mapping(acdb_block_reference)
merged_insert_group_codes = merge_group_code_mappings(
    acdb_entity_group_codes, acdb_block_reference_group_codes  # type: ignore
)


# Notes to SEQEND:
#
# The INSERT entity requires only a SEQEND if ATTRIB entities are attached.
#  So a loaded INSERT could have a missing SEQEND.
#
# A bounded INSERT needs a SEQEND to be valid at export if there are attached
# ATTRIB entities, but the LinkedEntities.post_bind_hook() method creates
# always a new SEQEND after binding the INSERT entity to a document.
#
# Nonetheless, the Insert.add_attrib() method also creates the required SEQEND entity if
# necessary.


@factory.register_entity
class Insert(LinkedEntities):
    """DXF INSERT entity

    The INSERT entity is hard owner of its ATTRIB entities and the SEQEND entity:

        ATTRIB.dxf.owner == INSERT.dxf.handle
        SEQEND.dxf.owner == INSERT.dxf.handle

    Note:

        The ATTDEF entity in block definitions is owned by the BLOCK_RECORD like
        all graphical entities.

    """

    DXFTYPE = "INSERT"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_block_reference)

    @property
    def attribs(self) -> list[Attrib]:
        return self._sub_entities  # type: ignore

    @property
    def attribs_follow(self) -> bool:
        return bool(len(self.attribs))

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        # bypass DXFGraphic, loading proxy graphic is skipped!
        dxf = super(DXFGraphic, self).load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, merged_insert_group_codes)
            if processor.r12:
                # Transform elevation attribute from R11 to z-axis values:
                elevation_to_z_axis(dxf, ("insert",))
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            if (self.dxf.column_count > 1) or (self.dxf.row_count > 1):
                tagwriter.write_tag2(SUBCLASS_MARKER, "AcDbMInsertBlock")
            else:
                tagwriter.write_tag2(SUBCLASS_MARKER, "AcDbBlockReference")
        if self.attribs_follow:
            tagwriter.write_tag2(66, 1)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "name",
                "insert",
                "xscale",
                "yscale",
                "zscale",
                "rotation",
                "column_count",
                "row_count",
                "column_spacing",
                "row_spacing",
                "extrusion",
            ],
        )

    def export_dxf(self, tagwriter: AbstractTagWriter):
        super().export_dxf(tagwriter)
        # Do no export SEQEND if no ATTRIBS attached:
        if self.attribs_follow:
            self.process_sub_entities(lambda e: e.export_dxf(tagwriter))

    def register_resources(self, registry: xref.Registry) -> None:
        # The attached ATTRIB entities are registered by the parent class LinkedEntities
        super().register_resources(registry)
        registry.add_block_name(self.dxf.name)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        # The attached ATTRIB entities are mapped by the parent class LinkedEntities
        super().map_resources(clone, mapping)
        clone.dxf.name = mapping.get_block_name(self.dxf.name)

    @property
    def has_scaling(self) -> bool:
        """Returns ``True`` if scaling is applied to any axis."""
        if self.dxf.hasattr("xscale") and self.dxf.xscale != 1:
            return True
        if self.dxf.hasattr("yscale") and self.dxf.yscale != 1:
            return True
        if self.dxf.hasattr("zscale") and self.dxf.zscale != 1:
            return True
        return False

    @property
    def has_uniform_scaling(self) -> bool:
        """Returns ``True`` if the scale factor is uniform for x-, y- and z-axis,
        ignoring reflections e.g. (1, 1, -1) is uniform scaling.

        """
        return abs(self.dxf.xscale) == abs(self.dxf.yscale) == abs(self.dxf.zscale)

    def set_scale(self, factor: float):
        """Set a uniform scale factor."""
        if factor == 0:
            raise ValueError("Invalid scale factor.")
        self.dxf.xscale = factor
        self.dxf.yscale = factor
        self.dxf.zscale = factor
        return self

    def block(self) -> Optional[BlockLayout]:
        """Returns the associated :class:`~ezdxf.layouts.BlockLayout`."""
        if self.doc:
            return self.doc.blocks.get(self.dxf.name)
        return None

    def is_xref(self) -> bool:
        """Return ``True`` if the INSERT entity represents a XREF or XREF_OVERLAY."""
        block = self.block()
        if block is not None:
            return block.block_record.is_xref
        return False

    def place(
        self,
        insert: Optional[UVec] = None,
        scale: Optional[tuple[float, float, float]] = None,
        rotation: Optional[float] = None,
    ) -> Insert:
        """
        Set the location, scaling and rotation attributes. Arguments which are ``None``
        will be ignored.

        Args:
            insert: insert location as (x, y [,z]) tuple
            scale: (x-scale, y-scale, z-scale) tuple
            rotation : rotation angle in degrees

        """
        if insert is not None:
            self.dxf.insert = insert
        if scale is not None:
            if len(scale) != 3:
                raise DXFValueError("Argument scale has to be a (x, y[, z]) tuple.")
            x, y, z = scale
            self.dxf.xscale = x
            self.dxf.yscale = y
            self.dxf.zscale = z
        if rotation is not None:
            self.dxf.rotation = rotation
        return self

    def grid(
        self, size: tuple[int, int] = (1, 1), spacing: tuple[float, float] = (1, 1)
    ) -> Insert:
        """Place block reference in a grid layout, grid `size` defines the
        row- and column count, `spacing` defines the distance between two block
        references.

        Args:
            size: grid size as (row_count, column_count) tuple
            spacing: distance between placing as (row_spacing, column_spacing) tuple

        """
        try:
            rows, cols = size
        except ValueError:
            raise DXFValueError("Size has to be a (row_count, column_count) tuple.")
        self.dxf.row_count = rows
        self.dxf.column_count = cols
        try:
            row_spacing, col_spacing = spacing
        except ValueError:
            raise DXFValueError(
                "Spacing has to be a (row_spacing, column_spacing) tuple."
            )
        self.dxf.row_spacing = row_spacing
        self.dxf.column_spacing = col_spacing
        return self

    def get_attrib(
        self, tag: str, search_const: bool = False
    ) -> Optional[Union[Attrib, AttDef]]:
        """Get an attached :class:`Attrib` entity with the given `tag`,
        returns ``None`` if not found.  Some applications do not attach constant
        ATTRIB entities, set `search_const` to ``True``, to get at least the
        associated :class:`AttDef` entity.

        Args:
            tag: tag name of the ATTRIB entity
            search_const: search also const ATTDEF entities

        """
        for attrib in self.attribs:
            if tag == attrib.dxf.tag:
                return attrib
        if search_const and self.doc is not None:
            block = self.doc.blocks[self.dxf.name]
            for attdef in block.get_const_attdefs():
                if tag == attdef.dxf.tag:
                    return attdef
        return None

    def get_attrib_text(
        self, tag: str, default: str = "", search_const: bool = False
    ) -> str:
        """Get content text of an attached :class:`Attrib` entity with
        the given `tag`, returns the `default` value if not found.
        Some applications do not attach constant ATTRIB entities, set
        `search_const` to ``True``, to get content text of the
        associated :class:`AttDef` entity.

        Args:
            tag: tag name of the ATTRIB entity
            default: default value if ATTRIB `tag` is absent
            search_const: search also const ATTDEF entities

        """
        attrib = self.get_attrib(tag, search_const)
        if attrib is None:
            return default
        return attrib.dxf.text

    def has_attrib(self, tag: str, search_const: bool = False) -> bool:
        """Returns ``True`` if the INSERT entity has an attached ATTRIB entity with the
        given `tag`.  Some applications do not attach constant ATTRIB entities, set
        `search_const` to ``True``, to check for an associated :class:`AttDef` entity
        with constant content.


        Args:
            tag: tag name fo the ATTRIB entity
            search_const: search also const ATTDEF entities

        """
        return self.get_attrib(tag, search_const) is not None

    def add_attrib(
        self, tag: str, text: str, insert: UVec = (0, 0), dxfattribs=None
    ) -> Attrib:
        """Attach an :class:`Attrib` entity to the block reference.

        Example for appending an attribute to an INSERT entity::

            e.add_attrib('EXAMPLETAG', 'example text').set_placement(
                (3, 7), align=TextEntityAlignment.MIDDLE_CENTER
            )

        Args:
            tag: tag name of the ATTRIB entity
            text: content text as string
            insert: insert location as (x, y[, z]) tuple in :ref:`OCS`
            dxfattribs: additional DXF attributes for the ATTRIB entity

        """
        dxfattribs = dict(dxfattribs or {})
        dxfattribs["tag"] = tag
        dxfattribs["text"] = text
        dxfattribs["insert"] = insert
        attrib = cast("Attrib", self._new_compound_entity("ATTRIB", dxfattribs))
        self.attribs.append(attrib)

        # This case is only possible if the INSERT was read from a file without
        # attached ATTRIB entities:
        if self.seqend is None:
            self.new_seqend()
        return attrib

    def delete_attrib(self, tag: str, ignore=False) -> None:
        """Delete an attached :class:`Attrib` entity from INSERT. Raises an
        :class:`DXFKeyError` exception, if no ATTRIB for the given `tag` exist if
        `ignore` is ``False``.

        Args:
            tag: tag name of the ATTRIB entity
            ignore: ``False`` for raising :class:`DXFKeyError` if ATTRIB `tag`
                does not exist.

        Raises:
            DXFKeyError: no ATTRIB for the given `tag` exist

        """
        for index, attrib in enumerate(self.attribs):
            if attrib.dxf.tag == tag:
                del self.attribs[index]
                attrib.destroy()
                return
        if not ignore:
            raise DXFKeyError(tag)

    def delete_all_attribs(self) -> None:
        """Delete all :class:`Attrib` entities attached to the INSERT entity."""
        if not self.is_alive:
            return

        for attrib in self.attribs:
            attrib.destroy()
        self._sub_entities = []

    def transform(self, m: Matrix44) -> Insert:
        """Transform INSERT entity by transformation matrix `m` inplace.

        Unlike the transformation matrix `m`, the INSERT entity can not
        represent a non-orthogonal target coordinate system and an
        :class:`InsertTransformationError` will be raised in that case.

        """
        dxf = self.dxf
        source_system = InsertCoordinateSystem(
            insert=Vec3(dxf.insert),
            scale=(dxf.xscale, dxf.yscale, dxf.zscale),
            rotation=dxf.rotation,
            extrusion=dxf.extrusion,
        )
        try:
            target_system = source_system.transform(m, ABS_TOL)
        except InsertTransformationError:
            raise InsertTransformationError(
                "INSERT entity can not represent a non-orthogonal target coordinate system."
            )
        dxf.insert = target_system.insert
        dxf.rotation = target_system.rotation
        dxf.extrusion = target_system.extrusion
        dxf.xscale = target_system.scale_factor_x
        dxf.yscale = target_system.scale_factor_y
        dxf.zscale = target_system.scale_factor_z

        for attrib in self.attribs:
            attrib.transform(m)
        self.post_transform(m)
        return self

    def translate(self, dx: float, dy: float, dz: float) -> Insert:
        """Optimized INSERT translation about `dx` in x-axis, `dy` in y-axis
        and `dz` in z-axis.

        """
        ocs = self.ocs()
        self.dxf.insert = ocs.from_wcs(Vec3(dx, dy, dz) + ocs.to_wcs(self.dxf.insert))
        for attrib in self.attribs:
            attrib.translate(dx, dy, dz)
        return self

    def matrix44(self) -> Matrix44:
        """Returns a transformation matrix to transform the block entities from the
        block reference coordinate system into the :ref:`WCS`.
        """
        dxf = self.dxf
        sx = dxf.xscale
        sy = dxf.yscale
        sz = dxf.zscale

        ocs = self.ocs()
        extrusion = ocs.uz
        ux = Vec3(ocs.to_wcs(X_AXIS))
        uy = Vec3(ocs.to_wcs(Y_AXIS))
        m = Matrix44.ucs(ux=ux * sx, uy=uy * sy, uz=extrusion * sz)
        # same as Matrix44.ucs(ux, uy, extrusion) * Matrix44.scale(sx, sy, sz)

        angle = math.radians(dxf.rotation)
        if angle:
            m *= Matrix44.axis_rotate(extrusion, angle)

        insert = ocs.to_wcs(dxf.get("insert", NULLVEC))

        block_layout = self.block()
        if block_layout is not None:
            # transform block base point into WCS without translation
            insert -= m.transform_direction(block_layout.block.dxf.base_point)  # type: ignore

        # set translation
        m.set_row(3, insert.xyz)
        return m

    def ucs(self):
        """Returns the block reference coordinate system as :class:`ezdxf.math.UCS`
        object.
        """
        m = self.matrix44()
        ucs = UCS()
        ucs.matrix = m
        return ucs

    def reset_transformation(self) -> None:
        """Reset block reference attributes location, rotation angle and
        the extrusion vector but preserves the scale factors.

        """
        self.dxf.insert = NULLVEC
        self.dxf.discard("rotation")
        self.dxf.discard("extrusion")

    def explode(
        self, target_layout: Optional[BaseLayout] = None, *, redraw_order=False
    ) -> EntityQuery:
        """Explodes the block reference entities into the target layout, if target
        layout is ``None``, the layout of the block reference will be used.
        This method destroys the source block reference entity.

        Transforms the block entities into the required :ref:`WCS` location by
        applying the block reference attributes `insert`, `extrusion`,
        `rotation` and the scale factors `xscale`, `yscale` and `zscale`.

        Attached ATTRIB entities are converted to TEXT entities, this is the
        behavior of the BURST command of the AutoCAD Express Tools.

        .. warning::

            **Non-uniform scale factors** may lead to incorrect results some entities
            (TEXT, MTEXT, ATTRIB).

        Args:
            target_layout: target layout for exploded entities, ``None`` for
                same layout as source entity.
            redraw_order: create entities in ascending redraw order if ``True``

        Returns:
            :class:`~ezdxf.query.EntityQuery` container referencing all exploded
            DXF entities.

        """
        if target_layout is None:
            target_layout = self.get_layout()
            if target_layout is None:
                raise DXFStructureError(
                    "INSERT without layout assignment, specify target layout"
                )
        return explode_block_reference(
            self, target_layout=target_layout, redraw_order=redraw_order
        )

    def __virtual_entities__(self) -> Iterator[DXFGraphic]:
        """Implements the SupportsVirtualEntities protocol.

        This protocol is for consistent internal usage and does not replace
        the method :meth:`virtual_entities`! Ignores the redraw-order!
        """
        return self.virtual_entities()

    def virtual_entities(
        self,
        *,
        skipped_entity_callback: Optional[Callable[[DXFGraphic, str], None]] = None,
        redraw_order=False,
    ) -> Iterator[DXFGraphic]:
        """
        Yields the transformed referenced block content as virtual entities.

        This method is meant to examine the block reference entities at the target
        location without exploding the block reference.
        These entities are not stored in the entity database, have no handle and
        are not assigned to any layout. It is possible to convert these entities
        into regular drawing entities by adding the entities to the entities
        database and a layout of the same DXF document as the block reference::

            doc.entitydb.add(entity)
            msp = doc.modelspace()
            msp.add_entity(entity)

        .. warning::

            **Non-uniform scale factors** may return incorrect results for some entities
            (TEXT, MTEXT, ATTRIB).

        This method does not resolve the MINSERT attributes, only the
        sub-entities of the first INSERT will be returned. To resolve MINSERT
        entities check if multi insert processing is required, that's the case
        if the property :attr:`Insert.mcount` > 1, use the :meth:`Insert.multi_insert`
        method to resolve the MINSERT entity into multiple INSERT entities.

        This method does not apply the clipping path created by the XCLIP command.
        The method returns all entities and ignores the clipping path polygon and no
        entity is clipped.

        The `skipped_entity_callback()` will be called for all entities which are not
        processed, signature:
        :code:`skipped_entity_callback(entity: DXFEntity, reason: str)`,
        `entity` is the original (untransformed) DXF entity of the block definition, the
        `reason` string is an explanation why the entity was skipped.

        Args:
            skipped_entity_callback: called whenever the transformation of an
                entity is not supported and so was skipped
            redraw_order: yield entities in ascending redraw order if ``True``

        """
        for e in virtual_block_reference_entities(
            self,
            skipped_entity_callback=skipped_entity_callback,
            redraw_order=redraw_order,
        ):
            e.set_source_block_reference(self)
            yield e

    @property
    def mcount(self) -> int:
        """Returns the multi-insert count, MINSERT (multi-insert) processing
        is required if :attr:`mcount` > 1.

        """
        return (self.dxf.row_count if self.dxf.row_spacing else 1) * (
            self.dxf.column_count if self.dxf.column_spacing else 1
        )

    def multi_insert(self) -> Iterator[Insert]:
        """Yields a virtual INSERT entity for each grid element of a MINSERT
        entity (multi-insert).
        """

        def transform_attached_attrib_entities(insert, offset):
            for attrib in insert.attribs:
                attrib.dxf.insert += offset

        def adjust_dxf_attribs(insert, offset):
            dxf = insert.dxf
            dxf.insert += offset
            dxf.discard("row_count")
            dxf.discard("column_count")
            dxf.discard("row_spacing")
            dxf.discard("column_spacing")

        done = set()
        row_spacing = self.dxf.row_spacing
        col_spacing = self.dxf.column_spacing
        rotation = self.dxf.rotation
        for row in range(self.dxf.row_count):
            for col in range(self.dxf.column_count):
                # All transformations in OCS:
                offset = Vec3(col * col_spacing, row * row_spacing)
                # If any spacing is 0, yield only unique locations:
                if offset not in done:
                    done.add(offset)
                    if rotation:  # Apply rotation to the grid.
                        offset = offset.rotate_deg(rotation)
                    # Do not apply scaling to the grid!
                    insert = self.copy()
                    adjust_dxf_attribs(insert, offset)
                    transform_attached_attrib_entities(insert, offset)
                    yield insert

    def add_auto_attribs(self, values: dict[str, str]) -> Insert:
        """
        Attach for each :class:`~ezdxf.entities.Attdef` entity, defined in the
        block definition, automatically an :class:`Attrib` entity to the block
        reference and set ``tag/value`` DXF attributes of the ATTRIB entities
        by the ``key/value`` pairs (both as strings) of the `values` dict.
        The ATTRIB entities are placed relative to the insert location of the
        block reference, which is identical to the block base point.

        This method avoids the wrapper block of the
        :meth:`~ezdxf.layouts.BaseLayout.add_auto_blockref` method, but the
        visual results may not match the results of CAD applications, especially
        for non-uniform scaling. If the visual result is very important to you,
        use the :meth:`add_auto_blockref` method.

        Args:
            values: :class:`~ezdxf.entities.Attrib` tag values as ``tag/value``
                pairs

        """
        def unpack(dxfattribs) -> tuple[str, str, UVec]:
            tag = dxfattribs.pop("tag")
            text = values.get(tag, None)
            if text is None:  # get default value from ATTDEF
                text = dxfattribs.get("text", "")
            location = dxfattribs.pop("insert")
            return tag, text, location

        def autofill() -> None:
            for attdef in block_layout.attdefs():  # type: ignore
                dxfattribs = attdef.dxfattribs(drop={"prompt", "handle"})

                # Caution! Some mandatory values may not exist!
                # These are DXF structure errors, but loaded DXF files may have errors!
                if "tag" not in dxfattribs:
                    # ATTRIB without "tag" makes no sense!
                    logger.warning(
                        f"Skipping {str(attdef)}: missing mandatory 'tag' attribute"
                    )
                    continue
                if "insert" not in dxfattribs:
                    # Don't know where to place the ATTRIB entity.
                    logger.warning(
                        f"Skipping {str(attdef)}: missing mandatory 'insert' attribute"
                    )
                    continue

                tag, text, location = unpack(dxfattribs)
                attrib = self.add_attrib(tag, text, location, dxfattribs)
                if attdef.has_embedded_mtext_entity:
                    mtext = attdef.virtual_mtext_entity()
                    mtext.text = text
                    attrib.embed_mtext(mtext)
                attrib.transform(m)

        block_layout = self.block()
        if block_layout is not None:
            m = self.matrix44()
            autofill()
        return self

    def audit(self, auditor: Auditor) -> None:
        """Validity check."""
        super().audit(auditor)
        doc = auditor.doc
        if doc and doc.blocks:
            name = self.dxf.name
            if name is None:
                auditor.fixed_error(
                    code=AuditError.UNDEFINED_BLOCK_NAME,
                    message=f"Deleted entity {str(self)} without a BLOCK name",
                )
                auditor.trash(self)
            elif name not in doc.blocks:
                auditor.fixed_error(
                    code=AuditError.UNDEFINED_BLOCK,
                    message=f"Deleted entity {str(self)} without required BLOCK"
                    f" definition.",
                )
                auditor.trash(self)

    def __referenced_blocks__(self) -> Iterable[str]:
        """Support for the "ReferencedBlocks" protocol."""
        block = self.block()
        if block is not None:
            return (block.block_record_handle,)
        return tuple()
