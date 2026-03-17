# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional, Iterable, Any
from typing_extensions import Self, TypeGuard

from ezdxf.entities import factory
from ezdxf import options
from ezdxf.lldxf import validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf import colors as clr
from ezdxf.lldxf import const
from ezdxf.lldxf.const import (
    DXF12,
    DXF2000,
    DXF2004,
    DXF2007,
    DXF2013,
    SUBCLASS_MARKER,
    TRANSPARENCY_BYBLOCK,
)
from ezdxf.math import OCS, Matrix44, UVec
from ezdxf.proxygraphic import load_proxy_graphic, export_proxy_graphic
from .dxfentity import DXFEntity, base_class, SubclassProcessor, DXFTagStorage

if TYPE_CHECKING:
    from ezdxf.audit import Auditor
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFNamespace
    from ezdxf.layouts import BaseLayout
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf import xref

__all__ = [
    "DXFGraphic",
    "acdb_entity",
    "acdb_entity_group_codes",
    "SeqEnd",
    "add_entity",
    "replace_entity",
    "elevation_to_z_axis",
    "is_graphic_entity",
    "get_font_name",
]

GRAPHIC_PROPERTIES = {
    "layer",
    "linetype",
    "color",
    "lineweight",
    "ltscale",
    "true_color",
    "color_name",
    "transparency",
}

acdb_entity: DefSubclass = DefSubclass(
    "AcDbEntity",
    {
        # Layer name as string, no auto fix for invalid names!
        "layer": DXFAttr(8, default="0", validator=validator.is_valid_layer_name),
        # Linetype name as string, no auto fix for invalid names!
        "linetype": DXFAttr(
            6,
            default="BYLAYER",
            optional=True,
            validator=validator.is_valid_table_name,
        ),
        # ACI color index, BYBLOCK=0, BYLAYER=256, BYOBJECT=257:
        "color": DXFAttr(
            62,
            default=256,
            optional=True,
            validator=validator.is_valid_aci_color,
            fixer=RETURN_DEFAULT,
        ),
        # modelspace=0, paperspace=1
        "paperspace": DXFAttr(
            67,
            default=0,
            optional=True,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Lineweight in mm times 100 (e.g. 0.13mm = 13). Smallest line weight is 13
        # and biggest line weight is 200, values outside this range prevents AutoCAD
        # from loading the file.
        # Special values: BYLAYER=-1, BYBLOCK=-2, DEFAULT=-3
        "lineweight": DXFAttr(
            370,
            default=-1,
            dxfversion=DXF2000,
            optional=True,
            validator=validator.is_valid_lineweight,
            fixer=validator.fix_lineweight,
        ),
        "ltscale": DXFAttr(
            48,
            default=1.0,
            dxfversion=DXF2000,
            optional=True,
            validator=validator.is_positive,
            fixer=RETURN_DEFAULT,
        ),
        # visible=0, invisible=1
        "invisible": DXFAttr(60, default=0, dxfversion=DXF2000, optional=True),
        # True color as 0x00RRGGBB 24-bit value
        # True color always overrides ACI "color"!
        "true_color": DXFAttr(420, dxfversion=DXF2004, optional=True),
        # Color name as string. Color books are stored in .stb config files?
        "color_name": DXFAttr(430, dxfversion=DXF2004, optional=True),
        # Transparency value 0x020000TT 0 = fully transparent / 255 = opaque
        # Special value 0x01000000 == ByBlock
        # unset value means ByLayer
        "transparency": DXFAttr(
            440,
            dxfversion=DXF2004,
            optional=True,
            validator=validator.is_transparency,
        ),
        # Shadow mode:
        # 0 = Casts and receives shadows
        # 1 = Casts shadows
        # 2 = Receives shadows
        # 3 = Ignores shadows
        "shadow_mode": DXFAttr(284, dxfversion=DXF2007, optional=True),
        "material_handle": DXFAttr(347, dxfversion=DXF2007, optional=True),
        "visualstyle_handle": DXFAttr(348, dxfversion=DXF2007, optional=True),
        # PlotStyleName type enum (AcDb::PlotStyleNameType). Stored and moved around
        # as a 16-bit integer. Custom non-entity
        "plotstyle_enum": DXFAttr(380, dxfversion=DXF2007, default=1, optional=True),
        # Handle value of the PlotStyleName object, basically a hard pointer, but
        # has a different range to make backward compatibility easier to deal with.
        "plotstyle_handle": DXFAttr(390, dxfversion=DXF2007, optional=True),
        # 92 or 160?: Number of bytes in the proxy entity graphics represented in
        # the subsequent 310 groups, which are binary chunk records (optional)
        # 310: Proxy entity graphics data (multiple lines; 256 characters max. per
        # line) (optional), compiled by TagCompiler() to a DXFBinaryTag() objects
    },
)
acdb_entity_group_codes = group_code_mapping(acdb_entity)


def elevation_to_z_axis(dxf: DXFNamespace, names: Iterable[str]):
    # The elevation group code (38) is only used for DXF R11 and prior and
    # ignored for DXF R2000 and later.
    # DXF R12 and later store the entity elevation in the z-axis of the
    # vertices, but AutoCAD supports elevation for R12 if no z-axis is present.
    # DXF types with legacy elevation support:
    # SOLID, TRACE, TEXT, CIRCLE, ARC, TEXT, ATTRIB, ATTDEF, INSERT, SHAPE

    # The elevation is only used for DXF R12 if no z-axis is stored in the DXF
    # file. This is a problem because ezdxf loads the vertices always as 3D
    # vertex including a z-axis even if no z-axis is present in DXF file.
    if dxf.hasattr("elevation"):
        elevation = dxf.elevation
        # ezdxf does not export the elevation attribute for any DXF version
        dxf.discard("elevation")
        if elevation == 0:
            return

        for name in names:
            v = dxf.get(name)
            # Only use elevation value if z-axis is 0, this will not work for
            # situations where an elevation and a z-axis=0 is present, but let's
            # assume if the elevation group code is used the z-axis is not
            # present if z-axis is 0.
            if v is not None and v.z == 0:
                dxf.set(name, v.replace(z=elevation))


class DXFGraphic(DXFEntity):
    """Common base class for all graphic entities, a subclass of
    :class:`~ezdxf.entities.dxfentity.DXFEntity`. These entities resides in
    entity spaces like modelspace, paperspace or block.
    """

    DXFTYPE = "DXFGFX"
    DEFAULT_ATTRIBS: dict[str, Any] = {"layer": "0"}
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Adds subclass processing for 'AcDbEntity', requires previous base
        class processing by parent class.

        (internal API)
        """
        # subclasses using simple_dxfattribs_loader() bypass this method!!!
        dxf = super().load_dxf_attribs(processor)
        if processor is None:
            return dxf
        r12 = processor.r12
        # It is valid to mix up the base class with AcDbEntity class.
        processor.append_base_class_to_acdb_entity()

        # Load proxy graphic data if requested
        if options.load_proxy_graphics:
            # length tag has group code 92 until DXF R2010
            if processor.dxfversion and processor.dxfversion < DXF2013:
                code = 92
            else:
                code = 160
            self.proxy_graphic = load_proxy_graphic(
                processor.subclasses[0 if r12 else 1],
                length_code=code,
            )
        processor.fast_load_dxfattribs(dxf, acdb_entity_group_codes, 1)
        return dxf

    def post_new_hook(self) -> None:
        """Post-processing and integrity validation after entity creation.

        (internal API)
        """
        if self.doc:
            if self.dxf.linetype not in self.doc.linetypes:
                raise const.DXFInvalidLineType(
                    f'Linetype "{self.dxf.linetype}" not defined.'
                )

    @property
    def rgb(self) -> tuple[int, int, int] | None:
        """Returns RGB true color as (r, g, b) tuple or None if true_color is not set."""
        if self.dxf.hasattr("true_color"):
            return clr.int2rgb(self.dxf.get("true_color"))
        return None

    @rgb.setter
    def rgb(self, rgb: clr.RGB | tuple[int, int, int]) -> None:
        """Set RGB true color as (r, g , b) tuple e.g. (12, 34, 56).

        Raises:
            TypeError: input value `rgb` has invalid type
        """
        self.dxf.set("true_color", clr.rgb2int(rgb))

    @rgb.deleter
    def rgb(self) -> None:
        """Delete RGB true color value."""
        self.dxf.discard("true_color")

    @property
    def transparency(self) -> float:
        """Get transparency as float value between 0 and 1, 0 is opaque and 1
        is 100% transparent (invisible). Transparency by block returns always 0.
        """
        if self.dxf.hasattr("transparency"):
            value = self.dxf.get("transparency")
            if validator.is_transparency(value):
                if value & TRANSPARENCY_BYBLOCK:  # just check flag state
                    return 0.0
                return clr.transparency2float(value)
        return 0.0

    @transparency.setter
    def transparency(self, transparency: float) -> None:
        """Set transparency as float value between 0 and 1, 0 is opaque and 1
        is 100% transparent (invisible).
        """
        self.dxf.set("transparency", clr.float2transparency(transparency))

    @property
    def is_transparency_by_layer(self) -> bool:
        """Returns ``True`` if entity inherits transparency from layer."""
        return not self.dxf.hasattr("transparency")

    @property
    def is_transparency_by_block(self) -> bool:
        """Returns ``True`` if entity inherits transparency from block."""
        return self.dxf.get("transparency", 0) == TRANSPARENCY_BYBLOCK

    def graphic_properties(self) -> dict:
        """Returns the important common properties layer, color, linetype,
        lineweight, ltscale, true_color and color_name as `dxfattribs` dict.
        """
        attribs = dict()
        for key in GRAPHIC_PROPERTIES:
            if self.dxf.hasattr(key):
                attribs[key] = self.dxf.get(key)
        return attribs

    def ocs(self) -> OCS:
        """Returns object coordinate system (:ref:`ocs`) for 2D entities like
        :class:`Text` or :class:`Circle`, returns a pass-through OCS for
        entities without OCS support.
        """
        # extrusion is only defined for 2D entities like Text, Circle, ...
        if self.dxf.is_supported("extrusion"):
            extrusion = self.dxf.get("extrusion", default=(0, 0, 1))
            return OCS(extrusion)
        else:
            return OCS()

    def set_owner(self, owner: Optional[str], paperspace: int = 0) -> None:
        """Set owner attribute and paperspace flag. (internal API)"""
        self.dxf.owner = owner
        if paperspace:
            self.dxf.paperspace = paperspace
        else:
            self.dxf.discard("paperspace")

    def link_entity(self, entity: DXFEntity) -> None:
        """Store linked or attached entities. Same API for both types of
        appended data, because entities with linked entities (POLYLINE, INSERT)
        have no attached entities and vice versa.

        (internal API)
        """
        pass

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags. (internal API)"""
        # Base class export is done by parent class.
        self.export_acdb_entity(tagwriter)
        # XDATA and embedded objects export is also done by the parent class.

    def export_acdb_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export subclass 'AcDbEntity' as DXF tags. (internal API)"""
        # Full control over tag order and YES, sometimes order matters
        not_r12 = tagwriter.dxfversion > DXF12
        if not_r12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_entity.name)

        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "paperspace",
                "layer",
                "linetype",
                "material_handle",
                "color",
                "lineweight",
                "ltscale",
                "invisible",
                "true_color",
                "color_name",
                "transparency",
                "plotstyle_enum",
                "plotstyle_handle",
                "shadow_mode",
                "visualstyle_handle",
            ],
        )

        if self.proxy_graphic and not_r12 and options.store_proxy_graphics:
            # length tag has group code 92 until DXF R2010
            export_proxy_graphic(
                self.proxy_graphic,
                tagwriter=tagwriter,
                length_code=(92 if tagwriter.dxfversion < DXF2013 else 160),
            )

    def get_layout(self) -> Optional[BaseLayout]:
        """Returns the owner layout or returns ``None`` if entity is not
        assigned to any layout.
        """
        if self.dxf.owner is None or self.doc is None:  # unlinked entity
            return None
        try:
            return self.doc.layouts.get_layout_by_key(self.dxf.owner)
        except const.DXFKeyError:
            pass
        try:
            return self.doc.blocks.get_block_layout_by_handle(self.dxf.owner)
        except const.DXFTableEntryError:
            return None

    def unlink_from_layout(self) -> None:
        """
        Unlink entity from associated layout. Does nothing if entity is already
        unlinked.

        It is more efficient to call the
        :meth:`~ezdxf.layouts.BaseLayout.unlink_entity` method of the associated
        layout, especially if you have to unlink more than one entity.
        """
        if not self.is_alive:
            raise TypeError("Can not unlink destroyed entity.")

        if self.doc is None:
            # no doc -> no layout
            self.dxf.owner = None
            return

        layout = self.get_layout()
        if layout:
            layout.unlink_entity(self)

    def move_to_layout(
        self, layout: BaseLayout, source: Optional[BaseLayout] = None
    ) -> None:
        """
        Move entity from model space or a paper space layout to another layout.
        For block layout as source, the block layout has to be specified. Moving
        between different DXF drawings is not supported.

        Args:
            layout: any layout (model space, paper space, block)
            source: provide source layout, faster for DXF R12, if entity is
                    in a block layout

        Raises:
            DXFStructureError: for moving between different DXF drawings
        """
        if source is None:
            source = self.get_layout()
            if source is None:
                raise const.DXFValueError("Source layout for entity not found.")
        source.move_to_layout(self, layout)

    def copy_to_layout(self, layout: BaseLayout) -> Self:
        """
        Copy entity to another `layout`, returns new created entity as
        :class:`DXFEntity` object. Copying between different DXF drawings is
        not supported.

        Args:
            layout: any layout (model space, paper space, block)

        Raises:
            DXFStructureError: for copying between different DXF drawings
        """
        if self.doc != layout.doc:
            raise const.DXFStructureError(
                "Copying between different DXF drawings is not supported."
            )

        new_entity = self.copy()
        layout.add_entity(new_entity)
        return new_entity

    def audit(self, auditor: Auditor) -> None:
        """Audit and repair graphical DXF entities.

        .. important::

            Do not delete entities while auditing process, because this
            would alter the entity database while iterating, instead use::

                auditor.trash(entity)

            to delete invalid entities after auditing automatically.
        """
        assert self.doc is auditor.doc, "Auditor for different DXF document."
        if not self.is_alive:
            return

        super().audit(auditor)
        auditor.check_owner_exist(self)
        dxf = self.dxf
        if dxf.hasattr("layer"):
            auditor.check_for_valid_layer_name(self)
        if dxf.hasattr("linetype"):
            auditor.check_entity_linetype(self)
        if dxf.hasattr("color"):
            auditor.check_entity_color_index(self)
        if dxf.hasattr("lineweight"):
            auditor.check_entity_lineweight(self)
        if dxf.hasattr("extrusion"):
            auditor.check_extrusion_vector(self)
        if dxf.hasattr("transparency"):
            auditor.check_transparency(self)

    def transform(self, m: Matrix44) -> Self:
        """Inplace transformation interface, returns `self` (floating interface).

        Args:
             m: 4x4 transformation matrix (:class:`ezdxf.math.Matrix44`)
        """
        raise NotImplementedError()

    def post_transform(self, m: Matrix44) -> None:
        """Should be called if the main entity transformation was successful."""
        if self.xdata is not None:
            self.xdata.transform(m)

    @property
    def is_post_transform_required(self) -> bool:
        """Check if post transform call is required."""
        return self.xdata is not None

    def translate(self, dx: float, dy: float, dz: float) -> Self:
        """Translate entity inplace about `dx` in x-axis, `dy` in y-axis and
        `dz` in z-axis, returns `self` (floating interface).

        Basic implementation uses the :meth:`transform` interface, subclasses
        may have faster implementations.
        """
        return self.transform(Matrix44.translate(dx, dy, dz))

    def scale(self, sx: float, sy: float, sz: float) -> Self:
        """Scale entity inplace about `dx` in x-axis, `dy` in y-axis and `dz`
        in z-axis, returns `self` (floating interface).
        """
        return self.transform(Matrix44.scale(sx, sy, sz))

    def scale_uniform(self, s: float) -> Self:
        """Scale entity inplace uniform about `s` in x-axis, y-axis and z-axis,
        returns `self` (floating interface).
        """
        return self.transform(Matrix44.scale(s))

    def rotate_axis(self, axis: UVec, angle: float) -> Self:
        """Rotate entity inplace about vector `axis`, returns `self`
        (floating interface).

        Args:
            axis: rotation axis as tuple or :class:`Vec3`
            angle: rotation angle in radians
        """
        return self.transform(Matrix44.axis_rotate(axis, angle))

    def rotate_x(self, angle: float) -> Self:
        """Rotate entity inplace about x-axis, returns `self`
        (floating interface).

        Args:
            angle: rotation angle in radians
        """
        return self.transform(Matrix44.x_rotate(angle))

    def rotate_y(self, angle: float) -> Self:
        """Rotate entity inplace about y-axis, returns `self`
        (floating interface).

        Args:
            angle: rotation angle in radians
        """
        return self.transform(Matrix44.y_rotate(angle))

    def rotate_z(self, angle: float) -> Self:
        """Rotate entity inplace about z-axis, returns `self`
        (floating interface).

        Args:
            angle: rotation angle in radians
        """
        return self.transform(Matrix44.z_rotate(angle))

    def has_hyperlink(self) -> bool:
        """Returns ``True`` if entity has an attached hyperlink."""
        return bool(self.xdata) and ("PE_URL" in self.xdata)  # type: ignore

    def set_hyperlink(
        self,
        link: str,
        description: Optional[str] = None,
        location: Optional[str] = None,
    ):
        """Set hyperlink of an entity."""
        xdata = [(1001, "PE_URL"), (1000, str(link))]
        if description:
            xdata.append((1002, "{"))
            xdata.append((1000, str(description)))
            if location:
                xdata.append((1000, str(location)))
            xdata.append((1002, "}"))

        self.discard_xdata("PE_URL")
        self.set_xdata("PE_URL", xdata)
        if self.doc and "PE_URL" not in self.doc.appids:
            self.doc.appids.new("PE_URL")
        return self

    def get_hyperlink(self) -> tuple[str, str, str]:
        """Returns hyperlink, description and location."""
        link = ""
        description = ""
        location = ""
        if self.xdata and "PE_URL" in self.xdata:
            xdata = [tag.value for tag in self.get_xdata("PE_URL") if tag.code == 1000]
            if len(xdata):
                link = xdata[0]
            if len(xdata) > 1:
                description = xdata[1]
            if len(xdata) > 2:
                location = xdata[2]
        return link, description, location

    def remove_dependencies(self, other: Optional[Drawing] = None) -> None:
        """Remove all dependencies from current document.

        (internal API)
        """
        if not self.is_alive:
            return

        super().remove_dependencies(other)
        # The layer attribute is preserved because layer doesn't need a layer
        # table entry, the layer attributes are reset to default attributes
        # like color is 7 and linetype is CONTINUOUS
        has_linetype = other is not None and (self.dxf.linetype in other.linetypes)
        if not has_linetype:
            self.dxf.linetype = "BYLAYER"
        self.dxf.discard("material_handle")
        self.dxf.discard("visualstyle_handle")
        self.dxf.discard("plotstyle_enum")
        self.dxf.discard("plotstyle_handle")

    def _new_compound_entity(self, type_: str, dxfattribs) -> Self:
        """Create and bind  new entity with same layout settings as `self`.

        Used by INSERT & POLYLINE to create appended DXF entities, don't use it
        to create new standalone entities.

        (internal API)
        """
        dxfattribs = dxfattribs or {}

        # if layer is not deliberately set, set same layer as creator entity,
        # at least VERTEX should have the same layer as the POLYGON entity.
        # Don't know if that is also important for the ATTRIB & INSERT entity.
        if "layer" not in dxfattribs:
            dxfattribs["layer"] = self.dxf.layer
        if self.doc:
            entity = factory.create_db_entry(type_, dxfattribs, self.doc)
        else:
            entity = factory.new(type_, dxfattribs)
        entity.dxf.owner = self.dxf.owner
        entity.dxf.paperspace = self.dxf.paperspace
        return entity  # type: ignore

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        super().register_resources(registry)
        dxf = self.dxf
        registry.add_layer(dxf.layer)
        registry.add_linetype(dxf.linetype)
        registry.add_handle(dxf.get("material_handle"))
        # unsupported resource attributes:
        # - visualstyle_handle
        # - plotstyle_handle

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        super().map_resources(clone, mapping)
        clone.dxf.layer = mapping.get_layer(self.dxf.layer)
        attrib_exist = self.dxf.hasattr
        if attrib_exist("linetype"):
            clone.dxf.linetype = mapping.get_linetype(self.dxf.linetype)
        if attrib_exist("material_handle"):
            clone.dxf.material_handle = mapping.get_handle(self.dxf.material_handle)

        # unsupported attributes:
        clone.dxf.discard("visualstyle_handle")
        clone.dxf.discard("plotstyle_handle")


@factory.register_entity
class SeqEnd(DXFGraphic):
    DXFTYPE = "SEQEND"

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        """Loading interface. (internal API)"""
        # bypass DXFGraphic, loading proxy graphic is skipped!
        dxf = super(DXFGraphic, self).load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(dxf, acdb_entity_group_codes)  # type: ignore
        return dxf


def add_entity(entity: DXFGraphic, layout: BaseLayout) -> None:
    """Add `entity` entity to the entity database and to the given `layout`."""
    assert entity.dxf.handle is None
    assert layout is not None
    if layout.doc:
        factory.bind(entity, layout.doc)
    layout.add_entity(entity)


def replace_entity(source: DXFGraphic, target: DXFGraphic, layout: BaseLayout) -> None:
    """Add `target` entity to the entity database and to the given `layout`
    and replace the `source` entity by the `target` entity.
    """
    assert target.dxf.handle is None
    assert layout is not None
    target.dxf.handle = source.dxf.handle
    if source in layout:
        layout.delete_entity(source)
        if layout.doc:
            factory.bind(target, layout.doc)
        layout.add_entity(target)
    else:
        source.destroy()


def is_graphic_entity(entity: DXFEntity) -> TypeGuard[DXFGraphic]:
    """Returns ``True`` if the `entity` has a graphical representations and
    can reside in the model space, a paper space or a block layout,
    otherwise the entity is a table or class entry or a DXF object from the
    OBJECTS section.
    """
    if isinstance(entity, DXFGraphic):
        return True
    if isinstance(entity, DXFTagStorage) and entity.is_graphic_entity:
        return True
    return False


def get_font_name(entity: DXFEntity) -> str:
    """Returns the font name of any DXF entity.

    This function always returns a font name even if the entity doesn't support text
    styles.  The default font name is "txt".
    """
    font_name = const.DEFAULT_TEXT_FONT
    doc = entity.doc
    if doc is None:
        return font_name
    try:
        style_name = entity.dxf.get("style", const.DEFAULT_TEXT_STYLE)
    except const.DXFAttributeError:
        return font_name
    try:
        style = doc.styles.get(style_name)
        return style.dxf.font
    except const.DXFTableEntryError:
        return font_name
