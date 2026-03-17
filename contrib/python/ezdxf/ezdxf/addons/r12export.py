#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
"""
Module to export any DXF document as DXF version R12 without modifying the source
document.

.. versionadded:: 1.1

To get the best result use the ODA File Converter add-on::

    from ezdxf.addons import odafc

    odafc.convert("any.dxf", "r12.dxf", version="R12")

"""

from __future__ import annotations
from typing import TYPE_CHECKING, TextIO, Callable, Optional
import os
from io import StringIO
import logging

import ezdxf
from ezdxf import const, proxygraphic, path
from ezdxf.document import Drawing
from ezdxf.entities import (
    BlockRecord,
    DXFEntity,
    DXFTagStorage,
    Ellipse,
    Hatch,
    Insert,
    LWPolyline,
    MPolygon,
    MText,
    Mesh,
    Polyface,
    Polyline,
    Spline,
    Textstyle,
)
from ezdxf.entities.polygon import DXFPolygon
from ezdxf.addons import MTextExplode
from ezdxf.entitydb import EntitySpace
from ezdxf.layouts import BlockLayout, VirtualLayout
from ezdxf.lldxf.tagwriter import TagWriter, AbstractTagWriter
from ezdxf.lldxf.types import DXFTag, TAG_STRING_FORMAT
from ezdxf.math import Z_AXIS, Vec3, NULLVEC
from ezdxf.r12strict import R12NameTranslator
from ezdxf.render import MeshBuilder
from ezdxf.sections.table import TextstyleTable

if TYPE_CHECKING:
    from ezdxf.eztypes import GenericLayoutType

__all__ = ["R12Exporter", "convert", "saveas", "write"]

MAX_SAGITTA = 0.01
logger = logging.getLogger("ezdxf")


def convert(doc: Drawing, *, max_sagitta: float = MAX_SAGITTA) -> Drawing:
    """Export and reload DXF document as DXF version R12.

    Writes the DXF document into a temporary file at the file-system and reloads this
    file by the :func:`ezdxf.readfile` function.
    """
    stream = StringIO()
    exporter = R12Exporter(doc, max_sagitta=max_sagitta)
    exporter.write(stream)
    stream.seek(0)
    return ezdxf.read(stream)


def write(doc: Drawing, stream: TextIO, *, max_sagitta: float = MAX_SAGITTA) -> None:
    """Write a DXF document as DXF version R12 to a text stream. The `max_sagitta`
    argument determines the accuracy of the curve flatting for SPLINE and ELLIPSE
    entities.

    Args:
        doc: DXF document to export
        stream: output stream, use :attr:`doc.encoding` as encoding
        max_sagitta: maximum distance from the center of the curve to the
            center of the line segment between two approximation points to
            determine if a segment should be subdivided.

    """
    exporter = R12Exporter(doc, max_sagitta=max_sagitta)
    exporter.write(stream)


def saveas(
    doc: Drawing, filepath: str | os.PathLike, *, max_sagitta: float = MAX_SAGITTA
) -> None:
    """Write a DXF document as DXF version R12 to a file. The `max_sagitta`
    argument determines the accuracy of the curve flatting for SPLINE and ELLIPSE
    entities.

    Args:
        doc: DXF document to export
        filepath: output filename
        max_sagitta: maximum distance from the center of the curve to the
            center of the line segment between two approximation points to
            determine if a segment should be subdivided.

    """
    with open(filepath, "wt", encoding=doc.encoding, errors="dxfreplace") as stream:
        write(
            doc,
            stream,
            max_sagitta=max_sagitta,
        )


def spline_to_polyline(
    spline: Spline, max_sagitta: float, min_segments: int
) -> Polyline:
    polyline = Polyline.new(
        dxfattribs={
            "layer": spline.dxf.layer,
            "linetype": spline.dxf.linetype,
            "color": spline.dxf.color,
            "flags": const.POLYLINE_3D_POLYLINE,
        }
    )

    polyline.append_vertices(points=spline.flattening(max_sagitta, min_segments))
    polyline.new_seqend()
    return polyline


def ellipse_to_polyline(
    ellipse: Ellipse, max_sagitta: float, min_segments: int
) -> Polyline:
    polyline = Polyline.new(
        dxfattribs={
            "layer": ellipse.dxf.layer,
            "linetype": ellipse.dxf.linetype,
            "color": ellipse.dxf.color,
            "flags": const.POLYLINE_3D_POLYLINE,
        }
    )
    polyline.append_vertices(points=ellipse.flattening(max_sagitta, min_segments))
    polyline.new_seqend()
    return polyline


def lwpolyline_to_polyline(lwpolyline: LWPolyline) -> Polyline:
    polyline = Polyline.new(
        dxfattribs={
            "layer": lwpolyline.dxf.layer,
            "linetype": lwpolyline.dxf.linetype,
            "color": lwpolyline.dxf.color,
        }
    )
    polyline.new_seqend()
    polyline.append_formatted_vertices(lwpolyline.get_points(), format="xyseb")
    if lwpolyline.is_closed:
        polyline.close()
    if lwpolyline.dxf.hasattr("const_width"):
        width = lwpolyline.dxf.const_width
        polyline.dxf.default_start_width = width
        polyline.dxf.default_end_width = width
    extrusion = Vec3(lwpolyline.dxf.extrusion)
    if not extrusion.isclose(Z_AXIS):
        polyline.dxf.extrusion = extrusion
        elevation = lwpolyline.dxf.elevation
        polyline.dxf.elevation = Vec3(0, 0, elevation)
        # Set z-axis of VERTEX.location to elevation?

    return polyline


def mesh_to_polyface_mesh(mesh: Mesh) -> Polyface:
    builder = MeshBuilder.from_mesh(mesh)
    return builder.render_polyface(
        VirtualLayout(),
        dxfattribs={
            "layer": mesh.dxf.layer,
            "linetype": mesh.dxf.linetype,
            "color": mesh.dxf.color,
        },
    )


def get_xpl_block_name(entity: DXFEntity) -> str:
    assert entity.dxf.handle is not None
    return f"EZDXF_XPL_{entity.dxftype()}_{entity.dxf.handle}"


def export_lwpolyline(exporter: R12Exporter, entity: DXFEntity):
    assert isinstance(entity, LWPolyline)
    polyline = lwpolyline_to_polyline(entity)
    if len(polyline.vertices):
        polyline.export_dxf(exporter.tagwriter())


def export_mesh(exporter: R12Exporter, entity: DXFEntity):
    assert isinstance(entity, Mesh)
    polyface_mesh = mesh_to_polyface_mesh(entity)
    if len(polyface_mesh.vertices):
        polyface_mesh.export_dxf(exporter.tagwriter())


def export_spline(exporter: R12Exporter, entity: DXFEntity):
    assert isinstance(entity, Spline)
    polyline = spline_to_polyline(
        entity, exporter.max_sagitta, exporter.min_spline_segments
    )
    if len(polyline.vertices):
        polyline.export_dxf(exporter.tagwriter())


def export_ellipse(exporter: R12Exporter, entity: DXFEntity):
    assert isinstance(entity, Ellipse)
    polyline = ellipse_to_polyline(
        entity, exporter.max_sagitta, exporter.min_ellipse_segments
    )
    if len(polyline.vertices):
        polyline.export_dxf(exporter.tagwriter())


def make_insert(name: str, entity: DXFEntity, location=NULLVEC) -> Insert:
    return Insert.new(
        dxfattribs={
            "name": name,
            "layer": entity.dxf.layer,
            "linetype": entity.dxf.linetype,
            "color": entity.dxf.color,
            "insert": location,
        }
    )


def export_proxy_graphic(exporter: R12Exporter, entity: DXFEntity):
    assert isinstance(entity.proxy_graphic, bytes)
    pg = proxygraphic.ProxyGraphic(entity.proxy_graphic)
    try:
        entities = EntitySpace(pg.virtual_entities())
    except proxygraphic.ProxyGraphicError:
        return
    exporter.export_entity_space(entities)


def export_mtext(exporter: R12Exporter, entity: DXFEntity):
    assert isinstance(entity, MText)
    layout = VirtualLayout()
    exporter.explode_mtext(entity, layout)
    exporter.export_entity_space(layout.entity_space)


def export_virtual_entities(exporter: R12Exporter, entity: DXFEntity):
    layout = VirtualLayout()
    try:
        for e in entity.virtual_entities():  # type: ignore
            layout.add_entity(e)
    except Exception:
        return
    exporter.export_entity_space(layout.entity_space)


def export_pattern_fill(entity: DXFEntity, block: BlockLayout) -> None:
    assert isinstance(entity, DXFPolygon)
    dxfattribs = {
        "layer": entity.dxf.layer,
        "color": entity.dxf.color,
    }
    for start, end in entity.render_pattern_lines():
        block.add_line(start, end, dxfattribs=dxfattribs)


def export_solid_fill(
    entity: DXFPolygon,
    block: BlockLayout,
    max_sagitta: float,
    min_segments: int,
) -> None:
    dxfattribs = {
        "layer": entity.dxf.layer,
        "color": entity.dxf.color,
    }

    extrusion = Vec3(entity.dxf.extrusion)
    if not extrusion.is_null and not extrusion.isclose(Z_AXIS):
        dxfattribs["extrusion"] = extrusion

    # triangulation in OCS coordinates, including elevation and offset values:
    for vertices in entity.triangulate(max_sagitta, min_segments):
        block.add_solid(vertices, dxfattribs=dxfattribs)


def export_hatch(exporter: R12Exporter, entity: DXFEntity) -> None:
    assert isinstance(entity, Hatch)
    # export hatch into an anonymous block
    block = exporter.new_block(entity)
    insert = make_insert(block.name, entity)
    insert.export_dxf(exporter.tagwriter())

    if entity.has_pattern_fill:
        export_pattern_fill(entity, block)
    else:
        export_solid_fill(
            entity, block, exporter.max_sagitta, exporter.min_spline_segments
        )


def export_mpolygon(exporter: R12Exporter, entity: DXFEntity) -> None:
    assert isinstance(entity, MPolygon)
    # export mpolygon into an anonymous block
    block = exporter.new_block(entity)
    insert = make_insert(block.name, entity)
    insert.export_dxf(exporter.tagwriter())

    # elevation is the z-axis of the vertices
    path.render_polylines2d(
        block,
        path.from_hatch_ocs(entity, offset=Vec3(entity.dxf.offset)),
        distance=exporter.max_sagitta,
        segments=exporter.min_spline_segments,
        extrusion=Vec3(entity.dxf.extrusion),
        dxfattribs={
            "layer": entity.dxf.layer,
            "linetype": entity.dxf.linetype,
            "color": entity.dxf.color,
        },
    )
    if entity.has_pattern_fill:
        export_pattern_fill(entity, block)
    else:
        export_solid_fill(
            entity, block, exporter.max_sagitta, exporter.min_spline_segments
        )


def export_acad_table(exporter: R12Exporter, entity: DXFEntity) -> None:
    from ezdxf.entities.acad_table import AcadTableBlockContent

    assert isinstance(entity, AcadTableBlockContent)
    table: AcadTableBlockContent = entity
    location = table.get_insert_location()
    block_name = table.get_block_name()
    if not block_name.startswith("*T"):
        return
    try:
        acdb_entity = table.xtags.get_subclass("AcDbEntity")
    except const.DXFIndexError:
        return
    layer = acdb_entity.get_first_value(8, "0")
    insert = Insert.new(
        dxfattribs={"name": block_name, "layer": layer, "insert": location}
    )
    insert.export_dxf(exporter.tagwriter())


# Planned features: explode complex newer entity types into DXF primitives.
# currently skipped entity types:
# - ACAD_TABLE: graphic as geometry block is available
# --------------------------------------------------------------------------------------
# - all ACIS based entities: tessellated meshes could be exported, but very much work
#   and beyond my current knowledge
# - IMAGE and UNDERLAY: no support possible
# - XRAY and XLINE: no support possible (infinite lines)

# Possible name tags to translate:
# 1 The primary text value for an entity - never a name
# 2 A name: Attribute tag, Block name, and so on. Also used to identify a DXF section or
#   table name
# 3 Other textual or name values - only in DIMENSION a name
# 4 Other textual values - never a name!
# 5 Entity handle expressed as a hexadecimal string (fixed)
# 6 Line type name (fixed)
# 7 Text style name (fixed)
# 8 Layer name (fixed)
# 1001: AppID
# 1003: layer name in XDATA (fixed)
NAME_TAG_CODES = {2, 3, 6, 7, 8, 1001, 1003}


class R12TagWriter(TagWriter):
    def __init__(self, stream: TextIO):
        super().__init__(stream, dxfversion=const.DXF12, write_handles=False)
        self.skip_xdata = False
        self.current_entity = ""
        self.translator = R12NameTranslator()

    def set_stream(self, stream: TextIO) -> None:
        self._stream = stream

    def write_tag(self, tag: DXFTag) -> None:
        code, value = tag
        if code == 0:
            self.current_entity = str(value)
        if code > 999 and self.skip_xdata:
            return
        if code in NAME_TAG_CODES:
            self._stream.write(
                TAG_STRING_FORMAT % (code, self.sanitize_name(code, value))
            )
        else:
            self._stream.write(tag.dxfstr())

    def write_tag2(self, code: int, value) -> None:
        if code > 999 and self.skip_xdata:
            return
        if code == 0:
            self.current_entity = str(value)
        if code in NAME_TAG_CODES:
            value = self.sanitize_name(code, value)
        self._stream.write(TAG_STRING_FORMAT % (code, value))

    def sanitize_name(self, code: int, name: str) -> str:
        # sanitize group code 3 + 4
        # LTYPE - <description> has group code - not a table name
        # STYLE - <font> has group code (3) - not a table name
        # STYLE - <bigfont> has group code (4) - not a table name
        # DIMSTYLE - <dimpost> has group code e.g. "<> mm" (3) - not a table name
        # DIMSTYLE - <dimapost> has group code (4) - not a table name
        # ATTDEF - <prompt> has group code (3) - not a table name
        # DIMENSION - <dimstyle> has group code (3) - is a table name!
        if code == 3 and self.current_entity != "DIMENSION":
            return name
        return self.translator.translate(name)


class SpecialStyleTable:
    def __init__(self, styles: TextstyleTable, extra_styles: TextstyleTable):
        self.styles = styles
        self.extra_styles = extra_styles

    def get_text_styles(self) -> list[Textstyle]:
        entries = list(self.styles.entries.values())
        for name, extra_style in self.extra_styles.entries.items():
            if not self.styles.has_entry(name):
                entries.append(extra_style)
        return entries

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        text_styles = self.get_text_styles()
        tagwriter.write_tag2(0, "TABLE")
        tagwriter.write_tag2(2, "STYLE")
        tagwriter.write_tag2(70, len(text_styles))
        for style in text_styles:
            style.export_dxf(tagwriter)
        tagwriter.write_tag2(0, "ENDTAB")


EOF_STR = "0\nEOF\n"


def detect_max_block_number(names: list[str]) -> int:
    max_number = 0
    for name in names:
        name = name.upper()
        if not name.startswith("*"):
            continue
        try:  # *U10
            number = int(name[2:])
        except ValueError:
            continue
        max_number = max(max_number, number)
    return max_number + 1


class R12Exporter:
    def __init__(self, doc: Drawing, max_sagitta: float = 0.01):
        assert isinstance(doc, Drawing)
        self._doc = doc
        self._tagwriter = R12TagWriter(StringIO())
        self.max_sagitta = float(max_sagitta)  # flattening SPLINE, ELLIPSE
        self.min_spline_segments: int = 4  # flattening SPLINE
        self.min_ellipse_segments: int = 8  # flattening ELLIPSE
        self._extra_doc = ezdxf.new("R12")
        self._next_block_number = detect_max_block_number(
            [br.dxf.name for br in doc.block_records]
        )
        # Exporters are required to convert newer entity types into DXF R12 types.
        # All newer entity types without an exporter will be ignored.
        self.exporters: dict[str, Callable[[R12Exporter, DXFEntity], None]] = {
            "LWPOLYLINE": export_lwpolyline,
            "MESH": export_mesh,
            "SPLINE": export_spline,
            "ELLIPSE": export_ellipse,
            "MTEXT": export_mtext,
            "LEADER": export_virtual_entities,
            "MLEADER": export_virtual_entities,
            "MULTILEADER": export_virtual_entities,
            "MLINE": export_virtual_entities,
            "HATCH": export_hatch,
            "MPOLYGON": export_mpolygon,
            "ACAD_TABLE": export_acad_table,
        }

    def disable_exporter(self, entity_type: str):
        del self.exporters[entity_type]

    @property
    def doc(self) -> Drawing:
        return self._doc

    def tagwriter(self, stream: Optional[TextIO] = None) -> R12TagWriter:
        if stream is not None:
            self._tagwriter.set_stream(stream)
        return self._tagwriter

    def write(self, stream: TextIO) -> None:
        """Write DXF document to text stream."""
        stream.write(self.to_string())

    def to_string(self) -> str:
        """Export DXF document as string."""
        # export layouts before blocks: may create new anonymous blocks
        entities = self.export_layouts_to_string()
        # export blocks before HEADER and TABLES sections: may create new text styles
        blocks = self.export_blocks_to_string()

        return "".join(
            (
                self.export_header_to_string(),
                self.export_tables_to_string(),
                blocks,
                entities,
                EOF_STR,
            )
        )

    def next_block_name(self, char: str) -> str:
        name = f"*{char}{self._next_block_number}"
        self._next_block_number += 1
        return name

    def new_block(self, entity: DXFEntity) -> BlockLayout:
        name = self.next_block_name("U")
        return self._extra_doc.blocks.new(
            name,
            dxfattribs={
                "layer": entity.dxf.get("layer", "0"),
                "flags": const.BLK_ANONYMOUS,
            },
        )

    def export_header_to_string(self) -> str:
        in_memory_stream = StringIO()
        self.doc.header.export_dxf(self.tagwriter(in_memory_stream))
        return in_memory_stream.getvalue()

    def export_tables_to_string(self) -> str:
        # DXF R12 does not support XDATA in tables according Autodesk DWG TrueView
        in_memory_stream = StringIO()
        tables = self.doc.tables
        preserve_table = tables.styles
        tables.styles = SpecialStyleTable(self.doc.styles, self._extra_doc.styles)  # type: ignore

        tagwriter = self.tagwriter(in_memory_stream)
        tagwriter.skip_xdata = True
        tables.export_dxf(tagwriter)
        tables.styles = preserve_table
        tagwriter.skip_xdata = False
        return in_memory_stream.getvalue()

    def export_blocks_to_string(self) -> str:
        in_memory_stream = StringIO()
        self._tagwriter.set_stream(in_memory_stream)

        self._write_section_header("BLOCKS")
        for block_record in self.doc.block_records:
            if block_record.is_any_paperspace and not block_record.is_active_paperspace:
                continue
            name = block_record.dxf.name.lower()
            if name in ("$model_space", "$paper_space"):
                # These block names collide with the translated names of the *Model_Space
                # and the *Paper_Space blocks.
                continue
            self._export_block_record(block_record)

        extra_blocks = self.get_extra_blocks()
        while len(extra_blocks):
            for block_record in extra_blocks:
                self._export_block_record(block_record)
                self.discard_extra_block(block_record.dxf.name)
            # block record export can create further blocks
            extra_blocks = self.get_extra_blocks()

        self._write_endsec()
        return in_memory_stream.getvalue()

    def discard_extra_block(self, name: str) -> None:
        self._extra_doc.block_records.discard(name)

    def get_extra_blocks(self) -> list[BlockRecord]:
        return [
            br for br in self._extra_doc.block_records if br.dxf.name.startswith("*U")
        ]

    def explode_mtext(self, mtext: MText, layout: GenericLayoutType):
        with MTextExplode(layout, self._extra_doc) as xpl:
            xpl.explode(mtext, destroy=False)

    def export_layouts_to_string(self) -> str:
        in_memory_stream = StringIO()
        self._tagwriter.set_stream(in_memory_stream)

        self._write_section_header("ENTITIES")
        self.export_entity_space(self.doc.modelspace().entity_space)
        self.export_entity_space(self.doc.paperspace().entity_space)
        self._write_endsec()
        return in_memory_stream.getvalue()

    def _export_block_record(self, block_record: BlockRecord):
        tagwriter = self._tagwriter
        assert block_record.block is not None
        block_record.block.export_dxf(tagwriter)
        if not block_record.is_any_layout:
            self.export_entity_space(block_record.entity_space)
        assert block_record.endblk is not None
        block_record.endblk.export_dxf(tagwriter)

    def export_entity_space(self, space: EntitySpace):
        tagwriter = self._tagwriter
        for entity in space:
            if entity.MIN_DXF_VERSION_FOR_EXPORT > const.DXF12 or isinstance(
                entity, DXFTagStorage
            ):
                exporter = self.exporters.get(entity.dxftype())
                if exporter:
                    exporter(self, entity)
                    continue
                if entity.proxy_graphic:
                    export_proxy_graphic(self, entity)
            else:
                entity.export_dxf(tagwriter)

    def _write_section_header(self, name: str) -> None:
        self._tagwriter.write_str(f"  0\nSECTION\n  2\n{name}\n")

    def _write_endsec(self) -> None:
        self._tagwriter.write_tag2(0, "ENDSEC")
