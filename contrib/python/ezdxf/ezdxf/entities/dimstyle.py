# Copyright (c) 2019-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Optional
from typing_extensions import Self
import logging
from ezdxf.enums import MTextLineAlignment
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    VIRTUAL_TAG,
    group_code_mapping,
    RETURN_DEFAULT,
)
from ezdxf.lldxf import const
from ezdxf.lldxf.const import DXF12, DXF2007, DXF2000
from ezdxf.lldxf import validator
from ezdxf.render.arrows import ARROWS
from .dxfentity import SubclassProcessor, DXFEntity, base_class
from .layer import acdb_symbol_table_record
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFNamespace
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf import xref

__all__ = ["DimStyle"]
logger = logging.getLogger("ezdxf")

acdb_dimstyle = DefSubclass(
    "AcDbDimStyleTableRecord",
    {
        "name": DXFAttr(2, default="Standard", validator=validator.is_valid_table_name),
        "flags": DXFAttr(70, default=0),
        "dimpost": DXFAttr(3, default=""),
        "dimapost": DXFAttr(4, default=""),
        # Arrow names are the base data -> handle (DXF2000) is set at export
        "dimblk": DXFAttr(5, default=""),
        "dimblk1": DXFAttr(6, default=""),
        "dimblk2": DXFAttr(7, default=""),
        "dimscale": DXFAttr(40, default=1),
        # 0 has a special but unknown meaning, handle as 1.0
        "dimasz": DXFAttr(41, default=2.5),
        "dimexo": DXFAttr(42, default=0.625),
        "dimdli": DXFAttr(43, default=3.75),
        "dimexe": DXFAttr(44, default=1.25),
        "dimrnd": DXFAttr(45, default=0),
        "dimdle": DXFAttr(46, default=0),  # dimension line extension
        "dimtp": DXFAttr(47, default=0),
        "dimtm": DXFAttr(48, default=0),
        # undocumented: length of extension line if fixed (dimfxlon = 1)
        "dimfxl": DXFAttr(49, dxfversion=DXF2007, default=2.5),
        # jog angle, Angle of oblique dimension line segment in jogged radius dimension
        "dimjogang": DXFAttr(50, dxfversion=DXF2007, default=90, optional=True),
        # measurement text height
        "dimtxt": DXFAttr(140, default=2.5),
        # center marks and center lines; 0 = off, <0 = center line, >0 = center mark
        "dimcen": DXFAttr(141, default=2.5),
        "dimtsz": DXFAttr(142, default=0),
        "dimaltf": DXFAttr(143, default=0.03937007874),
        # measurement length factor
        "dimlfac": DXFAttr(144, default=1),
        # text vertical position if dimtad=0
        "dimtvp": DXFAttr(145, default=0),
        "dimtfac": DXFAttr(146, default=1),
        # default gap around the measurement text
        "dimgap": DXFAttr(147, default=0.625),
        "dimaltrnd": DXFAttr(148, dxfversion=DXF2000, default=0),
        # 0=None, 1=canvas color, 2=dimtfillclr
        "dimtfill": DXFAttr(69, dxfversion=DXF2007, default=0),
        # color index for dimtfill==2
        "dimtfillclr": DXFAttr(70, dxfversion=DXF2007, default=0),
        "dimtol": DXFAttr(71, default=0),
        "dimlim": DXFAttr(72, default=0),
        # text inside horizontal
        "dimtih": DXFAttr(73, default=0),
        # text outside horizontal
        "dimtoh": DXFAttr(74, default=0),
        # suppress extension line 1
        "dimse1": DXFAttr(75, default=0),
        # suppress extension line 2
        "dimse2": DXFAttr(76, default=0),
        # text vertical location: 0=center; 1+2+3=above; 4=below
        "dimtad": DXFAttr(77, default=1),
        "dimzin": DXFAttr(78, default=8),
        # dimazin:
        # 0 = Displays all leading and trailing zeros
        # 1 = Suppresses leading zeros in decimal dimensions (for example, 0.5000 becomes .5000)
        # 2 = Suppresses trailing zeros in decimal dimensions (for example, 12.5000 becomes 12.5)
        # 3 = Suppresses leading and trailing zeros (for example, 0.5000 becomes .5)
        "dimazin": DXFAttr(79, default=3, dxfversion=DXF2000),
        # dimarcsym: show arc symbol
        # 0 = preceding text
        # 1 = above text
        # 2 = disable
        "dimarcsym": DXFAttr(90, dxfversion=DXF2000, optional=True),
        "dimalt": DXFAttr(170, default=0),
        "dimaltd": DXFAttr(171, default=3),
        "dimtofl": DXFAttr(172, default=1),
        "dimsah": DXFAttr(173, default=0),
        # force dimension text inside
        "dimtix": DXFAttr(174, default=0),
        "dimsoxd": DXFAttr(175, default=0),
        # dimension line color
        "dimclrd": DXFAttr(176, default=0),
        # extension line color
        "dimclre": DXFAttr(177, default=0),
        # text color
        "dimclrt": DXFAttr(178, default=0),
        "dimadec": DXFAttr(179, dxfversion=DXF2000, default=2),
        "dimunit": DXFAttr(270),  # obsolete
        "dimdec": DXFAttr(271, dxfversion=DXF2000, default=2),
        # can appear multiple times ???
        "dimtdec": DXFAttr(272, dxfversion=DXF2000, default=2),
        "dimaltu": DXFAttr(273, dxfversion=DXF2000, default=2),
        "dimalttd": DXFAttr(274, dxfversion=DXF2000, default=3),
        # 0 = Decimal degrees
        # 1 = Degrees/minutes/seconds
        # 2 = Grad
        # 3 = Radians
        "dimaunit": DXFAttr(
            275,
            dxfversion=DXF2000,
            default=0,
            validator=validator.is_in_integer_range(0, 4),
            fixer=RETURN_DEFAULT,
        ),
        "dimfrac": DXFAttr(276, dxfversion=DXF2000, default=0),
        "dimlunit": DXFAttr(277, dxfversion=DXF2000, default=2),
        "dimdsep": DXFAttr(278, dxfversion=DXF2000, default=44),
        # 0 = Moves the dimension line with dimension text
        # 1 = Adds a leader when dimension text is moved
        # 2 = Allows text to be moved freely without a leader
        "dimtmove": DXFAttr(279, dxfversion=DXF2000, default=0),
        # 0=center; 1=left; 2=right; 3=above ext1; 4=above ext2
        "dimjust": DXFAttr(280, dxfversion=DXF2000, default=0),
        # suppress first part of the dimension line
        "dimsd1": DXFAttr(281, dxfversion=DXF2000, default=0),
        # suppress second part of the dimension line
        "dimsd2": DXFAttr(282, dxfversion=DXF2000, default=0),
        "dimtolj": DXFAttr(283, dxfversion=DXF2000, default=0),
        "dimtzin": DXFAttr(284, dxfversion=DXF2000, default=8),
        "dimaltz": DXFAttr(285, dxfversion=DXF2000, default=0),
        "dimalttz": DXFAttr(286, dxfversion=DXF2000, default=0),
        "dimfit": DXFAttr(287),  # obsolete, now use DIMATFIT and DIMTMOVE
        "dimupt": DXFAttr(288, dxfversion=DXF2000, default=0),
        # Determines how dimension text and arrows are arranged when space is
        # not sufficient to place both within the extension lines.
        # 0 = Places both text and arrows outside extension lines
        # 1 = Moves arrows first, then text
        # 2 = Moves text first, then arrows
        # 3 = Moves either text or arrows, whichever fits best
        "dimatfit": DXFAttr(289, dxfversion=DXF2000, default=3),
        # undocumented: 1 = fixed extension line length
        "dimfxlon": DXFAttr(290, dxfversion=DXF2007, default=0),
        # Virtual tags are transformed at DXF export - for DIMSTYLE the
        # resource names are exported as <name>_handle tags:
        # virtual: set/get STYLE by name
        "dimtxsty": DXFAttr(VIRTUAL_TAG, dxfversion=DXF2000),
        # virtual: set/get leader arrow by block name
        "dimldrblk": DXFAttr(VIRTUAL_TAG, dxfversion=DXF2000),
        # virtual: set/get LINETYPE by name
        "dimltype": DXFAttr(VIRTUAL_TAG, dxfversion=DXF2007),
        # virtual: set/get referenced LINETYPE by name
        "dimltex2": DXFAttr(VIRTUAL_TAG, dxfversion=DXF2007),
        # virtual: set/get referenced LINETYPE by name
        "dimltex1": DXFAttr(VIRTUAL_TAG, dxfversion=DXF2007),
        # Entity handles are not used internally (see virtual tags above),
        # these handles are set at DXF export:
        # handle of referenced STYLE entry
        "dimtxsty_handle": DXFAttr(340, dxfversion=DXF2000),
        # handle of referenced BLOCK_RECORD
        "dimblk_handle": DXFAttr(342, dxfversion=DXF2000),
        # handle of referenced BLOCK_RECORD
        "dimblk1_handle": DXFAttr(343, dxfversion=DXF2000),
        # handle of referenced BLOCK_RECORD
        "dimblk2_handle": DXFAttr(344, dxfversion=DXF2000),
        # handle of referenced BLOCK_RECORD
        "dimldrblk_handle": DXFAttr(341, dxfversion=DXF2000),
        # handle of linetype for dimension line
        "dimltype_handle": DXFAttr(345, dxfversion=DXF2007),
        # handle of linetype for extension line 1
        "dimltex1_handle": DXFAttr(346, dxfversion=DXF2007),
        # handle of linetype for extension line 2
        "dimltex2_handle": DXFAttr(347, dxfversion=DXF2007),
        # dimension line lineweight enum value, default BYBLOCK
        "dimlwd": DXFAttr(371, default=const.LINEWEIGHT_BYBLOCK, dxfversion=DXF2000),
        # extension line lineweight enum value, default BYBLOCK
        "dimlwe": DXFAttr(372, default=const.LINEWEIGHT_BYBLOCK, dxfversion=DXF2000),
    },
)
acdb_dimstyle_group_codes = group_code_mapping(acdb_dimstyle)

EXPORT_MAP_R2007 = [
    "name",
    "flags",
    "dimscale",
    "dimasz",
    "dimexo",
    "dimdli",
    "dimexe",
    "dimrnd",
    "dimdle",
    "dimtp",
    "dimtm",
    "dimfxl",
    "dimjogang",
    "dimtxt",
    "dimcen",
    "dimtsz",
    "dimaltf",
    "dimlfac",
    "dimtvp",
    "dimtfac",
    "dimgap",
    "dimaltrnd",
    "dimtfill",
    "dimtfillclr",
    "dimtol",
    "dimlim",
    "dimtih",
    "dimtoh",
    "dimse1",
    "dimse2",
    "dimtad",
    "dimzin",
    "dimazin",
    "dimarcsym",
    "dimalt",
    "dimaltd",
    "dimtofl",
    "dimsah",
    "dimtix",
    "dimsoxd",
    "dimclrd",
    "dimclre",
    "dimclrt",
    "dimadec",
    "dimdec",
    "dimtdec",
    "dimaltu",
    "dimalttd",
    "dimaunit",
    "dimfrac",
    "dimlunit",
    "dimdsep",
    "dimtmove",
    "dimjust",
    "dimsd1",
    "dimsd2",
    "dimtolj",
    "dimtzin",
    "dimaltz",
    "dimalttz",
    "dimupt",
    "dimatfit",
    "dimfxlon",
    "dimtxsty_handle",
    "dimldrblk_handle",
    "dimblk_handle",
    "dimblk1_handle",
    "dimblk2_handle",
    "dimltype_handle",
    "dimltex1_handle",
    "dimltex2_handle",
    "dimlwd",
    "dimlwe",
]

EXPORT_MAP_R2000 = [
    "name",
    "flags",
    "dimpost",
    "dimapost",
    "dimscale",
    "dimasz",
    "dimexo",
    "dimdli",
    "dimexe",
    "dimrnd",
    "dimdle",
    "dimtp",
    "dimtm",
    "dimtxt",
    "dimcen",
    "dimtsz",
    "dimaltf",
    "dimlfac",
    "dimtvp",
    "dimtfac",
    "dimgap",
    "dimaltrnd",
    "dimtol",
    "dimlim",
    "dimtih",
    "dimtoh",
    "dimse1",
    "dimse2",
    "dimtad",
    "dimzin",
    "dimazin",
    "dimarcsym",
    "dimalt",
    "dimaltd",
    "dimtofl",
    "dimsah",
    "dimtix",
    "dimsoxd",
    "dimclrd",
    "dimclre",
    "dimclrt",
    "dimadec",
    "dimdec",
    "dimtdec",
    "dimaltu",
    "dimalttd",
    "dimaunit",
    "dimfrac",
    "dimlunit",
    "dimdsep",
    "dimtmove",
    "dimjust",
    "dimsd1",
    "dimsd2",
    "dimtolj",
    "dimtzin",
    "dimaltz",
    "dimalttz",
    "dimupt",
    "dimatfit",
    "dimtxsty_handle",
    "dimldrblk_handle",
    "dimblk_handle",
    "dimblk1_handle",
    "dimblk2_handle",
    "dimlwd",
    "dimlwe",
]

EXPORT_MAP_R12 = [
    "name",
    "flags",
    "dimpost",
    "dimapost",
    "dimblk",
    "dimblk1",
    "dimblk2",
    "dimscale",
    "dimasz",
    "dimexo",
    "dimdli",
    "dimexe",
    "dimrnd",
    "dimdle",
    "dimtp",
    "dimtm",
    "dimtxt",
    "dimcen",
    "dimtsz",
    "dimaltf",
    "dimlfac",
    "dimtvp",
    "dimtfac",
    "dimgap",
    "dimtol",
    "dimlim",
    "dimtih",
    "dimtoh",
    "dimse1",
    "dimse2",
    "dimtad",
    "dimzin",
    "dimalt",
    "dimaltd",
    "dimtofl",
    "dimsah",
    "dimtix",
    "dimsoxd",
    "dimclrd",
    "dimclre",
    "dimclrt",
]

DIM_TEXT_STYLE_ATTR = "dimtxsty"
DIM_ARROW_HEAD_ATTRIBS = ("dimblk", "dimblk1", "dimblk2", "dimldrblk")
DIM_LINETYPE_ATTRIBS = ("dimltype", "dimltex1", "dimltex2")


def dim_filter(name: str) -> bool:
    return name.startswith("dim")


@register_entity
class DimStyle(DXFEntity):
    """DXF BLOCK_RECORD table entity"""

    DXFTYPE = "DIMSTYLE"
    DXFATTRIBS = DXFAttributes(base_class, acdb_symbol_table_record, acdb_dimstyle)
    CODE_TO_DXF_ATTRIB = dict(DXFATTRIBS.build_group_code_items(dim_filter))

    @property
    def dxfversion(self):
        return self.doc.dxfversion

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            # group code 70 is used 2x, simple_dxfattribs_loader() can't be used!
            processor.fast_load_dxfattribs(dxf, acdb_dimstyle_group_codes, 2)
        return dxf

    def post_load_hook(self, doc: Drawing) -> None:
        # 2nd Loading stage: resolve handles to names.
        # ezdxf uses names for blocks, linetypes and text style as internal
        # data, handles are set at export.
        super().post_load_hook(doc)
        db = doc.entitydb
        for attrib_name in DIM_ARROW_HEAD_ATTRIBS:
            if self.dxf.hasattr(attrib_name):
                continue
            block_record_handle = self.dxf.get(attrib_name + "_handle")
            if block_record_handle and block_record_handle != "0":
                try:
                    name = db[block_record_handle].dxf.name
                except KeyError:
                    logger.info(
                        f"Replace undefined block reference "
                        f"#{block_record_handle} by default arrow."
                    )
                    name = ""  # default arrow name
            else:
                name = ""  # default arrow name
            self.dxf.set(attrib_name, name)

        style_handle = self.dxf.get("dimtxsty_handle", None)
        if style_handle and style_handle != "0":
            try:
                self.dxf.dimtxsty = db[style_handle].dxf.name
            except (KeyError, AttributeError):
                logger.info(f"Ignore undefined text style #{style_handle}.")

        for attrib_name in DIM_LINETYPE_ATTRIBS:
            lt_handle = self.dxf.get(attrib_name + "_handle", None)
            if lt_handle and lt_handle != "0":
                try:
                    name = db[lt_handle].dxf.name
                except (KeyError, AttributeError):
                    logger.info(f"Ignore undefined line type #{lt_handle}.")
                else:
                    self.dxf.set(attrib_name, name)
        # Remove all handles, to be sure setting handles for resource names
        # at export.
        self.discard_handles()

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_symbol_table_record.name)
            tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_dimstyle.name)

        if tagwriter.dxfversion > DXF12:
            # Set handles from dimblk names:
            self.set_handles()

        if tagwriter.dxfversion == DXF12:
            attribs = EXPORT_MAP_R12
        elif tagwriter.dxfversion < DXF2007:
            attribs = EXPORT_MAP_R2000
        else:
            attribs = EXPORT_MAP_R2007
        self.dxf.export_dxf_attribs(tagwriter, attribs)

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        assert self.doc is not None, "DIMSTYLE entity must be assigned to a document"
        super().register_resources(registry)
        # ezdxf uses names for blocks, linetypes and text style as internal data
        # register text style
        text_style_name = self.dxf.get(DIM_TEXT_STYLE_ATTR)
        if text_style_name:
            try:
                style = self.doc.styles.get(text_style_name)
                registry.add_entity(style)
            except const.DXFTableEntryError:
                pass

        # register linetypes
        for attr_name in DIM_LINETYPE_ATTRIBS:
            ltype_name = self.dxf.get(attr_name)
            if ltype_name is None:
                continue
            try:
                ltype = self.doc.linetypes.get(ltype_name)
                registry.add_entity(ltype)
            except const.DXFTableEntryError:
                pass

        # Note: ACAD arrow head blocks are created automatically at export in set_blk_handle()
        for attr_name in DIM_ARROW_HEAD_ATTRIBS:
            arrow_name = self.dxf.get(attr_name)
            if arrow_name is None:
                continue
            if not ARROWS.is_acad_arrow(arrow_name):
                # user defined arrow head block
                registry.add_block_name(arrow_name)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        assert isinstance(clone, DimStyle)
        super().map_resources(clone, mapping)
        # ezdxf uses names for blocks, linetypes and text style as internal data
        # map text style
        text_style = self.dxf.get(DIM_TEXT_STYLE_ATTR)
        if text_style:
            clone.dxf.dimtxsty = mapping.get_text_style(text_style)
        # map linetypes
        for attr_name in DIM_LINETYPE_ATTRIBS:
            ltype_name = self.dxf.get(attr_name)
            if ltype_name:
                clone.dxf.set(attr_name, mapping.get_linetype(ltype_name))

        # Note: ACAD arrow head blocks are created automatically at export in set_blk_handle()
        for attr_name in DIM_ARROW_HEAD_ATTRIBS:
            arrow_name = self.dxf.get(attr_name)
            if arrow_name is None:
                continue
            if not ARROWS.is_acad_arrow(arrow_name):
                # user defined arrow head block
                arrow_name = mapping.get_block_name(arrow_name)
            clone.dxf.set(attr_name, arrow_name)

    def set_handles(self):
        style = self.dxf.get(DIM_TEXT_STYLE_ATTR)
        if style:
            self.dxf.dimtxsty_handle = self.doc.styles.get(style).dxf.handle

        for attr_name in DIM_ARROW_HEAD_ATTRIBS:
            block_name = self.dxf.get(attr_name)
            if block_name:
                self.set_blk_handle(attr_name + "_handle", block_name)

        for attr_name in DIM_LINETYPE_ATTRIBS:
            get_linetype = self.doc.linetypes.get
            ltype_name = self.dxf.get(attr_name)
            if ltype_name:
                handle = get_linetype(ltype_name).dxf.handle
                self.dxf.set(attr_name + "_handle", handle)

    def discard_handles(self):
        for attr in (
            "dimblk",
            "dimblk1",
            "dimblk2",
            "dimldrblk",
            "dimltype",
            "dimltex1",
            "dimltex2",
            "dimtxsty",
        ):
            self.dxf.discard(attr + "_handle")

    def set_blk_handle(self, attr: str, arrow_name: str) -> None:
        if arrow_name == ARROWS.closed_filled:
            # special arrow, no handle needed (is '0' if set)
            # do not create block by default, this will be done if arrow is used
            # and block record handle is not needed here
            self.dxf.discard(attr)
            return
        assert self.doc is not None, "valid DXF document required"
        blocks = self.doc.blocks
        if ARROWS.is_acad_arrow(arrow_name):
            # create block, because the block record handle is needed here
            block_name = ARROWS.create_block(blocks, arrow_name)
        else:
            block_name = arrow_name

        blk = blocks.get(block_name)
        if blk is not None:
            self.set_dxf_attrib(attr, blk.block_record_handle)
        else:
            raise const.DXFValueError(f'Block "{arrow_name}" does not exist.')

    def get_arrow_block_name(self, name: str) -> str:
        assert self.doc is not None, "valid DXF document required"
        handle = self.get_dxf_attrib(name, None)
        if handle in (None, "0"):
            # unset handle or handle '0' is default closed filled arrow
            return ARROWS.closed_filled
        else:
            block_name = get_block_name_by_handle(handle, self.doc)
            # Returns standard arrow name or the user defined block name:
            return ARROWS.arrow_name(block_name)

    def set_linetypes(self, dimline=None, ext1=None, ext2=None) -> None:
        if self.dxfversion < DXF2007:
            logger.debug("Linetype support requires DXF R2007+.")

        if dimline is not None:
            self.dxf.dimltype = dimline
        if ext1 is not None:
            self.dxf.dimltex1 = ext1
        if ext2 is not None:
            self.dxf.dimltex2 = ext2

    def print_dim_attribs(self) -> None:
        attdef = self.DXFATTRIBS.get
        for name, value in self.dxfattribs().items():
            if name.startswith("dim"):
                print(f"{name} ({attdef(name).code}) = {value}")  # type: ignore

    def copy_to_header(self, doc: Drawing):
        """Copy all dimension style variables to HEADER section of `doc`."""
        attribs = self.dxfattribs()
        header = doc.header
        header["$DIMSTYLE"] = self.dxf.name
        for name, value in attribs.items():
            if name.startswith("dim"):
                header_var = "$" + name.upper()
                try:
                    header[header_var] = value
                except const.DXFKeyError:
                    logger.debug(f"Unsupported header variable: {header_var}.")

    def set_arrows(
        self, blk: str = "", blk1: str = "", blk2: str = "", ldrblk: str = ""
    ) -> None:
        """Set arrows by block names or AutoCAD standard arrow names, set
        DIMTSZ to ``0`` which disables tick.

        Args:
            blk: block/arrow name for both arrows, if DIMSAH is 0
            blk1: block/arrow name for first arrow, if DIMSAH is 1
            blk2: block/arrow name for second arrow, if DIMSAH is 1
            ldrblk: block/arrow name for leader

        """
        self.set_dxf_attrib("dimblk", blk)
        self.set_dxf_attrib("dimblk1", blk1)
        self.set_dxf_attrib("dimblk2", blk2)
        self.set_dxf_attrib("dimldrblk", ldrblk)
        self.set_dxf_attrib("dimtsz", 0)  # use blocks

        # only existing BLOCK definitions allowed
        if self.doc:
            blocks = self.doc.blocks
            for b in (blk, blk1, blk2, ldrblk):
                if ARROWS.is_acad_arrow(b):  # not real blocks
                    ARROWS.create_block(blocks, b)
                    continue
                if b and b not in blocks:
                    raise const.DXFValueError(f'BLOCK "{blk}" does not exist.')

    def set_tick(self, size: float = 1) -> None:
        """Set tick `size`, which also disables arrows, a tick is just an
        oblique stroke as marker.

        Args:
            size: arrow size in drawing units

        """
        self.set_dxf_attrib("dimtsz", size)

    def set_text_align(
        self,
        halign: Optional[str] = None,
        valign: Optional[str] = None,
        vshift: Optional[float] = None,
    ) -> None:
        """Set measurement text alignment, `halign` defines the horizontal
        alignment (requires DXF R2000+), `valign` defines the vertical
        alignment, `above1` and `above2` means above extension line 1 or 2 and
        aligned with extension line.

        Args:
            halign: "left", "right", "center", "above1", "above2",
                requires DXF R2000+
            valign: "above", "center", "below"
            vshift: vertical text shift, if `valign` is "center";
                >0 shift upward,
                <0 shift downwards

        """
        if valign:
            valign = valign.lower()
            self.set_dxf_attrib("dimtad", const.DIMTAD[valign])
            if valign == "center" and vshift is not None:
                self.set_dxf_attrib("dimtvp", vshift)

        if halign:
            self.set_dxf_attrib("dimjust", const.DIMJUST[halign.lower()])

    def set_text_format(
        self,
        prefix: str = "",
        postfix: str = "",
        rnd: Optional[float] = None,
        dec: Optional[int] = None,
        sep: Optional[str] = None,
        leading_zeros: bool = True,
        trailing_zeros: bool = True,
    ):
        """Set dimension text format, like prefix and postfix string, rounding
        rule and number of decimal places.

        Args:
            prefix: Dimension text prefix text as string
            postfix: Dimension text postfix text as string
            rnd: Rounds all dimensioning distances to the specified value, for
                instance, if DIMRND is set to 0.25, all distances round to the
                nearest 0.25 unit. If you set DIMRND to 1.0, all distances round
                to the nearest integer.
            dec: Sets the number of decimal places displayed for the primary
                units of a dimension, requires DXF R2000+
            sep: "." or "," as decimal separator, requires DXF R2000+
            leading_zeros: Suppress leading zeros for decimal dimensions
                if ``False``
            trailing_zeros: Suppress trailing zeros for decimal dimensions
                if ``False``

        """
        if prefix or postfix:
            self.dxf.dimpost = prefix + "<>" + postfix
        if rnd is not None:
            self.dxf.dimrnd = rnd

        # works only with decimal dimensions not inch and feet, US user set dimzin directly
        if leading_zeros is not None or trailing_zeros is not None:
            dimzin = 0
            if leading_zeros is False:
                dimzin = const.DIMZIN_SUPPRESSES_LEADING_ZEROS
            if trailing_zeros is False:
                dimzin += const.DIMZIN_SUPPRESSES_TRAILING_ZEROS
            self.dxf.dimzin = dimzin

        if dec is not None:
            self.dxf.dimdec = dec
        if sep is not None:
            self.dxf.dimdsep = ord(sep)

    def set_dimline_format(
        self,
        color: Optional[int] = None,
        linetype: Optional[str] = None,
        lineweight: Optional[int] = None,
        extension: Optional[float] = None,
        disable1: Optional[bool] = None,
        disable2: Optional[bool] = None,
    ):
        """Set dimension line properties

        Args:
            color: color index
            linetype: linetype as string, requires DXF R2007+
            lineweight: line weight as int, 13 = 0.13mm, 200 = 2.00mm,
                requires DXF R2000+
            extension: extension length
            disable1: ``True`` to suppress first part of dimension line,
                requires DXF R2000+
            disable2: ``True`` to suppress second part of dimension line,
                requires DXF R2000+

        """
        if color is not None:
            self.dxf.dimclrd = color
        if extension is not None:
            self.dxf.dimdle = extension

        if lineweight is not None:
            self.dxf.dimlwd = lineweight
        if disable1 is not None:
            self.dxf.dimsd1 = disable1
        if disable2 is not None:
            self.dxf.dimsd2 = disable2
        if linetype is not None:
            self.dxf.dimltype = linetype

    def set_extline_format(
        self,
        color: Optional[int] = None,
        lineweight: Optional[int] = None,
        extension: Optional[float] = None,
        offset: Optional[float] = None,
        fixed_length: Optional[float] = None,
    ):
        """Set common extension line attributes.

        Args:
            color: color index
            lineweight: line weight as int, 13 = 0.13mm, 200 = 2.00mm
            extension: extension length above dimension line
            offset: offset from measurement point
            fixed_length: set fixed length extension line, length below the
                dimension line

        """
        if color is not None:
            self.dxf.dimclre = color
        if extension is not None:
            self.dxf.dimexe = extension
        if offset is not None:
            self.dxf.dimexo = offset
        if lineweight is not None:
            self.dxf.dimlwe = lineweight
        if fixed_length is not None:
            self.dxf.dimfxlon = 1
            self.dxf.dimfxl = fixed_length

    def set_extline1(self, linetype: Optional[str] = None, disable=False):
        """Set extension line 1 attributes.

        Args:
            linetype: linetype for extension line 1, requires DXF R2007+
            disable: disable extension line 1 if ``True``

        """
        if disable:
            self.dxf.dimse1 = 1
        if linetype is not None:
            self.dxf.dimltex1 = linetype

    def set_extline2(self, linetype: Optional[str] = None, disable=False):
        """Set extension line 2 attributes.

        Args:
            linetype: linetype for extension line 2, requires DXF R2007+
            disable: disable extension line 2 if ``True``

        """
        if disable:
            self.dxf.dimse2 = 1
        if linetype is not None:
            self.dxf.dimltex2 = linetype

    def set_tolerance(
        self,
        upper: float,
        lower: Optional[float] = None,
        hfactor: float = 1.0,
        align: Optional[MTextLineAlignment] = None,
        dec: Optional[int] = None,
        leading_zeros: Optional[bool] = None,
        trailing_zeros: Optional[bool] = None,
    ) -> None:
        """Set tolerance text format, upper and lower value, text height
        factor, number of decimal places or leading and trailing zero
        suppression.

        Args:
            upper: upper tolerance value
            lower: lower tolerance value, if ``None`` same as upper
            hfactor: tolerance text height factor in relation to the dimension
                text height
            align: tolerance text alignment enum :class:`ezdxf.enums.MTextLineAlignment`
                requires DXF R2000+
            dec: Sets the number of decimal places displayed,
                requires DXF R2000+
            leading_zeros: suppress leading zeros for decimal dimensions
                if ``False``, requires DXF R2000+
            trailing_zeros: suppress trailing zeros for decimal dimensions
                if ``False``, requires DXF R2000+

        """
        # Exclusive tolerances mode, disable limits
        self.dxf.dimtol = 1
        self.dxf.dimlim = 0
        self.dxf.dimtp = float(upper)
        if lower is not None:
            self.dxf.dimtm = float(lower)
        else:
            self.dxf.dimtm = float(upper)
        if hfactor is not None:
            self.dxf.dimtfac = float(hfactor)

        # Works only with decimal dimensions not inch and feet, US user set
        # dimzin directly.
        if leading_zeros is not None or trailing_zeros is not None:
            dimtzin = 0
            if leading_zeros is False:
                dimtzin = const.DIMZIN_SUPPRESSES_LEADING_ZEROS
            if trailing_zeros is False:
                dimtzin += const.DIMZIN_SUPPRESSES_TRAILING_ZEROS
            self.dxf.dimtzin = dimtzin

        if align is not None:
            self.dxf.dimtolj = int()
        if dec is not None:
            self.dxf.dimtdec = int(dec)

    def set_limits(
        self,
        upper: float,
        lower: float,
        hfactor: float = 1.0,
        dec: Optional[int] = None,
        leading_zeros: Optional[bool] = None,
        trailing_zeros: Optional[bool] = None,
    ) -> None:
        """Set limits text format, upper and lower limit values, text height
        factor, number of decimal places or leading and trailing zero
        suppression.

        Args:
            upper: upper limit value added to measurement value
            lower: lower limit value subtracted from measurement value
            hfactor: limit text height factor in relation to the dimension
                text height
            dec: Sets the number of decimal places displayed,
                requires DXF R2000+
            leading_zeros: suppress leading zeros for decimal dimensions
                if ``False``, requires DXF R2000+
            trailing_zeros: suppress trailing zeros for decimal dimensions
                if ``False``, requires DXF R2000+

        """
        # Exclusive limits mode, disable tolerances
        self.dxf.dimlim = 1
        self.dxf.dimtol = 0
        self.dxf.dimtp = float(upper)
        self.dxf.dimtm = float(lower)
        self.dxf.dimtfac = float(hfactor)

        # Works only with decimal dimensions not inch and feet, US user set
        # dimzin directly.
        if leading_zeros is not None or trailing_zeros is not None:
            dimtzin = 0
            if leading_zeros is False:
                dimtzin = const.DIMZIN_SUPPRESSES_LEADING_ZEROS
            if trailing_zeros is False:
                dimtzin += const.DIMZIN_SUPPRESSES_TRAILING_ZEROS
            self.dxf.dimtzin = dimtzin
        self.dxf.dimtolj = 0  # set bottom as default
        if dec is not None:
            self.dxf.dimtdec = int(dec)

    def __referenced_blocks__(self) -> Iterable[str]:
        """Support for "ReferencedBlocks" protocol."""
        if self.doc:
            blocks = self.doc.blocks
            for attrib_name in ("dimblk", "dimblk1", "dimblk2", "dimldrblk"):
                name = self.dxf.get(attrib_name, None)
                if name:
                    block = blocks.get(name, None)
                    if block is not None:
                        yield block.block_record.dxf.handle


def get_block_name_by_handle(handle, doc: Drawing, default="") -> str:
    try:
        entry = doc.entitydb[handle]
    except const.DXFKeyError:
        block_name = default
    else:
        block_name = entry.dxf.name
    return block_name
