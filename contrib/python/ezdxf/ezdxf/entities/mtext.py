# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Union,
    Iterable,
    Iterator,
    Optional,
    Callable,
    cast,
)
from typing_extensions import Self
import enum
import math
import logging

from ezdxf.lldxf import const, validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
    VIRTUAL_TAG,
)
from ezdxf.lldxf.const import SUBCLASS_MARKER, DXF2000, DXF2018
from ezdxf.lldxf.types import DXFTag, dxftag
from ezdxf.lldxf.tags import (
    Tags,
    find_begin_and_end_of_encoded_xdata_tags,
    NotFoundException,
)

from ezdxf.math import Vec3, Matrix44, OCS, UCS, NULLVEC, Z_AXIS, X_AXIS, UVec
from ezdxf.math.transformtools import transform_extrusion
from ezdxf.colors import rgb2int, RGB
from ezdxf.tools.text import (
    split_mtext_string,
    escape_dxf_line_endings,
    fast_plain_mtext,
    plain_mtext,
    scale_mtext_inline_commands,
)
from . import factory
from .dxfentity import base_class, SubclassProcessor
from .dxfgfx import DXFGraphic, acdb_entity
from .xdata import XData
from .copy import default_copy

if TYPE_CHECKING:
    from ezdxf.audit import Auditor
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFNamespace, DXFEntity
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.entitydb import EntityDB
    from ezdxf import xref

__all__ = [
    "MText",
    "MTextColumns",
    "ColumnType",
    "acdb_mtext",
    "acdb_mtext_group_codes",
    "export_mtext_content",
]
logger = logging.getLogger("ezdxf")

BG_FILL_MASK = 1 + 2 + 16

acdb_mtext = DefSubclass(
    "AcDbMText",
    {
        # virtual text attribute, the actual content is stored in
        # multiple tags (1, 3, 3, ...)
        "text": DXFAttr(
            VIRTUAL_TAG,
            xtype=XType.callback,
            getter="_get_text",
            setter="_set_text",
        ),
        "insert": DXFAttr(10, xtype=XType.point3d, default=NULLVEC),
        # Nominal (initial) text height
        "char_height": DXFAttr(
            40,
            default=2.5,
            validator=validator.is_greater_zero,
            fixer=RETURN_DEFAULT,
        ),
        # Reference column width
        "width": DXFAttr(41, optional=True),
        # Found in BricsCAD export:
        "defined_height": DXFAttr(46, dxfversion="AC1021"),
        # Attachment point: enum const.MTextEntityAlignment
        # 1 = Top left
        # 2 = Top center
        # 3 = Top right
        # 4 = Middle left
        # 5 = Middle center
        # 6 = Middle right
        # 7 = Bottom left
        # 8 = Bottom center
        # 9 = Bottom right
        "attachment_point": DXFAttr(
            71,
            default=1,
            validator=validator.is_in_integer_range(1, 10),
            fixer=RETURN_DEFAULT,
        ),
        # Flow direction: enum MTextFlowDirection
        # 1 = Left to right
        # 3 = Top to bottom
        # 5 = By style (the flow direction is inherited from the associated
        #     text style)
        "flow_direction": DXFAttr(
            72,
            default=1,
            optional=True,
            validator=validator.is_one_of({1, 3, 5}),
            fixer=RETURN_DEFAULT,
        ),
        # Content text:
        # group code 1: text
        # group code 3: additional text (optional)
        # Text style name:
        "style": DXFAttr(
            7,
            default="Standard",
            optional=True,
            validator=validator.is_valid_table_name,  # do not fix!
        ),
        "extrusion": DXFAttr(
            210,
            xtype=XType.point3d,
            default=Z_AXIS,
            optional=True,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        # x-axis direction vector (in WCS)
        # If rotation and text_direction are present, text_direction wins
        "text_direction": DXFAttr(
            11,
            xtype=XType.point3d,
            default=X_AXIS,
            optional=True,
            validator=validator.is_not_null_vector,
            fixer=RETURN_DEFAULT,
        ),
        # Horizontal width of the characters that make up the mtext entity.
        # This value will always be equal to or less than the value of *width*,
        # (read-only, ignored if supplied)
        "rect_width": DXFAttr(42, optional=True),
        # Vertical height of the mtext entity (read-only, ignored if supplied)
        "rect_height": DXFAttr(43, optional=True),
        # Text rotation in degrees -  Error in DXF reference, which claims radians
        "rotation": DXFAttr(50, default=0, optional=True),
        # Line spacing style (optional): enum const.MTextLineSpacing
        # 1 = At least (taller characters will override)
        # 2 = Exact (taller characters will not override)
        "line_spacing_style": DXFAttr(
            73,
            default=1,
            optional=True,
            validator=validator.is_one_of({1, 2}),
            fixer=RETURN_DEFAULT,
        ),
        # Line spacing factor (optional): Percentage of default (3-on-5) line
        # spacing to be applied. Valid values range from 0.25 to 4.00
        "line_spacing_factor": DXFAttr(
            44,
            default=1,
            optional=True,
            validator=validator.is_in_float_range(0.25, 4.00),
            fixer=validator.fit_into_float_range(0.25, 4.00),
        ),
        # Determines how much border there is around the text.
        # (45) + (90) + (63) all three required, if one of them is used
        "box_fill_scale": DXFAttr(45, dxfversion="AC1021"),
        # background fill type flags: enum const.MTextBackgroundColor
        # 0 = off
        # 1 = color -> (63) < (421) or (431)
        # 2 = drawing window color
        # 3 = use background color (1 & 2)
        # 16 = text frame ODA specification 20.4.46
        # 2021-05-14: text frame only is supported bg_fill = 16,
        # but scaling is always 1.5 and tags 45 + 63 are not present
        "bg_fill": DXFAttr(
            90,
            dxfversion="AC1021",
            validator=validator.is_valid_bitmask(BG_FILL_MASK),
            fixer=validator.fix_bitmask(BG_FILL_MASK),
        ),
        # background fill color as ACI, required even true color is used
        "bg_fill_color": DXFAttr(
            63,
            dxfversion="AC1021",
            validator=validator.is_valid_aci_color,
        ),
        # 420-429? : background fill color as true color value, (63) also required
        # but ignored
        "bg_fill_true_color": DXFAttr(421, dxfversion="AC1021"),
        # 430-439? : background fill color as color name ???, (63) also required
        # but ignored
        "bg_fill_color_name": DXFAttr(431, dxfversion="AC1021"),
        # background fill color transparency - not used by AutoCAD/BricsCAD
        "bg_fill_transparency": DXFAttr(441, dxfversion="AC1021"),
    },
)
acdb_mtext_group_codes = group_code_mapping(acdb_mtext)


# -----------------------------------------------------------------------
# For more information go to docs/source/dxfinternals/entities/mtext.rst
# -----------------------------------------------------------------------

# MTEXT column support:
# MTEXT columns have the same appearance and handling for all DXF versions
# as a single MTEXT entity like in DXF R2018.


class ColumnType(enum.IntEnum):
    NONE = 0
    STATIC = 1
    DYNAMIC = 2


class MTextColumns:
    """The column count is not stored explicit in the columns definition for
    DXF versions R2018+.

    If column_type is DYNAMIC and auto_height is True the column
    count is defined by the content. The exact calculation of the column count
    requires an accurate rendering of the MTEXT content like AutoCAD does!

    If the column count is not defined, ezdxf tries to calculate the column
    count from total_width, width and gutter_width, if these attributes are set
    properly.

    """

    def __init__(self) -> None:
        self.column_type: ColumnType = ColumnType.STATIC
        # The embedded object in R2018 does not store the column count for
        # column type DYNAMIC and auto_height is True!
        # For DXF < R2018 the column count, may not match the count of linked
        # MTEXT entities!
        self.count: int = 1
        self.auto_height: bool = False
        self.reversed_column_flow: bool = False
        self.defined_height: float = 0.0
        self.width: float = 0.0
        self.gutter_width: float = 0.0
        self.total_width: float = 0.0
        self.total_height: float = 0.0
        # Storage for handles of linked MTEXT entities at loading stage:
        self.linked_handles: Optional[list[str]] = None
        # Storage for linked MTEXT entities for DXF versions < R2018:
        self.linked_columns: list[MText] = []
        # R2018+: heights of all columns if auto_height is False
        self.heights: list[float] = []

    def deep_copy(self) -> MTextColumns:
        columns = self.shallow_copy()
        columns.linked_columns = [mtext.copy() for mtext in self.linked_columns]
        return columns

    def shallow_copy(self) -> MTextColumns:
        columns = MTextColumns()
        columns.count = self.count
        columns.column_type = self.column_type
        columns.auto_height = self.auto_height
        columns.reversed_column_flow = self.reversed_column_flow
        columns.defined_height = self.defined_height
        columns.width = self.width
        columns.gutter_width = self.gutter_width
        columns.total_width = self.total_width
        columns.total_height = self.total_height
        columns.linked_columns = list(self.linked_columns)
        columns.heights = list(self.heights)
        return columns

    @classmethod
    def new_static_columns(
        cls, count: int, width: float, gutter_width: float, height: float
    ) -> MTextColumns:
        columns = cls()
        columns.column_type = ColumnType.STATIC
        columns.count = int(count)
        columns.width = float(width)
        columns.gutter_width = float(gutter_width)
        columns.defined_height = float(height)
        columns.update_total_width()
        columns.update_total_height()
        return columns

    @classmethod
    def new_dynamic_auto_height_columns(
        cls, count: int, width: float, gutter_width: float, height: float
    ) -> MTextColumns:
        columns = cls()
        columns.column_type = ColumnType.DYNAMIC
        columns.auto_height = True
        columns.count = int(count)
        columns.width = float(width)
        columns.gutter_width = float(gutter_width)
        columns.defined_height = float(height)
        columns.update_total_width()
        columns.update_total_height()
        return columns

    @classmethod
    def new_dynamic_manual_height_columns(
        cls, width: float, gutter_width: float, heights: Iterable[float]
    ) -> MTextColumns:
        columns = cls()
        columns.column_type = ColumnType.DYNAMIC
        columns.auto_height = False
        columns.width = float(width)
        columns.gutter_width = float(gutter_width)
        columns.defined_height = 0.0
        columns.heights = list(heights)
        columns.count = len(columns.heights)
        columns.update_total_width()
        columns.update_total_height()
        return columns

    def update_total_width(self):
        count = self.count
        if count > 0:
            self.total_width = count * self.width + (count - 1) * self.gutter_width
        else:
            self.total_width = 0.0

    def update_total_height(self):
        if self.has_dynamic_manual_height:
            self.total_height = max(self.heights)
        else:
            self.total_height = self.defined_height

    @property
    def has_dynamic_auto_height(self) -> bool:
        return self.column_type == ColumnType.DYNAMIC and self.auto_height

    @property
    def has_dynamic_manual_height(self) -> bool:
        return self.column_type == ColumnType.DYNAMIC and not self.auto_height

    def link_columns(self, doc: Drawing):
        # DXF R2018+ has no linked MTEXT entities.
        if doc.dxfversion >= DXF2018 or not self.linked_handles:
            return
        db = doc.entitydb
        assert db is not None, "entity database not initialized"
        linked_columns = []
        for handle in self.linked_handles:
            mtext = cast("MText", db.get(handle))
            if mtext:
                linked_columns.append(mtext)
            else:
                logger.debug(f"Linked MTEXT column #{handle} does not exist.")
        self.linked_handles = None
        self.linked_columns = linked_columns

    def transform(self, m: Matrix44, hscale: float = 1, vscale: float = 1):
        self.width *= hscale
        self.gutter_width *= hscale
        self.total_width *= hscale
        self.total_height *= vscale
        self.defined_height *= vscale
        self.heights = [h * vscale for h in self.heights]
        for mtext in self.linked_columns:
            mtext.transform(m)

    def acad_mtext_column_info_xdata(self) -> Tags:
        tags = Tags(
            [
                DXFTag(1000, "ACAD_MTEXT_COLUMN_INFO_BEGIN"),
                DXFTag(1070, 75),
                DXFTag(1070, int(self.column_type)),
                DXFTag(1070, 79),
                DXFTag(1070, int(self.auto_height)),
                DXFTag(1070, 76),
                DXFTag(1070, self.count),
                DXFTag(1070, 78),
                DXFTag(1070, int(self.reversed_column_flow)),
                DXFTag(1070, 48),
                DXFTag(1040, self.width),
                DXFTag(1070, 49),
                DXFTag(1040, self.gutter_width),
            ]
        )
        if self.has_dynamic_manual_height:
            tags.extend([DXFTag(1070, 50), DXFTag(1070, len(self.heights))])
            tags.extend(DXFTag(1040, height) for height in self.heights)
        tags.append(DXFTag(1000, "ACAD_MTEXT_COLUMN_INFO_END"))
        return tags

    def acad_mtext_columns_xdata(self) -> Tags:
        tags = Tags(
            [
                DXFTag(1000, "ACAD_MTEXT_COLUMNS_BEGIN"),
                DXFTag(1070, 47),
                DXFTag(1070, self.count),  # incl. main MTEXT
            ]
        )
        tags.extend(  # writes only (count - 1) handles!
            DXFTag(1005, handle) for handle in self.mtext_handles()
        )
        tags.append(DXFTag(1000, "ACAD_MTEXT_COLUMNS_END"))
        return tags

    def mtext_handles(self) -> list[str]:
        """Returns a list of all linked MTEXT handles."""
        if self.linked_handles:
            return self.linked_handles
        handles = []
        for column in self.linked_columns:
            if column.is_alive:
                handle = column.dxf.handle
                if handle is None:
                    raise const.DXFStructureError("Linked MTEXT column has no handle.")
                handles.append(handle)
            else:
                raise const.DXFStructureError("Linked MTEXT column deleted!")
        return handles

    def acad_mtext_defined_height_xdata(self) -> Tags:
        return Tags(
            [
                DXFTag(1000, "ACAD_MTEXT_DEFINED_HEIGHT_BEGIN"),
                DXFTag(1070, 46),
                DXFTag(1040, self.defined_height),
                DXFTag(1000, "ACAD_MTEXT_DEFINED_HEIGHT_END"),
            ]
        )


def load_columns_from_embedded_object(
    dxf: DXFNamespace, embedded_obj: Tags
) -> MTextColumns:
    columns = MTextColumns()
    insert = dxf.get("insert")  # mandatory attribute, but what if ...
    text_direction = dxf.get("text_direction")  # optional attribute
    reference_column_width = dxf.get("width")  # optional attribute
    for code, value in embedded_obj:
        # Update duplicated attributes if MTEXT attributes are not set:
        if code == 10 and text_direction is None:
            dxf.text_direction = Vec3(value)
            # rotation is not needed anymore:
            dxf.discard("rotation")
        elif code == 11 and insert is None:
            dxf.insert = Vec3(value)
        elif code == 40 and reference_column_width is None:
            dxf.width = value
        elif code == 41:
            # Column height if auto height is True.
            columns.defined_height = value
            # Keep in sync with DXF attribute:
            dxf.defined_height = value
        elif code == 42:
            columns.total_width = value
        elif code == 43:
            columns.total_height = value
        elif code == 44:
            # All columns have the same width.
            columns.width = value
        elif code == 45:
            # All columns have the same gutter width = space between columns.
            columns.gutter_width = value
        elif code == 71:
            columns.column_type = ColumnType(value)
        elif code == 72:  # column height count
            # The column height count can be 0 in some cases (dynamic & auto
            # height) in DXF version R2018+.
            columns.count = value
        elif code == 73:
            columns.auto_height = bool(value)
        elif code == 74:
            columns.reversed_column_flow = bool(value)
        elif code == 46:  # column heights
            # The last column height is 0; takes the rest?
            columns.heights.append(value)

    # The column count is not defined explicit:
    if columns.count == 0:
        if columns.heights:  # very unlikely
            columns.count = len(columns.heights)
        elif columns.total_width > 0:
            # calculate column count from total_width
            g = columns.gutter_width
            wg = abs(columns.width + g)
            if wg > 1e-6:
                columns.count = int(round((columns.total_width + g) / wg))
    return columns


def load_mtext_column_info(tags: Tags) -> Optional[MTextColumns]:
    try:  # has column info?
        start, end = find_begin_and_end_of_encoded_xdata_tags(
            "ACAD_MTEXT_COLUMN_INFO", tags
        )
    except NotFoundException:
        return None
    columns = MTextColumns()
    height_count = 0
    group_code = None
    for code, value in tags[start + 1 : end]:
        if height_count:
            if code == 1040:
                columns.heights.append(value)
                height_count -= 1
                continue
            else:  # error
                logger.error("missing column heights in MTEXT entity")
                height_count = 0

        if group_code is None:
            group_code = value
            continue

        if group_code == 75:
            columns.column_type = ColumnType(value)
        elif group_code == 79:
            columns.auto_height = bool(value)
        elif group_code == 76:
            # column count, may not match the count of linked MTEXT entities!
            columns.count = int(value)
        elif group_code == 78:
            columns.reversed_column_flow = bool(value)
        elif group_code == 48:
            columns.width = value
        elif group_code == 49:
            columns.gutter_width = value
        elif group_code == 50:
            height_count = int(value)
        group_code = None
    return columns


def load_mtext_linked_column_handles(tags: Tags) -> list[str]:
    handles: list[str] = []
    try:
        start, end = find_begin_and_end_of_encoded_xdata_tags(
            "ACAD_MTEXT_COLUMNS", tags
        )
    except NotFoundException:
        return handles
    for code, value in tags[start:end]:
        if code == 1005:
            handles.append(value)
    return handles


def load_mtext_defined_height(tags: Tags) -> float:
    # The defined height stored in the linked MTEXT entities, is not required:
    #
    # If all columns have the same height (static & dynamic auto height), the
    # "defined_height" is stored in the main MTEXT, but the linked MTEXT entities
    # also have a "ACAD_MTEXT_DEFINED_HEIGHT" group in the ACAD section of XDATA.
    #
    # If the columns have different heights (dynamic manual height), these
    # height values are only stored in the main MTEXT. The linked MTEXT
    # entities do not have an ACAD section at all.

    height = 0.0
    try:
        start, end = find_begin_and_end_of_encoded_xdata_tags(
            "ACAD_MTEXT_DEFINED_HEIGHT", tags
        )
    except NotFoundException:
        return height

    for code, value in tags[start:end]:
        if code == 1040:
            height = value
    return height


def load_columns_from_xdata(dxf: DXFNamespace, xdata: XData) -> Optional[MTextColumns]:
    # The ACAD section in XDATA of the main MTEXT entity stores all column
    # related information:
    if "ACAD" in xdata:
        acad = xdata.get("ACAD")
    else:
        return None

    name = f"MTEXT(#{dxf.get('handle')})"
    try:
        columns = load_mtext_column_info(acad)
    except const.DXFStructureError:
        logger.error(f"Invalid ACAD_MTEXT_COLUMN_INFO in {name}")
        return None

    if columns is None:  # no columns defined
        return None

    try:
        columns.linked_handles = load_mtext_linked_column_handles(acad)
    except const.DXFStructureError:
        logger.error(f"Invalid ACAD_MTEXT_COLUMNS in {name}")

    columns.update_total_width()
    if columns.heights:  # dynamic columns, manual heights
        # This is correct even if the last column is the tallest, which height
        # is not known. The height of last column is always stored as 0.
        columns.total_height = max(columns.heights)
    else:  # all columns have the same "defined" height
        try:
            columns.defined_height = load_mtext_defined_height(acad)
        except const.DXFStructureError:
            logger.error(f"Invalid ACAD_MTEXT_DEFINED_HEIGHT in {name}")
        columns.total_height = columns.defined_height

    return columns


def extract_mtext_text_frame_handles(xdata: XData) -> list[str]:
    # Stores information about the text frame until DXF R2018.
    # Newer CAD applications do not need that information nor the separated
    # LWPOLYLINE as text frame entity.
    handles: list[str] = []
    if "ACAD" in xdata:
        acad = xdata.get("ACAD")
    else:
        return handles

    try:
        start, end = find_begin_and_end_of_encoded_xdata_tags(
            "ACAD_MTEXT_TEXT_BORDERS", acad
        )
    except NotFoundException:
        return handles

    for code, value in acad[start:end]:
        # multiple handles to a LWPOLYLINE entity could be present:
        if code == 1005:
            handles.append(value)

    # remove MTEXT_TEXT_BORDERS data
    del acad[start:end]
    if len(acad) < 2:
        xdata.discard("ACAD")
    return handles


@factory.register_entity
class MText(DXFGraphic):
    """DXF MTEXT entity"""

    DXFTYPE = "MTEXT"
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_mtext)
    MIN_DXF_VERSION_FOR_EXPORT = DXF2000

    def __init__(self) -> None:
        super().__init__()
        self.text: str = ""
        # Linked MText columns do not have a MTextColumns() object!
        self._columns: Optional[MTextColumns] = None

    def _get_text(self):
        """Getter for virtual Mtext.dxf.text attribute.

        The MText content is stored in multiple tags (1, 3, 3, ...) and cannot
        be supported as a simple DXF tag. The virtual MText.dxf.text attribute
        adds compatibility to other text based entities: TEXT, ATTRIB, ATTDEF

        """
        return self.text

    def _set_text(self, value):
        """Setter for virtual Mtext.dxf.text attribute."""
        self.text = str(value)

    @property
    def columns(self) -> Optional[MTextColumns]:
        """Returns a copy of the column configuration."""
        # The column configuration is deliberately not editable.
        # Can't prevent access to _columns, but you are on your own if do this!
        return self._columns.shallow_copy() if self._columns else None

    @property
    def has_columns(self) -> bool:
        return self._columns is not None

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        assert isinstance(entity, MText)
        entity.text = self.text
        if self.has_columns:
            # copies also the linked MTEXT column entities!
            entity._columns = self._columns.deep_copy()  # type: ignore

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            tags = processor.subclass_by_index(2)
            if tags:
                tags = Tags(self.load_mtext_content(tags))
                processor.fast_load_dxfattribs(
                    dxf, acdb_mtext_group_codes, subclass=tags, recover=True
                )
                if processor.embedded_objects:
                    obj = processor.embedded_objects[0]
                    self._columns = load_columns_from_embedded_object(dxf, obj)
                elif self.xdata:
                    self._columns = load_columns_from_xdata(dxf, self.xdata)
            else:
                raise const.DXFStructureError(
                    f"missing 'AcDbMText' subclass in MTEXT(#{dxf.handle})"
                )
        return dxf

    def post_load_hook(self, doc: Drawing) -> Optional[Callable]:
        def destroy_text_frame_entity():
            entitydb = doc.entitydb
            if entitydb:
                for handle in extract_mtext_text_frame_handles(self.xdata):
                    text_frame = entitydb.get(handle)
                    if text_frame:
                        text_frame.destroy()

        def unlink_mtext_columns_from_layout():
            """Unlinked MTEXT entities from layout entity space."""
            layout = self.get_layout()
            if layout is not None:
                for mtext in self._columns.linked_columns:
                    layout.unlink_entity(mtext)
            else:
                for mtext in self._columns.linked_columns:
                    mtext.dxf.owner = None

        super().post_load_hook(doc)
        if self.xdata:
            destroy_text_frame_entity()

        if self.has_columns:
            # Link columns, one MTEXT entity for each column, to the main MTEXT
            # entity (DXF version <R2018).
            self._columns.link_columns(doc)  # type: ignore
            return unlink_mtext_columns_from_layout
        return None

    def preprocess_export(self, tagwriter: AbstractTagWriter) -> bool:
        """Pre requirement check and pre-processing for export.

        Returns False if MTEXT should not be exported at all.

        (internal API)
        """
        columns = self._columns
        if columns and tagwriter.dxfversion < const.DXF2018:
            if columns.count != len(columns.linked_columns) + 1:
                logger.debug(f"{str(self)}: column count does not match linked columns")
                # just log for debugging, because AutoCAD accept this!
            if not all(column.is_alive for column in columns.linked_columns):
                logger.debug(f"{str(self)}: contains destroyed linked columns")
                return False
            self.sync_common_attribs_of_linked_columns()
        return True

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        super().export_dxf(tagwriter)
        # Linked MTEXT entities are not stored in the layout entity space!
        if self.has_columns and tagwriter.dxfversion < const.DXF2018:
            self.export_linked_entities(tagwriter)

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        """Export entity specific data as DXF tags."""
        super().export_entity(tagwriter)
        tagwriter.write_tag2(SUBCLASS_MARKER, acdb_mtext.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "insert",
                "char_height",
                "width",
                "defined_height",
                "attachment_point",
                "flow_direction",
            ],
        )
        export_mtext_content(self.text, tagwriter)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "style",
                "extrusion",
                "text_direction",
                "rect_width",
                "rect_height",
                "rotation",
                "line_spacing_style",
                "line_spacing_factor",
                "box_fill_scale",
                "bg_fill",
                "bg_fill_color",
                "bg_fill_true_color",
                "bg_fill_color_name",
                "bg_fill_transparency",
            ],
        )
        columns = self._columns
        if columns is None or columns.column_type == ColumnType.NONE:
            return

        if tagwriter.dxfversion >= DXF2018:
            self.export_embedded_object(tagwriter)
        else:
            self.set_column_xdata()
            self.set_linked_columns_xdata()

    def load_mtext_content(self, tags: Tags) -> Iterator[DXFTag]:
        tail = ""
        parts = []
        for tag in tags:
            if tag.code == 1:
                tail = tag.value
            elif tag.code == 3:
                parts.append(tag.value)
            else:
                yield tag
        parts.append(tail)
        self.text = escape_dxf_line_endings("".join(parts))

    def export_embedded_object(self, tagwriter: AbstractTagWriter):
        dxf = self.dxf
        cols = self._columns
        assert cols is not None

        tagwriter.write_tag2(101, "Embedded Object")
        tagwriter.write_tag2(70, 1)  # unknown meaning
        tagwriter.write_tag(dxftag(10, dxf.text_direction))
        tagwriter.write_tag(dxftag(11, dxf.insert))
        tagwriter.write_tag2(40, dxf.width)  # repeated reference column width
        tagwriter.write_tag2(41, cols.defined_height)
        tagwriter.write_tag2(42, cols.total_width)
        tagwriter.write_tag2(43, cols.total_height)
        tagwriter.write_tag2(71, int(cols.column_type))

        if cols.has_dynamic_auto_height:
            count = 0
        else:
            count = cols.count
        tagwriter.write_tag2(72, count)

        tagwriter.write_tag2(44, cols.width)
        tagwriter.write_tag2(45, cols.gutter_width)
        tagwriter.write_tag2(73, int(cols.auto_height))
        tagwriter.write_tag2(74, int(cols.reversed_column_flow))
        for height in cols.heights:
            tagwriter.write_tag2(46, height)

    def export_linked_entities(self, tagwriter: AbstractTagWriter):
        for mtext in self._columns.linked_columns:  # type: ignore
            if mtext.dxf.handle is None:
                raise const.DXFStructureError("Linked MTEXT column has no handle.")
            # Export linked columns as separated DXF entities:
            mtext.export_dxf(tagwriter)

    def sync_common_attribs_of_linked_columns(self):
        common_attribs = self.dxfattribs(
            drop={"handle", "insert", "rect_width", "rect_height"}
        )
        for mtext in self._columns.linked_columns:
            mtext.update_dxf_attribs(common_attribs)

    def set_column_xdata(self):
        if self.xdata is None:
            self.xdata = XData()
        cols = self._columns
        acad = cols.acad_mtext_column_info_xdata()
        acad.extend(cols.acad_mtext_columns_xdata())
        if not cols.has_dynamic_manual_height:
            acad.extend(cols.acad_mtext_defined_height_xdata())

        xdata = self.xdata
        # Replace existing column data and therefore also removes
        # ACAD_MTEXT_TEXT_BORDERS information!
        xdata.discard("ACAD")
        xdata.add("ACAD", acad)

    def set_linked_columns_xdata(self):
        cols = self._columns
        for column in cols.linked_columns:
            column.discard_xdata("ACAD")
        if not cols.has_dynamic_manual_height:
            tags = cols.acad_mtext_defined_height_xdata()
            for column in cols.linked_columns:
                column.set_xdata("ACAD", tags)

    def get_rotation(self) -> float:
        """Returns the text rotation in degrees."""
        if self.dxf.hasattr("text_direction"):
            vector = self.dxf.text_direction
            radians = math.atan2(vector[1], vector[0])  # ignores z-axis
            rotation = math.degrees(radians)
        else:
            rotation = self.dxf.get("rotation", 0)
        return rotation

    def set_rotation(self, angle: float) -> MText:
        """Sets the attribute :attr:`rotation` to `angle` (in degrees) and deletes
        :attr:`dxf.text_direction` if present.
        """
        # text_direction has higher priority than rotation, therefore delete it
        self.dxf.discard("text_direction")
        self.dxf.rotation = angle
        return self  # fluent interface

    def set_location(
        self,
        insert: UVec,
        rotation: Optional[float] = None,
        attachment_point: Optional[int] = None,
    ) -> MText:
        """Sets the attributes :attr:`dxf.insert`, :attr:`dxf.rotation` and
        :attr:`dxf.attachment_point`, ``None`` for :attr:`dxf.rotation` or
        :attr:`dxf.attachment_point` preserves the existing value.

        """
        self.dxf.insert = Vec3(insert)
        if rotation is not None:
            self.set_rotation(rotation)
        if attachment_point is not None:
            self.dxf.attachment_point = attachment_point
        return self  # fluent interface

    def set_bg_color(
        self,
        color: Union[int, str, RGB, None],
        scale: float = 1.5,
        text_frame=False,
    ):
        """Sets the background color as :ref:`ACI` value, as name string or as
        (r, g, b) tuple.

        Use the special color name ``canvas``, to set the background color to the canvas
        background color.  Remove the background filling by setting argument `color` to
        ``None``.

        Args:
            color: color as :ref:`ACI`, string, (r, g, b) tuple or ``None``
            scale: determines how much border there is around the text, the
                value is based on the text height, and should be in the range
                of [1, 5], where 1 fits exact the MText entity.
            text_frame: draw a text frame in text color if ``True``

        """
        if 1 <= scale <= 5:
            self.dxf.box_fill_scale = scale
        else:
            raise ValueError("argument scale has to be in range from 1 to 5.")

        text_frame = const.MTEXT_TEXT_FRAME if text_frame else 0
        if color is None:
            self.dxf.discard("bg_fill")
            self.dxf.discard("box_fill_scale")
            self.dxf.discard("bg_fill_color")
            self.dxf.discard("bg_fill_true_color")
            self.dxf.discard("bg_fill_color_name")
            if text_frame:
                # special case, text frame only with scaling factor = 1.5
                self.dxf.bg_fill = 16
        elif color == "canvas":  # special case for use background color
            self.dxf.bg_fill = const.MTEXT_BG_CANVAS_COLOR | text_frame
            self.dxf.bg_fill_color = 0  # required but ignored
        else:
            self.dxf.bg_fill = const.MTEXT_BG_COLOR | text_frame
            if isinstance(color, int):
                self.dxf.bg_fill_color = color
            elif isinstance(color, str):
                self.dxf.bg_fill_color = 0  # required but ignored
                self.dxf.bg_fill_color_name = color
            elif isinstance(color, tuple):
                self.dxf.bg_fill_color = 0  # required but ignored
                self.dxf.bg_fill_true_color = rgb2int(color)
        return self  # fluent interface

    def __iadd__(self, text: str) -> MText:
        """Append `text` to existing content (:attr:`text` attribute)."""
        self.text += text
        return self

    append = __iadd__

    def get_text_direction(self) -> Vec3:
        """Returns the horizontal text direction as :class:`~ezdxf.math.Vec3`
        object, even if only the text rotation is defined.

        """
        dxf = self.dxf
        # "text_direction" has higher priority than "rotation"
        if dxf.hasattr("text_direction"):
            return dxf.text_direction
        if dxf.hasattr("rotation"):
            # MTEXT is not an OCS entity, but I don't know how else to convert
            # a rotation angle for an entity just defined by an extrusion vector.
            # It's correct for the most common case: extrusion=(0, 0, 1)
            return OCS(dxf.extrusion).to_wcs(Vec3.from_deg_angle(dxf.rotation))
        return X_AXIS

    def convert_rotation_to_text_direction(self):
        """Convert text rotation into text direction and discard text rotation."""
        dxf = self.dxf
        if dxf.hasattr("rotation"):
            if not dxf.hasattr("text_direction"):
                dxf.text_direction = self.get_text_direction()
            dxf.discard("rotation")

    def ucs(self) -> UCS:
        """Returns the :class:`~ezdxf.math.UCS` of the :class:`MText` entity,
        defined by the insert location (origin), the text direction or rotation
        (x-axis) and the extrusion vector (z-axis).

        """
        dxf = self.dxf
        return UCS(
            origin=dxf.insert,
            ux=self.get_text_direction(),
            uz=dxf.extrusion,
        )

    def transform(self, m: Matrix44) -> MText:
        """Transform the MTEXT entity by transformation matrix `m` inplace."""
        dxf = self.dxf
        old_extrusion = Vec3(dxf.extrusion)
        new_extrusion, _ = transform_extrusion(old_extrusion, m)
        self.convert_rotation_to_text_direction()

        old_text_direction = Vec3(dxf.text_direction)
        new_text_direction = m.transform_direction(old_text_direction)

        old_vertical_direction = old_extrusion.cross(old_text_direction)
        old_char_height = float(dxf.char_height)
        old_char_height_vec = old_vertical_direction.normalize(old_char_height)
        new_char_height_vec = m.transform_direction(old_char_height_vec)
        oblique = new_text_direction.angle_between(new_char_height_vec)
        new_char_height = new_char_height_vec.magnitude * math.sin(oblique)
        dxf.char_height = new_char_height
        if (
            not math.isclose(old_char_height, new_char_height)
            and abs(old_char_height) > 1e-12
        ):
            factor = new_char_height / old_char_height
            # Column content is transformed by the sub-entities itself!
            self.text = scale_mtext_inline_commands(self.text, factor)

        if dxf.hasattr("width"):
            width_vec = old_text_direction.normalize(dxf.width)
            dxf.width = m.transform_direction(width_vec).magnitude

        dxf.insert = m.transform(dxf.insert)
        dxf.text_direction = new_text_direction
        dxf.extrusion = new_extrusion
        if self.has_columns:
            hscale = m.transform_direction(old_text_direction.normalize()).magnitude
            vscale = m.transform_direction(old_vertical_direction.normalize()).magnitude
            self._columns.transform(m, hscale, vscale)  # type: ignore
        self.post_transform(m)
        return self

    def plain_text(self, split=False, fast=True) -> Union[list[str], str]:
        """Returns the text content without inline formatting codes.

        The "fast" mode is accurate if the DXF content was created by
        reliable (and newer) CAD applications like AutoCAD or BricsCAD.
        The "accurate" mode is for some rare cases where the content was
        created by older CAD applications or unreliable DXF libraries and CAD
        applications.

        Args:
            split: split content text at line breaks if ``True`` and
                returns a list of strings without line endings
            fast: uses the "fast" mode to extract the plain MTEXT content if
                ``True`` or the "accurate" mode if set to ``False``

        """
        if fast:
            return fast_plain_mtext(self.text, split=split)
        else:
            return plain_mtext(self.text, split=split)

    def all_columns_plain_text(self, split=False) -> Union[list[str], str]:
        """Returns the text content of all columns without inline formatting
        codes.

        Args:
            split: split content text at line breaks if ``True`` and
                returns a list of strings without line endings

        """

        def merged_content():
            content = [fast_plain_mtext(self.text, split=False)]
            if self.has_columns:
                for c in self._columns.linked_columns:
                    content.append(c.plain_text(split=False))
            return "".join(content)

        def split_content():
            content = fast_plain_mtext(self.text, split=True)
            if self.has_columns:
                if content and content[-1] == "":
                    content.pop()
                for c in self._columns.linked_columns:
                    content.extend(c.plain_text(split=True))
                    if content and content[-1] == "":
                        content.pop()
            return content

        if split:
            return split_content()
        else:
            return merged_content()

    def all_columns_raw_content(self) -> str:
        """Returns the text content of all columns as a single string
        including the inline formatting codes.

        """
        content = [self.text]
        if self.has_columns:
            for column in self._columns.linked_columns:  # type: ignore
                content.append(column.text)
        return "".join(content)

    def audit(self, auditor: Auditor):
        """Validity check."""
        if not self.is_alive:
            return
        if self.dxf.owner is not None:
            # Kills linked columns, because owner (None) does not exist!
            super().audit(auditor)
        else:  # linked columns: owner is None
            # TODO: special audit for linked columns
            pass
        auditor.check_text_style(self)
        # TODO: audit column structure

    def destroy(self) -> None:
        if not self.is_alive:
            return

        if self.has_columns:
            for column in self._columns.linked_columns:  # type: ignore
                column.destroy()

        del self._columns
        super().destroy()

    # Linked MTEXT columns are not the same structure as
    # POLYLINE & INSERT with sub-entities and SEQEND :(
    def add_sub_entities_to_entitydb(self, db: EntityDB) -> None:
        """Add linked columns (MTEXT) entities to entity database `db`,
        called from EntityDB. (internal API)

        """
        if self.is_alive and self._columns:
            doc = self.doc
            for column in self._columns.linked_columns:
                if column.is_alive and column.is_virtual:
                    column.doc = doc
                    db.add(column)

    def process_sub_entities(self, func: Callable[[DXFEntity], None]):
        """Call `func` for linked columns. (internal API)"""
        if self.is_alive and self._columns:
            for entity in self._columns.linked_columns:
                if entity.is_alive:
                    func(entity)

    def setup_columns(self, columns: MTextColumns, linked: bool = False) -> None:
        assert columns.column_type != ColumnType.NONE
        assert columns.count > 0, "one or more columns required"
        assert columns.width > 0, "column width has to be > 0"
        assert columns.gutter_width >= 0, "gutter width has to be >= 0"

        if self.has_columns:
            raise const.DXFStructureError("Column setup already exist.")
        self._columns = columns
        self.dxf.width = columns.width
        self.dxf.defined_height = columns.defined_height
        if columns.total_height < 1e-6:
            columns.total_height = columns.defined_height
        if columns.total_width < 1e-6:
            columns.update_total_width()
        if linked:
            self._create_linked_columns()

    def _create_linked_columns(self) -> None:
        """Create linked MTEXT columns for DXF versions before R2018."""
        # creates virtual MTEXT entities
        dxf = self.dxf
        attribs = self.dxfattribs(drop={"handle", "owner"})
        doc = self.doc
        cols = self._columns
        assert cols is not None

        insert = dxf.get("insert", Vec3())
        default_direction = Vec3.from_deg_angle(dxf.get("rotation", 0))
        text_direction = Vec3(dxf.get("text_direction", default_direction))
        offset = text_direction.normalize(cols.width + cols.gutter_width)
        linked_columns = cols.linked_columns
        for _ in range(cols.count - 1):
            insert += offset
            column = MText.new(dxfattribs=attribs, doc=doc)
            column.dxf.insert = insert
            linked_columns.append(column)

    def remove_dependencies(self, other: Optional[Drawing] = None) -> None:
        if not self.is_alive:
            return

        super().remove_dependencies()
        style_exist = bool(other) and self.dxf.style in other.styles  # type: ignore
        if not style_exist:
            self.dxf.style = "Standard"

        if self.has_columns:
            for column in self._columns.linked_columns:  # type: ignore
                column.remove_dependencies(other)

    def ocs(self) -> OCS:
        # WCS entity which supports the "extrusion" attribute in a
        # different way!
        return OCS()

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        super().register_resources(registry)
        if self.dxf.hasattr("style"):
            registry.add_text_style(self.dxf.style)
        if self._columns:
            for mtext in self._columns.linked_columns:
                mtext.register_resources(registry)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        assert isinstance(clone, MText)
        super().map_resources(clone, mapping)

        if clone.dxf.hasattr("style"):
            clone.dxf.style = mapping.get_text_style(clone.dxf.style)
        if self._columns and clone._columns:
            for col_self, col_clone in zip(
                self._columns.linked_columns, clone._columns.linked_columns
            ):
                col_self.map_resources(col_clone, mapping)


def export_mtext_content(text, tagwriter: AbstractTagWriter) -> None:
    txt = escape_dxf_line_endings(text)
    str_chunks = split_mtext_string(txt, size=250)
    if len(str_chunks) == 0:
        str_chunks.append("")
    while len(str_chunks) > 1:
        tagwriter.write_tag2(3, str_chunks.pop(0))
    tagwriter.write_tag2(1, str_chunks[0])
