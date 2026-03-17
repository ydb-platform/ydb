# Copyright (c) 2021, Manfred Moitzi
# License: MIT License
# mypy: ignore_errors=True
from __future__ import annotations
from typing import Any, Optional
import textwrap
from ezdxf.lldxf.types import (
    render_tag,
    DXFVertex,
    GROUP_MARKERS,
    POINTER_CODES,
)
from ezdxf.addons.xqt import QModelIndex, QAbstractTableModel, Qt, QtWidgets
from ezdxf.addons.xqt import QStandardItemModel, QStandardItem, QColor
from .tags import compile_tags, Tags

__all__ = [
    "DXFTagsModel",
    "DXFStructureModel",
    "EntityContainer",
    "Entity",
    "DXFTagsRole",
]

DXFTagsRole = Qt.UserRole + 1  # type: ignore


def name_fmt(handle, name: str) -> str:
    if handle is None:
        return name
    else:
        return f"<{handle}> {name}"


HEADER_LABELS = ["Group Code", "Data Type", "Content", "4", "5"]


def calc_line_numbers(start: int, tags: Tags) -> list[int]:
    numbers = [start]
    index = start
    for tag in tags:
        if isinstance(tag, DXFVertex):
            index += len(tag.value) * 2
        else:
            index += 2
        numbers.append(index)
    return numbers


class DXFTagsModel(QAbstractTableModel):
    def __init__(
        self, tags: Tags, start_line_number: int = 1, valid_handles=None
    ):
        super().__init__()
        self._tags = compile_tags(tags)
        self._line_numbers = calc_line_numbers(start_line_number, self._tags)
        self._valid_handles = valid_handles or set()
        palette = QtWidgets.QApplication.palette()
        self._group_marker_color = palette.highlight().color()

    def data(self, index: QModelIndex, role: int = ...) -> Any:  # type: ignore
        def is_invalid_handle(tag):
            if (
                tag.code in POINTER_CODES
                and not tag.value.upper() in self._valid_handles
            ):
                return True
            return False

        if role == Qt.DisplayRole:
            tag = self._tags[index.row()]
            return render_tag(tag, index.column())
        elif role == Qt.ForegroundRole:
            tag = self._tags[index.row()]
            if tag.code in GROUP_MARKERS:
                return self._group_marker_color
            elif is_invalid_handle(tag):
                return QColor("red")
        elif role == DXFTagsRole:
            return self._tags[index.row()]
        elif role == Qt.ToolTipRole:
            code, value = self._tags[index.row()]
            if index.column() == 0:  # group code column
                return GROUP_CODE_TOOLTIPS_DICT.get(code)

            code, value = self._tags[index.row()]
            if code in POINTER_CODES:
                if value.upper() in self._valid_handles:
                    return f"Double click to go to the referenced entity"
                else:
                    return f"Handle does not exist"
            elif code == 0:
                return f"Double click to go to the DXF reference provided by Autodesk"

    def headerData(
        self, section: int, orientation: Qt.Orientation, role: int = ...  # type: ignore
    ) -> Any:
        if orientation == Qt.Horizontal:
            if role == Qt.DisplayRole:
                return HEADER_LABELS[section]
            elif role == Qt.TextAlignmentRole:
                return Qt.AlignLeft
        elif orientation == Qt.Vertical:
            if role == Qt.DisplayRole:
                return self._line_numbers[section]
            elif role == Qt.ToolTipRole:
                return "Line number in DXF file"

    def rowCount(self, parent: QModelIndex = ...) -> int:  # type: ignore
        return len(self._tags)

    def columnCount(self, parent: QModelIndex = ...) -> int:  # type: ignore
        return 3

    def compiled_tags(self) -> Tags:
        """Returns the compiled tags. Only points codes are compiled, group
        code 10, ...
        """
        return self._tags

    def line_number(self, row: int) -> int:
        """Return the DXF file line number of the widget-row."""
        try:
            return self._line_numbers[row]
        except IndexError:
            return 0


class EntityContainer(QStandardItem):
    def __init__(self, name: str, entities: list[Tags]):
        super().__init__()
        self.setEditable(False)
        self.setText(name + f" ({len(entities)})")
        self.setup_content(entities)

    def setup_content(self, entities):
        self.appendRows([Entity(e) for e in entities])


class Classes(EntityContainer):
    def setup_content(self, entities):
        self.appendRows([Class(e) for e in entities])


class AcDsData(EntityContainer):
    def setup_content(self, entities):
        self.appendRows([AcDsEntry(e) for e in entities])


class NamedEntityContainer(EntityContainer):
    def setup_content(self, entities):
        self.appendRows([NamedEntity(e) for e in entities])


class Tables(EntityContainer):
    def setup_content(self, entities):
        container = []
        name = ""
        for e in entities:
            container.append(e)
            dxftype = e.dxftype()
            if dxftype == "TABLE":
                try:
                    handle = e.get_handle()
                except ValueError:
                    handle = None
                name = e.get_first_value(2, default="UNDEFINED")
                name = name_fmt(handle, name)
            elif dxftype == "ENDTAB":
                if container:
                    container.pop()  # remove ENDTAB
                    self.appendRow(NamedEntityContainer(name, container))
                container.clear()


class Blocks(EntityContainer):
    def setup_content(self, entities):
        container = []
        name = "UNDEFINED"
        for e in entities:
            container.append(e)
            dxftype = e.dxftype()
            if dxftype == "BLOCK":
                try:
                    handle = e.get_handle()
                except ValueError:
                    handle = None
                name = e.get_first_value(2, default="UNDEFINED")
                name = name_fmt(handle, name)
            elif dxftype == "ENDBLK":
                if container:
                    self.appendRow(EntityContainer(name, container))
                container.clear()


def get_section_name(section: list[Tags]) -> str:
    if len(section) > 0:
        header = section[0]
        if len(header) > 1 and header[0].code == 0 and header[1].code == 2:
            return header[1].value
    return "INVALID SECTION HEADER!"


class Entity(QStandardItem):
    def __init__(self, tags: Tags):
        super().__init__()
        self.setEditable(False)
        self._tags = tags
        self._handle: Optional[str]
        try:
            self._handle = tags.get_handle()
        except ValueError:
            self._handle = None
        self.setText(self.entity_name())

    def entity_name(self):
        name = "INVALID ENTITY!"
        tags = self._tags
        if tags and tags[0].code == 0:
            name = name_fmt(self._handle, tags[0].value)
        return name

    def data(self, role: int = ...) -> Any:  # type: ignore
        if role == DXFTagsRole:
            return self._tags
        else:
            return super().data(role)


class Header(Entity):
    def entity_name(self):
        return "HEADER"


class ThumbnailImage(Entity):
    def entity_name(self):
        return "THUMBNAILIMAGE"


class NamedEntity(Entity):
    def entity_name(self):
        name = self._tags.get_first_value(2, "<noname>")
        return name_fmt(str(self._handle), name)


class Class(Entity):
    def entity_name(self):
        tags = self._tags
        name = "INVALID CLASS!"
        if len(tags) > 1 and tags[0].code == 0 and tags[1].code == 1:
            name = tags[1].value
        return name


class AcDsEntry(Entity):
    def entity_name(self):
        return self._tags[0].value


class DXFStructureModel(QStandardItemModel):
    def __init__(self, filename: str, doc):
        super().__init__()
        root = QStandardItem(filename)
        root.setEditable(False)
        self.appendRow(root)
        row: Any
        for section in doc.sections.values():
            name = get_section_name(section)
            if name == "HEADER":
                row = Header(section[0])
            elif name == "THUMBNAILIMAGE":
                row = ThumbnailImage(section[0])
            elif name == "CLASSES":
                row = Classes(name, section[1:])
            elif name == "TABLES":
                row = Tables(name, section[1:])
            elif name == "BLOCKS":
                row = Blocks(name, section[1:])
            elif name == "ACDSDATA":
                row = AcDsData(name, section[1:])
            else:
                row = EntityContainer(name, section[1:])
            root.appendRow(row)

    def index_of_entity(self, entity: Tags) -> QModelIndex:
        root = self.item(0, 0)
        index = find_index(root, entity)
        if index is None:
            return root.index()
        else:
            return index


def find_index(item: QStandardItem, entity: Tags) -> Optional[QModelIndex]:
    def _find(sub_item: QStandardItem):
        for index in range(sub_item.rowCount()):
            child = sub_item.child(index, 0)
            tags = child.data(DXFTagsRole)
            if tags and tags is entity:
                return child.index()
            if child.rowCount() > 0:
                index2 = _find(child)
                if index2 is not None:
                    return index2
        return None

    return _find(item)


GROUP_CODE_TOOLTIPS = [
    (0, "Text string indicating the entity type (fixed)"),
    (1, "Primary text value for an entity"),
    (2, "Name (attribute tag, block name, and so on)"),
    ((3, 4), "Other text or name values"),
    (5, "Entity handle; text string of up to 16 hexadecimal digits (fixed)"),
    (6, "Linetype name (fixed)"),
    (7, "Text style name (fixed)"),
    (8, "Layer name (fixed)"),
    (
        9,
        "DXF: variable name identifier (used only in HEADER section of the DXF file)",
    ),
    (
        10,
        "Primary point; this is the start point of a line or text entity, center "
        "of a circle, and so on DXF: X value of the primary point (followed by Y "
        "and Z value codes 20 and 30) APP: 3D point (list of three reals)",
    ),
    (
        (11, 18),
        "Other points DXF: X value of other points (followed by Y value codes "
        "21-28 and Z value codes 31-38) APP: 3D point (list of three reals)",
    ),
    (20, "DXF: Y value of the primary point"),
    (30, "DXF: Z value of the primary point"),
    ((21, 28), "DXF: Y values of other points"),
    ((31, 37), "DXF: Z values of other points"),
    (38, "DXF: entity's elevation if nonzero"),
    (39, "Entity's thickness if nonzero (fixed)"),
    (
        (40, 47),
        "Double-precision floating-point values (text height, scale factors, and so on)",
    ),
    (48, "Linetype scale; default value is defined for all entity types"),
    (
        49,
        "Multiple 49 groups may appear in one entity for variable-length tables "
        "(such as the dash lengths in the LTYPE table). A 7x group always appears "
        "before the first 49 group to specify the table length",
    ),
    (
        (50, 58),
        "Angles (output in degrees to DXF files and radians through AutoLISP and ObjectARX applications)",
    ),
    (
        60,
        "Entity visibility; absence or 0 indicates visibility; 1 indicates invisibility",
    ),
    (62, "Color number (fixed)"),
    (66, "Entities follow flag (fixed)"),
    (67, "0 for model space or 1 for paper space (fixed)"),
    (
        68,
        "APP: identifies whether viewport is on but fully off screen; is not active or is off",
    ),
    (69, "APP: viewport identification number"),
    ((70, 79), "Integer values, such as repeat counts, flag bits, or modes"),
    ((90, 99), "32-bit integer values"),
    (
        100,
        "Subclass data marker (with derived class name as a string). "
        "Required for all objects and entity classes that are derived from "
        "another concrete class. The subclass data marker segregates data defined by different "
        "classes in the inheritance chain for the same object. This is in addition "
        "to the requirement for DXF names for each distinct concrete class derived "
        "from ObjectARX (see Subclass Markers)",
    ),
    (101, "Embedded object marker"),
    (
        102,
        "Control string, followed by '{arbitrary name' or '}'. Similar to the "
        "xdata 1002 group code, except that when the string begins with '{', it "
        "can be followed by an arbitrary string whose interpretation is up to the "
        "application. The only other control string allowed is '}' as a group "
        "terminator. AutoCAD does not interpret these strings except during d"
        "rawing audit operations. They are for application use.",
    ),
    (105, "Object handle for DIMVAR symbol table entry"),
    (
        110,
        "UCS origin (appears only if code 72 is set to 1); DXF: X value; APP: 3D point",
    ),
    (
        111,
        "UCS Y-axis (appears only if code 72 is set to 1); DXF: Y value; APP: 3D vector",
    ),
    (
        112,
        "UCS Z-axis (appears only if code 72 is set to 1); DXF: Z value; APP: 3D vector",
    ),
    ((120, 122), "DXF: Y value of UCS origin, UCS X-axis, and UCS Y-axis"),
    ((130, 132), "DXF: Z value of UCS origin, UCS X-axis, and UCS Y-axis"),
    (
        (140, 149),
        "Double-precision floating-point values (points, elevation, and DIMSTYLE settings, for example)",
    ),
    (
        (170, 179),
        "16-bit integer values, such as flag bits representing DIMSTYLE settings",
    ),
    (
        210,
        "Extrusion direction (fixed) "
        + "DXF: X value of extrusion direction "
        + "APP: 3D extrusion direction vector",
    ),
    (220, "DXF: Y value of the extrusion direction"),
    (230, "DXF: Z value of the extrusion direction"),
    ((270, 279), "16-bit integer values"),
    ((280, 289), "16-bit integer value"),
    ((290, 299), "Boolean flag value; 0 = False; 1 = True"),
    ((300, 309), "Arbitrary text strings"),
    (
        (310, 319),
        "Arbitrary binary chunks with same representation and limits as 1004 "
        "group codes: hexadecimal strings of up to 254 characters represent data "
        "chunks of up to 127 bytes",
    ),
    (
        (320, 329),
        "Arbitrary object handles; handle values that are taken 'as is'. They "
        "are not translated during INSERT and XREF operations",
    ),
    (
        (330, 339),
        "Soft-pointer handle; arbitrary soft pointers to other objects within "
        "same DXF file or drawing. Translated during INSERT and XREF operations",
    ),
    (
        (340, 349),
        "Hard-pointer handle; arbitrary hard pointers to other objects within "
        "same DXF file or drawing. Translated during INSERT and XREF operations",
    ),
    (
        (350, 359),
        "Soft-owner handle; arbitrary soft ownership links to other objects "
        "within same DXF file or drawing. Translated during INSERT and XREF "
        "operations",
    ),
    (
        (360, 369),
        "Hard-owner handle; arbitrary hard ownership links to other objects within "
        "same DXF file or drawing. Translated during INSERT and XREF operations",
    ),
    (
        (370, 379),
        "Lineweight enum value (AcDb::LineWeight). Stored and moved around as a 16-bit integer. "
        "Custom non-entity objects may use the full range, but entity classes only use 371-379 DXF "
        "group codes in their representation, because AutoCAD and AutoLISP both always assume a 370 "
        "group code is the entity's lineweight. This allows 370 to behave like other 'common' entity fields",
    ),
    (
        (380, 389),
        "PlotStyleName type enum (AcDb::PlotStyleNameType). Stored and moved around as a 16-bit integer. "
        "Custom non-entity objects may use the full range, but entity classes only use 381-389 "
        "DXF group codes in their representation, for the same reason as the lineweight range",
    ),
    (
        (390, 399),
        "String representing handle value of the PlotStyleName object, basically a hard pointer, but has "
        "a different range to make backward compatibility easier to deal with. Stored and moved around "
        "as an object ID (a handle in DXF files) and a special type in AutoLISP. Custom non-entity objects "
        "may use the full range, but entity classes only use 391-399 DXF group codes in their representation, "
        "for the same reason as the lineweight range",
    ),
    ((400, 409), "16-bit integers"),
    ((410, 419), "String"),
    (
        (420, 427),
        "32-bit integer value. When used with True Color; a 32-bit integer representing a 24-bit color value. "
        "The high-order byte (8 bits) is 0, the low-order byte an unsigned char holding the Blue value (0-255), "
        "then the Green value, and the next-to-high order byte is the Red Value. Converting this integer value to "
        "hexadecimal yields the following bit mask: 0x00RRGGBB. "
        "For example, a true color with Red==200, Green==100 and Blue==50 is 0x00C86432, and in DXF, in decimal, 13132850",
    ),
    (
        (430, 437),
        "String; when used for True Color, a string representing the name of the color",
    ),
    (
        (440, 447),
        "32-bit integer value. When used for True Color, the transparency value",
    ),
    ((450, 459), "Long"),
    ((460, 469), "Double-precision floating-point value"),
    ((470, 479), "String"),
    (
        (480, 481),
        "Hard-pointer handle; arbitrary hard pointers to other objects within same DXF file or drawing. "
        "Translated during INSERT and XREF operations",
    ),
    (
        999,
        "DXF: The 999 group code indicates that the line following it is a comment string. SAVEAS does "
        "not include such groups in a DXF output file, but OPEN honors them and ignores the comments. "
        "You can use the 999 group to include comments in a DXF file that you have edited",
    ),
    (1000, "ASCII string (up to 255 bytes long) in extended data"),
    (
        1001,
        "Registered application name (ASCII string up to 31 bytes long) for extended data",
    ),
    (1002, "Extended data control string ('{' or '}')"),
    (1003, "Extended data layer name"),
    (1004, "Chunk of bytes (up to 127 bytes long) in extended data"),
    (
        1005,
        "Entity handle in extended data; text string of up to 16 hexadecimal digits",
    ),
    (
        1010,
        "A point in extended data; DXF: X value (followed by 1020 and 1030 groups); APP: 3D point",
    ),
    (1020, "DXF: Y values of a point"),
    (1030, "DXF: Z values of a point"),
    (
        1011,
        "A 3D world space position in extended data "
        "DXF: X value (followed by 1021 and 1031 groups) "
        "APP: 3D point",
    ),
    (1021, "DXF: Y value of a world space position"),
    (1031, "DXF: Z value of a world space position"),
    (
        1012,
        "A 3D world space displacement in extended data "
        "DXF: X value (followed by 1022 and 1032 groups) "
        "APP: 3D vector",
    ),
    (1022, "DXF: Y value of a world space displacement"),
    (1032, "DXF: Z value of a world space displacement"),
    (
        1013,
        "A 3D world space direction in extended data "
        "DXF: X value (followed by 1022 and 1032 groups) "
        "APP: 3D vector",
    ),
    (1023, "DXF: Y value of a world space direction"),
    (1033, "DXF: Z value of a world space direction"),
    (1040, "Extended data double-precision floating-point value"),
    (1041, "Extended data distance value"),
    (1042, "Extended data scale factor"),
    (1070, "Extended data 16-bit signed integer"),
    (1071, "Extended data 32-bit signed long"),
]


def build_group_code_tooltip_dict() -> dict[int, str]:
    tooltips = dict()
    for code, tooltip in GROUP_CODE_TOOLTIPS:
        tooltip = "\n".join(textwrap.wrap(tooltip, width=80))
        if isinstance(code, int):
            tooltips[code] = tooltip
        elif isinstance(code, tuple):
            s, e = code
            for group_code in range(s, e + 1):
                tooltips[group_code] = tooltip
        else:
            raise ValueError(type(code))

    return tooltips


GROUP_CODE_TOOLTIPS_DICT = build_group_code_tooltip_dict()
