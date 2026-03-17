# Purpose: read and write AutoCAD CTB files
# Copyright (c) 2010-2023, Manfred Moitzi
# License: MIT License
# IMPORTANT: use only standard 7-Bit ascii code
from __future__ import annotations
from typing import (
    Union,
    Optional,
    BinaryIO,
    TextIO,
    Iterable,
    Iterator,
    Any,
)
import os
from abc import abstractmethod
from io import StringIO
from array import array
from struct import pack
import zlib

END_STYLE_BUTT = 0
END_STYLE_SQUARE = 1
END_STYLE_ROUND = 2
END_STYLE_DIAMOND = 3
END_STYLE_OBJECT = 4

JOIN_STYLE_MITER = 0
JOIN_STYLE_BEVEL = 1
JOIN_STYLE_ROUND = 2
JOIN_STYLE_DIAMOND = 3
JOIN_STYLE_OBJECT = 5

FILL_STYLE_SOLID = 64
FILL_STYLE_CHECKERBOARD = 65
FILL_STYLE_CROSSHATCH = 66
FILL_STYLE_DIAMONDS = 67
FILL_STYLE_HORIZONTAL_BARS = 68
FILL_STYLE_SLANT_LEFT = 69
FILL_STYLE_SLANT_RIGHT = 70
FILL_STYLE_SQUARE_DOTS = 71
FILL_STYLE_VERICAL_BARS = 72
FILL_STYLE_OBJECT = 73

DITHERING_ON = 1  # bit coded color_policy
GRAYSCALE_ON = 2  # bit coded color_policy
NAMED_COLOR = 4  # bit coded color_policy

AUTOMATIC = 0
OBJECT_LINEWEIGHT = 0
OBJECT_LINETYPE = 31
OBJECT_COLOR = -1
OBJECT_COLOR2 = -1006632961

STYLE_COUNT = 255

DEFAULT_LINE_WEIGHTS = [
    0.00,  # 0
    0.05,  # 1
    0.09,  # 2
    0.10,  # 3
    0.13,  # 4
    0.15,  # 5
    0.18,  # 6
    0.20,  # 7
    0.25,  # 8
    0.30,  # 9
    0.35,  # 10
    0.40,  # 11
    0.45,  # 12
    0.50,  # 13
    0.53,  # 14
    0.60,  # 15
    0.65,  # 16
    0.70,  # 17
    0.80,  # 18
    0.90,  # 19
    1.00,  # 20
    1.06,  # 21
    1.20,  # 22
    1.40,  # 23
    1.58,  # 24
    2.00,  # 25
    2.11,  # 26
]

# color_type: (thx to Rammi)

# Take color from layer, ignore other bytes.
COLOR_BY_LAYER = 0xC0

# Take color from insertion, ignore other bytes
COLOR_BY_BLOCK = 0xC1

# RGB value, other bytes are R,G,B.
COLOR_RGB = 0xC2

# ACI, AutoCAD color index, other bytes are 0,0,index ???
COLOR_ACI = 0xC3


def color_name(index: int) -> str:
    return "Color_%d" % (index + 1)


def get_bool(value: Union[str, bool]) -> bool:
    if isinstance(value, str):
        upperstr = value.upper()
        if upperstr == "TRUE":
            value = True
        elif upperstr == "FALSE":
            value = False
        else:
            raise ValueError("Unknown bool value '%s'." % str(value))
    return value


class PlotStyle:
    def __init__(
        self,
        index: int,
        data: Optional[dict] = None,
        parent: Optional[PlotStyleTable] = None,
    ):
        data = data or {}
        self.parent = parent
        self.index = int(index)
        self.name = str(data.get("name", color_name(index)))
        self.localized_name = str(data.get("localized_name", color_name(index)))
        self.description = str(data.get("description", ""))
        # do not set _color, _mode_color or _color_policy directly
        # use set_color() method, and the properties dithering and grayscale
        self._color = int(data.get("color", OBJECT_COLOR))
        self._color_type = COLOR_RGB
        if self._color != OBJECT_COLOR:
            self._mode_color = int(data.get("mode_color", self._color))
        self._color_policy = int(data.get("color_policy", DITHERING_ON))
        self.physical_pen_number = int(data.get("physical_pen_number", AUTOMATIC))
        self.virtual_pen_number = int(data.get("virtual_pen_number", AUTOMATIC))
        self.screen = int(data.get("screen", 100))
        self.linepattern_size = float(data.get("linepattern_size", 0.5))
        self.linetype = int(data.get("linetype", OBJECT_LINETYPE))  # 0 .. 30
        self.adaptive_linetype = get_bool(data.get("adaptive_linetype", True))

        # lineweight index
        self.lineweight = int(data.get("lineweight", OBJECT_LINEWEIGHT))
        self.end_style = int(data.get("end_style", END_STYLE_OBJECT))
        self.join_style = int(data.get("join_style", JOIN_STYLE_OBJECT))
        self.fill_style = int(data.get("fill_style", FILL_STYLE_OBJECT))

    @property
    def color(self) -> Optional[tuple[int, int, int]]:
        """Get style color as ``(r, g, b)`` tuple or ``None``, if style has
        object color.
        """
        if self.has_object_color():
            return None  # object color
        else:
            return int2color(self._mode_color)[:3]

    @color.setter
    def color(self, rgb: tuple[int, int, int]) -> None:
        """Set color as RGB values."""
        r, g, b = rgb
        # when defining a user-color, `mode_color` represents the real
        # true_color as (r, g, b) tuple and color_type = COLOR_RGB (0xC2) as
        # highest byte, the `color` value calculated for a user-color is not a
        # (r, g, b) tuple and has color_type = COLOR_ACI (0xC3) (sometimes), set
        # for `color` the same value as for `mode_color`, because AutoCAD
        # corrects the `color` value by itself.
        self._mode_color = mode_color2int(r, g, b, color_type=self._color_type)
        self._color = self._mode_color

    @property
    def color_type(self):
        if self.has_object_color():
            return None  # object color
        else:
            return self._color_type

    @color_type.setter
    def color_type(self, value: int):
        self._color_type = value

    def set_object_color(self) -> None:
        """Set color to object color."""
        self._color = OBJECT_COLOR
        self._mode_color = OBJECT_COLOR

    def set_lineweight(self, lineweight: float) -> None:
        """Set `lineweight` in millimeters. Use ``0.0`` to set lineweight by
        object.
        """
        assert self.parent is not None
        self.lineweight = self.parent.get_lineweight_index(lineweight)

    def get_lineweight(self) -> float:
        """Returns the lineweight in millimeters or `0.0` for use entity
        lineweight.
        """
        assert self.parent is not None
        return self.parent.lineweights[self.lineweight]

    def has_object_color(self) -> bool:
        """``True`` if style has object color."""
        return self._color in (OBJECT_COLOR, OBJECT_COLOR2)

    @property
    def aci(self) -> int:
        """:ref:`ACI` in range from ``1`` to ``255``. Has no meaning for named
        plot styles. (int)
        """
        return self.index + 1

    @property
    def dithering(self) -> bool:
        """Depending on the capabilities of your plotter, dithering approximates
        the colors with dot patterns. When this option is ``False``, the colors
        are mapped to the nearest color, resulting in a smaller range of
        colors when plotting.

        Dithering is available only whether you select the object’s color or
        assign a plot style color.

        """
        return bool(self._color_policy & DITHERING_ON)

    @dithering.setter
    def dithering(self, status: bool) -> None:
        if status:
            self._color_policy |= DITHERING_ON
        else:
            self._color_policy &= ~DITHERING_ON

    @property
    def grayscale(self) -> bool:
        """Plot colors in grayscale. (bool)"""
        return bool(self._color_policy & GRAYSCALE_ON)

    @grayscale.setter
    def grayscale(self, status: bool) -> None:
        if status:
            self._color_policy |= GRAYSCALE_ON
        else:
            self._color_policy &= ~GRAYSCALE_ON

    @property
    def named_color(self) -> bool:
        return bool(self._color_policy & NAMED_COLOR)

    @named_color.setter
    def named_color(self, status: bool) -> None:
        if status:
            self._color_policy |= NAMED_COLOR
        else:
            self._color_policy &= ~NAMED_COLOR

    def write(self, stream: TextIO) -> None:
        """Write style data to file-like object `stream`."""
        index = self.index
        stream.write(" %d{\n" % index)
        stream.write('  name="%s\n' % self.name)
        stream.write('  localized_name="%s\n' % self.localized_name)
        stream.write('  description="%s\n' % self.description)
        stream.write("  color=%d\n" % self._color)
        if self._color != OBJECT_COLOR:
            stream.write("  mode_color=%d\n" % self._mode_color)
        stream.write("  color_policy=%d\n" % self._color_policy)
        stream.write("  physical_pen_number=%d\n" % self.physical_pen_number)
        stream.write("  virtual_pen_number=%d\n" % self.virtual_pen_number)
        stream.write("  screen=%d\n" % self.screen)
        stream.write("  linepattern_size=%s\n" % str(self.linepattern_size))
        stream.write("  linetype=%d\n" % self.linetype)
        stream.write(
            "  adaptive_linetype=%s\n" % str(bool(self.adaptive_linetype)).upper()
        )
        stream.write("  lineweight=%s\n" % str(self.lineweight))
        stream.write("  fill_style=%d\n" % self.fill_style)
        stream.write("  end_style=%d\n" % self.end_style)
        stream.write("  join_style=%d\n" % self.join_style)
        stream.write(" }\n")


class PlotStyleTable:
    """PlotStyle container"""

    def __init__(
        self,
        description: str = "",
        scale_factor: float = 1.0,
        apply_factor: bool = False,
    ):
        self.description = description
        self.scale_factor = scale_factor
        self.apply_factor = apply_factor

        # set custom_lineweight_display_units to 1 for showing lineweight in inch in
        # AutoCAD CTB editor window, but lineweight is always defined in mm
        self.custom_lineweight_display_units = 0
        self.lineweights = array("f", DEFAULT_LINE_WEIGHTS)

    def get_lineweight_index(self, lineweight: float) -> int:
        """Get index of `lineweight` in the lineweight table or append
        `lineweight` to lineweight table.
        """
        try:
            return self.lineweights.index(lineweight)
        except ValueError:
            self.lineweights.append(lineweight)
            return len(self.lineweights) - 1

    def set_table_lineweight(self, index: int, lineweight: float) -> int:
        """Argument `index` is the lineweight table index, not the :ref:`ACI`.

        Args:
            index: lineweight table index = :attr:`PlotStyle.lineweight`
            lineweight: in millimeters

        """
        try:
            self.lineweights[index] = lineweight
            return index
        except IndexError:
            self.lineweights.append(lineweight)
            return len(self.lineweights) - 1

    def get_table_lineweight(self, index: int) -> float:
        """Returns lineweight in millimeters of lineweight table entry `index`.

        Args:
            index: lineweight table index = :attr:`PlotStyle.lineweight`

        Returns:
            lineweight in mm or ``0.0`` for use entity lineweight

        """
        return self.lineweights[index]

    def save(self, filename: str | os.PathLike) -> None:
        """Save CTB or STB file as `filename` to the file system."""
        with open(filename, "wb") as stream:
            self.write(stream)

    def write(self, stream: BinaryIO) -> None:
        """Compress and write the CTB or STB file to binary `stream`."""
        memfile = StringIO()
        self.write_content(memfile)
        memfile.write(chr(0))  # end of file
        body = memfile.getvalue()
        memfile.close()
        _compress(stream, body)

    @abstractmethod
    def write_content(self, stream: TextIO) -> None:
        pass

    def _write_lineweights(self, stream: TextIO) -> None:
        """Write custom lineweight table to text `stream`."""
        stream.write("custom_lineweight_table{\n")
        for index, weight in enumerate(self.lineweights):
            stream.write(" %d=%.2f\n" % (index, weight))
        stream.write("}\n")

    def parse(self, text: str) -> None:
        """Parse plot styles from CTB string `text`."""

        def set_lineweights(lineweights):
            if lineweights is None:
                return
            self.lineweights = array("f", [0.0] * len(lineweights))
            for key, value in lineweights.items():
                self.lineweights[int(key)] = float(value)

        parser = PlotStyleFileParser(text)
        self.description = parser.get("description", "")
        self.scale_factor = float(parser.get("scale_factor", 1.0))
        self.apply_factor = get_bool(parser.get("apply_factor", True))
        self.custom_lineweight_display_units = int(
            parser.get("custom_lineweight_display_units", 0)
        )
        set_lineweights(parser.get("custom_lineweight_table", None))
        self.load_styles(parser.get("plot_style", {}))

    @abstractmethod
    def load_styles(self, styles):
        pass


class ColorDependentPlotStyles(PlotStyleTable):
    def __init__(
        self,
        description: str = "",
        scale_factor: float = 1.0,
        apply_factor: bool = False,
    ):
        super().__init__(description, scale_factor, apply_factor)
        self._styles: list[PlotStyle] = [
            PlotStyle(index, parent=self) for index in range(STYLE_COUNT)
        ]
        self._styles.insert(
            0, PlotStyle(256)
        )  # 1-based array: insert dummy value for index 0

    def __getitem__(self, aci: int) -> PlotStyle:
        """Returns :class:`PlotStyle` for :ref:`ACI` `aci`."""
        if 0 < aci < 256:
            return self._styles[aci]
        else:
            raise IndexError(aci)

    def __setitem__(self, aci: int, style: PlotStyle):
        """Set plot `style` for `aci`."""
        if 0 < aci < 256:
            style.parent = self
            self._styles[aci] = style
        else:
            raise IndexError(aci)

    def __iter__(self):
        """Iterable of all plot styles."""
        return iter(self._styles[1:])

    def new_style(self, aci: int, data: Optional[dict] = None) -> PlotStyle:
        """Set `aci` to new attributes defined by `data` dict.

        Args:
            aci: :ref:`ACI`
            data: ``dict`` of :class:`PlotStyle` attributes: description, color,
                physical_pen_number, virtual_pen_number, screen,
                linepattern_size, linetype, adaptive_linetype,
                lineweight, end_style, join_style, fill_style

        """
        # ctb table index = aci - 1
        # ctb table starts with index 0, where aci == 0 means BYBLOCK
        style = PlotStyle(index=aci - 1, data=data)
        style.color_type = COLOR_RGB
        self[aci] = style
        return style

    def get_lineweight(self, aci: int):
        """Returns the assigned lineweight for :class:`PlotStyle` `aci` in
        millimeter.
        """
        style = self[aci]
        lineweight = style.get_lineweight()
        if lineweight == 0.0:
            return None
        else:
            return lineweight

    def write_content(self, stream: TextIO) -> None:
        """Write the CTB-file to text `stream`."""
        self._write_header(stream)
        self._write_aci_table(stream)
        self._write_plot_styles(stream)
        self._write_lineweights(stream)

    def _write_header(self, stream: TextIO) -> None:
        """Write header values of CTB-file to text `stream`."""
        stream.write('description="%s\n' % self.description)
        stream.write("aci_table_available=TRUE\n")
        stream.write("scale_factor=%.1f\n" % self.scale_factor)
        stream.write("apply_factor=%s\n" % str(self.apply_factor).upper())
        stream.write(
            "custom_lineweight_display_units=%s\n"
            % str(self.custom_lineweight_display_units)
        )

    def _write_aci_table(self, stream: TextIO) -> None:
        """Write AutoCAD Color Index table to text `stream`."""
        stream.write("aci_table{\n")
        for style in self:
            index = style.index
            stream.write(' %d="%s\n' % (index, color_name(index)))
        stream.write("}\n")

    def _write_plot_styles(self, stream: TextIO) -> None:
        """Write user styles to text `stream`."""
        stream.write("plot_style{\n")
        for style in self:
            style.write(stream)
        stream.write("}\n")

    def load_styles(self, styles):
        for index, style in styles.items():
            index = int(index)
            style = PlotStyle(index, style)
            style.color_type = COLOR_RGB
            aci = index + 1
            self[aci] = style


class NamedPlotStyles(PlotStyleTable):
    def __init__(
        self,
        description: str = "",
        scale_factor: float = 1.0,
        apply_factor: bool = False,
    ):
        super().__init__(description, scale_factor, apply_factor)
        normal = PlotStyle(
            0,
            data={
                "name": "Normal",
                "localized_name": "Normal",
            },
        )
        self._styles: dict[str, PlotStyle] = {"Normal": normal}

    def __iter__(self) -> Iterable[str]:
        """Iterable of all plot style names."""
        return self.keys()

    def __getitem__(self, name: str) -> PlotStyle:
        """Returns :class:`PlotStyle` by `name`."""
        return self._styles[name]

    def __delitem__(self, name: str) -> None:
        """Delete plot style `name`. Plot style ``'Normal'`` is not deletable."""
        if name != "Normal":
            del self._styles[name]
        else:
            raise ValueError("Can't delete plot style 'Normal'. ")

    def keys(self) -> Iterable[str]:
        """Iterable of all plot style names."""
        keys = set(self._styles.keys())
        keys.discard("Normal")
        result = ["Normal"]
        result.extend(sorted(keys))
        return iter(result)

    def items(self) -> Iterator[tuple[str, PlotStyle]]:
        """Iterable of all plot styles as (``name``, class:`PlotStyle`) tuples."""
        for key in self.keys():
            yield key, self._styles[key]

    def values(self) -> Iterable[PlotStyle]:
        """Iterable of all class:`PlotStyle` objects."""
        for key, value in self.items():
            yield value

    def new_style(
        self,
        name: str,
        data: Optional[dict] = None,
        localized_name: Optional[str] = None,
    ) -> PlotStyle:
        """Create new class:`PlotStyle` `name` by attribute dict `data`, replaces
        existing class:`PlotStyle` objects.

        Args:
            name: plot style name
            localized_name: name shown in plot style editor, uses `name` if ``None``
            data: ``dict`` of :class:`PlotStyle` attributes: description, color,
                physical_pen_number, virtual_pen_number, screen,
                linepattern_size, linetype, adaptive_linetype, lineweight,
                end_style, join_style, fill_style

        """
        if name.lower() == "Normal":
            raise ValueError("Can't replace or modify plot style 'Normal'. ")
        data = data or {}
        data["name"] = name
        data["localized_name"] = localized_name or name
        index = len(self._styles)
        style = PlotStyle(index=index, data=data, parent=self)
        style.color_type = COLOR_ACI
        style.named_color = True
        self._styles[name] = style
        return style

    def get_lineweight(self, name: str):
        """Returns the assigned lineweight for :class:`PlotStyle` `name` in
        millimeter.
        """
        style = self[name]
        lineweight = style.get_lineweight()
        if lineweight == 0.0:
            return None
        else:
            return lineweight

    def write_content(self, stream: TextIO) -> None:
        """Write the STB-file to text `stream`."""
        self._write_header(stream)
        self._write_plot_styles(stream)
        self._write_lineweights(stream)

    def _write_header(self, stream: TextIO) -> None:
        """Write header values of CTB-file to text `stream`."""
        stream.write('description="%s\n' % self.description)
        stream.write("aci_table_available=FALSE\n")
        stream.write("scale_factor=%.1f\n" % self.scale_factor)
        stream.write("apply_factor=%s\n" % str(self.apply_factor).upper())
        stream.write(
            "custom_lineweight_display_units=%s\n"
            % str(self.custom_lineweight_display_units)
        )

    def _write_plot_styles(self, stream: TextIO) -> None:
        """Write user styles to text `stream`."""
        stream.write("plot_style{\n")
        for index, style in enumerate(self.values()):
            style.index = index
            style.write(stream)
        stream.write("}\n")

    def load_styles(self, styles):
        for index, style in styles.items():
            index = int(index)
            style = PlotStyle(index, style)
            style.color_type = COLOR_ACI
            self._styles[style.name] = style


def _read_ctb(stream: BinaryIO) -> ColorDependentPlotStyles:
    """Read a CTB-file from binary `stream`."""
    content: bytes = _decompress(stream)
    styles = ColorDependentPlotStyles()
    styles.parse(content.decode())
    return styles


def _read_stb(stream: BinaryIO) -> NamedPlotStyles:
    """Read a STB-file from binary `stream`."""
    content: bytes = _decompress(stream)
    styles = NamedPlotStyles()
    styles.parse(content.decode())
    return styles


def load(
    filename: str | os.PathLike,
) -> Union[ColorDependentPlotStyles, NamedPlotStyles]:
    """Load the CTB or STB file `filename` from file system."""
    filename = str(filename)
    with open(filename, "rb") as stream:
        if filename.lower().endswith(".ctb"):
            return _read_ctb(stream)
        elif filename.lower().endswith(".stb"):
            return _read_stb(stream)
        else:
            raise ValueError('Invalid file type: "{}"'.format(filename))


def new_ctb() -> ColorDependentPlotStyles:
    """Create a new CTB file."""
    return ColorDependentPlotStyles()


def new_stb() -> NamedPlotStyles:
    """Create a new STB file."""
    return NamedPlotStyles()


def _decompress(stream: BinaryIO) -> bytes:
    """Read and decompress the file content from binray `stream`."""
    content = stream.read()
    data = zlib.decompress(content[60:])  # type: bytes
    return data[:-1]  # truncate trailing \nul


def _compress(stream: BinaryIO, content: str):
    """Compress `content` and write to binary `stream`."""
    comp_body = zlib.compress(content.encode())
    adler_chksum = zlib.adler32(comp_body)
    stream.write(b"PIAFILEVERSION_2.0,CTBVER1,compress\r\npmzlibcodec")
    stream.write(pack("LLL", adler_chksum, len(content), len(comp_body)))
    stream.write(comp_body)


class PlotStyleFileParser:
    """A very simple CTB/STB file parser. CTB/STB files are created by
    applications, so the file structure should be correct in the most cases.
    """

    def __init__(self, text: str):
        self.data = {}
        for element, value in PlotStyleFileParser.iteritems(text):
            self.data[element] = value

    @staticmethod
    def iteritems(text: str):
        """Iterate over all first level (start at col 0) elements."""
        line_index = 0

        def get_name() -> str:
            """Get element name of line <line_index>."""
            line = lines[line_index]
            if line.endswith("{"):  # start of a list like 'plot_style{'
                name = line[:-1]
            else:  # simple name=value line
                name = line.split("=", 1)[0]
            return name.strip()

        def get_mapping() -> dict:
            """Get mapping of elements enclosed by { }.

            e. g. lineweights, plot_styles, aci_table

            """

            def end_of_list():
                return lines[line_index].endswith("}")

            nonlocal line_index
            data = dict()
            while not end_of_list():
                name = get_name()
                value = get_value()  # get value or sub-list
                data[name] = value
            line_index += 1
            return data  # skip '}' - end of list

        def get_value() -> Union[str, dict]:
            """Get value of line <line_index> or the list that starts in line
            <line_index>.
            """
            nonlocal line_index
            line = lines[line_index]
            if line.endswith("{"):  # start of a list
                line_index += 1
                return get_mapping()
            else:  # it's a simple name=value line
                value: str = line.split("=", 1)[1]
                value = sanitized_value(value)
                line_index += 1
            return value

        def skip_empty_lines():
            nonlocal line_index
            while line_index < len(lines) and len(lines[line_index]) == 0:
                line_index += 1

        lines = text.split("\n")
        while line_index < len(lines):
            name = get_name()
            value = get_value()
            yield name, value
            skip_empty_lines()

    def get(self, name: str, default: Any) -> Any:
        return self.data.get(name, default)


def sanitized_value(value: str) -> str:
    value = value.strip()
    if value.startswith('"'):  # strings: <name>="string
        return value[1:]

    # remove unknown appendix like this: "0.0076200000000 (+7.Z+"8V?S_LC )"
    # the pattern is "<float|int> (<some data>)", see issue #1069
    if value.endswith(")"):
        return value.split(" ")[0]
    return value


def int2color(color: int) -> tuple[int, int, int, int]:
    """Convert color integer value from CTB-file to ``(r, g, b, color_type)
    tuple.
    """
    # Take color from layer, ignore other bytes.
    color_type = (color & 0xFF000000) >> 24
    red = (color & 0xFF0000) >> 16
    green = (color & 0xFF00) >> 8
    blue = color & 0xFF
    return red, green, blue, color_type


def mode_color2int(red: int, green: int, blue: int, color_type=COLOR_RGB) -> int:
    """Convert mode_color (r, g, b, color_type) tuple to integer."""
    return -color2int(red, green, blue, color_type)


def color2int(red: int, green: int, blue: int, color_type: int) -> int:
    """Convert color (r, g, b, color_type) to integer."""
    return -((color_type << 24) + (red << 16) + (green << 8) + blue) & 0xFFFFFFFF
