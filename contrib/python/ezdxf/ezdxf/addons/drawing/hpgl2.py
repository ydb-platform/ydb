#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Iterable, Sequence, no_type_check
import copy
import numpy as np

from ezdxf import colors
from ezdxf.math import Vec2, BoundingBox2d, Matrix44
from ezdxf.path import Command

from .type_hints import Color
from .backend import BackendInterface, BkPath2d, BkPoints2d, ImageData
from .config import Configuration, LineweightPolicy
from .properties import BackendProperties
from . import layout, recorder


__all__ = ["PlotterBackend"]

SEMICOLON = ord(";")
PRELUDE = b"%0B;IN;BP;"
EPILOG = b"PU;PA0,0;"
FLATTEN_MAX = 10  # plot units
MM_TO_PLU = 40  # 40 plu = 1mm
DEFAULT_PEN = 0
WHITE = colors.RGB(255, 255, 255)
BLACK = colors.RGB(0, 0, 0)
MAX_FLATTEN = 10

# comparing Command.<attrib> to ints is very slow
CMD_MOVE_TO = int(Command.MOVE_TO)
CMD_LINE_TO = int(Command.LINE_TO)
CMD_CURVE3_TO = int(Command.CURVE3_TO)
CMD_CURVE4_TO = int(Command.CURVE4_TO)


class PlotterBackend(recorder.Recorder):
    """The :class:`PlotterBackend` creates HPGL/2 plot files for output on raster
    plotters. This backend does not need any additional packages.  This backend support
    content cropping at page margins.

    The plot files are tested by the plot file viewer `ViewCompanion Standard`_
    but not on real hardware - please use with care and give feedback.

    .. _ViewCompanion Standard: http://www.softwarecompanions.com/

    """

    def __init__(self) -> None:
        super().__init__()

    def get_bytes(
        self,
        page: layout.Page,
        *,
        settings: layout.Settings = layout.Settings(),
        render_box: BoundingBox2d | None = None,
        curves=True,
        decimal_places: int = 1,
        base=64,
    ) -> bytes:
        """Returns the HPGL/2 data as bytes.

        Args:
            page: page definition, see :class:`~ezdxf.addons.drawing.layout.Page`
            settings: layout settings, see :class:`~ezdxf.addons.drawing.layout.Settings`
            render_box: set explicit region to render, default is content bounding box
            curves: use Bèzier curves for HPGL/2 output
            decimal_places: HPGL/2 output precision, less decimal places creates smaller
                files but for the price of imprecise curves (text)
            base: base for polyline encoding, 32 for 7 bit encoding or 64 for 8 bit encoding

        """
        top_origin = False
        settings = copy.copy(settings)
        # This player changes the original recordings!
        player = self.player()
        if render_box is None:
            render_box = player.bbox()

        # the page origin (0, 0) is in the bottom-left corner.
        output_layout = layout.Layout(render_box, flip_y=False)
        page = output_layout.get_final_page(page, settings)
        if page.width == 0 or page.height == 0:
            return b""  # empty page
        # DXF coordinates are mapped to integer coordinates (plu) in the first
        # quadrant: 40 plu = 1mm
        settings.output_coordinate_space = (
            max(page.width_in_mm, page.height_in_mm) * MM_TO_PLU
        )
        # transform content to the output coordinates space:
        m = output_layout.get_placement_matrix(
            page, settings=settings, top_origin=top_origin
        )
        player.transform(m)
        if settings.crop_at_margins:
            p1, p2 = page.get_margin_rect(top_origin=top_origin)  # in mm
            # scale factor to map page coordinates to output space coordinates:
            output_scale = settings.page_output_scale_factor(page)
            max_sagitta = 0.1 * MM_TO_PLU  # curve approximation 0.1 mm
            # crop content inplace by the margin rect:
            player.crop_rect(p1 * output_scale, p2 * output_scale, max_sagitta)
        backend = _RenderBackend(
            page,
            settings=settings,
            curves=curves,
            decimal_places=decimal_places,
            base=base,
        )
        player.replay(backend)
        return backend.get_bytes()

    def compatible(
        self, page: layout.Page, settings: layout.Settings = layout.Settings()
    ) -> bytes:
        """Returns the HPGL/2 data as 7-bit encoded bytes curves as approximated
        polylines and coordinates are rounded to integer values.
        Has often the smallest file size and should be compatible to all output devices
        but has a low quality text rendering.
        """
        return self.get_bytes(
            page, settings=settings, curves=False, decimal_places=0, base=32
        )

    def low_quality(
        self, page: layout.Page, settings: layout.Settings = layout.Settings()
    ) -> bytes:
        """Returns the HPGL/2 data as 8-bit encoded bytes, curves as Bézier
        curves and coordinates are rounded to integer values.
        Has a smaller file size than normal quality and the output device must support
        8-bit encoding and Bèzier curves.
        """
        return self.get_bytes(
            page, settings=settings, curves=True, decimal_places=0, base=64
        )

    def normal_quality(
        self, page: layout.Page, settings: layout.Settings = layout.Settings()
    ) -> bytes:
        """Returns the HPGL/2 data as 8-bit encoded bytes, curves as Bézier
        curves and coordinates are floats rounded to one decimal place.
        Has a smaller file size than high quality and the output device must support
        8-bit encoding, Bèzier curves and fractional coordinates.
        """
        return self.get_bytes(
            page, settings=settings, curves=True, decimal_places=1, base=64
        )

    def high_quality(
        self, page: layout.Page, settings: layout.Settings = layout.Settings()
    ) -> bytes:
        """Returns the HPGL/2 data as 8-bit encoded bytes and all curves as Bézier
        curves and coordinates are floats rounded to two decimal places.
        Has the largest file size and the output device must support 8-bit encoding,
        Bèzier curves and fractional coordinates.
        """
        return self.get_bytes(
            page, settings=settings, curves=True, decimal_places=2, base=64
        )


class PenTable:
    def __init__(self, max_pens: int = 64) -> None:
        self.pens: dict[int, colors.RGB] = dict()
        self.max_pens = int(max_pens)

    def __contains__(self, index: int) -> bool:
        return index in self.pens

    def __getitem__(self, index: int) -> colors.RGB:
        return self.pens[index]

    def add_pen(self, index: int, color: colors.RGB):
        self.pens[index] = color

    def to_bytes(self) -> bytes:
        command: list[bytes] = [f"NP{self.max_pens-1};".encode()]
        pens: list[tuple[int, colors.RGB]] = [
            (index, rgb) for index, rgb in self.pens.items()
        ]
        pens.sort()
        for index, rgb in pens:
            command.append(make_pc(index, rgb))
        return b"".join(command)


def make_pc(pen: int, rgb: colors.RGB) -> bytes:
    # pen color
    return f"PC{pen},{rgb.r},{rgb.g},{rgb.b};".encode()


class _RenderBackend(BackendInterface):
    """Creates the HPGL/2 output.

    This backend requires some preliminary work, record the frontend output via the
    Recorder backend to accomplish the following requirements:

    - Move content in the first quadrant of the coordinate system.
    - The output coordinates are integer values, scale the content appropriately:
        - 1 plot unit (plu) = 0.025mm
        - 40 plu = 1mm
        - 1016 plu = 1 inch
        - 3.39 plu = 1 dot @300 dpi
    - Replay the recorded output on this backend.

    """

    def __init__(
        self,
        page: layout.Page,
        *,
        settings: layout.Settings,
        curves=True,
        decimal_places: int = 2,
        base: int = 64,
    ) -> None:
        self.settings = settings
        self.curves = curves
        self.factional_bits = round(decimal_places * 3.33)
        self.decimal_places: int | None = (
            int(decimal_places) if decimal_places else None
        )
        self.base = base
        self.header: list[bytes] = []
        self.data: list[bytes] = []
        self.pen_table = PenTable(max_pens=256)
        self.current_pen: int = 0
        self.current_pen_width: float = 0.0
        self.setup(page)

        self._stroke_width_cache: dict[float, float] = dict()
        # StrokeWidthPolicy.absolute:
        # pen width in mm as resolved by the frontend
        self.min_lineweight = 0.05  # in mm, set by configure()
        self.lineweight_scaling = 1.0  # set by configure()
        self.lineweight_policy = LineweightPolicy.ABSOLUTE  # set by configure()
        # fixed lineweight for all strokes in ABSOLUTE mode:
        # set Configuration.min_lineweight to the desired lineweight in 1/300 inch!
        # set Configuration.lineweight_scaling to 0

        # LineweightPolicy.RELATIVE:
        # max_stroke_width is determined as a certain percentage of max(width, height)
        max_size = max(page.width_in_mm, page.height_in_mm)
        self.max_stroke_width: float = round(max_size * settings.max_stroke_width, 2)
        # min_stroke_width is determined as a certain percentage of max_stroke_width
        self.min_stroke_width: float = round(
            self.max_stroke_width * settings.min_stroke_width, 2
        )
        # LineweightPolicy.RELATIVE_FIXED:
        # all strokes have a fixed stroke-width as a certain percentage of max_stroke_width
        self.fixed_stroke_width: float = round(
            self.max_stroke_width * settings.fixed_stroke_width, 2
        )

    def setup(self, page: layout.Page) -> None:
        cmd = f"PS{page.width_in_mm*MM_TO_PLU:.0f},{page.height_in_mm*MM_TO_PLU:.0f};"
        self.header.append(cmd.encode())
        self.header.append(b"FT1;PA;")  # solid fill; plot absolute;

    def get_bytes(self) -> bytes:
        header: list[bytes] = list(self.header)
        header.append(self.pen_table.to_bytes())
        return compile_hpgl2(header, self.data)

    def switch_current_pen(self, pen_number: int, rgb: colors.RGB) -> int:
        if pen_number in self.pen_table:
            pen_color = self.pen_table[pen_number]
            if rgb != pen_color:
                self.data.append(make_pc(DEFAULT_PEN, rgb))
                pen_number = DEFAULT_PEN
        else:
            self.pen_table.add_pen(pen_number, rgb)
        return pen_number

    def set_pen(self, pen_number: int) -> None:
        if self.current_pen == pen_number:
            return
        self.data.append(f"SP{pen_number};".encode())
        self.current_pen = pen_number

    def set_pen_width(self, width: float) -> None:
        if self.current_pen_width == width:
            return
        self.data.append(f"PW{width:g};".encode())  # pen width in mm
        self.current_pen_width = width

    def enter_polygon_mode(self, start_point: Vec2) -> None:
        x = round(start_point.x, self.decimal_places)
        y = round(start_point.y, self.decimal_places)
        self.data.append(f"PA;PU{x},{y};PM;".encode())

    def close_current_polygon(self) -> None:
        self.data.append(b"PM1;")

    def fill_polygon(self) -> None:
        self.data.append(b"PM2;FP;")  # even/odd fill method

    def set_properties(self, properties: BackendProperties) -> None:
        pen_number = properties.pen
        pen_color, opacity = self.resolve_pen_color(properties.color)
        pen_width = self.resolve_pen_width(properties.lineweight)
        pen_number = self.switch_current_pen(pen_number, pen_color)
        self.set_pen(pen_number)
        self.set_pen_width(pen_width)

    def add_polyline_encoded(
        self, vertices: Iterable[Vec2], properties: BackendProperties
    ):
        self.set_properties(properties)
        self.data.append(polyline_encoder(vertices, self.factional_bits, self.base))

    def add_path(self, path: BkPath2d, properties: BackendProperties):
        if self.curves and path.has_curves:
            self.set_properties(properties)
            self.data.append(path_encoder(path, self.decimal_places))
        else:
            points = list(path.flattening(MAX_FLATTEN, segments=4))
            self.add_polyline_encoded(points, properties)

    @staticmethod
    def resolve_pen_color(color: Color) -> tuple[colors.RGB, float]:
        rgb = colors.RGB.from_hex(color)
        if rgb == WHITE:
            rgb = BLACK
        return rgb, alpha_to_opacity(color[7:9])

    def resolve_pen_width(self, width: float) -> float:
        try:
            return self._stroke_width_cache[width]
        except KeyError:
            pass
            stroke_width = self.fixed_stroke_width
        policy = self.lineweight_policy
        if policy == LineweightPolicy.ABSOLUTE:
            if self.lineweight_scaling:
                width = max(self.min_lineweight, width) * self.lineweight_scaling
            else:
                width = self.min_lineweight
            stroke_width = round(width, 2)  # in mm
        elif policy == LineweightPolicy.RELATIVE:
            stroke_width = round(
                map_lineweight_to_stroke_width(
                    width, self.min_stroke_width, self.max_stroke_width
                ),
                2,
            )
        self._stroke_width_cache[width] = stroke_width
        return stroke_width

    def set_background(self, color: Color) -> None:
        # background is always a white paper
        pass

    def draw_point(self, pos: Vec2, properties: BackendProperties) -> None:
        self.add_polyline_encoded([pos], properties)

    def draw_line(self, start: Vec2, end: Vec2, properties: BackendProperties) -> None:
        self.add_polyline_encoded([start, end], properties)

    def draw_solid_lines(
        self, lines: Iterable[tuple[Vec2, Vec2]], properties: BackendProperties
    ) -> None:
        lines = list(lines)
        if len(lines) == 0:
            return
        for line in lines:
            self.add_polyline_encoded(line, properties)

    def draw_path(self, path: BkPath2d, properties: BackendProperties) -> None:
        for sub_path in path.sub_paths():
            if len(sub_path) == 0:
                continue
            self.add_path(sub_path, properties)

    def draw_filled_paths(
        self, paths: Iterable[BkPath2d], properties: BackendProperties
    ) -> None:
        paths = list(paths)
        if len(paths) == 0:
            return
        self.enter_polygon_mode(paths[0].start)
        for p in paths:
            for sub_path in p.sub_paths():
                if len(sub_path) == 0:
                    continue
                self.add_path(sub_path, properties)
                self.close_current_polygon()
        self.fill_polygon()

    def draw_filled_polygon(
        self, points: BkPoints2d, properties: BackendProperties
    ) -> None:
        points2d: list[Vec2] = points.vertices()
        if points2d:
            self.enter_polygon_mode(points2d[0])
            self.add_polyline_encoded(points2d, properties)
            self.fill_polygon()

    def draw_image(self, image_data: ImageData, properties: BackendProperties) -> None:
        pass  # TODO: not implemented

    def configure(self, config: Configuration) -> None:
        self.lineweight_policy = config.lineweight_policy
        if config.min_lineweight:
            # config.min_lineweight in 1/300 inch!
            min_lineweight_mm = config.min_lineweight * 25.4 / 300
            self.min_lineweight = max(0.05, min_lineweight_mm)
        self.lineweight_scaling = config.lineweight_scaling

    def clear(self) -> None:
        pass

    def finalize(self) -> None:
        pass

    def enter_entity(self, entity, properties) -> None:
        pass

    def exit_entity(self, entity) -> None:
        pass


def alpha_to_opacity(alpha: str) -> float:
    # stroke-opacity: 0.0 = transparent; 1.0 = opaque
    # alpha: "00" = transparent; "ff" = opaque
    if len(alpha):
        try:
            return int(alpha, 16) / 255
        except ValueError:
            pass
    return 1.0


def map_lineweight_to_stroke_width(
    lineweight: float,
    min_stroke_width: float,
    max_stroke_width: float,
    min_lineweight=0.05,  # defined by DXF
    max_lineweight=2.11,  # defined by DXF
) -> float:
    lineweight = max(min(lineweight, max_lineweight), min_lineweight) - min_lineweight
    factor = (max_stroke_width - min_stroke_width) / (max_lineweight - min_lineweight)
    return round(min_stroke_width + round(lineweight * factor), 2)


def flatten_path(path: BkPath2d) -> Sequence[Vec2]:
    points = list(path.flattening(distance=FLATTEN_MAX))
    return points


def compile_hpgl2(header: Sequence[bytes], commands: Sequence[bytes]) -> bytes:
    output = bytearray(PRELUDE)
    output.extend(b"".join(header))
    output.extend(b"".join(commands))
    output.extend(EPILOG)
    return bytes(output)


def pe_encode(value: float, frac_bits: int = 0, base: int = 64) -> bytes:
    if frac_bits:
        value *= 1 << frac_bits
    x = round(value)
    if x >= 0:
        x *= 2
    else:
        x = abs(x) * 2 + 1

    chars = bytearray()
    while x >= base:
        x, r = divmod(x, base)
        chars.append(63 + r)
    if base == 64:
        chars.append(191 + x)
    else:
        chars.append(95 + x)
    return bytes(chars)


def polyline_encoder(vertices: Iterable[Vec2], frac_bits: int, base: int) -> bytes:
    cmd = b"PE"
    if base == 32:
        cmd = b"PE7"
    if frac_bits:
        cmd += b">" + pe_encode(frac_bits, 0, base)
    data = [cmd + b"<="]
    vertices = list(vertices)
    # first point as absolute coordinates
    current = vertices[0]
    data.append(pe_encode(current.x, frac_bits, base))
    data.append(pe_encode(current.y, frac_bits, base))
    for vertex in vertices[1:]:
        # remaining points as relative coordinates
        delta = vertex - current
        data.append(pe_encode(delta.x, frac_bits, base))
        data.append(pe_encode(delta.y, frac_bits, base))
        current = vertex
    data.append(b";")
    return b"".join(data)


@no_type_check
def path_encoder(path: BkPath2d, decimal_places: int | None) -> bytes:
    # first point as absolute coordinates
    current = path.start
    x = round(current.x, decimal_places)
    y = round(current.y, decimal_places)
    data = [f"PU;PA{x:g},{y:g};PD;".encode()]
    prev_command = Command.MOVE_TO
    if len(path):
        commands: list[bytes] = []
        for cmd in path.commands():
            delta = cmd.end - current
            xe = round(delta.x, decimal_places)
            ye = round(delta.y, decimal_places)
            if cmd.type == Command.LINE_TO:
                coords = f"{xe:g},{ye:g};".encode()
                if prev_command == Command.LINE_TO:
                    # extend previous PR command
                    commands[-1] = commands[-1][:-1] + b"," + coords
                else:
                    commands.append(b"PR" + coords)
                prev_command = Command.LINE_TO
            else:
                if cmd.type == Command.CURVE3_TO:
                    control = cmd.ctrl - current
                    end = cmd.end - current
                    control_1 = 2.0 * control / 3.0
                    control_2 = end + 2.0 * (control - end) / 3.0
                elif cmd.type == Command.CURVE4_TO:
                    control_1 = cmd.ctrl1 - current
                    control_2 = cmd.ctrl2 - current
                else:
                    raise ValueError("internal error: MOVE_TO command is illegal here")
                x1 = round(control_1.x, decimal_places)
                y1 = round(control_1.y, decimal_places)
                x2 = round(control_2.x, decimal_places)
                y2 = round(control_2.y, decimal_places)
                coords = f"{x1:g},{y1:g},{x2:g},{y2:g},{xe:g},{ye:g};".encode()
                if prev_command == Command.CURVE4_TO:
                    # extend previous BR command
                    commands[-1] = commands[-1][:-1] + b"," + coords
                else:
                    commands.append(b"BR" + coords)
                prev_command = Command.CURVE4_TO
            current = cmd.end
        data.append(b"".join(commands))
    data.append(b"PU;")
    return b"".join(data)
