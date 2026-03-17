#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
import enum
from xml.etree import ElementTree as ET

import ezdxf
from ezdxf.document import Drawing
from ezdxf import zoom, colors
from ezdxf.addons import xplayer
from ezdxf.addons.drawing import svg, layout, pymupdf, dxf
from ezdxf.addons.drawing.dxf import ColorMode

from .tokenizer import hpgl2_commands
from .plotter import Plotter
from .interpreter import Interpreter
from .backend import Recorder, placement_matrix, Player


DEBUG = False
ENTER_HPGL2_MODE = b"%1B"


class Hpgl2Error(Exception):
    """Base exception for the :mod:`hpgl2` add-on."""

    pass


class Hpgl2DataNotFound(Hpgl2Error):
    """No HPGL/2 data was found, maybe the "Enter HPGL/2 mode" escape sequence is missing."""

    pass


class EmptyDrawing(Hpgl2Error):
    """The HPGL/2 commands do not produce any content."""

    pass


class MergeControl(enum.IntEnum):
    NONE = 0  # print order
    LUMINANCE = 1  # sort filled polygons by luminance
    AUTO = 2  # guess best method


def to_dxf(
    b: bytes,
    *,
    rotation: int = 0,
    mirror_x: bool = False,
    mirror_y: bool = False,
    color_mode=ColorMode.RGB,
    merge_control: MergeControl = MergeControl.AUTO,
) -> Drawing:
    """
    Exports the HPGL/2 commands of the byte stream `b` as a DXF document.

    The page content is created at the origin of the modelspace and 1 drawing unit is 1
    plot unit (1 plu = 0.025mm) unless scaling values are provided.

    The content of HPGL files is intended to be plotted on white paper, therefore a white
    filling will be added as background in color mode :attr:`RGB`.

    All entities are assigned to a layer according to the pen number with the name scheme
    ``PEN_<###>``. In order to be able to process the file better, it is also possible to
    assign the :term:`ACI` color by layer by setting the argument `color_mode` to
    :attr:`ColorMode.ACI`, but then the RGB color is lost because the RGB color has
    always the higher priority over the :term:`ACI`.

    The first paperspace layout "Layout1" of the DXF document is set up to print the entire
    modelspace on one sheet, the size of the page is the size of the original plot file in
    millimeters.

    HPGL/2's merge control works at the pixel level and cannot be replicated by DXF,
    but to prevent fillings from obscuring text, the filled polygons are
    sorted by luminance - this can be forced or disabled by the argument `merge_control`,
    see also :class:`MergeControl` enum.

    Args:
        b: plot file content as bytes
        rotation: rotation angle of 0, 90, 180 or 270 degrees
        mirror_x: mirror in x-axis direction
        mirror_y:  mirror in y-axis direction
        color_mode: the color mode controls how color values are assigned to DXF entities,
            see :class:`ColorMode`
        merge_control: how to order filled polygons, see :class:`MergeControl`

    Returns: DXF document as instance of class :class:`~ezdxf.document.Drawing`

    """
    if rotation not in (0, 90, 180, 270):
        raise ValueError("rotation angle must be 0, 90, 180, or 270")

    # 1st pass records output of the plotting commands and detects the bounding box
    doc = ezdxf.new()
    try:
        player = record_plotter_output(b, merge_control)
    except Hpgl2Error:
        return doc

    bbox = player.bbox()
    m = placement_matrix(bbox, -1 if mirror_x else 1, -1 if mirror_y else 1, rotation)
    player.transform(m)
    bbox = player.bbox()
    msp = doc.modelspace()
    dxf_backend = dxf.DXFBackend(msp, color_mode=color_mode)
    bg_color = colors.RGB(255, 255, 255)
    if color_mode == ColorMode.RGB:
        doc.layers.add("BACKGROUND")
        bg = dxf.add_background(msp, bbox, color=bg_color)
        bg.dxf.layer = "BACKGROUND"

    # 2nd pass replays the plotting commands to plot the DXF
    # exports the HPGL/2 content in plot units (plu) as modelspace:
    # 1 plu = 0.025mm or 40 plu == 1mm
    xplayer.hpgl2_to_drawing(player, dxf_backend, bg_color=bg_color.to_hex())
    del player

    if bbox.has_data:  # non-empty page
        zoom.window(msp, bbox.extmin, bbox.extmax)
        dxf.update_extents(doc, bbox)
        # paperspace is set up in mm:
        dxf.setup_paperspace(doc, bbox)
    return doc


def to_svg(
    b: bytes,
    *,
    rotation: int = 0,
    mirror_x: bool = False,
    mirror_y: bool = False,
    merge_control=MergeControl.AUTO,
) -> str:
    """
    Exports the HPGL/2 commands of the byte stream `b` as SVG string.

    The plot units are mapped 1:1 to ``viewBox`` units and the size of image is the size
    of the original plot file in millimeters.

    HPGL/2's merge control works at the pixel level and cannot be replicated by the
    backend, but to prevent fillings from obscuring text, the filled polygons are
    sorted by luminance - this can be forced or disabled by the argument `merge_control`,
    see also :class:`MergeControl` enum.

    Args:
        b: plot file content as bytes
        rotation: rotation angle of 0, 90, 180 or 270 degrees
        mirror_x: mirror in x-axis direction
        mirror_y:  mirror in y-axis direction
        merge_control: how to order filled polygons, see :class:`MergeControl`

    Returns: SVG content as ``str``

    """
    if rotation not in (0, 90, 180, 270):
        raise ValueError("rotation angle must be 0, 90, 180, or 270")
    # 1st pass records output of the plotting commands and detects the bounding box
    try:
        player = record_plotter_output(b, merge_control)
    except Hpgl2Error:
        return ""

    # transform content for the SVGBackend of the drawing add-on
    bbox = player.bbox()
    size = bbox.size

    # HPGL/2 uses (integer) plot units, 1 plu = 0.025mm or 1mm = 40 plu
    width_in_mm = size.x / 40
    height_in_mm = size.y / 40

    if rotation in (0, 180):
        page = layout.Page(width_in_mm, height_in_mm)
    else:
        page = layout.Page(height_in_mm, width_in_mm)
        # adjust rotation for y-axis mirroring
        rotation += 180

    # The SVGBackend expects coordinates in the range [0, output_coordinate_space]
    max_plot_units = round(max(size.x, size.y))
    settings = layout.Settings(output_coordinate_space=max_plot_units)
    m = layout.placement_matrix(
        bbox,
        sx=-1 if mirror_x else 1,
        sy=1 if mirror_y else -1,  # inverted y-axis
        rotation=rotation,
        page=page,
        output_coordinate_space=settings.output_coordinate_space,
    )
    player.transform(m)

    # 2nd pass replays the plotting commands on the SVGBackend of the drawing add-on
    svg_backend = svg.SVGRenderBackend(page, settings)
    xplayer.hpgl2_to_drawing(player, svg_backend, bg_color="#ffffff")
    del player
    xml = svg_backend.get_xml_root_element()
    return ET.tostring(xml, encoding="unicode", xml_declaration=True)


def to_pdf(
    b: bytes,
    *,
    rotation: int = 0,
    mirror_x: bool = False,
    mirror_y: bool = False,
    merge_control=MergeControl.AUTO,
) -> bytes:
    """
    Exports the HPGL/2 commands of the byte stream `b` as PDF data.

    The plot units (1 plu = 0.025mm) are converted to PDF units (1/72 inch) so the image
    has the size of the original plot file.

    HPGL/2's merge control works at the pixel level and cannot be replicated by the
    backend, but to prevent fillings from obscuring text, the filled polygons are
    sorted by luminance - this can be forced or disabled by the argument `merge_control`,
    see also :class:`MergeControl` enum.

    Python module PyMuPDF is required: https://pypi.org/project/PyMuPDF/

    Args:
        b: plot file content as bytes
        rotation: rotation angle of 0, 90, 180 or 270 degrees
        mirror_x: mirror in x-axis direction
        mirror_y:  mirror in y-axis direction
        merge_control: how to order filled polygons, see :class:`MergeControl`

    Returns: PDF content as ``bytes``

    """
    return _pymupdf(
        b,
        rotation=rotation,
        mirror_x=mirror_x,
        mirror_y=mirror_y,
        merge_control=merge_control,
        fmt="pdf",
    )


def to_pixmap(
    b: bytes,
    *,
    rotation: int = 0,
    mirror_x: bool = False,
    mirror_y: bool = False,
    merge_control=MergeControl.AUTO,
    fmt: str = "png",
    dpi: int = 96,
) -> bytes:
    """
    Exports the HPGL/2 commands of the byte stream `b` as pixel image.

    Supported image formats:

        === =========================
        png Portable Network Graphics
        ppm Portable Pixmap
        pbm Portable Bitmap
        === =========================

    The plot units (1 plu = 0.025mm) are converted to dot per inch (dpi) so the image
    has the size of the original plot file.

    HPGL/2's merge control works at the pixel level and cannot be replicated by the
    backend, but to prevent fillings from obscuring text, the filled polygons are
    sorted by luminance - this can be forced or disabled by the argument `merge_control`,
    see also :class:`MergeControl` enum.

    Python module PyMuPDF is required: https://pypi.org/project/PyMuPDF/

    Args:
        b: plot file content as bytes
        rotation: rotation angle of 0, 90, 180 or 270 degrees
        mirror_x: mirror in x-axis direction
        mirror_y:  mirror in y-axis direction
        merge_control: how to order filled polygons, see :class:`MergeControl`
        fmt: image format
        dpi: output resolution in dots per inch

    Returns: image content as ``bytes``

    """
    fmt = fmt.lower()
    if fmt not in pymupdf.SUPPORTED_IMAGE_FORMATS:
        raise ValueError(f"image format '{fmt}' not supported")
    return _pymupdf(
        b,
        rotation=rotation,
        mirror_x=mirror_x,
        mirror_y=mirror_y,
        merge_control=merge_control,
        fmt=fmt,
        dpi=dpi,
    )


def _pymupdf(
    b: bytes,
    *,
    rotation: int = 0,
    mirror_x: bool = False,
    mirror_y: bool = False,
    merge_control=MergeControl.AUTO,
    fmt: str = "pdf",
    dpi=96,
) -> bytes:
    if not pymupdf.is_pymupdf_installed:
        print("Python module PyMuPDF is required: https://pypi.org/project/PyMuPDF/")
        return b""

    if rotation not in (0, 90, 180, 270):
        raise ValueError("rotation angle must be 0, 90, 180, or 270")
    # 1st pass records output of the plotting commands and detects the bounding box
    try:
        player = record_plotter_output(b, merge_control)
    except Hpgl2Error:
        return b""

    # transform content for the SVGBackend of the drawing add-on
    bbox = player.bbox()
    size = bbox.size

    # HPGL/2 uses (integer) plot units, 1 plu = 0.025mm or 1mm = 40 plu
    width_in_mm = size.x / 40
    height_in_mm = size.y / 40

    if rotation in (0, 180):
        page = layout.Page(width_in_mm, height_in_mm)
    else:
        page = layout.Page(height_in_mm, width_in_mm)
        # adjust rotation for y-axis mirroring
        rotation += 180

    # The PDFBackend expects coordinates as pt = 1/72 inch; 1016 plu = 1 inch
    max_plot_units = max(size.x, size.y) / 1016 * 72
    settings = layout.Settings(output_coordinate_space=max_plot_units)
    m = layout.placement_matrix(
        bbox,
        sx=-1 if mirror_x else 1,
        sy=-1 if mirror_y else 1,
        rotation=rotation,
        page=page,
        output_coordinate_space=settings.output_coordinate_space,
    )
    player.transform(m)

    # 2nd pass replays the plotting commands on the PyMuPdfBackend of the drawing add-on
    pymupdf_backend = pymupdf.PyMuPdfBackend()
    xplayer.hpgl2_to_drawing(player, pymupdf_backend, bg_color="#ffffff")
    del player
    if fmt == "pdf":
        return pymupdf_backend.get_pdf_bytes(page, settings=settings)
    else:
        return pymupdf_backend.get_pixmap_bytes(
            page, fmt=fmt, settings=settings, dpi=dpi
        )


def print_interpreter_log(interpreter: Interpreter) -> None:
    print("HPGL/2 interpreter log:")
    print(f"unsupported commands: {interpreter.not_implemented_commands}")
    if interpreter.errors:
        print("parsing errors:")
        for err in interpreter.errors:
            print(err)


def record_plotter_output(
    b: bytes,
    merge_control: MergeControl,
) -> Player:
    commands = hpgl2_commands(b)
    if len(commands) == 0:
        print("HPGL2 data not found.")
        raise Hpgl2DataNotFound

    recorder = Recorder()
    plotter = Plotter(recorder)
    interpreter = Interpreter(plotter)
    interpreter.run(commands)
    if DEBUG:
        print_interpreter_log(interpreter)
    player = recorder.player()
    bbox = player.bbox()
    if not bbox.has_data:
        raise EmptyDrawing

    if merge_control == MergeControl.AUTO:
        if plotter.has_merge_control:
            merge_control = MergeControl.LUMINANCE
    if merge_control == MergeControl.LUMINANCE:
        if DEBUG:
            print("merge control on: sorting filled polygons by luminance")
        player.sort_filled_paths()
    return player
