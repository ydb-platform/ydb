#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import NamedTuple, TYPE_CHECKING
from typing_extensions import Self

import math
import enum
import dataclasses
from ezdxf.math import Vec2, BoundingBox2d, Matrix44

if TYPE_CHECKING:
    from ezdxf.layouts.layout import Layout as DXFLayout


class Units(enum.IntEnum):
    """Page units as enum.

    Attributes:
        inch: 25.4 mm
        px: 1/96 inch
        pt: 1/72 inch
        mm:
        cm:

    """

    # equivalent to ezdxf.units if possible
    inch = 1
    px = 2  # no equivalent DXF unit
    pt = 3  # no equivalent DXF unit
    mm = 4
    cm = 5


# all page sizes in landscape orientation
PAGE_SIZES = {
    "ISO A0": (1189, 841, Units.mm),
    "ISO A1": (841, 594, Units.mm),
    "ISO A2": (594, 420, Units.mm),
    "ISO A3": (420, 297, Units.mm),
    "ISO A4": (297, 210, Units.mm),
    "ANSI A": (11, 8.5, Units.inch),
    "ANSI B": (17, 11, Units.inch),
    "ANSI C": (22, 17, Units.inch),
    "ANSI D": (34, 22, Units.inch),
    "ANSI E": (44, 34, Units.inch),
    "ARCH C": (24, 18, Units.inch),
    "ARCH D": (36, 24, Units.inch),
    "ARCH E": (48, 36, Units.inch),
    "ARCH E1": (42, 30, Units.inch),
    "Letter": (11, 8.5, Units.inch),
    "Legal": (14, 8.5, Units.inch),
}


UNITS_TO_MM = {
    Units.mm: 1.0,
    Units.cm: 10.0,
    Units.inch: 25.4,
    Units.px: 25.4 / 96.0,
    Units.pt: 25.4 / 72.0,
}


class PageAlignment(enum.IntEnum):
    """Page alignment of content as enum.

    Attributes:
        TOP_LEFT:
        TOP_CENTER:
        TOP_RIGHT:
        MIDDLE_LEFT:
        MIDDLE_CENTER:
        MIDDLE_RIGHT:
        BOTTOM_LEFT:
        BOTTOM_CENTER:
        BOTTOM_RIGHT:

    """

    TOP_LEFT = 1
    TOP_CENTER = 2
    TOP_RIGHT = 3
    MIDDLE_LEFT = 4
    MIDDLE_CENTER = 5
    MIDDLE_RIGHT = 6
    BOTTOM_LEFT = 7
    BOTTOM_CENTER = 8
    BOTTOM_RIGHT = 9


class Margins(NamedTuple):
    """Page margins definition class

    Attributes:
        top:
        left:
        bottom:
        right:

    """

    top: float
    right: float
    bottom: float
    left: float

    @classmethod
    def all(cls, margin: float) -> Self:
        """Returns a page margins definition class with four equal margins."""
        return cls(margin, margin, margin, margin)

    @classmethod
    def all2(cls, top_bottom: float, left_right: float) -> Self:
        """Returns a page margins definition class with equal top-bottom and
        left-right margins.
        """
        return cls(top_bottom, left_right, top_bottom, left_right)

    # noinspection PyArgumentList
    def scale(self, factor: float) -> Self:
        return self.__class__(
            self.top * factor,
            self.right * factor,
            self.bottom * factor,
            self.left * factor,
        )


@dataclasses.dataclass
class Page:
    """Page definition class

    Attributes:

        width: page width, 0 for auto-detect
        height: page height, 0 for auto-detect
        units: page units as enum :class:`Units`
        margins: page margins in page units
        max_width: limit width for auto-detection, 0 for unlimited
        max_height: limit height for auto-detection, 0 for unlimited

    """

    width: float
    height: float
    units: Units = Units.mm
    margins: Margins = Margins.all(0)
    max_width: float = 0.0
    max_height: float = 0.0

    def __post_init__(self):
        assert isinstance(self.units, Units), "units require type <Units>"
        assert isinstance(self.margins, Margins), "margins require type <Margins>"

    @property
    def to_mm_factor(self) -> float:
        return UNITS_TO_MM[self.units]

    @property
    def width_in_mm(self) -> float:
        """Returns the page width in mm."""
        return round(self.width * self.to_mm_factor, 1)

    @property
    def max_width_in_mm(self) -> float:
        """Returns max page width in mm."""
        return round(self.max_width * self.to_mm_factor, 1)

    @property
    def height_in_mm(self) -> float:
        """Returns the page height in mm."""
        return round(self.height * self.to_mm_factor, 1)

    @property
    def max_height_in_mm(self) -> float:
        """Returns max page height in mm."""
        return round(self.max_height * self.to_mm_factor, 1)

    @property
    def margins_in_mm(self) -> Margins:
        """Returns the page margins in mm."""
        return self.margins.scale(self.to_mm_factor)

    @property
    def is_landscape(self) -> bool:
        """Returns ``True`` if the page has landscape orientation."""
        return self.width > self.height

    @property
    def is_portrait(self) -> bool:
        """Returns ``True`` if the page has portrait orientation. (square is portrait)"""
        return self.width <= self.height

    def to_landscape(self) -> None:
        """Converts the page to landscape orientation."""
        if self.is_portrait:
            self.width, self.height = self.height, self.width

    def to_portrait(self) -> None:
        """Converts the page to portrait orientation."""
        if self.is_landscape:
            self.width, self.height = self.height, self.width

    def get_margin_rect(self, top_origin=True) -> tuple[Vec2, Vec2]:
        """Returns the bottom-left and the top-right corner of the page margins in mm.
        The origin (0, 0) is the top-left corner of the page if `top_origin` is
        ``True`` or in the bottom-left corner otherwise.
        """
        margins = self.margins_in_mm
        right_margin = self.width_in_mm - margins.right
        page_height = self.height_in_mm
        if top_origin:
            bottom_left = Vec2(margins.left, margins.top)
            top_right = Vec2(right_margin, page_height - margins.bottom)
        else:  # bottom origin
            bottom_left = Vec2(margins.left, margins.bottom)
            top_right = Vec2(right_margin, page_height - margins.top)
        return bottom_left, top_right

    @classmethod
    def from_dxf_layout(cls, layout: DXFLayout) -> Self:
        """Returns the :class:`Page` based on the DXF attributes stored in the LAYOUT 
        entity. The modelspace layout often **doesn't** have usable page settings!

        Args:
            layout: any paperspace layout or the modelspace layout

        """
        # all layout measurements in mm
        width = round(layout.dxf.paper_width, 1)
        height = round(layout.dxf.paper_height, 1)
        top = round(layout.dxf.top_margin, 1)
        right = round(layout.dxf.right_margin, 1)
        bottom = round(layout.dxf.bottom_margin, 1)
        left = round(layout.dxf.left_margin, 1)

        rotation = layout.dxf.plot_rotation
        if rotation == 1:  # 90 degrees
            return cls(
                height,
                width,
                Units.mm,
                margins=Margins(top=right, right=bottom, bottom=left, left=top),
            )
        elif rotation == 2:  # 180 degrees
            return cls(
                width,
                height,
                Units.mm,
                margins=Margins(top=bottom, right=left, bottom=top, left=right),
            )
        elif rotation == 3:  # 270 degrees
            return cls(
                height,
                width,
                Units.mm,
                margins=Margins(top=left, right=top, bottom=right, left=bottom),
            )
        return cls(  # 0 degrees
            width,
            height,
            Units.mm,
            margins=Margins(top=top, right=right, bottom=bottom, left=left),
        )


@dataclasses.dataclass
class Settings:
    """The Layout settings.

    Attributes:
        content_rotation: Rotate content about 0, 90,  180 or 270 degrees
        fit_page: Scale content to fit the page.
        page_alignment: Supported by backends that use the :class:`Page` class to define
            the size of the output media, default alignment is :attr:`PageAlignment.MIDDLE_CENTER`
        crop_at_margins: crops the content at the page margins if ``True``, when
            supported by the backend, default is ``False``
        scale: Factor to scale the DXF units of model- or paperspace, to represent 1mm
            in the rendered output drawing. Only uniform scaling is supported.

            e.g. scale 1:100 and DXF units are meters, 1m = 1000mm corresponds 10mm in
            the output drawing = 10 / 1000 = 0.01;

            e.g. scale 1:1; DXF units are mm = 1 / 1 = 1.0 the default value

            The value is ignored if the page size is defined and the content fits the page and
            the value is also used to determine missing page sizes (width or height).
        max_stroke_width: Used for :class:`LineweightPolicy.RELATIVE` policy,
            :attr:`max_stroke_width` is defined as percentage of the content extents,
            e.g. 0.001 is 0.1% of max(page-width, page-height)
        min_stroke_width: Used for :class:`LineweightPolicy.RELATIVE` policy,
            :attr:`min_stroke_width` is defined as percentage of :attr:`max_stroke_width`,
            e.g. 0.05 is 5% of :attr:`max_stroke_width`
        fixed_stroke_width: Used for :class:`LineweightPolicy.RELATIVE_FIXED` policy,
            :attr:`fixed_stroke_width` is defined as percentage of :attr:`max_stroke_width`,
            e.g. 0.15 is 15% of :attr:`max_stroke_width`
        output_coordinate_space: expert feature to map the DXF coordinates to the
            output coordinate system [0, output_coordinate_space]
        output_layers: For supported backends, separate the entities into 'layers' in the output

    """

    content_rotation: int = 0
    fit_page: bool = True
    scale: float = 1.0
    page_alignment: PageAlignment = PageAlignment.MIDDLE_CENTER
    crop_at_margins: bool = False
    # for LineweightPolicy.RELATIVE
    # max_stroke_width is defined as percentage of the content extents
    max_stroke_width: float = 0.001  # 0.1% of max(width, height) in viewBox coords
    # min_stroke_width is defined as percentage of max_stroke_width
    min_stroke_width: float = 0.05  # 5% of max_stroke_width
    # StrokeWidthPolicy.fixed_1
    # fixed_stroke_width is defined as percentage of max_stroke_width
    fixed_stroke_width: float = 0.15  # 15% of max_stroke_width
    # PDF, HPGL expect the coordinates in the first quadrant and SVG has an inverted
    # y-axis, so transformation from DXF to the output coordinate system is required.
    # The output_coordinate_space defines the space into which the DXF coordinates are
    # mapped, the range is [0, output_coordinate_space] for the larger page
    # dimension - aspect ratio is always preserved - these are CAD drawings!
    # The SVGBackend uses this feature to map all coordinates to integer values:
    output_coordinate_space: float = 1_000_000  # recommended value for the SVGBackend
    output_layers: bool = True

    def __post_init__(self) -> None:
        if self.content_rotation not in (0, 90, 180, 270):
            raise ValueError(
                f"invalid content rotation {self.content_rotation}, "
                f"expected: 0, 90, 180, 270"
            )

    def page_output_scale_factor(self, page: Page) -> float:
        """Returns the scaling factor to map page coordinates in mm to output space
        coordinates.
        """
        try:
            return self.output_coordinate_space / max(
                page.width_in_mm, page.height_in_mm
            )
        except ZeroDivisionError:
            return 1.0


class Layout:
    def __init__(self, render_box: BoundingBox2d, flip_y=False) -> None:
        super().__init__()
        self.flip_y: float = -1.0 if flip_y else 1.0
        self.render_box = render_box

    def get_rotation(self, settings: Settings) -> int:
        if settings.content_rotation not in (0, 90, 180, 270):
            raise ValueError("content rotation must be 0, 90, 180 or 270 degrees")
        rotation = settings.content_rotation
        if self.flip_y == -1.0:
            if rotation == 90:
                rotation = 270
            elif rotation == 270:
                rotation = 90
        return rotation

    def get_content_size(self, rotation: int) -> Vec2:
        content_size = self.render_box.size
        if rotation in (90, 270):
            # swap x, y to apply rotation to content_size
            content_size = Vec2(content_size.y, content_size.x)
        return content_size

    def get_final_page(self, page: Page, settings: Settings) -> Page:
        rotation = self.get_rotation(settings)
        content_size = self.get_content_size(rotation)
        return final_page_size(content_size, page, settings)

    def get_placement_matrix(
        self, page: Page, settings=Settings(), top_origin=True
    ) -> Matrix44:
        # Argument `page` has to be the resolved final page size!
        rotation = self.get_rotation(settings)

        content_size = self.get_content_size(rotation)
        content_size_mm = content_size * settings.scale
        if settings.fit_page:
            content_size_mm *= fit_to_page(content_size_mm, page)
        try:
            scale_dxf_to_mm = content_size_mm.x / content_size.x
        except ZeroDivisionError:
            scale_dxf_to_mm = 1.0
        # map output coordinates to range [0, output_coordinate_space]
        scale_mm_to_output_space = settings.page_output_scale_factor(page)
        scale = scale_dxf_to_mm * scale_mm_to_output_space
        m = placement_matrix(
            self.render_box,
            sx=scale,
            sy=scale * self.flip_y,
            rotation=rotation,
            page=page,
            output_coordinate_space=settings.output_coordinate_space,
            page_alignment=settings.page_alignment,
            top_origin=top_origin,
        )
        return m


def final_page_size(content_size: Vec2, page: Page, settings: Settings) -> Page:
    scale = settings.scale
    width = page.width_in_mm
    height = page.height_in_mm
    margins = page.margins_in_mm
    if width == 0.0:
        width = scale * content_size.x + margins.left + margins.right
    if height == 0.0:
        height = scale * content_size.y + margins.top + margins.bottom

    width, height = limit_page_size(
        width, height, page.max_width_in_mm, page.max_height_in_mm
    )
    return Page(round(width, 1), round(height, 1), Units.mm, margins)


def limit_page_size(
    width: float, height: float, max_width: float, max_height: float
) -> tuple[float, float]:
    try:
        ar = width / height
    except ZeroDivisionError:
        return width, height
    if max_height:
        height = min(max_height, height)
        width = height * ar
    if max_width and width > max_width:
        width = min(max_width, width)
        height = width / ar
    return width, height


def fit_to_page(content_size_mm: Vec2, page: Page) -> float:
    margins = page.margins_in_mm
    try:
        sx = (page.width_in_mm - margins.left - margins.right) / content_size_mm.x
        sy = (page.height_in_mm - margins.top - margins.bottom) / content_size_mm.y
    except ZeroDivisionError:
        return 1.0
    return min(sx, sy)


def placement_matrix(
    bbox: BoundingBox2d,
    sx: float,
    sy: float,
    rotation: float,
    page: Page,
    output_coordinate_space: float,
    page_alignment: PageAlignment = PageAlignment.MIDDLE_CENTER,
    # top_origin True: page origin (0, 0) in top-left corner, +y axis pointing down
    # top_origin False: page origin (0, 0) in bottom-left corner, +y axis pointing up
    top_origin=True,
) -> Matrix44:
    """Returns a matrix to place the bbox in the first quadrant of the coordinate
    system (+x, +y).
    """
    try:
        scale_mm_to_vb = output_coordinate_space / max(
            page.width_in_mm, page.height_in_mm
        )
    except ZeroDivisionError:
        scale_mm_to_vb = 1.0
    margins = page.margins_in_mm

    # create scaling and rotation matrix:
    if abs(sx) < 1e-9:
        sx = 1.0
    if abs(sy) < 1e-9:
        sy = 1.0
    m = Matrix44.scale(sx, sy, 1.0)
    if rotation:
        m @= Matrix44.z_rotate(math.radians(rotation))

    # calc bounding box of the final output canvas:
    corners = m.transform_vertices(bbox.rect_vertices())
    canvas = BoundingBox2d(corners)

    # shift content to first quadrant +x/+y
    tx, ty = canvas.extmin

    # align content within margins
    view_box_content_x = (
        page.width_in_mm - margins.left - margins.right
    ) * scale_mm_to_vb
    view_box_content_y = (
        page.height_in_mm - margins.top - margins.bottom
    ) * scale_mm_to_vb
    dx = view_box_content_x - canvas.size.x
    dy = view_box_content_y - canvas.size.y
    offset_x = margins.left * scale_mm_to_vb  # left
    if top_origin:
        offset_y = margins.top * scale_mm_to_vb
    else:
        offset_y = margins.bottom * scale_mm_to_vb

    if is_center_aligned(page_alignment):
        offset_x += dx / 2
    elif is_right_aligned(page_alignment):
        offset_x += dx
    if is_middle_aligned(page_alignment):
        offset_y += dy / 2
    elif is_bottom_aligned(page_alignment):
        if top_origin:
            offset_y += dy
    else:  # top aligned
        if not top_origin:
            offset_y += dy
    return m @ Matrix44.translate(-tx + offset_x, -ty + offset_y, 0)


def is_left_aligned(align: PageAlignment) -> bool:
    return align in (
        PageAlignment.TOP_LEFT,
        PageAlignment.MIDDLE_LEFT,
        PageAlignment.BOTTOM_LEFT,
    )


def is_center_aligned(align: PageAlignment) -> bool:
    return align in (
        PageAlignment.TOP_CENTER,
        PageAlignment.MIDDLE_CENTER,
        PageAlignment.BOTTOM_CENTER,
    )


def is_right_aligned(align: PageAlignment) -> bool:
    return align in (
        PageAlignment.TOP_RIGHT,
        PageAlignment.MIDDLE_RIGHT,
        PageAlignment.BOTTOM_RIGHT,
    )


def is_top_aligned(align: PageAlignment) -> bool:
    return align in (
        PageAlignment.TOP_LEFT,
        PageAlignment.TOP_CENTER,
        PageAlignment.TOP_RIGHT,
    )


def is_middle_aligned(align: PageAlignment) -> bool:
    return align in (
        PageAlignment.MIDDLE_LEFT,
        PageAlignment.MIDDLE_CENTER,
        PageAlignment.MIDDLE_RIGHT,
    )


def is_bottom_aligned(align: PageAlignment) -> bool:
    return align in (
        PageAlignment.BOTTOM_LEFT,
        PageAlignment.BOTTOM_CENTER,
        PageAlignment.BOTTOM_RIGHT,
    )
