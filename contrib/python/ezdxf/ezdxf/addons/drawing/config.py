# Copyright (c) 2021-2024, Matthew Broadway
# License: MIT License
from __future__ import annotations
from typing import Optional
import warnings
import dataclasses
from dataclasses import dataclass
from enum import Enum, auto

from ezdxf import disassemble
from ezdxf.enums import Measurement
from .type_hints import Color


class LinePolicy(Enum):
    """This enum is used to define how to render linetypes.

    .. note::

        Text and shapes in linetypes are not supported.

    Attributes:
        SOLID: draw all lines as solid regardless of the linetype style
        ACCURATE: render styled lines as accurately as possible
        APPROXIMATE: ignored since v0.18.1 - uses always ACCURATE by default

    """

    SOLID = auto()
    APPROXIMATE = auto()  # ignored since v0.18.1
    ACCURATE = auto()


class ProxyGraphicPolicy(Enum):
    """The action to take when an entity with a proxy graphic is encountered

    .. note::

        To get proxy graphics support proxy graphics have to be loaded:
        Set the global option :attr:`ezdxf.options.load_proxy_graphics` to
        ``True``, which is the default value.

        This can not prevent drawing proxy graphic inside of blocks,
        because this is beyond the domain of the drawing add-on!

    Attributes:
        IGNORE: do not display proxy graphics (skip_entity will be called instead)
        SHOW: if the entity cannot be rendered directly (e.g. if not implemented)
            but a proxy is present: display the proxy
        PREFER: display proxy graphics even for entities where direct rendering
            is available
    """

    IGNORE = auto()
    SHOW = auto()
    PREFER = auto()


class HatchPolicy(Enum):
    """The action to take when a HATCH entity is encountered

    Attributes:
        NORMAL: render pattern and solid fillings
        IGNORE: do not show HATCH entities at all
        SHOW_OUTLINE: show only the outline of HATCH entities
        SHOW_SOLID: show HATCH entities as solid filling regardless of the pattern

    """

    NORMAL = auto()
    IGNORE = auto()
    SHOW_OUTLINE = auto()
    SHOW_SOLID = auto()
    SHOW_APPROXIMATE_PATTERN = auto()  # ignored since v0.18.1 == NORMAL


class LineweightPolicy(Enum):
    """This enum is used to define how to determine the lineweight.

    Attributes:
        ABSOLUTE: in mm as resolved by the :class:`Frontend` class
        RELATIVE: lineweight is relative to page size
        RELATIVE_FIXED: fixed lineweight relative to page size for all strokes

    """

    ABSOLUTE = auto()
    # set fixed lineweight for all strokes in absolute mode:
    # set Configuration.min_lineweight to the desired lineweight in 1/300 inch!
    # set Configuration.lineweight_scaling to 0

    # The RELATIVE policy is a backend feature and is not supported by all backends!
    RELATIVE = auto()
    RELATIVE_FIXED = auto()


class ColorPolicy(Enum):
    """This enum is used to define how to determine the line/fill color.

    Attributes:
        COLOR: as resolved by the :class:`Frontend` class
        COLOR_SWAP_BW: as resolved by the :class:`Frontend` class but swaps black and white
        COLOR_NEGATIVE: invert all colors
        MONOCHROME: maps all colors to gray scale in range [0%, 100%]
        MONOCHROME_DARK_BG: maps all colors to gray scale in range [30%, 100%], brightens
            colors for dark backgrounds
        MONOCHROME_LIGHT_BG:  maps all colors to gray scale in range [0%, 70%], darkens
            colors for light backgrounds
        BLACK: maps all colors to black
        WHITE: maps all colors to white
        CUSTOM: maps all colors to custom color :attr:`Configuration.custom_fg_color`

    """

    COLOR = auto()
    COLOR_SWAP_BW = auto()
    COLOR_NEGATIVE = auto()
    MONOCHROME = auto()
    MONOCHROME_DARK_BG = auto()
    MONOCHROME_LIGHT_BG = auto()
    BLACK = auto()
    WHITE = auto()
    CUSTOM = auto()


class BackgroundPolicy(Enum):
    """This enum is used to define the background color.

    Attributes:
        DEFAULT: as resolved by the :class:`Frontend` class
        WHITE: white background
        BLACK: black background
        PAPERSPACE: default paperspace background
        MODELSPACE: default modelspace background
        OFF: fully transparent background
        CUSTOM: custom background color by :attr:`Configuration.custom_bg_color`

    """

    DEFAULT = auto()
    WHITE = auto()
    BLACK = auto()
    PAPERSPACE = auto()
    MODELSPACE = auto()
    OFF = auto()
    CUSTOM = auto()


class TextPolicy(Enum):
    """This enum is used to define the text rendering.

    Attributes:
        FILLING: text is rendered as solid filling (default)
        OUTLINE: text is rendered as outline paths
        REPLACE_RECT: replace text by a rectangle
        REPLACE_FILL: replace text by a filled rectangle
        IGNORE: ignore text entirely

    """

    FILLING = auto()
    OUTLINE = auto()
    REPLACE_RECT = auto()
    REPLACE_FILL = auto()
    IGNORE = auto()


class ImagePolicy(Enum):
    """This enum is used to define the image rendering.

    Attributes:
        DISPLAY: display images as they would appear in a regular CAD application
        RECT: display images as rectangles
        MISSING: images are always rendered as-if they are missing (rectangle + path text)
        PROXY: images are rendered using their proxy representations (rectangle)
        IGNORE: ignore images entirely

    """

    DISPLAY = auto()
    RECT = auto()
    MISSING = auto()
    PROXY = auto()
    IGNORE = auto()


@dataclass(frozen=True)
class Configuration:
    """Configuration options for the :mod:`drawing` add-on.

    Attributes:
        pdsize: the size to draw POINT entities (in drawing units)
            set to None to use the $PDSIZE value from the dxf document header

            ======= ====================================================
            0       5% of draw area height
            <0      Specifies a percentage of the viewport size
            >0      Specifies an absolute size
            None    use the $PDMODE value from the dxf document header
            ======= ====================================================

        pdmode: point styling mode (see POINT documentation)

            see :class:`~ezdxf.entities.Point` class documentation

        measurement: whether to use metric or imperial units as enum :class:`ezdxf.enums.Measurement`

            ======= ======================================================
            0       use imperial units (in, ft, yd, ...)
            1       use metric units (ISO meters)
            None    use the $MEASUREMENT value from the dxf document header
            ======= ======================================================

        show_defpoints: whether to show or filter out POINT entities on the defpoints layer
        proxy_graphic_policy: the action to take when a proxy graphic is encountered
        line_policy: the method to use when drawing styled lines (eg dashed,
            dotted etc)
        hatch_policy: the method to use when drawing HATCH entities
        infinite_line_length: the length to use when drawing infinite lines
        lineweight_scaling:
            multiplies every lineweight by this factor; set this factor to 0.0 for a
            constant minimum line width defined by the :attr:`min_lineweight` setting
            for all lineweights;
            the correct DXF lineweight often looks too thick in SVG, so setting a
            factor < 1 can improve the visual appearance
        min_lineweight: the minimum line width in 1/300 inch; set to ``None`` for
            let the backend choose.
        min_dash_length: the minimum length for a dash when drawing a styled line
            (default value is arbitrary)
        max_flattening_distance: Max flattening distance in drawing units
            see Path.flattening documentation.
            The backend implementation should calculate an appropriate value,
            like 1 screen- or paper pixel on the output medium, but converted
            into drawing units. Sets Path() approximation accuracy
        circle_approximation_count: Approximate a full circle by `n` segments, arcs
            have proportional less segments. Only used for approximation of arcs
            in banded polylines.
        hatching_timeout: hatching timeout for a single entity, very dense
            hatching patterns can cause a very long execution time, the default
            timeout for a single entity is 30 seconds.
        min_hatch_line_distance: minimum hatch line distance to render, narrower pattern
            lines are rendered as solid filling
        color_policy:
        custom_fg_color: Used for :class:`ColorPolicy.custom` policy, custom foreground
            color as "#RRGGBBAA" color string (RGB+alpha)
        background_policy:
        custom_bg_color: Used for :class:`BackgroundPolicy.custom` policy, custom
            background color as "#RRGGBBAA" color string (RGB+alpha)
        lineweight_policy:
        text_policy:
        image_policy: the method for drawing IMAGE entities

    """

    pdsize: Optional[int] = None  # use $PDSIZE from HEADER section
    pdmode: Optional[int] = None  # use $PDMODE from HEADER section
    measurement: Optional[Measurement] = None
    show_defpoints: bool = False
    proxy_graphic_policy: ProxyGraphicPolicy = ProxyGraphicPolicy.SHOW
    line_policy: LinePolicy = LinePolicy.ACCURATE
    hatch_policy: HatchPolicy = HatchPolicy.NORMAL
    infinite_line_length: float = 20
    lineweight_scaling: float = 1.0
    min_lineweight: Optional[float] = None
    min_dash_length: float = 0.1
    max_flattening_distance: float = disassemble.Primitive.max_flattening_distance
    circle_approximation_count: int = 128
    hatching_timeout: float = 30.0
    # Keep value in sync with ezdxf.render.hatching.MIN_HATCH_LINE_DISTANCE
    min_hatch_line_distance: float = 1e-4
    color_policy: ColorPolicy = ColorPolicy.COLOR
    custom_fg_color: Color = "#000000"
    background_policy: BackgroundPolicy = BackgroundPolicy.DEFAULT
    custom_bg_color: Color = "#ffffff"
    lineweight_policy: LineweightPolicy = LineweightPolicy.ABSOLUTE
    text_policy: TextPolicy = TextPolicy.FILLING
    image_policy: ImagePolicy = ImagePolicy.DISPLAY

    @staticmethod
    def defaults() -> Configuration:
        warnings.warn(
            "use Configuration() instead of Configuration.defaults()",
            DeprecationWarning,
        )
        return Configuration()

    def with_changes(self, **kwargs) -> Configuration:
        """Returns a new frozen :class:`Configuration` object with modified values."""
        params = dataclasses.asdict(self)
        for k, v in kwargs.items():
            params[k] = v
        return Configuration(**params)
