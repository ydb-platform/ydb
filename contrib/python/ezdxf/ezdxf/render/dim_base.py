# Copyright (c) 2018-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Any,
    cast,
)
import math
import abc
from ezdxf.math import (
    Vec3,
    Vec2,
    UVec,
    ConstructionLine,
    ConstructionBox,
    ConstructionArc,
)
from ezdxf.math import UCS, PassTroughUCS, xround, Z_AXIS
from ezdxf.lldxf import const
from ezdxf.enums import TextEntityAlignment
from ezdxf._options import options
from ezdxf.lldxf.const import DXFValueError, DXFUndefinedBlockError
from ezdxf.tools import suppress_zeros
from ezdxf.render.arrows import ARROWS
from ezdxf.entities import DimStyleOverride, Dimension

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import Textstyle
    from ezdxf.eztypes import GenericLayoutType


class TextBox(ConstructionBox):
    """Text boundaries representation."""

    def __init__(
        self,
        center: Vec2 = Vec2(0, 0),
        width: float = 0.0,
        height: float = 0.0,
        angle: float = 0.0,
        hgap: float = 0.0,  # horizontal gap - width
        vgap: float = 0.0,  # vertical gap - height
    ):
        super().__init__(center, width + 2.0 * hgap, height + 2.0 * vgap, angle)


PLUS_MINUS = "±"
_TOLERANCE_COMMON = r"\A{align};{txt}{{\H{fac:.2f}x;"
TOLERANCE_TEMPLATE1 = _TOLERANCE_COMMON + r"{tol}}}"
TOLERANCE_TEMPLATE2 = _TOLERANCE_COMMON + r"\S{upr}^ {lwr};}}"
LIMITS_TEMPLATE = r"{{\H{fac:.2f}x;\S{upr}^ {lwr};}}"


def OptionalVec2(v) -> Optional[Vec2]:
    if v is not None:
        return Vec2(v)
    else:
        return None


def sign_char(value: float) -> str:
    if value < 0.0:
        return "-"
    elif value > 0:
        return "+"
    else:
        return " "


def format_text(
    value: float,
    dimrnd: Optional[float] = None,
    dimdec: int = 2,
    dimzin: int = 0,
    dimdsep: str = ".",
) -> str:
    if dimrnd is not None:
        value = xround(value, dimrnd)

    if dimdec is None:
        fmt = "{:f}"
        # Remove pending zeros for undefined decimal places:
        # '{:f}'.format(0) -> '0.000000'
        dimzin = dimzin | 8
    else:
        fmt = "{:." + str(dimdec) + "f}"
    text = fmt.format(value)

    leading = bool(dimzin & 4)
    pending = bool(dimzin & 8)
    text = suppress_zeros(text, leading, pending)
    if dimdsep != ".":
        text = text.replace(".", dimdsep)
    return text


def apply_dimpost(text: str, dimpost: str) -> str:
    if "<>" in dimpost:
        fmt = dimpost.replace("<>", "{}", 1)
        return fmt.format(text)
    else:
        raise DXFValueError(f'Invalid dimpost string: "{dimpost}"')


class Tolerance:  # and Limits
    def __init__(
        self,
        dim_style: DimStyleOverride,
        cap_height: float = 1.0,
        width_factor: float = 1.0,
        dim_scale: float = 1.0,
    ):
        self.text_width_factor = width_factor
        self.dim_scale = dim_scale
        get = dim_style.get
        # Appends tolerances to dimension text.
        # enabling DIMTOL disables DIMLIM.
        self.has_tolerance = bool(get("dimtol", 0))
        self.has_limits = False
        if not self.has_tolerance:
            # Limits generates dimension limits as the default text.
            self.has_limits = bool(get("dimlim", 0))

        # Scale factor for the text height of fractions and tolerance values
        # relative to the dimension text height
        self.text_scale_factor: float = get("dimtfac", 0.5)

        self.text_decimal_separator = dim_style.get_decimal_separator()

        # Default MTEXT line spacing for tolerances (BricsCAD)
        self.line_spacing: float = 1.35

        # Sets the minimum (or lower) tolerance limit for dimension text when
        # DIMTOL or DIMLIM is on.
        # DIMTM accepts signed values.
        # If DIMTOL is on and DIMTP and DIMTM are set to the same value, a
        # tolerance value is drawn.
        # If DIMTM and DIMTP values differ, the upper tolerance is drawn above
        # the lower, and a plus sign is added to the DIMTP value if it is
        # positive.
        # For DIMTM, the program uses the negative of the value you enter
        # (adding a minus sign if you specify a positive number and a plus sign
        # if you specify a negative number).
        self.minimum: float = get("dimtm", 0.0)

        # Sets the maximum (or upper) tolerance limit for dimension text when
        # DIMTOL or DIMLIM is on.
        # DIMTP accepts signed values.
        # If DIMTOL is on and DIMTP and DIMTM are set to the same value, a
        # tolerance value is drawn.
        # If DIMTM and DIMTP values differ, the upper tolerance is drawn above
        # the lower and a plus sign is added to the DIMTP value if it is
        # positive.
        self.maximum: float = get("dimtp", 0.0)

        # Number of decimal places to display in tolerance values
        # Same value for linear and angular measurements!
        self.decimal_places: int = get("dimtdec", 2)

        # Vertical justification for tolerance values relative to the nominal dimension text
        # 0 = Bottom
        # 1 = Middle
        # 2 = Top
        self.valign: int = get("dimtolj", 0)

        # Same as DIMZIN for tolerances (self.text_suppress_zeros)
        # Same value for linear and angular measurements!
        self.suppress_zeros: int = get("dimtzin", 0)
        self.text: str = ""
        self.text_height: float = 0.0
        self.text_width: float = 0.0
        self.text_upper: str = ""
        self.text_lower: str = ""
        self.char_height: float = cap_height * self.text_scale_factor * self.dim_scale
        if self.has_tolerance:
            self.init_tolerance()
        elif self.has_limits:
            self.init_limits()

    @property
    def enabled(self) -> bool:
        return self.has_tolerance or self.has_limits

    def disable(self):
        self.has_tolerance = False
        self.has_limits = False

    def init_tolerance(self):
        # The tolerance values are stored in the dimension style, they are
        # independent from the actual measurement:
        # Single tolerance value +/- value
        if self.minimum == self.maximum:
            self.text_height = self.char_height
            self.text_width = self.get_text_width(self.text, self.text)
        else:  # 2 stacked values: +upper tolerance <above> -lower tolerance
            # requires 2 text lines
            self.text_height = self.char_height + (self.text_height * self.line_spacing)
            self.text_width = self.get_text_width(self.text_upper, self.text_lower)
        self.update_tolerance_text(self.maximum, self.minimum)

    def update_tolerance_text(self, tol_upper: float, tol_lower: float):
        if tol_upper == tol_lower:
            self.text = PLUS_MINUS + self.format_text(abs(tol_upper))
        else:
            self.text_upper = sign_char(tol_upper) + self.format_text(abs(tol_upper))
            self.text_lower = sign_char(tol_lower * -1) + self.format_text(
                abs(tol_lower)
            )

    def init_limits(self):
        # self.text is always an empty string (default value)
        # Limit text are always 2 stacked numbers and requires the actual
        # measurement!
        self.text_height = self.char_height + (self.text_height * self.line_spacing)

    def format_text(self, value: float) -> str:
        """Rounding and text formatting of tolerance `value`, removes leading
        and trailing zeros if necessary.

        """
        # dimpost is not applied to limits or tolerances!
        return format_text(
            value=value,
            dimrnd=None,
            dimdec=self.decimal_places,
            dimzin=self.suppress_zeros,
            dimdsep=self.text_decimal_separator,
        )

    def get_text_width(self, upr: str, lwr: str) -> float:
        """Returns the text width of the tolerance (upr/lwr) in drawing units."""
        # todo: use matplotlib support
        count = max(len(upr), len(lwr))
        return self.text_height * self.text_width_factor * count

    def compile_mtext(self, text: str) -> str:
        if self.has_tolerance:
            align = max(int(self.valign), 0)
            align = min(align, 2)
            if not self.text:
                text = TOLERANCE_TEMPLATE2.format(
                    align=align,
                    txt=text,
                    fac=self.text_scale_factor,
                    upr=self.text_upper,
                    lwr=self.text_lower,
                )
            else:
                text = TOLERANCE_TEMPLATE1.format(
                    align=align,
                    txt=text,
                    fac=self.text_scale_factor,
                    tol=self.text,
                )
        elif self.has_limits:
            text = LIMITS_TEMPLATE.format(
                upr=self.text_upper,
                lwr=self.text_lower,
                fac=self.text_scale_factor,
            )
        return text

    def update_limits(self, measurement: float) -> None:
        upper_limit = measurement + self.maximum
        lower_limit = measurement - self.minimum
        self.text_upper = self.format_text(upper_limit)
        self.text_lower = self.format_text(lower_limit)
        self.text_width = self.get_text_width(self.text_upper, self.text_lower)


class ExtensionLines:
    default_lineweight: int = const.LINEWEIGHT_BYBLOCK

    def __init__(self, dim_style: DimStyleOverride, default_color: int, scale: float):
        get = dim_style.get
        self.color: int = get("dimclre", default_color)  # ACI
        self.linetype1: str = get("dimltex1", "")
        self.linetype2: str = get("dimltex2", "")
        self.lineweight: int = get("dimlwe", self.default_lineweight)
        self.suppress1: bool = bool(get("dimse1", 0))
        self.suppress2: bool = bool(get("dimse2", 0))

        # Extension of extension line above the dimension line, in extension
        # line direction in most cases perpendicular to dimension line
        # (oblique!)
        self.extension_above: float = get("dimexe", 0.0) * scale

        # Distance of extension line from the measurement point in extension
        # line direction
        self.offset: float = get("dimexo", 0.0) * scale

        # Fixed length extension line, length above dimension line is still
        # self.ext_line_extension
        self.has_fixed_length: bool = bool(get("dimfxlon", 0))

        # Length below the dimension line:
        self.length_below: float = get("dimfxl", self.extension_above) * scale

    def dxfattribs(self, num: int = 1) -> Any:
        """Returns default dimension line DXF attributes as dict."""
        attribs: dict[str, Any] = {"color": self.color}
        if num == 1:
            linetype = self.linetype1
        elif num == 2:
            linetype = self.linetype2
        else:
            raise ValueError(f"invalid argument num:{num}")

        if linetype:
            attribs["linetype"] = linetype
        if self.lineweight != self.default_lineweight:
            attribs["lineweight"] = self.lineweight
        return attribs


class DimensionLine:
    default_lineweight: int = const.LINEWEIGHT_BYBLOCK

    def __init__(self, dim_style: DimStyleOverride, default_color: int, scale: float):
        get = dim_style.get
        self.color: int = get("dimclrd", default_color)  # ACI

        # Dimension line extension, along the dimension line direction ('left'
        # and 'right')
        self.extension: float = get("dimdle", 0.0) * scale
        self.linetype: str = get("dimltype", "")
        self.lineweight: int = get("dimlwd", self.default_lineweight)

        # Suppress first part of the dimension line
        self.suppress1: bool = bool(get("dimsd1", 0))

        # Suppress second part of the dimension line
        self.suppress2: bool = bool(get("dimsd2", 0))

        # Controls whether a dimension line is drawn between the extension lines
        # even when the text is placed outside.
        # For radius and diameter dimensions (when DIMTIX is off), draws a
        # dimension line inside the circle or arc and places the text,
        # arrowheads, and leader outside.
        # 0 = no dimension line
        # 1 = draw dimension line
        # not supported yet - ezdxf behaves like option 1
        self.has_dim_line_if_text_outside: bool = bool(get("dimtofl", 1))

    def dxfattribs(self) -> Any:
        """Returns default dimension line DXF attributes as dict."""
        attribs: dict[str, Any] = {"color": self.color}
        if self.linetype:
            attribs["linetype"] = self.linetype
        if self.lineweight != self.default_lineweight:
            attribs["lineweight"] = self.lineweight
        return attribs


class Arrows:
    def __init__(self, dim_style: DimStyleOverride, color: int, scale: float):
        get = dim_style.get
        self.color: int = get("dimclrd", color)
        self.tick_size: float = get("dimtsz", 0.0) * scale
        self.arrow1_name: str = ""  # empty string is a closed filled arrow
        self.arrow2_name: str = ""  # empty string is a closed filled arrow
        self.arrow_size: float = get("dimasz", 0.25) * scale
        self.suppress1 = False  # ezdxf only
        self.suppress2 = False  # ezdxf only

        if self.tick_size > 0.0:
            # Use oblique strokes as 'arrows', disables usual 'arrows' and user
            # defined blocks tick size is per definition double the size of
            # arrow size adjust arrow size to reuse the 'oblique' arrow block
            self.arrow_size = self.tick_size * 2.0
        else:
            # Arrow name or block name if user defined arrow
            (
                self.arrow1_name,
                self.arrow2_name,
            ) = dim_style.get_arrow_names()

    @property
    def has_ticks(self) -> bool:
        return self.tick_size > 0.0

    def dxfattribs(self) -> Any:
        return {"color": self.color}


class Measurement:
    def __init__(
        self,
        dim_style: DimStyleOverride,
        color: int,
        scale: float,
    ):
        # update this values in method Measurement.update()
        # raw measured value
        self.raw_value: float = 0.0
        # scaled measured value
        self.value: float = 0.0
        # Final formatted dimension text
        self.text: str = ""

        dimension = dim_style.dimension
        doc = dimension.doc
        assert doc is not None, "valid DXF document required"

        # ezdxf specific attributes beyond DXF reference, therefore not stored
        # in the DXF file (DSTYLE).
        # Some of these are just an rendering effect, which will be ignored by
        # CAD applications if they modify the DIMENSION entity

        # User location override as UCS coordinates, stored as text_midpoint in
        # the DIMENSION entity
        self.user_location: Optional[Vec2] = OptionalVec2(
            dim_style.pop("user_location", None)
        )

        # User location override relative to dimline center if True
        self.relative_user_location: bool = dim_style.pop(
            "relative_user_location", False
        )

        # Shift text away from default text location - implemented as user
        # location override without leader
        # Shift text along in text direction:
        self.text_shift_h: float = dim_style.pop("text_shift_h", 0.0)
        # Shift text perpendicular to text direction:
        self.text_shift_v: float = dim_style.pop("text_shift_v", 0.0)
        # End of ezdxf specific attributes

        get = dim_style.get
        # ezdxf locates attachment points always in the text center.
        # Fixed predefined value for ezdxf rendering:
        self.text_attachment_point: int = 5

        # Ignored by ezdxf:
        self.horizontal_direction: Optional[float] = dimension.get_dxf_attrib(
            "horizontal_direction", None
        )

        # Dimension measurement factor:
        self.measurement_factor: float = get("dimlfac", 1.0)

        # Text style
        style_name: str = get("dimtxsty", options.default_dimension_text_style)
        if style_name not in doc.tables.styles:
            style_name = "Standard"
        self.text_style_name: str = style_name
        text_style = get_text_style(doc, style_name)
        self.text_height: float = get_char_height(dim_style, text_style) * scale
        self.text_width_factor: float = text_style.get_dxf_attrib("width", 1.0)
        self.stored_dim_text: str = dimension.dxf.text

        # text_gap: gap between dimension line an dimension text
        self.text_gap: float = get("dimgap", 0.625) * scale

        # User defined text rotation - overrides everything:
        self.user_text_rotation: float = dimension.get_dxf_attrib("text_rotation", None)
        # calculated text rotation
        self.text_rotation: float = self.user_text_rotation
        self.text_color: int = get("dimclrt", color)  # ACI
        self.text_round: Optional[float] = get("dimrnd", None)
        self.decimal_places: int = get("dimdec", 2)
        self.angular_decimal_places: int = get("dimadec", 2)

        # Controls the suppression of zeros in the primary unit value.
        # Values 0-3 affect feet-and-inch dimensions only and are not supported
        # 4 (Bit 3) = Suppresses leading zeros in decimal dimensions,
        #   e.g. 0.5000 becomes .5000
        # 8 (Bit 4) = Suppresses trailing zeros in decimal dimensions,
        #   e.g. 12.5000 becomes 12.5
        # 12 (Bit 3+4) = Suppresses both leading and trailing zeros,
        #   e.g. 0.5000 becomes .5)
        self.suppress_zeros: int = get("dimzin", 8)

        # special setting for angular dimensions  (dimzin << 2) & 3
        # 0 = Displays all leading and trailing zeros
        # 1 = Suppresses leading zeros (for example, 0.5000 becomes .5000)
        # 2 = Suppresses trailing zeros (for example, 12.5000 becomes 12.5)
        # 3 = Suppresses leading and trailing zeros (for example, 0.5000 becomes .5)
        self.angular_suppress_zeros: int = get("dimazin", 2)

        # decimal separator char, default is ",":
        self.decimal_separator: str = dim_style.get_decimal_separator()

        self.text_post_process_format: str = get("dimpost", "")
        # text_fill:
        # 0 = None
        # 1 = Background
        # 2 = DIMTFILLCLR
        self.text_fill: int = get("dimtfill", 0)
        self.text_fill_color: int = get("dimtfillclr", 1)  # ACI
        self.text_box_fill_scale: float = 1.1

        # text_halign:
        # 0 = center
        # 1 = left
        # 2 = right
        # 3 = above ext1
        # 4 = above ext2
        self.text_halign: int = get("dimjust", 0)

        # text_valign:
        # 0 = center
        # 1 = above
        # 2 = farthest away?
        # 3 = JIS;
        # 4 = below
        # Options 2, 3 are ignored by ezdxf
        self.text_valign: int = get("dimtad", 0)

        # Controls the vertical position of dimension text above or below the
        # dimension line, when DIMTAD = 0.
        # The magnitude of the vertical offset of text is the product of the
        # text height (+gap?) and DIMTVP.
        # Setting DIMTVP to 1.0 is equivalent to setting DIMTAD = 1.
        self.text_vertical_position: float = get("dimtvp", 0.0)

        # Move text freely:
        # 0 = Moves the dimension line with dimension text
        # 1 = Adds a leader when dimension text is moved
        # 2 = Allows text to be moved freely without a leader
        self.text_movement_rule: int = get("dimtmove", 2)

        self.has_leader: bool = (
            self.user_location is not None and self.text_movement_rule == 1
        )

        # text_rotation is 0 if dimension text is 'inside', ezdxf defines
        # 'inside' as at the default text location:
        self.text_inside_horizontal: bool = get("dimtih", 0)

        # text_rotation is 0 if dimension text is 'outside', ezdxf defines
        # 'outside' as NOT at the default text location:
        self.text_outside_horizontal: bool = get("dimtoh", 0)

        # Force text location 'inside', even if the text should be moved
        # 'outside':
        self.force_text_inside: bool = bool(get("dimtix", 0))

        # How dimension text and arrows are arranged when space is not
        # sufficient to place both 'inside':
        # 0 = Places both text and arrows outside extension lines
        # 1 = Moves arrows first, then text
        # 2 = Moves text first, then arrows
        # 3 = Moves either text or arrows, whichever fits best
        # not supported - ezdxf behaves like 2
        self.text_fitting_rule: int = get("dimatfit", 2)

        # Units for all dimension types except Angular.
        # 1 = Scientific
        # 2 = Decimal
        # 3 = Engineering
        # 4 = Architectural (always displayed stacked)
        # 5 = Fractional (always displayed stacked)
        # not supported - ezdxf behaves like 2
        self.length_unit: int = get("dimlunit", 2)

        # Fraction format when DIMLUNIT is set to 4 (Architectural) or
        # 5 (Fractional).
        # 0 = Horizontal stacking
        # 1 = Diagonal stacking
        # 2 = Not stacked (for example, 1/2)
        self.fraction_format: int = get("dimfrac", 0)  # not supported

        # Units format for angular dimensions
        # 0 = Decimal degrees
        # 1 = Degrees/minutes/seconds
        # 2 = Grad
        # 3 = Radians
        self.angle_units: int = get("dimaunit", 0)

        self.has_arc_length_prefix: bool = False
        if get("dimarcsym", 2) == 0:
            self.has_arc_length_prefix = True

        # Text_outside is only True if really placed outside of default text
        # location
        # remark: user defined text location is always outside per definition
        # (not by real location)
        self.text_is_outside: bool = False

        # Final calculated or overridden dimension text location
        self.text_location: Vec2 = Vec2(0, 0)

        # True if dimension text doesn't fit between extension lines
        self.is_wide_text: bool = False

        # Text rotation was corrected to make upside down text better readable
        self.has_upside_down_correction: bool = False

    @property
    def text_is_inside(self):
        return not self.text_is_outside

    @property
    def has_relative_text_movement(self):
        return bool(self.text_shift_h or self.text_shift_v)

    def apply_text_shift(self, location: Vec2, text_rotation: float) -> Vec2:
        """Add `self.text_shift_h` and `sel.text_shift_v` to point `location`,
        shifting along and perpendicular to text orientation defined by
        `text_rotation`.

        Args:
            location: location point
            text_rotation: text rotation in degrees

        Returns: new location

        """
        shift_vec = Vec2((self.text_shift_h, self.text_shift_v))
        location += shift_vec.rotate_deg(text_rotation)
        return location

    @property
    def vertical_placement(self) -> float:
        """Returns vertical placement of dimension text as 1 for above, 0 for
        center and -1 for below dimension line.

        """
        if self.text_valign == 0:
            return 0
        elif self.text_valign == 4:
            return -1
        else:
            return 1

    def text_vertical_distance(self) -> float:
        """Returns the vertical distance for dimension line to text midpoint.
        Positive values are above the line, negative values are below the line.

        """
        if self.text_valign == 0:
            return self.text_height * self.text_vertical_position
        else:
            return (self.text_height / 2.0 + self.text_gap) * self.vertical_placement

    def text_width(self, text: str) -> float:
        """
        Return width of `text` in drawing units.

        """
        # todo: use matplotlib support
        char_width = self.text_height * self.text_width_factor
        return len(text) * char_width

    def text_override(self, measurement: float) -> str:
        """Create dimension text for `measurement` in drawing units and applies
        text overriding properties.

        """
        text = self.stored_dim_text
        if text == " ":  # suppresses text
            return ""
        formatted_measurement = self.format_text(measurement)
        if text:
            # only replace the first "<>", like BricsCAD
            return text.replace("<>", formatted_measurement, 1)
        return formatted_measurement

    def location_override(self, location: UVec, leader=False, relative=False) -> None:
        """Set user defined dimension text location. ezdxf defines a user
        defined location per definition as 'outside'.

        Args:
            location: text midpoint
            leader: use leader or not (movement rules)
            relative: is location absolute (in UCS) or relative to dimension
                line center.

        """
        self.user_location = Vec2(location)
        self.text_movement_rule = 1 if leader else 2
        self.relative_user_location = relative
        self.text_is_outside = True

    def dxfattribs(self) -> Any:
        return {"color": self.text_color}

    @abc.abstractmethod
    def update(self, raw_measurement_value: float) -> None:
        """Update raw measurement value, scaled measurement value and
        dimension text.

        """

    @abc.abstractmethod
    def format_text(self, value: float) -> str:
        """Rounding and text formatting of `value`, removes leading and
        trailing zeros if necessary.

        """


class LengthMeasurement(Measurement):
    def update(self, raw_measurement_value: float) -> None:
        """Update raw measurement value, scaled measurement value and
        dimension text.
        """
        self.raw_value = raw_measurement_value
        self.value = raw_measurement_value * self.measurement_factor
        self.text = self.text_override(self.value)

    def format_text(self, value: float) -> str:
        """Rounding and text formatting of `value`, removes leading and
        trailing zeros if necessary.

        """
        text = format_text(
            value,
            self.text_round,
            self.decimal_places,
            self.suppress_zeros,
            self.decimal_separator,
        )
        if self.text_post_process_format:
            text = apply_dimpost(text, self.text_post_process_format)
        return text


class Geometry:
    """
    Geometry layout entities are located in the OCS defined by the extrusion
    vector of the DIMENSION entity and the z-axis of the OCS
    point 'text_midpoint' (group code 11).

    """

    def __init__(
        self,
        dimension: Dimension,
        ucs: UCS,
        layout: "GenericLayoutType",
    ):
        assert dimension.doc is not None, "valid DXF document required"
        self.dimension: Dimension = dimension
        self.doc: Drawing = dimension.doc
        self.dxfversion: str = self.doc.dxfversion
        self.supports_dxf_r2000: bool = self.dxfversion >= "AC1015"
        self.supports_dxf_r2007: bool = self.dxfversion >= "AC1021"
        self.ucs: UCS = ucs
        self.extrusion: Vec3 = ucs.uz
        self.requires_extrusion: bool = not self.extrusion.isclose(Z_AXIS)
        self.layout: GenericLayoutType = layout
        self._text_box: TextBox = TextBox()

    @property
    def has_text_box(self) -> bool:
        return self._text_box.width > 0.0 and self._text_box.height > 0.0

    def set_layout(self, layout: GenericLayoutType) -> None:
        self.layout = layout

    def set_text_box(self, text_box: TextBox) -> None:
        self._text_box = text_box

    def has_block(self, name: str) -> bool:
        return name in self.doc.blocks

    def add_arrow_blockref(
        self,
        name: str,
        insert: Vec2,
        size: float,
        rotation: float,
        dxfattribs,
    ) -> None:
        # OCS of the arrow blocks is defined by the DIMENSION entity!
        # Therefore remove OCS elevation, the elevation is defined by the
        # DIMENSION 'text_midpoint' (group code 11) and do not set 'extrusion'
        # either!
        insert = self.ucs.to_ocs(Vec3(insert)).vec2
        rotation = self.ucs.to_ocs_angle_deg(rotation)
        self.layout.add_arrow_blockref(name, insert, size, rotation, dxfattribs)

    def add_blockref(
        self,
        name: str,
        insert: Vec2,
        rotation: float,
        dxfattribs,
    ) -> None:
        # OCS of the arrow blocks is defined by the DIMENSION entity!
        # Therefore remove OCS elevation, the elevation is defined by the
        # DIMENSION 'text_midpoint' (group code 11) and do not set 'extrusion'
        # either!
        insert = self.ucs.to_ocs(Vec3(insert)).vec2
        dxfattribs["rotation"] = self.ucs.to_ocs_angle_deg(rotation)
        self.layout.add_blockref(name, insert, dxfattribs)

    def add_text(self, text: str, pos: Vec2, rotation: float, dxfattribs) -> None:
        dxfattribs["rotation"] = self.ucs.to_ocs_angle_deg(rotation)
        entity = self.layout.add_text(text, dxfattribs=dxfattribs)
        # OCS of the measurement text is defined by the DIMENSION entity!
        # Therefore remove OCS elevation, the elevation is defined by the
        # DIMENSION 'text_midpoint' (group code 11) and do not set 'extrusion'
        # either!
        entity.set_placement(
            self.ucs.to_ocs(Vec3(pos)).vec2,
            align=TextEntityAlignment.MIDDLE_CENTER,
        )

    def add_mtext(self, text: str, pos: Vec2, rotation: float, dxfattribs) -> None:
        # OCS of the measurement text is defined by the DIMENSION entity!
        # Therefore remove OCS elevation, the elevation is defined by the
        # DIMENSION 'text_midpoint' (group code 11) and do not set 'extrusion'
        # either!
        dxfattribs["rotation"] = self.ucs.to_ocs_angle_deg(rotation)
        dxfattribs["insert"] = self.ucs.to_ocs(Vec3(pos)).vec2
        self.layout.add_mtext(text, dxfattribs)

    def add_defpoints(self, points: Iterable[Vec2]) -> None:
        attribs = {
            "layer": "Defpoints",
        }
        for point in points:
            # Despite the fact that the POINT entity has WCS coordinates,
            # the coordinates of defpoints in DIMENSION entities have OCS
            # coordinates.
            location = self.ucs.to_ocs(Vec3(point)).replace(z=0.0)
            self.layout.add_point(location, dxfattribs=attribs)

    def add_line(
        self,
        start: Vec2,
        end: Vec2,
        dxfattribs,
        remove_hidden_lines=False,
    ) -> None:
        """Add a LINE entity to the geometry layout. Removes parts of the line
        hidden by dimension text if `remove_hidden_lines` is True.

        Args:
            start: start point of line
            end: end point of line
            dxfattribs: additional or overridden DXF attributes
            remove_hidden_lines: removes parts of the line hidden by dimension
                text if ``True``

        """

        def add_line_to_block(start, end):
            # LINE is handled like an OCS entity !?
            self.layout.add_line(
                to_ocs(Vec3(start)).vec2,
                to_ocs(Vec3(end)).vec2,
                dxfattribs=dxfattribs,
            )

        def order(a: Vec2, b: Vec2) -> tuple[Vec2, Vec2]:
            if (start - a).magnitude < (start - b).magnitude:
                return a, b
            else:
                return b, a

        to_ocs = self.ucs.to_ocs
        if remove_hidden_lines and self.has_text_box:
            text_box = self._text_box
            start_inside = int(text_box.is_inside(start))
            end_inside = int(text_box.is_inside(end))
            inside = start_inside + end_inside
            if inside == 2:  # start and end inside text_box
                return  # do not draw line
            elif inside == 1:  # one point inside text_box or on a border line
                intersection_points = text_box.intersect(ConstructionLine(start, end))
                if len(intersection_points) == 1:
                    # one point inside one point outside -> one intersection point
                    p1 = intersection_points[0]
                else:
                    # second point on a text box border line
                    p1, _ = order(*intersection_points)
                p2 = start if end_inside else end
                add_line_to_block(p1, p2)
                return
            else:
                intersection_points = text_box.intersect(ConstructionLine(start, end))
                if len(intersection_points) == 2:
                    # sort intersection points by distance to start point
                    p1, p2 = order(intersection_points[0], intersection_points[1])
                    # line[start-p1] - gap - line[p2-end]
                    add_line_to_block(start, p1)
                    add_line_to_block(p2, end)
                    return
                # else: fall through
        add_line_to_block(start, end)

    def add_arc(
        self,
        center: Vec2,
        radius: float,
        start_angle: float,
        end_angle: float,
        dxfattribs=None,
        remove_hidden_lines=False,
    ) -> None:
        """Add a ARC entity to the geometry layout. Removes parts of the arc
        hidden by dimension text if `remove_hidden_lines` is True.

        Args:
            center: center of arc
            radius: radius of arc
            start_angle: start angle in radians
            end_angle: end angle in radians
            dxfattribs: additional or overridden DXF attributes
            remove_hidden_lines: removes parts of the arc hidden by dimension
                text if ``True``

        """

        def add_arc(s: float, e: float) -> None:
            """Add ARC entity to geometry block."""
            self.layout.add_arc(
                center=ocs_center,
                radius=radius,
                start_angle=math.degrees(ocs_angle(s)),
                end_angle=math.degrees(ocs_angle(e)),
                dxfattribs=dxfattribs,
            )

        # OCS of the ARC is defined by the DIMENSION entity!
        # Therefore remove OCS elevation, the elevation is defined by the
        # DIMENSION 'text_midpoint' (group code 11) and do not set 'extrusion'
        # either!
        ocs_center = self.ucs.to_ocs(Vec3(center)).vec2
        ocs_angle = self.ucs.to_ocs_angle_rad
        if remove_hidden_lines and self.has_text_box:
            for start, end in visible_arcs(
                center,
                radius,
                start_angle,
                end_angle,
                self._text_box,
            ):
                add_arc(start, end)
        else:
            add_arc(start_angle, end_angle)


class BaseDimensionRenderer:
    """Base rendering class for DIMENSION entities."""

    def __init__(
        self,
        dimension: Dimension,
        ucs: Optional[UCS] = None,
        override: Optional[DimStyleOverride] = None,
    ):
        self.dimension: Dimension = dimension
        self.geometry = self.init_geometry(dimension, ucs)

        # DimStyleOverride object, manages dimension style overriding
        self.dim_style: DimStyleOverride
        if override:
            self.dim_style = override
        else:
            self.dim_style = DimStyleOverride(dimension)

        # ---------------------------------------------
        # GENERAL PROPERTIES
        # ---------------------------------------------
        self.default_color: int = self.dimension.dxf.color  # ACI
        self.default_layer: str = self.dimension.dxf.layer

        get = self.dim_style.get
        # Overall scaling of DIMENSION entity:
        self.dim_scale: float = get("dimscale", 1.0)
        if abs(self.dim_scale) < 1e-9:
            self.dim_scale = 1.0

        # Controls drawing of circle or arc center marks and center lines, for
        # DIMDIAMETER and DIMRADIUS, the center mark is drawn only if you place
        # the dimension line outside the circle or arc.
        # 0 = No center marks or lines are drawn
        # <0 = Center lines are drawn
        # >0 = Center marks are drawn
        self.dim_center_marks: int = get("dimcen", 0)

        self.measurement = self.init_measurement(self.default_color, self.dim_scale)
        self.dimension_line: DimensionLine = self.init_dimension_line(
            self.default_color, self.dim_scale
        )
        self.arrows: Arrows = self.init_arrows(self.default_color, self.dim_scale)
        # Suppress arrow rendering - only rendering is suppressed (rendering
        # effect).
        # All placing related calculations are done without this settings.
        # Used for multi point linear dimensions to avoid double rendering of
        # non arrow ticks. These are ezdxf specific attributes!
        self.arrows.suppress1 = self.dim_style.pop("suppress_arrow1", False)
        self.arrows.suppress2 = self.dim_style.pop("suppress_arrow2", False)

        self.extension_lines: ExtensionLines = self.init_extension_lines(
            self.default_color, self.dim_scale
        )
        # tolerances have to be initialized after measurement:
        self.tol: Tolerance = self.init_tolerance(self.dim_scale, self.measurement)

        # Update text height
        self.measurement.text_height = max(
            self.measurement.text_height, self.tol.text_height
        )

    def init_geometry(self, dimension: Dimension, ucs: Optional[UCS] = None):
        from ezdxf.layouts import VirtualLayout

        return Geometry(dimension, ucs or PassTroughUCS(), VirtualLayout())

    def init_tolerance(self, scale: float, measurement: Measurement) -> Tolerance:
        return Tolerance(
            self.dim_style,
            cap_height=measurement.text_height,
            width_factor=measurement.text_width_factor,
            dim_scale=scale,
        )

    def init_extension_lines(self, color: int, scale: float) -> ExtensionLines:
        return ExtensionLines(self.dim_style, color, scale)

    def init_dimension_line(self, color: int, scale: float) -> DimensionLine:
        return DimensionLine(self.dim_style, color, scale)

    def init_arrows(self, color: int, scale: float) -> Arrows:
        return Arrows(self.dim_style, color, scale)

    def init_measurement(self, color: int, scale: float) -> Measurement:
        return LengthMeasurement(self.dim_style, color, scale)

    def init_text_box(self) -> TextBox:
        measurement = self.measurement
        return TextBox(
            center=measurement.text_location,
            width=self.total_text_width(),
            height=measurement.text_height,
            angle=measurement.text_rotation or 0.0,
            # The currently used monospaced abstract font, returns a too large
            # text width.
            # Therefore, the horizontal text gap is ignored at all - yet!
            hgap=0.0,
            # Arbitrary choice to reduce the too large vertical gap!
            vgap=measurement.text_gap * 0.75,
        )

    def get_required_defpoint(self, name: str) -> Vec2:
        return get_required_defpoint(self.dimension, name)

    def render(self, block: GenericLayoutType):
        # Block entities are located in the OCS defined by the extrusion vector
        # of the DIMENSION entity and the z-axis of the OCS point
        # 'text_midpoint' (group code 11).
        self.geometry.set_layout(block)
        # Tolerance requires MTEXT support, switch off rendering of tolerances
        # and limits
        if not self.geometry.supports_dxf_r2000:
            self.tol.disable()

    def total_text_width(self) -> float:
        width = 0.0
        text = self.measurement.text
        if text:
            if self.tol.has_limits:  # only limits are displayed
                width = self.tol.text_width
            else:
                width = self.measurement.text_width(text)
                if self.tol.has_tolerance:
                    width += self.tol.text_width
        return width

    def default_attributes(self) -> dict[str, Any]:
        """Returns default DXF attributes as dict."""
        return {
            "layer": self.default_layer,
            "color": self.default_color,
        }

    def location_override(self, location: UVec, leader=False, relative=False) -> None:
        """Set user defined dimension text location. ezdxf defines a user
        defined location per definition as 'outside'.

        Args:
            location: text midpoint
            leader: use leader or not (movement rules)
            relative: is location absolute (in UCS) or relative to dimension
                line center.

        """
        self.dim_style.set_location(location, leader, relative)
        self.measurement.location_override(location, leader, relative)

    def add_line(
        self,
        start: Vec2,
        end: Vec2,
        dxfattribs,
        remove_hidden_lines=False,
    ) -> None:
        """Add a LINE entity to the dimension BLOCK. Remove parts of the line
        hidden by dimension text if `remove_hidden_lines` is True.

        Args:
            start: start point of line
            end: end point of line
            dxfattribs: additional or overridden DXF attributes
            remove_hidden_lines: removes parts of the line hidden by dimension
                text if ``True``

        """

        attribs = self.default_attributes()
        if dxfattribs:
            attribs.update(dxfattribs)
        self.geometry.add_line(start, end, dxfattribs, remove_hidden_lines)

    def add_arc(
        self,
        center: Vec2,
        radius: float,
        start_angle: float,
        end_angle: float,
        dxfattribs=None,
        remove_hidden_lines=False,
    ) -> None:
        """Add a ARC entity to the geometry layout. Remove parts of the arc
        hidden by dimension text if `remove_hidden_lines` is True.

        Args:
            center: center of arc
            radius: radius of arc
            start_angle: start angle in radians
            end_angle: end angle in radians
            dxfattribs: additional or overridden DXF attributes
            remove_hidden_lines: removes parts of the arc hidden by dimension
                text if ``True``

        """
        attribs = self.default_attributes()
        if dxfattribs:
            attribs.update(dxfattribs)
        self.geometry.add_arc(
            center, radius, start_angle, end_angle, attribs, remove_hidden_lines
        )

    def add_blockref(
        self,
        name: str,
        insert: Vec2,
        rotation: float,
        scale: float,
        dxfattribs,
    ) -> None:
        """
        Add block references and standard arrows to the dimension BLOCK.

        Args:
            name: block or arrow name
            insert: insertion point in UCS
            rotation: rotation angle in degrees in UCS (x-axis is 0 degrees)
            scale: scaling factor for x- and y-direction
            dxfattribs: additional or overridden DXF attributes

        """
        attribs = self.default_attributes()
        if dxfattribs:
            attribs.update(dxfattribs)

        if name in ARROWS:
            # generates automatically BLOCK definitions for arrows if needed
            self.geometry.add_arrow_blockref(name, insert, scale, rotation, attribs)
        else:
            if name is None or not self.geometry.has_block(name):
                raise DXFUndefinedBlockError(f'Undefined block: "{name}"')
            if scale != 1.0:
                attribs["xscale"] = scale
                attribs["yscale"] = scale
            self.geometry.add_blockref(name, insert, rotation, attribs)

    def add_text(self, text: str, pos: Vec2, rotation: float, dxfattribs) -> None:
        """
        Add TEXT (DXF R12) or MTEXT (DXF R2000+) entity to the dimension BLOCK.

        Args:
            text: text as string
            pos: insertion location in UCS
            rotation: rotation angle in degrees in UCS (x-axis is 0 degrees)
            dxfattribs: additional or overridden DXF attributes

        """
        geometry = self.geometry
        measurement = self.measurement
        attribs = self.default_attributes()
        attribs["style"] = measurement.text_style_name
        attribs["color"] = measurement.text_color

        if geometry.supports_dxf_r2000:  # use MTEXT entity
            attribs["char_height"] = measurement.text_height
            attribs["attachment_point"] = measurement.text_attachment_point
            if measurement.text_fill:
                attribs["box_fill_scale"] = measurement.text_box_fill_scale
                attribs["bg_fill_color"] = measurement.text_fill_color
                attribs["bg_fill"] = 3 if measurement.text_fill == 1 else 1

            if dxfattribs:
                attribs.update(dxfattribs)
            geometry.add_mtext(text, pos, rotation, dxfattribs=attribs)
        else:  # use TEXT entity
            attribs["height"] = measurement.text_height
            if dxfattribs:
                attribs.update(dxfattribs)
            geometry.add_text(text, pos, rotation, dxfattribs=attribs)

    def add_leader(self, p1: Vec2, p2: Vec2, p3: Vec2):
        """
        Add simple leader line from p1 to p2 to p3.

        Args:
            p1: target point
            p2: first text point
            p3: second text point

        """
        # use only  color and ignore linetype!
        dxfattribs = {"color": self.dimension_line.color}
        self.add_line(p1, p2, dxfattribs)
        self.add_line(p2, p3, dxfattribs)

    def transform_ucs_to_wcs(self) -> None:
        """Transforms dimension definition points into WCS or if required into
        OCS.

        Can not be called in __init__(), because inherited classes may be need
        unmodified values.

        """
        pass

    def finalize(self) -> None:
        self.transform_ucs_to_wcs()
        if self.geometry.requires_extrusion:
            self.dimension.dxf.extrusion = self.geometry.extrusion


def order_leader_points(p1: Vec2, p2: Vec2, p3: Vec2) -> tuple[Vec2, Vec2]:
    if (p1 - p2).magnitude > (p1 - p3).magnitude:
        return p3, p2
    else:
        return p2, p3


def get_center_leader_points(
    target_point: Vec2, text_box: TextBox, leg_length: float
) -> tuple[Vec2, Vec2]:
    """Returns the leader points of the "leg" for a vertical centered leader."""
    c0, c1, c2, c3 = text_box.corners
    #              c3-------c2
    # left leg /---x  text  x---\ right leg
    #         /    c0-------c1   \
    left_center = c0.lerp(c3)
    right_center = c1.lerp(c2)
    connection_point = left_center
    leg_vector = (c0 - c1).normalize(leg_length)
    if target_point.distance(left_center) > target_point.distance(right_center):
        connection_point = right_center
        leg_vector = -leg_vector
    # leader line: target_point -> leg_point -> connection_point
    # The text gap between the text and the connection point is already included
    # in the text_box corners!
    # Do not order leader points!
    return connection_point + leg_vector, connection_point


def get_required_defpoint(dim: Dimension, name: str) -> Vec2:
    dxf = dim.dxf
    if dxf.hasattr(name):  # has to exist, ignore default value!
        return Vec2(dxf.get(name))
    raise const.DXFMissingDefinitionPoint(name)


def visible_arcs(
    center: Vec2,
    radius: float,
    start_angle: float,
    end_angle: float,
    box: ConstructionBox,
) -> list[tuple[float, float]]:
    """Returns the visible parts of an arc intersecting with a construction box
    as (start angle, end angle) tuples.

    Args:
        center: center of the arc
        radius: radius of the arc
        start_angle: start angle of arc in radians
        end_angle: end angle of arc in radians
        box: construction box which may intersect the arc

    """

    intersection_angles: list[float] = []  # angles are in the range 0 to 2pi
    start_angle %= math.tau
    end_angle %= math.tau
    arc = ConstructionArc(
        center, radius, math.degrees(start_angle), math.degrees(end_angle)
    )
    for line in box.border_lines():
        for intersection_point in arc.intersect_line(line):
            angle = (intersection_point - center).angle % math.tau
            if not intersection_angles:
                intersection_angles.append(angle)
            # new angle should be different than the last added angle:
            elif not math.isclose(intersection_angles[-1], angle):
                intersection_angles.append(angle)
    # Arc has to intersect the box in exact two locations!
    if len(intersection_angles) == 2:
        if start_angle > end_angle:  # arc passes 0 degrees
            intersection_angles = [
                (a if a >= start_angle else a + math.tau) for a in intersection_angles
            ]
        intersection_angles.sort()
        return [
            (start_angle, intersection_angles[0]),
            (intersection_angles[1], end_angle),
        ]
    else:
        # Ignore cases where the start- or the end point is inside the box.
        # Ignore cases where the box touches the arc in one point.
        return [(start_angle, end_angle)]


def get_text_style(doc: "Drawing", name: str) -> Textstyle:
    assert doc is not None, "valid DXF document required"
    get_style = doc.tables.styles.get
    try:
        style = get_style(name)
    except const.DXFTableEntryError:
        style = get_style("Standard")
    return cast("Textstyle", style)


def get_char_height(dim_style: DimStyleOverride, text_style: Textstyle) -> float:
    """Unscaled character height defined by text style or DIMTXT."""
    height: float = text_style.dxf.get("height", 0.0)
    if height == 0.0:  # variable text height (not fixed)
        height = dim_style.get("dimtxt", 1.0)
    return height


def compile_mtext(measurement: Measurement, tol: Tolerance) -> str:
    text = measurement.text
    if tol.enabled:
        text = tol.compile_mtext(text)
    return text
