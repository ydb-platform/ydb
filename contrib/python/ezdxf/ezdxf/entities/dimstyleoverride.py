# Copyright (c) 2019-2023 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Any, TYPE_CHECKING, Optional
from typing_extensions import Protocol
import logging

from ezdxf.enums import MTextLineAlignment
from ezdxf.lldxf import const
from ezdxf.lldxf.const import DXFAttributeError, DIMJUST, DIMTAD
from ezdxf.math import Vec3, UVec, UCS
from ezdxf.render.arrows import ARROWS

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DimStyle, Dimension
    from ezdxf.render.dim_base import BaseDimensionRenderer
    from ezdxf import xref

logger = logging.getLogger("ezdxf")


class SupportsOverride(Protocol):
    def override(self) -> DimStyleOverride:
        ...


class DimStyleOverride:
    def __init__(self, dimension: Dimension, override: Optional[dict] = None):
        self.dimension = dimension
        dim_style_name: str = dimension.get_dxf_attrib("dimstyle", "STANDARD")
        self.dimstyle: DimStyle = self.doc.dimstyles.get(dim_style_name)
        self.dimstyle_attribs: dict = self.get_dstyle_dict()

        # Special ezdxf attributes beyond the DXF reference, therefore not
        # stored in the DSTYLE data.
        # These are only rendering effects or data transfer objects
        # user_location: Vec3 - user location override if not None
        # relative_user_location: bool - user location override relative to
        #   dimline center if True
        # text_shift_h: float - shift text in text direction, relative to
        #   standard text location
        # text_shift_v: float - shift text perpendicular to text direction,
        #   relative to standard text location
        self.update(override or {})

    @property
    def doc(self) -> Drawing:
        """Drawing object (internal API)"""
        return self.dimension.doc  # type: ignore

    @property
    def dxfversion(self) -> str:
        """DXF version (internal API)"""
        return self.doc.dxfversion

    def get_dstyle_dict(self) -> dict:
        """Get XDATA section ACAD:DSTYLE, to override DIMSTYLE attributes for
        this DIMENSION entity.

        Returns a ``dict`` with DIMSTYLE attribute names as keys.

        (internal API)
        """
        return self.dimension.get_acad_dstyle(self.dimstyle)

    def get(self, attribute: str, default: Any = None) -> Any:
        """Returns DIMSTYLE `attribute` from override dict
        :attr:`dimstyle_attribs` or base :class:`DimStyle`.

        Returns `default` value for attributes not supported by DXF R12. This
        is a hack to use the same algorithm to render DXF R2000 and DXF R12
        DIMENSION entities. But the DXF R2000 attributes are not stored in the
        DXF R12 file! This method does not catch invalid attribute names!
        Check debug log for ignored DIMSTYLE attributes.

        """
        if attribute in self.dimstyle_attribs:
            result = self.dimstyle_attribs[attribute]
        else:
            try:
                result = self.dimstyle.get_dxf_attrib(attribute, default)
            except DXFAttributeError:
                result = default
        return result

    def pop(self, attribute: str, default: Any = None) -> Any:
        """Returns DIMSTYLE `attribute` from override dict :attr:`dimstyle_attribs` and
        removes this `attribute` from override dict.
        """
        value = self.get(attribute, default)
        # delete just from override dict
        del self[attribute]
        return value

    def update(self, attribs: dict) -> None:
        """Update override dict :attr:`dimstyle_attribs`.

        Args:
            attribs: ``dict`` of DIMSTYLE attributes

        """
        self.dimstyle_attribs.update(attribs)

    def __getitem__(self, key: str) -> Any:
        """Returns DIMSTYLE attribute `key`, see also :meth:`get`."""
        return self.get(key)

    def __setitem__(self, key: str, value: Any) -> None:
        """Set DIMSTYLE attribute `key` in :attr:`dimstyle_attribs`."""
        self.dimstyle_attribs[key] = value

    def __delitem__(self, key: str) -> None:
        """Deletes DIMSTYLE attribute `key` from :attr:`dimstyle_attribs`,
        ignores :class:`KeyErrors` silently.
        """
        try:
            del self.dimstyle_attribs[key]
        except KeyError:  # silent discard
            pass

    def register_resources_r12(self, registry: xref.Registry) -> None:
        # DXF R2000+ references overridden resources by group code 1005 handles in the
        # XDATA section, which are automatically mapped by the parent class DXFEntity!
        assert self.doc.dxfversion == const.DXF12
        # register arrow heads
        for attrib_name in ("dimblk", "dimblk1", "dimblk2", "dimldrblk"):
            arrow_name = self.get(attrib_name, "")
            if arrow_name:
                # arrow head names will be renamed like user blocks
                # e.g. "_DOT" -> "xref$0$_DOT"
                registry.add_block_name(ARROWS.block_name(arrow_name))
        # linetype and text style attributes are not supported by DXF R12!

    def map_resources_r12(
        self, copy: SupportsOverride, mapping: xref.ResourceMapper
    ) -> None:
        # DXF R2000+ references overridden resources by group code 1005 handles in the
        # XDATA section, which are automatically mapped by the parent class DXFEntity!
        assert self.doc.dxfversion == const.DXF12
        copy_override = copy.override()
        # map arrow heads
        for attrib_name in ("dimblk", "dimblk1", "dimblk2", "dimldrblk"):
            arrow_name = self.get(attrib_name, "")
            if arrow_name:
                block_name = mapping.get_block_name(ARROWS.block_name(arrow_name))
                copy_override[attrib_name] = ARROWS.arrow_name(block_name)
        copy_override.commit()
        # The linetype attributes dimltype, dimltex1 and dimltex2 and the text style
        # attribute dimtxsty are not supported by DXF R12!
        #
        # Weired behavior for DXF R12 detected
        # ------------------------------------
        # BricsCAD writes the handles of overridden linetype- and text style attributes
        # into the ACAD-DSTYLE dictionary like for DXF R2000+, but exports the table
        # entries without handles, so remapping of these handles is only possible if the
        # application (which loads this DXF file) assigns internally the same handles
        # as BricsCAD does and this also works with Autodesk TrueView (oO = wtf!).
        # Ezdxf cannot remap these handles!

    def commit(self) -> None:
        """Writes overridden DIMSTYLE attributes into ACAD:DSTYLE section of
        XDATA of the DIMENSION entity.

        """
        self.dimension.set_acad_dstyle(self.dimstyle_attribs)

    def set_arrows(
        self,
        blk: Optional[str] = None,
        blk1: Optional[str] = None,
        blk2: Optional[str] = None,
        ldrblk: Optional[str] = None,
        size: Optional[float] = None,
    ) -> None:
        """Set arrows or user defined blocks and disable oblique stroke as tick.

        Args:
            blk: defines both arrows at once as name str or user defined block
            blk1: defines left arrow as name str or as user defined block
            blk2: defines right arrow as name str or as user defined block
            ldrblk: defines leader arrow as name str or as user defined block
            size: arrow size in drawing units

        """

        def set_arrow(dimvar: str, name: str) -> None:
            self.dimstyle_attribs[dimvar] = name

        if size is not None:
            self.dimstyle_attribs["dimasz"] = float(size)
        if blk is not None:
            set_arrow("dimblk", blk)
            self.dimstyle_attribs["dimsah"] = 0
            self.dimstyle_attribs["dimtsz"] = 0.0  # use arrows
        if blk1 is not None:
            set_arrow("dimblk1", blk1)
            self.dimstyle_attribs["dimsah"] = 1
            self.dimstyle_attribs["dimtsz"] = 0.0  # use arrows
        if blk2 is not None:
            set_arrow("dimblk2", blk2)
            self.dimstyle_attribs["dimsah"] = 1
            self.dimstyle_attribs["dimtsz"] = 0.0  # use arrows
        if ldrblk is not None:
            set_arrow("dimldrblk", ldrblk)

    def get_arrow_names(self) -> tuple[str, str]:
        """Get arrow names as strings like 'ARCHTICK' as tuple (dimblk1, dimblk2)."""
        dimtsz = self.get("dimtsz", 0)
        blk1, blk2 = "", ""
        if dimtsz == 0.0:
            if bool(self.get("dimsah")):
                blk1 = self.get("dimblk1", "")
                blk2 = self.get("dimblk2", "")
            else:
                blk = self.get("dimblk", "")
                blk1 = blk
                blk2 = blk
        return blk1, blk2

    def get_decimal_separator(self) -> str:
        dimdsep: int = self.get("dimdsep", 0)
        return "," if dimdsep == 0 else chr(dimdsep)

    def set_tick(self, size: float = 1) -> None:
        """Use oblique stroke as tick, disables arrows.

        Args:
            size: arrow size in daring units

        """
        self.dimstyle_attribs["dimtsz"] = float(size)

    def set_text_align(
        self,
        halign: Optional[str] = None,
        valign: Optional[str] = None,
        vshift: Optional[float] = None,
    ) -> None:
        """Set measurement text alignment, `halign` defines the horizontal
        alignment, `valign` defines the vertical alignment, `above1` and
        `above2` means above extension line 1 or 2 and aligned with extension
        line.

        Args:
            halign: `left`, `right`, `center`, `above1`, `above2`,
                requires DXF R2000+
            valign: `above`, `center`, `below`
            vshift: vertical text shift, if `valign` is `center`;
                >0 shift upward, <0 shift downwards

        """
        if halign:
            self.dimstyle_attribs["dimjust"] = DIMJUST[halign.lower()]

        if valign:
            valign = valign.lower()
            self.dimstyle_attribs["dimtad"] = DIMTAD[valign]
            if valign == "center" and vshift is not None:
                self.dimstyle_attribs["dimtvp"] = float(vshift)

    def set_tolerance(
        self,
        upper: float,
        lower: Optional[float] = None,
        hfactor: Optional[float] = None,
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
            lower: lower tolerance value, if None same as upper
            hfactor: tolerance text height factor in relation to the dimension
                text height
            align: tolerance text alignment enum :class:`ezdxf.enums.MTextLineAlignment`
            dec: Sets the number of decimal places displayed
            leading_zeros: suppress leading zeros for decimal dimensions if ``False``
            trailing_zeros: suppress trailing zeros for decimal dimensions if ``False``

        """
        self.dimstyle_attribs["dimtol"] = 1
        self.dimstyle_attribs["dimlim"] = 0
        self.dimstyle_attribs["dimtp"] = float(upper)
        if lower is not None:
            self.dimstyle_attribs["dimtm"] = float(lower)
        else:
            self.dimstyle_attribs["dimtm"] = float(upper)
        if hfactor is not None:
            self.dimstyle_attribs["dimtfac"] = float(hfactor)
        if align is not None:
            self.dimstyle_attribs["dimtolj"] = int(align)
        if dec is not None:
            self.dimstyle_attribs["dimtdec"] = dec

        # Works only with decimal dimensions not inch and feet, US user set
        # dimzin directly
        if leading_zeros is not None or trailing_zeros is not None:
            dimtzin = 0
            if leading_zeros is False:
                dimtzin = const.DIMZIN_SUPPRESSES_LEADING_ZEROS
            if trailing_zeros is False:
                dimtzin += const.DIMZIN_SUPPRESSES_TRAILING_ZEROS
            self.dimstyle_attribs["dimtzin"] = dimtzin

    def set_limits(
        self,
        upper: float,
        lower: float,
        hfactor: Optional[float] = None,
        dec: Optional[int] = None,
        leading_zeros: Optional[bool] = None,
        trailing_zeros: Optional[bool] = None,
    ) -> None:
        """Set limits text format, upper and lower limit values, text
        height factor, number of decimal places or leading and trailing zero
        suppression.

        Args:
            upper: upper limit value added to measurement value
            lower: lower limit value subtracted from measurement value
            hfactor: limit text height factor in relation to the dimension
                text height
            dec: Sets the number of decimal places displayed,
                requires DXF R2000+
            leading_zeros: suppress leading zeros for decimal dimensions if
                ``False``, requires DXF R2000+
            trailing_zeros: suppress trailing zeros for decimal dimensions if
                ``False``, requires DXF R2000+

        """
        # exclusive limits
        self.dimstyle_attribs["dimlim"] = 1
        self.dimstyle_attribs["dimtol"] = 0
        self.dimstyle_attribs["dimtp"] = float(upper)
        self.dimstyle_attribs["dimtm"] = float(lower)
        if hfactor is not None:
            self.dimstyle_attribs["dimtfac"] = float(hfactor)

        # Works only with decimal dimensions not inch and feet, US user set
        # dimzin directly.
        if leading_zeros is not None or trailing_zeros is not None:
            dimtzin = 0
            if leading_zeros is False:
                dimtzin = const.DIMZIN_SUPPRESSES_LEADING_ZEROS
            if trailing_zeros is False:
                dimtzin += const.DIMZIN_SUPPRESSES_TRAILING_ZEROS
            self.dimstyle_attribs["dimtzin"] = dimtzin

        if dec is not None:
            self.dimstyle_attribs["dimtdec"] = int(dec)

    def set_text_format(
        self,
        prefix: str = "",
        postfix: str = "",
        rnd: Optional[float] = None,
        dec: Optional[int] = None,
        sep: Optional[str] = None,
        leading_zeros: Optional[bool] = None,
        trailing_zeros: Optional[bool] = None,
    ) -> None:
        """Set dimension text format, like prefix and postfix string, rounding
        rule and number of decimal places.

        Args:
            prefix: dimension text prefix text as string
            postfix: dimension text postfix text as string
            rnd: Rounds all dimensioning distances to the specified value, for
                instance, if DIMRND is set to 0.25, all distances round to the
                nearest 0.25 unit. If you set DIMRND to 1.0, all distances round
                to the nearest integer.
            dec: Sets the number of decimal places displayed for the primary
                units of a dimension. requires DXF R2000+
            sep: "." or "," as decimal separator
            leading_zeros: suppress leading zeros for decimal dimensions if ``False``
            trailing_zeros: suppress trailing zeros for decimal dimensions if ``False``

        """
        if prefix or postfix:
            self.dimstyle_attribs["dimpost"] = prefix + "<>" + postfix
        if rnd is not None:
            self.dimstyle_attribs["dimrnd"] = rnd
        if dec is not None:
            self.dimstyle_attribs["dimdec"] = dec
        if sep is not None:
            self.dimstyle_attribs["dimdsep"] = ord(sep)
        # Works only with decimal dimensions not inch and feet, US user set
        # dimzin directly.
        if leading_zeros is not None or trailing_zeros is not None:
            dimzin = 0
            if leading_zeros is False:
                dimzin = const.DIMZIN_SUPPRESSES_LEADING_ZEROS
            if trailing_zeros is False:
                dimzin += const.DIMZIN_SUPPRESSES_TRAILING_ZEROS
            self.dimstyle_attribs["dimzin"] = dimzin

    def set_dimline_format(
        self,
        color: Optional[int] = None,
        linetype: Optional[str] = None,
        lineweight: Optional[int] = None,
        extension: Optional[float] = None,
        disable1: Optional[bool] = None,
        disable2: Optional[bool] = None,
    ):
        """Set dimension line properties.

        Args:
            color: color index
            linetype: linetype as string
            lineweight: line weight as int, 13 = 0.13mm, 200 = 2.00mm
            extension: extension length
            disable1: True to suppress first part of dimension line
            disable2: True to suppress second part of dimension line

        """
        if color is not None:
            self.dimstyle_attribs["dimclrd"] = color
        if linetype is not None:
            self.dimstyle_attribs["dimltype"] = linetype
        if lineweight is not None:
            self.dimstyle_attribs["dimlwd"] = lineweight
        if extension is not None:
            self.dimstyle_attribs["dimdle"] = extension
        if disable1 is not None:
            self.dimstyle_attribs["dimsd1"] = disable1
        if disable2 is not None:
            self.dimstyle_attribs["dimsd2"] = disable2

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
            self.dimstyle_attribs["dimclre"] = color
        if lineweight is not None:
            self.dimstyle_attribs["dimlwe"] = lineweight
        if extension is not None:
            self.dimstyle_attribs["dimexe"] = extension
        if offset is not None:
            self.dimstyle_attribs["dimexo"] = offset
        if fixed_length is not None:
            self.dimstyle_attribs["dimfxlon"] = 1
            self.dimstyle_attribs["dimfxl"] = fixed_length

    def set_extline1(self, linetype: Optional[str] = None, disable=False):
        """Set attributes of the first extension line.

        Args:
            linetype: linetype for the first extension line
            disable: disable first extension line if ``True``

        """
        if linetype is not None:
            self.dimstyle_attribs["dimltex1"] = linetype
        if disable:
            self.dimstyle_attribs["dimse1"] = 1

    def set_extline2(self, linetype: Optional[str] = None, disable=False):
        """Set attributes of the second extension line.

        Args:
            linetype: linetype for the second extension line
            disable: disable the second extension line if ``True``

        """
        if linetype is not None:
            self.dimstyle_attribs["dimltex2"] = linetype
        if disable:
            self.dimstyle_attribs["dimse2"] = 1

    def set_text(self, text: str = "<>") -> None:
        """Set dimension text.

        - `text` = " " to suppress dimension text
        - `text` = "" or "<>" to use measured distance as dimension text
        - otherwise display `text` literally

        """
        self.dimension.dxf.text = text

    def shift_text(self, dh: float, dv: float) -> None:
        """Set relative text movement, implemented as user location override
        without leader.

        Args:
            dh: shift text in text direction
            dv: shift text perpendicular to text direction

        """
        self.dimstyle_attribs["text_shift_h"] = dh
        self.dimstyle_attribs["text_shift_v"] = dv

    def set_location(self, location: UVec, leader=False, relative=False) -> None:
        """Set text location by user, special version for linear dimensions,
        behaves for other dimension types like :meth:`user_location_override`.

        Args:
            location: user defined text location
            leader: create leader from text to dimension line
            relative: `location` is relative to default location.

        """
        self.user_location_override(location)
        linear = self.dimension.dimtype < 2
        curved = self.dimension.dimtype in (2, 5, 8)
        if linear or curved:
            self.dimstyle_attribs["dimtmove"] = 1 if leader else 2
            self.dimstyle_attribs["relative_user_location"] = relative

    def user_location_override(self, location: UVec) -> None:
        """Set text location by user, `location` is relative to the origin of
        the UCS defined in the :meth:`render` method or WCS if the `ucs`
        argument is ``None``.

        """
        self.dimension.set_flag_state(
            self.dimension.USER_LOCATION_OVERRIDE, state=True, name="dimtype"
        )
        self.dimstyle_attribs["user_location"] = Vec3(location)

    def get_renderer(self, ucs: Optional[UCS] = None):
        """Get designated DIMENSION renderer. (internal API)"""
        return self.doc.dimension_renderer.dispatch(self, ucs)

    def render(self, ucs: Optional[UCS] = None, discard=False) -> BaseDimensionRenderer:
        """Starts the dimension line rendering process and also writes overridden
        dimension style attributes into the DSTYLE XDATA section. The rendering process
        "draws" the graphical representation of the DIMENSION entity as DXF primitives
        (TEXT, LINE, ARC, ...) into an anonymous content BLOCK.

        You can discard the content BLOCK for a friendly CAD applications like BricsCAD,
        because the rendering of the dimension entity is done automatically by BricsCAD
        if the content BLOCK is missing, and the result is in most cases better than the
        rendering done by `ezdxf`.

        AutoCAD does not render DIMENSION entities automatically, therefore I see
        AutoCAD as an unfriendly CAD application.

        Args:
            ucs: user coordinate system
            discard: discard the content BLOCK created by `ezdxf`, this works for
                BricsCAD, AutoCAD refuses to open DXF files containing DIMENSION
                entities without a content BLOCK

        Returns:
            The rendering object of the DIMENSION entity for analytics

        """

        renderer = self.get_renderer(ucs)
        if discard:
            self.doc.add_acad_incompatibility_message(
                "DIMENSION entity without geometry BLOCK (discard=True)"
            )
        else:
            block = self.doc.blocks.new_anonymous_block(type_char="D")
            self.dimension.dxf.geometry = block.name
            renderer.render(block)
        renderer.finalize()
        if len(self.dimstyle_attribs):
            self.commit()
        return renderer
