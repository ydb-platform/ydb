#  Copyright (c) 2022, Manfred Moitzi
#  License: MIT License
#  module to set application specific data
from __future__ import annotations
from typing import TYPE_CHECKING

from ezdxf.lldxf import const, validator
from ezdxf.math import BoundingBox
from ezdxf import bbox
from ezdxf.enums import EndCaps, JoinStyle

if TYPE_CHECKING:
    from ezdxf.document import Drawing

CURRENT_LAYER = "$CLAYER"
CURRENT_COLOR = "$CECOLOR"
CURRENT_LINETYPE = "$CELTYPE"
CURRENT_LINEWEIGHT = "$CELWEIGHT"
CURRENT_LINETYPE_SCALE = "$CELTSCALE"
CURRENT_TEXTSTYLE = "$TEXTSTYLE"
CURRENT_DIMSTYLE = "$DIMSTYLE"
EXTMIN = "$EXTMIN"
EXTMAX = "$EXTMAX"


def set_current_layer(doc: Drawing, name: str):
    """Set current layer."""
    if name not in doc.layers:
        raise const.DXFValueError(f'undefined layer: "{name}"')
    doc.header[CURRENT_LAYER] = name


def set_current_color(doc: Drawing, color: int):
    """Set current :ref:`ACI`."""
    if not validator.is_valid_aci_color(color):
        raise const.DXFValueError(f'invalid ACI color value: "{color}"')
    doc.header[CURRENT_COLOR] = color


def set_current_linetype(doc: Drawing, name: str):
    """Set current linetype."""
    if name not in doc.linetypes:
        raise const.DXFValueError(f'undefined linetype: "{name}"')
    doc.header[CURRENT_LINETYPE] = name


def set_current_lineweight(doc: Drawing, lineweight: int):
    """Set current lineweight, see :ref:`lineweights` reference for valid
    values.
    """
    if not validator.is_valid_lineweight(lineweight):
        raise const.DXFValueError(f'invalid lineweight value: "{lineweight}"')
    doc.header[CURRENT_LINEWEIGHT] = lineweight


def set_current_linetype_scale(doc: Drawing, scale: float):
    """Set current linetype scale."""
    if scale <= 0.0:
        raise const.DXFValueError(f'invalid linetype scale: "{scale}"')
    doc.header[CURRENT_LINETYPE_SCALE] = scale


def set_current_textstyle(doc: Drawing, name: str):
    """Set current text style."""
    if name not in doc.styles:
        raise const.DXFValueError(f'undefined textstyle: "{name}"')
    doc.header[CURRENT_TEXTSTYLE] = name


def set_current_dimstyle(doc: Drawing, name: str):
    """Set current dimstyle."""
    if name not in doc.dimstyles:
        raise const.DXFValueError(f'undefined dimstyle: "{name}"')
    doc.header[CURRENT_DIMSTYLE] = name


def set_current_dimstyle_attribs(doc: Drawing, name: str):
    """Set current dimstyle and copy all dimstyle attributes to the HEADER section."""
    set_current_dimstyle(doc, name)
    dimstyle = doc.dimstyles.get(name)
    dimstyle.copy_to_header(doc)


def restore_wcs(doc: Drawing):
    """Restore the UCS settings in the HEADER section to the :ref:`WCS` and
    reset all active viewports to the WCS.
    """
    doc.header.reset_wcs()
    for vport in doc.viewports.get_config("*Active"):
        vport.reset_wcs()


def update_extents(doc: Drawing) -> BoundingBox:
    """Calculate the extents of the model space, update the HEADER variables
    $EXTMIN and $EXTMAX and returns the result as :class:`ezdxf.math.BoundingBox`.
    Note that this function uses the :mod:`ezdxf.bbox` module to calculate the
    extent of the model space. This module is not very fast and not very
    accurate for text and ignores all :term:`ACIS` based entities.

    The function updates only the values in the HEADER section, to zoom the
    active viewport to this extents, use this recipe::

        import ezdxf
        from ezdxf import zoom, appsettings

        doc = ezdxf.readfile("your.dxf")
        extents = appsettings.update_extents(doc)
        zoom.center(doc.modelspace(), extents.center, extents.size)

    .. seealso::

        - the :mod:`ezdxf.bbox` module to understand the limitations of the
          extent calculation
        - the :mod:`ezdxf.zoom` module

    """
    msp = doc.modelspace()
    extents = bbox.extents(msp, fast=True)
    if extents.has_data:
        msp.dxf.extmin = extents.extmin
        msp.dxf.extmax = extents.extmax
        doc.header[EXTMIN] = extents.extmin
        doc.header[EXTMAX] = extents.extmax
    return extents


def show_lineweight(doc: Drawing, state=True) -> None:
    """The CAD application or DXF viewer should show lines and curves with
    "thickness" (lineweight) if `state` is ``True``.
    """
    doc.header["$LWDISPLAY"] = int(state)


def set_lineweight_display_style(
    doc: Drawing, end_caps: EndCaps, join_style: JoinStyle
) -> None:
    """Set the style of end caps and joints for linear entities when displaying
    line weights. These settings only affect objects created afterwards.
    """
    doc.header["$ENDCAPS"] = int(end_caps)
    doc.header["$JOINSTYLE"] = int(join_style)
