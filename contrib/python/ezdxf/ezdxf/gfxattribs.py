#  Copyright (c) 2021-2023, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import Any, Optional, Iterator, TYPE_CHECKING

from ezdxf import colors
from ezdxf.lldxf import validator, const

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFEntity

__all__ = ["GfxAttribs", "TRANSPARENCY_BYBLOCK"]


DEFAULT_LAYER = "0"
DEFAULT_ACI_COLOR = colors.BYLAYER
DEFAULT_LINETYPE = "ByLayer"
DEFAULT_LINEWEIGHT = const.LINEWEIGHT_BYLAYER
DEFAULT_LTSCALE = 1.0
TRANSPARENCY_BYBLOCK = -1.0  # special value


class GfxAttribs:
    """
    Represents often used DXF attributes of graphical entities.

    Args:
        layer (str): layer name as string
        color (int): :ref:`ACI` color value as integer
        rgb: RGB true color (red, green, blue) tuple, each channel value in the
            range from 0 to 255, ``None`` for not set
        linetype (str): linetype name, does not check if the linetype exist!
        lineweight (int):  see :ref:`lineweights` documentation for valid values
        transparency (float): transparency value in the range from 0.0 to 1.0,
            where 0.0 is opaque and 1.0 if fully transparent, -1.0 for
            transparency by block, ``None`` for transparency by layer
        ltscale (float): linetype scaling factor > 0.0, default factor is 1.0

    Raises:
        DXFValueError: invalid attribute value

    """

    _layer: str = DEFAULT_LAYER
    _aci_color: int = DEFAULT_ACI_COLOR
    _true_color: Optional[colors.RGB] = None
    _linetype: str = DEFAULT_LINETYPE
    _lineweight: int = DEFAULT_LINEWEIGHT
    _transparency: Optional[float] = None
    _ltscale: float = DEFAULT_LTSCALE

    def __init__(
        self,
        *,
        layer: str = DEFAULT_LAYER,
        color: int = DEFAULT_ACI_COLOR,
        rgb: Optional[colors.RGB] = None,
        linetype: str = DEFAULT_LINETYPE,
        lineweight: int = DEFAULT_LINEWEIGHT,
        transparency: Optional[float] = None,
        ltscale: float = DEFAULT_LTSCALE,
    ):
        self.layer = layer
        self.color = color
        self.rgb = rgb
        self.linetype = linetype
        self.lineweight = lineweight
        self.transparency = transparency
        self.ltscale = ltscale

    def __str__(self) -> str:
        s = []
        if self._layer != DEFAULT_LAYER:
            s.append(f"layer='{self._layer}'")
        if self._aci_color != DEFAULT_ACI_COLOR:
            s.append(f"color={self._aci_color}")
        if self._true_color is not None:
            s.append(f"rgb={self._true_color}")
        if self._linetype != DEFAULT_LINETYPE:
            s.append(f"linetype='{self._linetype}'")
        if self._lineweight != DEFAULT_LINEWEIGHT:
            s.append(f"lineweight={self._lineweight}")
        if self._transparency is not None:
            s.append(f"transparency={round(self._transparency, 3)}")
        if self._ltscale != DEFAULT_LTSCALE:
            s.append(f"ltscale={round(self._ltscale, 3)}")

        return ", ".join(s)

    def __repr__(self) -> str:
        return f"{self.__class__.__name__}({self.__str__()})"

    def __iter__(self) -> Iterator[tuple[str, Any]]:
        """Returns iter(self)."""
        return iter(self.items())

    def items(self, default_values=False) -> list[tuple[str, Any]]:
        """Returns the DXF attributes as list of name, value pairs, returns
        also the default values if argument `default_values` is ``True``.
        The true_color and transparency attributes do not have default values,
        the absence of these attributes is the default value.
        """
        data: list[tuple[str, Any]] = []
        if default_values or self._layer != DEFAULT_LAYER:
            data.append(("layer", self._layer))
        if default_values or self._aci_color != DEFAULT_ACI_COLOR:
            data.append(("color", self._aci_color))
        if self._true_color is not None:
            # absence is the default value
            data.append(("true_color", colors.rgb2int(self._true_color)))
        if default_values or self._linetype != DEFAULT_LINETYPE:
            data.append(("linetype", self._linetype))
        if default_values or self._lineweight != DEFAULT_LINEWEIGHT:
            data.append(("lineweight", self.lineweight))
        if self._transparency is not None:
            # absence is the default value
            if self._transparency == TRANSPARENCY_BYBLOCK:
                data.append(("transparency", colors.TRANSPARENCY_BYBLOCK))
            else:
                data.append(
                    (
                        "transparency",
                        colors.float2transparency(self._transparency),
                    )
                )
        if default_values or self._ltscale != DEFAULT_LTSCALE:
            data.append(("ltscale", self._ltscale))
        return data

    def asdict(self, default_values=False) -> dict[str, Any]:
        """Returns the DXF attributes as :class:`dict`, returns also the
        default values if argument `default_values` is ``True``.
        The true_color and transparency attributes do not have default values,
        the absence of these attributes is the default value.
        """
        return dict(self.items(default_values))

    @property
    def layer(self) -> str:
        """layer name"""
        return self._layer

    @layer.setter
    def layer(self, name: str):
        if validator.is_valid_layer_name(name):
            self._layer = name
        else:
            raise const.DXFValueError(f"invalid layer name '{name}'")

    @property
    def color(self) -> int:
        """:ref:`ACI` color value"""
        return self._aci_color

    @color.setter
    def color(self, value: int):
        if validator.is_valid_aci_color(value):
            self._aci_color = value
        else:
            raise const.DXFValueError(f"invalid ACI color value '{value}'")

    @property
    def rgb(self) -> Optional[colors.RGB]:
        """true color value as (red, green, blue) tuple, ``None`` for not set"""
        return self._true_color

    @rgb.setter
    def rgb(self, value: Optional[colors.RGB]):
        if value is None:
            self._true_color = None
        elif validator.is_valid_rgb(value):
            self._true_color = value
        else:
            raise const.DXFValueError(f"invalid true color value '{value}'")

    @property
    def linetype(self) -> str:
        """linetype name"""
        return self._linetype

    @linetype.setter
    def linetype(self, name: str):
        if validator.is_valid_table_name(name):
            self._linetype = name
        else:
            raise const.DXFValueError(f"invalid linetype name '{name}'")

    @property
    def lineweight(self) -> int:
        """lineweight"""
        return self._lineweight

    @lineweight.setter
    def lineweight(self, value: int):
        if validator.is_valid_lineweight(value):
            self._lineweight = value
        else:
            raise const.DXFValueError(f"invalid lineweight value '{value}'")

    @property
    def transparency(self) -> Optional[float]:
        """transparency value from 0.0 for opaque to 1.0 is fully transparent,
        -1.0 is for transparency by block and ``None`` if for transparency
        by layer
        """
        return self._transparency

    @transparency.setter
    def transparency(self, value: Optional[float]):
        if value is None:
            self._transparency = None
        elif value == TRANSPARENCY_BYBLOCK:
            self._transparency = TRANSPARENCY_BYBLOCK
        elif isinstance(value, float) and (0.0 <= value <= 1.0):
            self._transparency = value
        else:
            raise const.DXFValueError(f"invalid transparency value '{value}'")

    @property
    def ltscale(self) -> float:
        """linetype scaling factor"""
        return self._ltscale

    @ltscale.setter
    def ltscale(self, value: float):
        if isinstance(value, (float, int)) and (value > 1e-6):
            self._ltscale = float(value)
        else:
            raise const.DXFValueError(f"invalid linetype scale value '{value}'")

    @classmethod
    def load_from_header(cls, doc: Drawing) -> GfxAttribs:
        """Load default DXF attributes from the HEADER section.

        There is no default true color value and the default transparency is not
        stored in the HEADER section.

        Loads following header variables:

            - ``$CLAYER`` - current layer name
            - ``$CECOLOR`` - current ACI color
            - ``$CELTYPE`` - current linetype name
            - ``$CELWEIGHT`` - current lineweight
            - ``$CELTSCALE`` - current linetype scaling factor

        """
        header = doc.header
        return cls(
            layer=header.get("$CLAYER", DEFAULT_LAYER),
            color=header.get("$CECOLOR", DEFAULT_ACI_COLOR),
            linetype=header.get("$CELTYPE", DEFAULT_LINETYPE),
            lineweight=header.get("$CELWEIGHT", DEFAULT_LINEWEIGHT),
            ltscale=header.get("$CELTSCALE", DEFAULT_LTSCALE),
        )

    def write_to_header(self, doc: "Drawing") -> None:
        """Write DXF attributes as default values to the HEADER section.

        Writes following header variables:

            - ``$CLAYER`` - current layer name, if a layer table entry exist in `doc`
            - ``$CECOLOR`` - current ACI color
            - ``$CELTYPE`` - current linetype name, if a linetype table entry exist in `doc`
            - ``$CELWEIGHT`` - current lineweight
            - ``$CELTSCALE`` - current linetype scaling factor

        """
        header = doc.header
        if doc.layers.has_entry(self.layer):
            header["$CLAYER"] = self.layer
        header["$CECOLOR"] = self.color
        if doc.linetypes.has_entry(self.linetype):
            header["$CELTYPE"] = self.linetype
        header["$CELWEIGHT"] = self.lineweight
        header["$CELTSCALE"] = self.ltscale

    @classmethod
    def from_entity(cls, entity: DXFEntity) -> GfxAttribs:
        """Get the graphical attributes of an `entity` as :class:`GfxAttribs`
        object.
        """
        try:
            d = entity.graphic_properties()  # type: ignore
        except AttributeError:
            return cls()
        return cls.from_dict(d)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> GfxAttribs:
        """Construct :class:`GfxAttribs` from a dictionary of raw DXF values.

        Supported attributes are:

            - layer: layer name as string
            - color: :ref:`ACI` value as int
            - true_color: raw DXF integer value for RGB colors
            - rgb: RGB tuple of int or ``None``
            - linetype: linetype name as string
            - lineweight: lineweight as int, see basic concept of :ref:`lineweights`
            - transparency: raw DXF integer value of transparency or a float in the
              range from 0.0 to 1.0
            - ltscale: linetype scaling factor as float

        """
        attribs = cls()
        for attrib_name in [
            "layer",
            "color",
            "linetype",
            "lineweight",
            "ltscale",
            "rgb",
        ]:
            if attrib_name in d:
                setattr(attribs, attrib_name, d[attrib_name])
        if "true_color" in d:
            attribs.rgb = colors.int2rgb(d["true_color"])
        if "transparency" in d:
            transparency = d["transparency"]
            if isinstance(transparency, float):
                attribs.transparency = transparency
            elif transparency == colors.TRANSPARENCY_BYBLOCK:
                attribs.transparency = TRANSPARENCY_BYBLOCK
            else:
                attribs.transparency = colors.transparency2float(transparency)
        return attribs
