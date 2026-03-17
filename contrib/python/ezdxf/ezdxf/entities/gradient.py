# Copyright (c) 2019-2022 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import Optional, TYPE_CHECKING
import enum
import math

from ezdxf.colors import RGB, int2rgb, rgb2int
from ezdxf.lldxf.tags import Tags

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter

__all__ = ["Gradient", "GradientType"]

GRADIENT_CODES = {450, 451, 452, 453, 460, 461, 462, 463, 470, 421, 63}


class GradientType(enum.IntEnum):
    NONE = 0
    LINEAR = 1
    CYLINDER = 2
    INVCYLINDER = 3
    SPHERICAL = 4
    INVSPHERICAL = 5
    HEMISPHERICAL = 6
    INVHEMISPHERICAL = 7
    CURVED = 8
    INVCURVED = 9


gradient_names = [
    "",
    "LINEAR",
    "CYLINDER",
    "INVCYLINDER",
    "SPHERICAL",
    "INVSPHERICAL",
    "HEMISPHERICAL",
    "INVHEMISPHERICAL",
    "CURVED",
    "INVCURVED",
]


class Gradient:
    def __init__(self, kind: int = 1, num: int = 2, type=GradientType.LINEAR):
        # 1 for gradient by default, 0 for Solid
        self.kind: int = kind
        self.number_of_colors: int = num
        self.color1: RGB = RGB(0, 0, 0)
        self.aci1: Optional[int] = None
        self.color2: RGB = RGB(255, 255, 255)
        self.aci2: Optional[int] = None

        # 1 = use a smooth transition between color1 and a specified tint
        self.one_color: int = 0

        # Use degree NOT radians for rotation, because there should be one
        # system for all angles:
        self.rotation: float = 0.0
        self.centered: float = 0.0
        self.tint: float = 0.0
        self.name: str = gradient_names[type]

    @classmethod
    def load_tags(cls, tags: Tags) -> Gradient:
        gdata = cls()
        assert tags[0].code == 450
        gdata.kind = tags[0].value  # 0 = solid; 1 = gradient
        first_color_value = True
        first_aci_value = True
        for code, value in tags:
            if code == 460:
                gdata.rotation = math.degrees(value)
            elif code == 461:
                gdata.centered = value
            elif code == 452:
                gdata.one_color = value
            elif code == 462:
                gdata.tint = value
            elif code == 470:
                gdata.name = value
            elif code == 453:
                gdata.number_of_colors = value
            elif code == 63:
                if first_aci_value:
                    gdata.aci1 = value
                    first_aci_value = False
                else:
                    gdata.aci2 = value
            elif code == 421:
                if first_color_value:
                    gdata.color1 = int2rgb(value)
                    first_color_value = False
                else:
                    gdata.color2 = int2rgb(value)
        return gdata

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        # Tag order matters!
        write_tag = tagwriter.write_tag2
        write_tag(450, self.kind)  # gradient or solid
        write_tag(451, 0)  # reserved for the future

        # rotation angle in radians:
        write_tag(460, math.radians(self.rotation))
        write_tag(461, self.centered)
        write_tag(452, self.one_color)
        write_tag(462, self.tint)
        write_tag(453, self.number_of_colors)
        if self.number_of_colors > 0:
            write_tag(463, 0)  # first value, see DXF standard
            if self.aci1 is not None:
                # code 63 "color as ACI" could be left off
                write_tag(63, self.aci1)
            write_tag(421, rgb2int(self.color1))  # first color
        if self.number_of_colors > 1:
            write_tag(463, 1)  # second value, see DXF standard
            if self.aci2 is not None:
                # code 63 "color as ACI" could be left off
                write_tag(63, self.aci2)
            write_tag(421, rgb2int(self.color2))  # second color
        write_tag(470, self.name)
