# Copyright (c) 2019-2023 Manfred Moitzi
# License: MIT License
from typing import Optional
from ezdxf.enums import InsertUnits

# Documentation: https://ezdxf.mozman.at/docs/concepts/units.html#insunits

MSP_METRIC_UNITS_FACTORS = {
    # length in units / factor = length in meters
    # length in meters * factor = length in units
    "km": 0.001,
    "m": 1.0,
    "dm": 10.0,
    "cm": 100.0,
    "mm": 1000.0,
    "µm": 1000000.0,
    "yd": 1.093613298,
    "ft": 3.280839895,
    "in": 39.37007874,
    "mi": 0.00062137119,
}

IN = 1
FT = 2
MI = 3
MM = 4
CM = 5
M = 6
KM = 7
YD = 10
DM = 14

IMPERIAL_UNITS = {
    InsertUnits.Inches,
    InsertUnits.Feet,
    InsertUnits.Miles,
    InsertUnits.Microinches,
    InsertUnits.Mils,
    InsertUnits.Yards,
    InsertUnits.USSurveyFeet,
    InsertUnits.USSurveyInch,
    InsertUnits.USSurveyYard,
    InsertUnits.USSurveyMile,
}

# Conversion factor from meters to unit
# 1 meter is ... [unit]
METER_FACTOR = [
    None,  # 0 = Unitless - not supported
    39.37007874,  # 1 = Inches
    3.280839895,  # 2 = Feet
    0.00062137119,  # 3 = Miles
    1000.0,  # 4 = Millimeters
    100.0,  # 5 = Centimeters
    1.0,  # 6 = Meters
    0.001,  # 7 = Kilometers
    None,  # 8 = Microinches = 1e-6 in
    None,  # 9 = Mils = 0.001 in
    1.093613298,  # 10 = Yards
    10000000000.0,  # 11 = Angstroms = 1e-10m
    1000000000.0,  # 12 = Nanometers = 1e-9m
    1000000.0,  # 13 = Microns = 1e-6m
    10.0,  # 14 = Decimeters = 0.1m
    0.1,  # 15 = Decameters = 10m
    0.01,  # 16 = Hectometers = 100m
    0.000000001,  # 17 = Gigameters = 1e+9 m
    1.0 / 149597870700,  # 18 = Astronomical units = 149597870700m
    1.0 / 9.46e15,  # 19 = Light years = 9.46e15 m
    1.0 / 3.09e16,  # 20 = Parsecs =  3.09e16 m
    None,  # 21 = US Survey Feet
    None,  # 22 = US Survey Inch
    None,  # 23 = US Survey Yard
    None,  # 24 = US Survey Mile
]


class DrawingUnits:
    def __init__(self, base: float = 1.0, unit: str = "m"):
        self.base = float(base)
        self.unit = unit.lower()
        self._units = MSP_METRIC_UNITS_FACTORS
        self._msp_factor = base * self._units[self.unit]

    def factor(self, unit: str = "m") -> float:
        return self._msp_factor / self._units[unit.lower()]

    def __call__(self, unit: str) -> float:
        return self.factor(unit)


class PaperSpaceUnits:
    def __init__(self, msp=DrawingUnits(), unit: str = "mm", scale: float = 1):
        self.unit = unit.lower()
        self.scale = scale
        self._msp = msp
        self._psp = DrawingUnits(1, self.unit)

    def from_msp(self, value: float, unit: str):
        drawing_units = value * self._msp(unit.lower())
        return drawing_units / (self._msp(self.unit) * self.scale)

    def to_msp(self, value: float, unit: str):
        paper_space_units = value * self.scale * self._psp.factor(unit)
        model_space_units = paper_space_units * self._msp.factor(self.unit)
        return model_space_units


# Layout units are stored as enum in the associated BLOCK_RECORD: BlockRecord.dxf.units
# or as  optional XDATA for all DXF versions
# 1000: "ACAD"
# 1001: "DesignCenter Data" (optional)
# 1002: "{"
# 1070: Autodesk Design Center version number
# 1070: Insert units: like 'units'
# 1002: "}"

# The units of the modelspace block record is always 0, the real modelspace
# units and therefore the document units are stored as enum in the header var
# $INSUNITS

# units stored as enum in BlockRecord.dxf.units
# 0 = Unitless
# 1 = Inches
# 2 = Feet
# 3 = Miles
# 4 = Millimeters
# 5 = Centimeters
# 6 = Meters
# 7 = Kilometers
# 8 = Microinches = 1e-6 in
# 9 = Mils = 0.001 in
# 10 = Yards
# 11 = Angstroms = 1e-10m
# 12 = Nanometers = 1e-9m
# 13 = Microns = 1e-6m
# 14 = Decimeters = 0.1m
# 15 = Decameters = 10m
# 16 = Hectometers = 100m
# 17 = Gigameters = 1e+9 m
# 18 = Astronomical units = 149597870700m = 1.58125074e−5 ly =  4.84813681e−6 Parsec
# 19 = Light years = 9.46e15 m
# 20 = Parsecs =  3.09e16 m
# 21 = US Survey Feet
# 22 = US Survey Inch
# 23 = US Survey Yard
# 24 = US Survey Mile
_unit_spec = [
    None,
    "in",
    "ft",
    "mi",
    "mm",
    "cm",
    "m",
    "km",
    "µin",
    "mil",
    "yd",
    "Å",
    "nm",
    "µm",
    "dm",
    "dam",
    "hm",
    "gm",
    "au",
    "ly",
    "pc",
    None,
    None,
    None,
    None,
]


def decode(enum: int) -> Optional[str]:
    return _unit_spec[int(enum)]


def conversion_factor(
    source_units: InsertUnits, target_units: InsertUnits
) -> float:
    """Returns the conversion factor to represent `source_units` in
    `target_units`.

    E.g. millimeter in centimeter :code:`conversion_factor(MM, CM)` returns 0.1,
    because 1 mm = 0.1 cm

    """
    try:
        source_factor = METER_FACTOR[source_units]
        target_factor = METER_FACTOR[target_units]
        if source_factor is None or target_factor is None:
            raise TypeError("Unsupported conversion.")
        return target_factor / source_factor

    except IndexError:
        raise ValueError("Invalid unit enum.")


def unit_name(enum: int) -> str:
    """Returns the name of the unit enum."""
    try:
        return InsertUnits(enum).name
    except ValueError:
        return "unitless"


ANGLE_UNITS = {
    0: "Decimal Degrees",
    1: "Degrees/Minutes/Seconds",
    2: "Grad",
    3: "Radians",
}


def angle_unit_name(enum: int) -> str:
    """Returns the name of the angle unit enum."""
    return ANGLE_UNITS.get(enum, f"unknown unit <{enum}>")
