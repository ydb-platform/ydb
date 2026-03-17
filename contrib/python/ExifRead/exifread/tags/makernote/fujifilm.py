"""
Makernote (proprietary) tag definitions for FujiFilm.

http://www.sno.phy.queensu.ca/~phil/exiftool/TagNames/FujiFilm.html
"""

from exifread.tags import SubIfdTagDict
from exifread.tags.str_utils import make_string

TAGS: SubIfdTagDict = {
    0x0000: ("NoteVersion", make_string),
    0x0010: ("InternalSerialNumber", None),
    0x1000: ("Quality", None),
    0x1001: (
        "Sharpness",
        {
            0x1: "Soft",
            0x2: "Soft",
            0x3: "Normal",
            0x4: "Hard",
            0x5: "Hard2",
            0x82: "Medium Soft",
            0x84: "Medium Hard",
            0x8000: "Film Simulation",
        },
    ),
    0x1002: (
        "WhiteBalance",
        {
            0x0: "Auto",
            0x100: "Daylight",
            0x200: "Cloudy",
            0x300: "Daylight Fluorescent",
            0x301: "Day White Fluorescent",
            0x302: "White Fluorescent",
            0x303: "Warm White Fluorescent",
            0x304: "Living Room Warm White Fluorescent",
            0x400: "Incandescent",
            0x500: "Flash",
            0x600: "Underwater",
            0xF00: "Custom",
            0xF01: "Custom2",
            0xF02: "Custom3",
            0xF03: "Custom4",
            0xF04: "Custom5",
            0xFF0: "Kelvin",
        },
    ),
    0x1003: (
        "Saturation",
        {
            0x0: "Normal",
            0x80: "Medium High",
            0x100: "High",
            0x180: "Medium Low",
            0x200: "Low",
            0x300: "None (B&W)",
            0x301: "B&W Red Filter",
            0x302: "B&W Yellow Filter",
            0x303: "B&W Green Filter",
            0x310: "B&W Sepia",
            0x400: "Low 2",
            0x8000: "Film Simulation",
        },
    ),
    0x1004: (
        "Contrast",
        {
            0x0: "Normal",
            0x80: "Medium High",
            0x100: "High",
            0x180: "Medium Low",
            0x200: "Low",
            0x8000: "Film Simulation",
        },
    ),
    0x1005: ("ColorTemperature", None),
    0x1006: (
        "Contrast",
        {
            0x0: "Normal",
            0x100: "High",
            0x300: "Low",
        },
    ),
    0x100A: ("WhiteBalanceFineTune", None),
    0x1010: (
        "FlashMode",
        {
            0: "Auto",
            1: "On",
            2: "Off",
            3: "Red Eye Reduction",
        },
    ),
    0x1011: ("FlashStrength", None),
    0x1020: ("Macro", {0: "Off", 1: "On"}),
    0x1021: ("FocusMode", {0: "Auto", 1: "Manual"}),
    0x1022: ("AFPointSet", {0: "Yes", 1: "No"}),
    0x1023: ("FocusPixel", None),
    0x1030: ("SlowSync", {0: "Off", 1: "On"}),
    0x1031: (
        "PictureMode",
        {
            0: "Auto",
            1: "Portrait",
            2: "Landscape",
            4: "Sports",
            5: "Night",
            6: "Program AE",
            256: "Aperture Priority AE",
            512: "Shutter Priority AE",
            768: "Manual Exposure",
        },
    ),
    0x1032: ("ExposureCount", None),
    0x1100: ("MotorOrBracket", {0: "Off", 1: "On"}),
    0x1210: (
        "ColorMode",
        {
            0x0: "Standard",
            0x10: "Chrome",
            0x30: "B & W",
        },
    ),
    0x1300: ("BlurWarning", {0: "Off", 1: "On"}),
    0x1301: ("FocusWarning", {0: "Off", 1: "On"}),
    0x1302: ("ExposureWarning", {0: "Off", 1: "On"}),
}
