"""
Makernote (proprietary) tag definitions for Casio.
"""

from exifread.tags import SubIfdTagDict

TAGS: SubIfdTagDict = {
    0x0001: (
        "RecordingMode",
        {
            1: "Single Shutter",
            2: "Panorama",
            3: "Night Scene",
            4: "Portrait",
            5: "Landscape",
        },
    ),
    0x0002: (
        "Quality",
        {
            1: "Economy",
            2: "Normal",
            3: "Fine",
        },
    ),
    0x0003: (
        "FocusingMode",
        {
            2: "Macro",
            3: "Auto Focus",
            4: "Manual Focus",
            5: "Infinity",
        },
    ),
    0x0004: (
        "FlashMode",
        {
            1: "Auto",
            2: "On",
            3: "Off",
            4: "Red Eye Reduction",
        },
    ),
    0x0005: (
        "FlashIntensity",
        {
            11: "Weak",
            13: "Normal",
            15: "Strong",
        },
    ),
    0x0006: ("Object Distance", None),
    0x0007: (
        "WhiteBalance",
        {
            1: "Auto",
            2: "Tungsten",
            3: "Daylight",
            4: "Fluorescent",
            5: "Shade",
            129: "Manual",
        },
    ),
    0x000A: ("DigitalZoom", None),
    0x000B: (
        "Sharpness",
        {
            0: "Normal",
            1: "Soft",
            2: "Hard",
        },
    ),
    0x000C: (
        "Contrast",
        {
            0: "Normal",
            1: "Low",
            2: "High",
        },
    ),
    0x000D: (
        "Saturation",
        {
            0: "Normal",
            1: "Low",
            2: "High",
        },
    ),
    0x0014: (
        "CCDSpeed",
        {
            64: "Normal",
            80: "Normal",
            100: "High",
            125: "+1.0",
            244: "+3.0",
            250: "+2.0",
        },
    ),
    0x0015: ("FirmwareDate", None),
}
