"""
Makernote (proprietary) tag definitions for Olympus.
"""

from typing import Dict

from exifread.tags import SubIfdTagDictValue
from exifread.tags.str_utils import make_string


def special_mode(val: bytes) -> str:
    """Decode Olympus SpecialMode tag in MakerNote"""
    mode1 = {
        0: "Normal",
        1: "Unknown",
        2: "Fast",
        3: "Panorama",
    }
    mode2 = {
        0: "Non-panoramic",
        1: "Left to right",
        2: "Right to left",
        3: "Bottom to top",
        4: "Top to bottom",
    }

    if not val:
        return ""

    mode1_val = mode1.get(val[0], "Unknown")
    mode2_val = mode2.get(val[2], "Unknown")
    return "%s - Sequence %d - %s" % (mode1_val, val[1], mode2_val)


TAGS: Dict[int, SubIfdTagDictValue] = {
    # Ah HAH! those sneeeeeaky bastids! this is how they get past the fact
    # that a JPEG thumbnail is not allowed in an uncompressed TIFF file
    0x0100: ("JPEGThumbnail", None),
    0x0200: ("SpecialMode", special_mode),
    0x0201: (
        "JPEGQual",
        {
            1: "SQ",
            2: "HQ",
            3: "SHQ",
        },
    ),
    0x0202: (
        "Macro",
        {
            0: "Normal",
            1: "Macro",
            2: "SuperMacro",
        },
    ),
    0x0203: ("BWMode", {0: "Off", 1: "On"}),
    0x0204: ("DigitalZoom", None),
    0x0205: ("FocalPlaneDiagonal", None),
    0x0206: ("LensDistortionParams", None),
    0x0207: ("SoftwareRelease", None),
    0x0208: ("PictureInfo", None),
    0x0209: ("CameraID", make_string),  # print as string
    0x0F00: ("DataDump", None),
    0x0300: ("PreCaptureFrames", None),
    0x0404: ("SerialNumber", None),
    0x1000: ("ShutterSpeedValue", None),
    0x1001: ("ISOValue", None),
    0x1002: ("ApertureValue", None),
    0x1003: ("BrightnessValue", None),
    0x1004: ("FlashMode", {2: "On", 3: "Off"}),
    0x1005: (
        "FlashDevice",
        {
            0: "None",
            1: "Internal",
            4: "External",
            5: "Internal + External",
        },
    ),
    0x1006: ("ExposureCompensation", None),
    0x1007: ("SensorTemperature", None),
    0x1008: ("LensTemperature", None),
    0x100B: ("FocusMode", {0: "Auto", 1: "Manual"}),
    0x1017: ("RedBalance", None),
    0x1018: ("BlueBalance", None),
    0x101A: ("SerialNumber", None),
    0x1023: ("FlashExposureComp", None),
    0x1026: ("ExternalFlashBounce", {0: "No", 1: "Yes"}),
    0x1027: ("ExternalFlashZoom", None),
    0x1028: ("ExternalFlashMode", None),
    0x1029: ("Contrast  int16u", {0: "High", 1: "Normal", 2: "Low"}),
    0x102A: ("SharpnessFactor", None),
    0x102B: ("ColorControl", None),
    0x102C: ("ValidBits", None),
    0x102D: ("CoringFilter", None),
    0x102E: ("OlympusImageWidth", None),
    0x102F: ("OlympusImageHeight", None),
    0x1034: ("CompressionRatio", None),
    0x1035: ("PreviewImageValid", {0: "No", 1: "Yes"}),
    0x1036: ("PreviewImageStart", None),
    0x1037: ("PreviewImageLength", None),
    0x1039: ("CCDScanMode", {0: "Interlaced", 1: "Progressive"}),
    0x103A: ("NoiseReduction", {0: "Off", 1: "On"}),
    0x103B: ("InfinityLensStep", None),
    0x103C: ("NearLensStep", None),
    # TODO - these need extra definitions
    # http://search.cpan.org/src/EXIFTOOL/Image-ExifTool-6.90/html/TagNames/Olympus.html
    0x2010: ("Equipment", None),
    0x2020: ("CameraSettings", None),
    0x2030: ("RawDevelopment", None),
    0x2040: ("ImageProcessing", None),
    0x2050: ("FocusInfo", None),
    0x3000: ("RawInfo ", None),
}

# 0x2020 CameraSettings
TAG_0x2020: Dict[int, SubIfdTagDictValue] = {
    0x0100: ("PreviewImageValid", {0: "No", 1: "Yes"}),
    0x0101: ("PreviewImageStart", None),
    0x0102: ("PreviewImageLength", None),
    0x0200: (
        "ExposureMode",
        {
            1: "Manual",
            2: "Program",
            3: "Aperture-priority AE",
            4: "Shutter speed priority AE",
            5: "Program-shift",
        },
    ),
    0x0201: ("AELock", {0: "Off", 1: "On"}),
    0x0202: (
        "MeteringMode",
        {
            2: "Center Weighted",
            3: "Spot",
            5: "ESP",
            261: "Pattern+AF",
            515: "Spot+Highlight control",
            1027: "Spot+Shadow control",
        },
    ),
    0x0300: ("MacroMode", {0: "Off", 1: "On"}),
    0x0301: (
        "FocusMode",
        {
            0: "Single AF",
            1: "Sequential shooting AF",
            2: "Continuous AF",
            3: "Multi AF",
            10: "MF",
        },
    ),
    0x0302: (
        "FocusProcess",
        {
            0: "AF Not Used",
            1: "AF Used",
        },
    ),
    0x0303: ("AFSearch", {0: "Not Ready", 1: "Ready"}),
    0x0304: ("AFAreas", None),
    0x0401: ("FlashExposureCompensation", None),
    0x0500: (
        "WhiteBalance2",
        {
            0: "Auto",
            16: "7500K (Fine Weather with Shade)",
            17: "6000K (Cloudy)",
            18: "5300K (Fine Weather)",
            20: "3000K (Tungsten light)",
            21: "3600K (Tungsten light-like)",
            33: "6600K (Daylight fluorescent)",
            34: "4500K (Neutral white fluorescent)",
            35: "4000K (Cool white fluorescent)",
            48: "3600K (Tungsten light-like)",
            256: "Custom WB 1",
            257: "Custom WB 2",
            258: "Custom WB 3",
            259: "Custom WB 4",
            512: "Custom WB 5400K",
            513: "Custom WB 2900K",
            514: "Custom WB 8000K",
        },
    ),
    0x0501: ("WhiteBalanceTemperature", None),
    0x0502: ("WhiteBalanceBracket", None),
    0x0503: ("CustomSaturation", None),  # (3 numbers: 1. CS Value, 2. Min, 3. Max)
    0x0504: (
        "ModifiedSaturation",
        {
            0: "Off",
            1: "CM1 (Red Enhance)",
            2: "CM2 (Green Enhance)",
            3: "CM3 (Blue Enhance)",
            4: "CM4 (Skin Tones)",
        },
    ),
    0x0505: ("ContrastSetting", None),  # (3 numbers: 1. Contrast, 2. Min, 3. Max)
    0x0506: ("SharpnessSetting", None),  # (3 numbers: 1. Sharpness, 2. Min, 3. Max)
    0x0507: (
        "ColorSpace",
        {
            0: "sRGB",
            1: "Adobe RGB",
            2: "Pro Photo RGB",
        },
    ),
    0x0509: (
        "SceneMode",
        {
            0: "Standard",
            6: "Auto",
            7: "Sport",
            8: "Portrait",
            9: "Landscape+Portrait",
            10: "Landscape",
            11: "Night scene",
            13: "Panorama",
            16: "Landscape+Portrait",
            17: "Night+Portrait",
            19: "Fireworks",
            20: "Sunset",
            22: "Macro",
            25: "Documents",
            26: "Museum",
            28: "Beach&Snow",
            30: "Candle",
            35: "Underwater Wide1",
            36: "Underwater Macro",
            39: "High Key",
            40: "Digital Image Stabilization",
            44: "Underwater Wide2",
            45: "Low Key",
            46: "Children",
            48: "Nature Macro",
        },
    ),
    0x050A: (
        "NoiseReduction",
        {
            0: "Off",
            1: "Noise Reduction",
            2: "Noise Filter",
            3: "Noise Reduction + Noise Filter",
            4: "Noise Filter (ISO Boost)",
            5: "Noise Reduction + Noise Filter (ISO Boost)",
        },
    ),
    0x050B: ("DistortionCorrection", {0: "Off", 1: "On"}),
    0x050C: ("ShadingCompensation", {0: "Off", 1: "On"}),
    0x050D: ("CompressionFactor", None),
    0x050F: (
        "Gradation",
        {
            "-1 -1 1": "Low Key",
            "0 -1 1": "Normal",
            "1 -1 1": "High Key",
        },
    ),
    0x0520: (
        "PictureMode",
        {
            1: "Vivid",
            2: "Natural",
            3: "Muted",
            256: "Monotone",
            512: "Sepia",
        },
    ),
    0x0521: ("PictureModeSaturation", None),
    0x0522: ("PictureModeHue?", None),
    0x0523: ("PictureModeContrast", None),
    0x0524: ("PictureModeSharpness", None),
    0x0525: (
        "PictureModeBWFilter",
        {
            0: "n/a",
            1: "Neutral",
            2: "Yellow",
            3: "Orange",
            4: "Red",
            5: "Green",
        },
    ),
    0x0526: (
        "PictureModeTone",
        {
            0: "n/a",
            1: "Neutral",
            2: "Sepia",
            3: "Blue",
            4: "Purple",
            5: "Green",
        },
    ),
    0x0600: ("Sequence", None),  # 2 or 3 numbers: 1. Mode, 2. Shot number, 3. Mode bits
    0x0601: ("PanoramaMode", None),  # (2 numbers: 1. Mode, 2. Shot number)
    0x0603: (
        "ImageQuality2",
        {
            1: "SQ",
            2: "HQ",
            3: "SHQ",
            4: "RAW",
        },
    ),
    0x0901: ("ManometerReading", None),
}
