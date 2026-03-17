"""
Makernote (proprietary) tag definitions for Nikon.
"""

from exifread.tags import SubIfdTagDict
from exifread.tags.str_utils import make_string
from exifread.utils import Ratio


def ev_bias(seq) -> str:
    """
    First digit seems to be in steps of 1/6 EV.
    Does the third value mean the step size?  It is usually 6,
    but it is 12 for the ExposureDifference.
    Check for an error condition that could cause a crash.
    This only happens if something has gone really wrong in
    reading the Nikon MakerNote.
    http://tomtia.plala.jp/DigitalCamera/MakerNote/index.asp
    """

    if len(seq) < 4:
        return ""
    if seq == [252, 1, 6, 0]:
        return "-2/3 EV"
    if seq == [253, 1, 6, 0]:
        return "-1/2 EV"
    if seq == [254, 1, 6, 0]:
        return "-1/3 EV"
    if seq == [0, 1, 6, 0]:
        return "0 EV"
    if seq == [2, 1, 6, 0]:
        return "+1/3 EV"
    if seq == [3, 1, 6, 0]:
        return "+1/2 EV"
    if seq == [4, 1, 6, 0]:
        return "+2/3 EV"
    # Handle combinations not in the table.
    i = seq[0]
    # Causes headaches for the +/- logic, so special case it.
    if i == 0:
        return "0 EV"
    if i > 127:
        i = 256 - i
        ret_str = "-"
    else:
        ret_str = "+"
    step = seq[2]  # Assume third value means the step size
    whole = i / step
    i = i % step
    if whole != 0:
        ret_str = "%s%s " % (ret_str, str(whole))
    if i == 0:
        ret_str += "EV"
    else:
        ratio = Ratio(i, step)
        ret_str = ret_str + str(ratio) + " EV"
    return ret_str


# Nikon E99x MakerNote Tags
TAGS_NEW: SubIfdTagDict = {
    0x0001: ("MakernoteVersion", make_string),  # Sometimes binary
    0x0002: ("ISOSetting", None),
    0x0003: ("ColorMode", None),
    0x0004: ("Quality", None),
    0x0005: ("Whitebalance", None),
    0x0006: ("ImageSharpening", None),
    0x0007: ("FocusMode", None),
    0x0008: ("FlashSetting", None),
    0x0009: ("AutoFlashMode", None),
    0x000A: ("LensMount", None),  # According to LibRAW
    0x000B: ("WhiteBalanceBias", None),
    0x000C: ("WhiteBalanceRBCoeff", None),
    0x000D: ("ProgramShift", ev_bias),
    # Nearly the same as the other EV vals, but step size is 1/12 EV (?)
    0x000E: ("ExposureDifference", ev_bias),
    0x000F: ("ISOSelection", None),
    0x0010: ("DataDump", None),
    0x0011: ("NikonPreview", None),
    0x0012: ("FlashCompensation", ev_bias),
    0x0013: ("ISOSpeedRequested", None),
    0x0014: ("ColorBalance", None),  # According to LibRAW
    0x0016: ("PhotoCornerCoordinates", None),
    0x0017: ("ExternalFlashExposureComp", ev_bias),
    0x0018: ("FlashBracketCompensationApplied", ev_bias),
    0x0019: ("AEBracketCompensationApplied", None),
    0x001A: ("ImageProcessing", None),
    0x001B: (
        "CropHiSpeed",
        None,
        # TODO: investigate, returns incoherent results
        #
        #          {
        #     0: "Off",
        #     1: "1.3x Crop",
        #     2: "DX Crop",
        #     3: "5:4 Crop",
        #     4: "3:2 Crop",
        #     6: "16:9 Crop",
        #     8: "2.7x Crop",
        #     9: "DX Movie Crop",
        #     10: "1.3x Movie Crop",
        #     11: "FX Uncropped",
        #     12: "DX Uncropped",
        #     13: "2.8x Movie Crop",
        #     14: "1.4x Movie Crop",
        #     15: "1.5x Movie Crop",
        #     17: "1:1 Crop",
        # }
    ),
    0x001C: ("ExposureTuning", None),
    0x001D: ("SerialNumber", None),  # Conflict with 0x00A0 ?
    0x001E: (
        "ColorSpace",
        {
            1: "sRGB",
            2: "Adobe RGB",
            4: "BT.2100",
        },
    ),
    0x001F: ("VRInfo", None),
    0x0020: ("ImageAuthentication", None),
    0x0021: ("FaceDetect", None),
    0x0022: (
        "ActiveDLighting",
        {
            0: "Off",
            1: "Low",
            3: "Normal",
            5: "High",
            7: "Extra High",
            8: "Extra High 1",
            9: "Extra High 2",
            10: "Extra High 3",
            11: "Extra High 4",
            65535: "Auto",
        },
    ),
    0x0023: ("PictureControl", None),
    0x0024: ("WorldTime", None),
    0x0025: ("ISOInfo", None),
    0x002A: (
        "VignetteControl",
        {
            0: "Off",
            1: "Low",
            3: "Normal",
            5: "High",
        },
    ),
    0x002B: ("DistortInfo", None),
    0x002C: ("UnknownInfo", None),
    0x0032: ("UnknownInfo2", None),
    0x0034: (
        "ShutterMode",
        {
            0: "Mechanical",
            16: "Electronic",
            48: "Electronic Front Curtain",
            64: "Electronic (Movie)",
            80: "Auto (Mechanical)",
            81: "Auto (Electronic Front Curtain)",
            96: "Electronic (High Speed)",
        },
    ),
    0x0035: ("HDRInfo", None),
    0x0037: ("MechanicalShutterCount", None),
    0x0039: ("LocationInfo", None),
    0x003B: ("MultiExposureWhiteBalance", None),
    0x003D: ("BlackLevel", None),
    0x003E: ("ImageSizeRAW", None),
    0x0045: ("CropArea", None),
    0x004E: ("NikonSettings", None),
    0x004F: ("ColorTemperatureAuto", None),
    0x0080: ("ImageAdjustment", None),
    0x0081: ("ToneCompensation", None),
    0x0082: ("AuxiliaryLens", None),
    0x0083: ("LensType", None),
    0x0084: ("LensMinMaxFocalMaxAperture", None),
    0x0085: ("ManualFocusDistance", None),
    0x0086: ("DigitalZoomFactor", None),
    0x0087: (
        "FlashMode",
        {
            0: "Did Not Fire",
            1: "Fired, Manual",
            3: "Not Ready",
            7: "Fired, External",
            8: "Fired, Commander Mode ",
            9: "Fired, TTL Mode",
            18: "LED Light",
        },
    ),
    0x0088: (
        "AFFocusPosition",
        {
            0x0000: "Center",
            0x0100: "Top",
            0x0200: "Bottom",
            0x0300: "Left",
            0x0400: "Right",
        },
    ),
    0x0089: (
        "BracketingMode",
        {
            0x00: "Single frame, no bracketing",
            0x01: "Continuous, no bracketing",
            0x02: "Timer, no bracketing",
            0x10: "Single frame, exposure bracketing",
            0x11: "Continuous, exposure bracketing",
            0x12: "Timer, exposure bracketing",
            0x40: "Single frame, white balance bracketing",
            0x41: "Continuous, white balance bracketing",
            0x42: "Timer, white balance bracketing",
        },
    ),
    0x008A: ("AutoBracketRelease", None),
    0x008B: ("LensFStops", None),
    0x008C: ("NEFCurve1", None),  # ExifTool calls this 'ContrastCurve'
    0x008D: ("ColorMode", None),
    0x008F: ("SceneMode", None),
    0x0090: ("LightingType", None),
    0x0091: ("ShotInfo", None),  # First 4 bytes are a version number in ASCII
    0x0092: ("HueAdjustment", None),
    0x0093: (
        "NEFCompression",
        {
            1: "Lossy (type 1)",
            2: "Uncompressed",
            3: "Lossless",
            4: "Lossy (type 2)",
            5: "Striped packed 12 bits",
            6: "Uncompressed (reduced to 12 bit)",
            7: "Unpacked 12 bits",
            8: "Small",
            9: "Packed 12 bits",
            10: "Packed 14 bits",
            13: "High Efficiency",
            14: "High Efficiency*",
        },
    ),
    0x0094: (
        "Saturation",
        {
            -3: "B&W",
            -2: "-2",
            -1: "-1",
            0: "0",
            1: "1",
            2: "2",
        },
    ),
    0x0095: ("NoiseReduction", None),
    0x0096: ("NEFCurve2", None),  # ExifTool calls this 'LinearizationTable'
    0x0097: ("ColorBalance", None),  # First 4 bytes are a version number in ASCII
    0x0098: ("LensData", None),  # First 4 bytes are a version number in ASCII
    0x0099: ("RawImageCenter", None),
    0x009A: ("SensorPixelSize", None),
    0x009C: ("Scene Assist", None),
    0x009D: (
        "DateStampMode",
        {0: "Off", 1: "Date & Time", 2: "Date", 3: "Date Counter"},
    ),
    0x009E: (
        "RetouchHistory",
        None,
        # TODO: investigate, returns incoherent results
        #
        #     {
        #     0: "None",
        #     3: "B & W",
        #     4: "Sepia",
        #     5: "Trim",
        #     6: "Small Picture",
        #     7: "D-Lighting",
        #     8: "Red Eye",
        #     9: "Cyanotype",
        #     10: "Sky Light",
        #     11: "Warm Tone",
        #     12: "Color Custom",
        #     13: "Image Overlay",
        #     14: "Red Intensifier",
        #     15: "Green Intensifier",
        #     16: "Blue Intensifier",
        #     17: "Cross Screen",
        #     18: "Quick Retouch",
        #     19: "NEF Processing",
        #     23: "Distortion Control",
        #     25: "Fisheye",
        #     26: "Straighten",
        #     29: "Perspective Control",
        #     30: "Color Outline",
        #     31: "Soft Filter",
        #     32: "Resize",
        #     33: "Miniature Effect",
        #     34: "Skin Softening",
        #     35: "Selected Frame",
        #     37: "Color Sketch",
        #     38: "Selective Color",
        #     39: "Glamour",
        #     40: "Drawing",
        #     44: "Pop",
        #     45: "Toy Camera Effect 1",
        #     46: "Toy Camera Effect 2",
        #     47: "Cross Process (red)",
        #     48: "Cross Process (blue)",
        #     49: "Cross Process (green)",
        #     50: "Cross Process (yellow)",
        #     51: "Super Vivid",
        #     52: "High-contrast Monochrome",
        #     53: "High Key",
        #     54: "Low Key",
        # },
    ),
    0x00A0: ("SerialNumber", None),
    0x00A2: ("ImageDataSize", None),
    # 00A3: unknown - a single byte 0
    # 00A4: In NEF, looks like a 4 byte ASCII version number ('0200')
    0x00A5: ("ImageCount", None),
    0x00A6: ("DeletedImageCount", None),
    0x00A7: ("TotalShutterReleases", None),
    # First 4 bytes are a version number in ASCII, with version specific
    # info to follow.  It's hard to treat it as a string due to embedded nulls.
    0x00A8: ("FlashInfo", None),
    0x00A9: ("ImageOptimization", None),
    0x00AA: ("Saturation", None),
    0x00AB: ("DigitalVariProgram", None),
    0x00AC: ("ImageStabilization", None),
    0x00AD: ("AFResponse", None),
    0x00B0: ("MultiExposure", None),
    0x00B1: (
        "HighISONoiseReduction",
        {
            0: "Off",
            1: "Minimal",
            2: "Low",
            3: "Medium Low",
            4: "Normal",
            5: "Medium High",
            6: "High",
        },
    ),
    0x00B3: ("ToningEffect", None),
    0x00B6: ("PowerUpTime", None),
    0x00B7: ("AFInfo2", None),
    0x00B8: ("FileInfo", None),
    0x00B9: ("AFTune", None),
    0x00BB: ("RetouchInfo", None),
    0x00BD: ("PictureControlData", None),
    0x00BF: ("SilentPhotography", {0: "Off", 1: "On"}),
    0x00C3: ("BarometerInfo", None),
    0x0100: ("DigitalICE", None),
    0x0201: ("PreviewImageStart", None),
    0x0202: ("PreviewImageLength", None),
    0x0213: (
        "PreviewYCbCrPositioning",
        {
            1: "Centered",
            2: "Co-sited",
        },
    ),
    0x0E00: ("PrintIM", None),
    0x0E01: ("InCameraEditNote", None),
    0x0E09: ("NikonCaptureVersion", None),
    0x0E0E: ("NikonCaptureOffsets", None),
    0x0E10: ("NikonScan", None),
    0x0E13: ("NikonCaptureEditVersions", None),
    0x0E1D: ("NikonICCProfile", None),
    0x0E1E: ("NikonCaptureOutput", None),
    0x0E22: ("NEFBitDepth", None),
}

TAGS_OLD: SubIfdTagDict = {
    0x0003: (
        "Quality",
        {
            1: "VGA Basic",
            2: "VGA Normal",
            3: "VGA Fine",
            4: "SXGA Basic",
            5: "SXGA Normal",
            6: "SXGA Fine",
        },
    ),
    0x0004: (
        "ColorMode",
        {
            1: "Color",
            2: "Monochrome",
        },
    ),
    0x0005: (
        "ImageAdjustment",
        {
            0: "Normal",
            1: "Bright+",
            2: "Bright-",
            3: "Contrast+",
            4: "Contrast-",
        },
    ),
    0x0006: (
        "CCDSpeed",
        {
            0: "ISO 80",
            2: "ISO 160",
            4: "ISO 320",
            5: "ISO 100",
        },
    ),
    0x0007: (
        "WhiteBalance",
        {
            0: "Auto",
            1: "Preset",
            2: "Daylight",
            3: "Incandescent",
            4: "Fluorescent",
            5: "Cloudy",
            6: "Speed Light",
        },
    ),
}
