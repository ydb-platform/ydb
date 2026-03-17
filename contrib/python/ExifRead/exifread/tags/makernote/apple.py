"""
Makernote (proprietary) tag definitions for Apple iOS

Based on version 1.01 of ExifTool -> Image/ExifTool/Apple.pm
http://owl.phy.queensu.ca/~phil/exiftool/
"""

from exifread.tags import SubIfdTagDict

TAGS: SubIfdTagDict = {
    0x0001: ("MakerNoteVersion", None),
    0x0004: (
        "AEStable",
        {
            0: "No",
            1: "Yes",
        },
    ),
    0x0005: ("AETarget", None),
    0x0006: ("AEAverage", None),
    0x0007: (
        "AFStable",
        {
            0: "No",
            1: "Yes",
        },
    ),
    0x000A: (
        "HDRImageType",
        {
            3: "HDR Image",
            4: "Original Image",
        },
    ),
    0x0014: (
        "ImageCaptureType",
        {
            1: "ProRAW",
            2: "Portrait",
            10: "Photo",
            11: "Manual Focus",
            12: "Scene",
        },
    ),
    0x0015: ("ImageUniqueID", None),
    0x002E: (
        "CameraType",
        {
            0: "Back Wide Angle",
            1: "Back Normal",
            6: "Front",
        },
    ),
}
