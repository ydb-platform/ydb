"""
Makernote (proprietary) tag definitions for DJI cameras

Based on https://github.com/exiftool/exiftool/blob/master/lib/Image/ExifTool/DJI.pm
"""

from exifread.tags import SubIfdTagDict

TAGS: SubIfdTagDict = {
    0x01: ("Make", None),
    0x03: ("SpeedX", None),
    0x04: ("SpeedY", None),
    0x05: ("SpeedZ", None),
    0x06: ("Pitch", None),
    0x07: ("Yaw", None),
    0x08: ("Roll", None),
    0x09: ("CameraPitch", None),
    0x0A: ("CameraYaw", None),
    0x0B: ("CameraRoll", None),
}
