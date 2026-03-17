"""
Find Exif data in a JPEG XL file
"""

from typing import Tuple

from exifread.core.heic import HEICExifFinder


class JXLExifFinder(HEICExifFinder):
    """Find JPEG XL EXIF tags."""

    def find_exif(self) -> Tuple[int, bytes]:
        ftyp = self.expect_parse("ftyp")
        assert ftyp.major_brand == b"jxl "
        assert ftyp.minor_version == 0
        exif = self.expect_parse("Exif")

        offset = exif.pos + 4
        self.file_handle.seek(offset - 8)
        assert self.get(8)[:6] == b"Exif\x00\x00"
        endian = self.file_handle.read(1)
        return offset, endian
