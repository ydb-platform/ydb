"""Utilities to find the EXIF offset and endian."""

import struct
from typing import BinaryIO, Dict, Tuple

from exifread.core.exceptions import ExifNotFound, InvalidExif
from exifread.core.heic import HEICExifFinder, find_heic_tiff
from exifread.core.jpeg import find_jpeg_exif
from exifread.core.jxl import JXLExifFinder
from exifread.core.utils import ord_
from exifread.exif_log import get_logger

logger = get_logger()


ENDIAN_TYPES: Dict[str, str] = {
    "I": "Intel",
    "M": "Motorola",
    "\x01": "Adobe Ducky",
    "b": "XMP/Adobe unknown",
}


def get_endian_str(endian_bytes) -> Tuple[str, str]:
    endian_str = chr(ord_(endian_bytes[0]))
    return endian_str, ENDIAN_TYPES.get(endian_str, "Unknown")


def find_tiff_exif(fh: BinaryIO) -> Tuple[int, bytes]:
    logger.debug("TIFF format recognized in data[0:2]")
    fh.seek(0)
    endian = fh.read(1)
    fh.read(1)
    offset = 0
    return offset, endian


def find_webp_exif(fh: BinaryIO) -> Tuple[int, bytes]:
    logger.debug("WebP format recognized in data[0:4], data[8:12]")
    # file specification: https://developers.google.com/speed/webp/docs/riff_container
    data = fh.read(5)
    if data[0:4] == b"VP8X" and data[4] & 8:
        # https://developers.google.com/speed/webp/docs/riff_container#extended_file_format
        fh.seek(13, 1)
        while True:
            data = fh.read(8)  # Chunk FourCC (32 bits) and Chunk Size (32 bits)
            if len(data) != 8:
                raise InvalidExif("Invalid webp file chunk header.")
            if data[0:4] == b"EXIF":
                fh.seek(6, 1)
                offset = fh.tell()
                endian = fh.read(1)
                return offset, endian
            size = struct.unpack("<L", data[4:8])[0]
            fh.seek(size, 1)
    raise ExifNotFound("Webp file does not have exif data.")


def find_png_exif(fh: BinaryIO, data: bytes) -> Tuple[int, bytes]:
    logger.debug("PNG format recognized in data[0:8]=%s", data[:8].hex())
    fh.seek(8)

    while True:
        data = fh.read(8)
        chunk = data[4:8]
        logger.debug("PNG found chunk %s", chunk.decode("ascii"))

        if chunk in (b"", b"IEND"):
            break
        if chunk == b"eXIf":
            offset = fh.tell()
            return offset, fh.read(1)

        chunk_size = int.from_bytes(data[:4], "big")
        fh.seek(fh.tell() + chunk_size + 4)

    raise ExifNotFound("PNG file does not have exif data.")


def find_jxl_exif(fh: BinaryIO) -> Tuple[int, bytes]:
    logger.debug("JPEG XL format recognized in data[0:12]")

    fh.seek(0)
    jxl = JXLExifFinder(fh)
    offset, endian = jxl.find_exif()
    if offset > 0:
        return offset, endian

    raise ExifNotFound("JPEG XL file does not have exif data.")


def determine_type(fh: BinaryIO) -> Tuple[int, bytes, int]:
    # by default do not fake an EXIF beginning
    fake_exif = 0

    data = fh.read(12)
    if data[0:2] in [b"II", b"MM"]:
        # it's a TIFF file
        offset, endian = find_tiff_exif(fh)
    elif data[4:12] in [b"ftypheic", b"ftypavif", b"ftypmif1"]:
        fh.seek(0)
        heic = HEICExifFinder(fh)
        offset, endian = heic.find_exif()
        if offset == 0:
            offset, endian = find_heic_tiff(fh)
            # It's a HEIC file with a TIFF header
    elif data[0:4] == b"RIFF" and data[8:12] == b"WEBP":
        offset, endian = find_webp_exif(fh)
    elif data[0:2] == b"\xff\xd8":
        # it's a JPEG file
        offset, endian, fake_exif = find_jpeg_exif(fh, data, fake_exif)
    elif data[0:8] == b"\x89PNG\r\n\x1a\n":
        offset, endian = find_png_exif(fh, data)
    elif data == b"\0\0\0\x0cJXL\x20\x0d\x0a\x87\x0a":
        offset, endian = find_jxl_exif(fh)
    else:
        raise ExifNotFound("File format not recognized.")
    return offset, endian, fake_exif
