"""XMP related utilities.."""

from pyexpat import ExpatError
from typing import BinaryIO
from xml.dom.minidom import parseString

from exifread.exif_log import get_logger

logger = get_logger()


def find_xmp_data(fh: BinaryIO) -> bytes:
    xmp_bytes = b""
    logger.debug("XMP not in Exif, searching file for XMP info...")
    xml_started = False
    xml_finished = False
    for line in fh:
        open_tag = line.find(b"<x:xmpmeta")
        close_tag = line.find(b"</x:xmpmeta>")
        if open_tag != -1:
            xml_started = True
            line = line[open_tag:]
            logger.debug("XMP found opening tag at line position %s", open_tag)
        if close_tag != -1:
            logger.debug("XMP found closing tag at line position %s", close_tag)
            line_offset = 0
            if open_tag != -1:
                line_offset = open_tag
            line = line[: (close_tag - line_offset) + 12]
            xml_finished = True
        if xml_started:
            xmp_bytes += line
        if xml_finished:
            break
    logger.debug("Found %s XMP bytes", len(xmp_bytes))
    return xmp_bytes


def xmp_bytes_to_str(xmp_bytes: bytes) -> str:
    """Adobe's Extensible Metadata Platform, just dump the pretty XML."""

    logger.debug("Cleaning XMP data ...")

    # Pray that it's encoded in UTF-8
    # TODO: allow user to specify encoding
    xmp_string = xmp_bytes.decode("utf-8")

    try:
        pretty = parseString(xmp_string).toprettyxml()
    except ExpatError:
        logger.warning("XMP: XML is not well formed")
        return xmp_string
    cleaned = []
    for line in pretty.splitlines():
        if line.strip():
            cleaned.append(line)
    return "\n".join(cleaned)
