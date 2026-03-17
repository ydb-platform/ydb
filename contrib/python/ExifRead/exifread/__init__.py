"""
Read Exif metadata from image files
Supported formats: TIFF, JPEG, PNG, Webp, HEIC
"""

from typing import Any, BinaryIO, Dict

from exifread.core.exceptions import ExifNotFound, InvalidExif
from exifread.core.exif_header import ExifHeader
from exifread.core.find_exif import determine_type, get_endian_str
from exifread.core.xmp import find_xmp_data
from exifread.exif_log import get_logger
from exifread.serialize import convert_types
from exifread.tags import DEFAULT_STOP_TAG

__version__ = "3.5.1"

logger = get_logger()


def _extract_xmp_data(hdr: ExifHeader, fh: BinaryIO):
    # Easy we already have them
    xmp_tag = hdr.tags.get("Image ApplicationNotes")
    if xmp_tag:
        logger.debug("XMP present in Exif")
        xmp_bytes = bytes(xmp_tag.values)
    # We need to look in the entire file for the XML
    else:
        xmp_bytes = find_xmp_data(fh)
    if xmp_bytes:
        hdr.parse_xmp(xmp_bytes)


def process_file(
    fh: BinaryIO,
    stop_tag: str = DEFAULT_STOP_TAG,
    details=True,
    strict=False,
    debug=False,
    truncate_tags=True,
    auto_seek=True,
    extract_thumbnail=True,
    builtin_types=False,
) -> Dict[str, Any]:
    """
    Process an image file to extract EXIF metadata.

    This is the function that has to deal with all the arbitrary nasty bits
    of the EXIF standard.

    :param fh: the file to process, must be opened in binary mode.
    :param stop_tag: Stop processing when the given tag is retrieved.
    :param details: If `True`, process MakerNotes.
    :param strict: If `True`, raise exceptions on errors.
    :param debug: Output debug information.
    :param truncate_tags: If `True`, truncate the `printable` tag output.
        There is no effect on tag `values`.
    :param auto_seek: If `True`, automatically `seek` to the start of the file.
    :param extract_thumbnail: If `True`, extract the JPEG thumbnail.
        The thumbnail is not always present in the EXIF metadata.
    :param builtin_types: If `True`, convert tags to standard Python types.

    :returns: A `dict` containing the EXIF metadata.
        The keys are a string in the format `"IFD_NAME TAG_NAME"`.
        If `builtin_types` is `False`, the value will be a `IfdTag` class, or bytes.
        IF `builtin_types` is `True`, the value will be a standard Python type.
    """

    if auto_seek:
        fh.seek(0)

    try:
        offset, endian_bytes, fake_exif = determine_type(fh)
    except ExifNotFound as err:
        logger.warning(err)
        return {}
    except InvalidExif as err:
        logger.debug(err)
        return {}

    endian_str, endian_type = get_endian_str(endian_bytes)
    # deal with the EXIF info we found
    logger.debug("Endian format is %s (%s)", endian_str, endian_type)

    hdr = ExifHeader(
        fh, endian_str, offset, fake_exif, strict, debug, details, truncate_tags
    )
    thumb_ifd = 0
    ctr = 0
    for ifd in hdr.list_ifd():
        if ctr == 0:
            ifd_name = "Image"
        elif ctr == 1:
            ifd_name = "Thumbnail"
            thumb_ifd = ifd
        else:
            ifd_name = "IFD %d" % ctr
        logger.debug("IFD %d (%s) at offset %s:", ctr, ifd_name, ifd)
        hdr.dump_ifd(ifd=ifd, ifd_name=ifd_name, stop_tag=stop_tag)
        ctr += 1
    # EXIF IFD
    exif_off = hdr.tags.get("Image ExifOffset")
    if exif_off:
        logger.debug("Exif SubIFD at offset %s:", exif_off.values[0])
        hdr.dump_ifd(ifd=exif_off.values[0], ifd_name="EXIF", stop_tag=stop_tag)

    # EXIF SubIFD
    sub_ifds = hdr.tags.get("Image SubIFDs")
    if details and sub_ifds:
        for subifd_id, subifd_offset in enumerate(sub_ifds.values):
            logger.debug("Exif SubIFD%d at offset %d:", subifd_id, subifd_offset)
            hdr.dump_ifd(
                ifd=subifd_offset, ifd_name=f"EXIF SubIFD{subifd_id}", stop_tag=stop_tag
            )

    # deal with MakerNote contained in EXIF IFD
    # (Some apps use MakerNote tags but do not use a format for which we
    # have a description, do not process these).
    if details and "EXIF MakerNote" in hdr.tags and "Image Make" in hdr.tags:
        try:
            hdr.decode_maker_note()
        except ValueError as err:
            if not strict:
                logger.debug("Failed to decode EXIF MakerNote: %s", str(err))
            else:
                raise err

    # extract thumbnails
    if thumb_ifd and extract_thumbnail:
        hdr.extract_tiff_thumbnail(thumb_ifd)
        hdr.extract_jpeg_thumbnail()

    # parse XMP tags (experimental)
    if debug and details:
        _extract_xmp_data(hdr=hdr, fh=fh)

    if builtin_types:
        return convert_types(hdr.tags)

    return hdr.tags
