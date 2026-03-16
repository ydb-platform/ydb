"""Extract EXIF from JPEG files."""

from typing import BinaryIO, Tuple

from exifread.core.exceptions import InvalidExif
from exifread.core.utils import ord_
from exifread.exif_log import get_logger

logger = get_logger()


def _increment_base(data, base) -> int:
    return ord_(data[base + 2]) * 256 + ord_(data[base + 3]) + 2


def _get_initial_base(fh: BinaryIO, data: bytes, fake_exif: int) -> Tuple[int, int]:
    base = 2
    logger.debug(
        "data[2]=0x%X data[3]=0x%X data[6:10]=%s",
        ord_(data[2]),
        ord_(data[3]),
        data[6:10],
    )
    while ord_(data[2]) == 0xFF and data[6:10] in (b"JFIF", b"JFXX", b"OLYM", b"Phot"):
        length = ord_(data[4]) * 256 + ord_(data[5])
        logger.debug(" Length offset is %s", length)
        fh.read(length - 8)
        # fake an EXIF beginning of file
        # I don't think this is used. --gd
        data = b"\xff\x00" + fh.read(10)
        fake_exif = 1
        if base > 2:
            logger.debug(" Added to base")
            base = base + length + 4 - 2
        else:
            logger.debug(" Added to zero")
            base = length + 4
        logger.debug(" Set segment base to 0x%X", base)
    return base, fake_exif


def _get_base(base: int, data: bytes) -> int:
    # pylint: disable=too-many-statements
    while True:
        logger.debug(" Segment base 0x%X", base)
        if data[base : base + 2] == b"\xff\xe1":
            # APP1
            logger.debug("  APP1 at base 0x%X", base)
            logger.debug(
                "  Length: 0x%X 0x%X", ord_(data[base + 2]), ord_(data[base + 3])
            )
            logger.debug("  Code: %s", data[base + 4 : base + 8])
            if data[base + 4 : base + 8] == b"Exif":
                logger.debug(
                    "  Decrement base by 2 to get to pre-segment header (for compatibility with later code)"
                )
                base -= 2
                break
            increment = _increment_base(data, base)
            logger.debug(" Increment base by %s", increment)
            base += increment
        elif data[base : base + 2] == b"\xff\xe0":
            # APP0
            logger.debug("  APP0 at base 0x%X", base)
            logger.debug(
                "  Length: 0x%X 0x%X", ord_(data[base + 2]), ord_(data[base + 3])
            )
            logger.debug("  Code: %s", data[base + 4 : base + 8])
            increment = _increment_base(data, base)
            logger.debug(" Increment base by %s", increment)
            base += increment
        elif data[base : base + 2] == b"\xff\xe2":
            # APP2
            logger.debug("  APP2 at base 0x%X", base)
            logger.debug(
                "  Length: 0x%X 0x%X", ord_(data[base + 2]), ord_(data[base + 3])
            )
            logger.debug(" Code: %s", data[base + 4 : base + 8])
            increment = _increment_base(data, base)
            logger.debug(" Increment base by %s", increment)
            base += increment
        elif data[base : base + 2] == b"\xff\xee":
            # APP14
            logger.debug("  APP14 Adobe segment at base 0x%X", base)
            logger.debug(
                "  Length: 0x%X 0x%X", ord_(data[base + 2]), ord_(data[base + 3])
            )
            logger.debug("  Code: %s", data[base + 4 : base + 8])
            increment = _increment_base(data, base)
            logger.debug(" Increment base by %s", increment)
            base += increment
            logger.debug(
                "  There is useful EXIF-like data here, but we have no parser for it."
            )
        elif data[base : base + 2] == b"\xff\xdb":
            logger.debug(
                "  JPEG image data at base 0x%X No more segments are expected.", base
            )
            break
        elif data[base : base + 2] == b"\xff\xd8":
            # APP12
            logger.debug("  FFD8 segment at base 0x%X", base)
            logger.debug(
                "  Got 0x%X 0x%X and %s instead",
                ord_(data[base]),
                ord_(data[base + 1]),
                data[4 + base : 10 + base],
            )
            logger.debug(
                "  Length: 0x%X 0x%X", ord_(data[base + 2]), ord_(data[base + 3])
            )
            logger.debug("  Code: %s", data[base + 4 : base + 8])
            increment = _increment_base(data, base)
            logger.debug("  Increment base by %s", increment)
            base += increment
        elif data[base : base + 2] == b"\xff\xec":
            # APP12
            logger.debug(
                "  APP12 XMP (Ducky) or Pictureinfo segment at base 0x%X", base
            )
            logger.debug(
                "  Got 0x%X and 0x%X instead", ord_(data[base]), ord_(data[base + 1])
            )
            logger.debug(
                "  Length: 0x%X 0x%X", ord_(data[base + 2]), ord_(data[base + 3])
            )
            logger.debug("Code: %s", data[base + 4 : base + 8])
            increment = _increment_base(data, base)
            logger.debug("  Increment base by %s", increment)
            base += increment
            logger.debug(
                (
                    "  There is useful EXIF-like data here (quality, comment, copyright), "
                    "but we have no parser for it."
                )
            )
        else:
            try:
                increment = _increment_base(data, base)
                logger.debug(
                    "  Got 0x%X and 0x%X instead",
                    ord_(data[base]),
                    ord_(data[base + 1]),
                )
            except IndexError as err:
                raise InvalidExif(
                    "Unexpected/unhandled segment type or file content."
                ) from err
            logger.debug("  Increment base by %s", increment)
            base += increment
    return base


def find_jpeg_exif(fh: BinaryIO, data: bytes, fake_exif: int) -> Tuple[int, bytes, int]:
    logger.debug(
        "JPEG format recognized data[0:2]=0x%X%X", ord_(data[0]), ord_(data[1])
    )

    base, fake_exif = _get_initial_base(fh, data, fake_exif)

    # Big ugly patch to deal with APP2 (or other) data coming before APP1
    fh.seek(0)
    # in theory, this could be insufficient since 64K is the maximum size--gd
    data = fh.read(base + 4000)

    base = _get_base(base, data)

    fh.seek(base + 12)
    if ord_(data[2 + base]) == 0xFF and data[6 + base : 10 + base] == b"Exif":
        # detected EXIF header
        offset = fh.tell()
        endian = fh.read(1)
        # HACK TEST:  endian = 'M'
    elif ord_(data[2 + base]) == 0xFF and data[6 + base : 10 + base + 1] == b"Ducky":
        # detected Ducky header.
        logger.debug(
            "EXIF-like header (normally 0xFF and code): 0x%X and %s",
            ord_(data[2 + base]),
            data[6 + base : 10 + base + 1],
        )
        offset = fh.tell()
        endian = fh.read(1)
    elif ord_(data[2 + base]) == 0xFF and data[6 + base : 10 + base + 1] == b"Adobe":
        # detected APP14 (Adobe)
        logger.debug(
            "EXIF-like header (normally 0xFF and code): 0x%X and %s",
            ord_(data[2 + base]),
            data[6 + base : 10 + base + 1],
        )
        offset = fh.tell()
        endian = fh.read(1)
    else:
        # no EXIF information
        msg = "No EXIF header expected data[2+base]==0xFF and data[6+base:10+base]===Exif (or Duck)"
        msg += " Did get 0x%X and %r" % (
            ord_(data[2 + base]),
            data[6 + base : 10 + base + 1],
        )
        raise InvalidExif(msg)
    return offset, endian, fake_exif
