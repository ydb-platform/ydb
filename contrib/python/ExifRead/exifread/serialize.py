"""
Enable conversion of Exif IfdTags to native Python types
"""

from typing import Callable, Dict, List, Union

from exifread.core.exif_header import IfdTag
from exifread.exif_log import get_logger
from exifread.tags.fields import FieldType

logger = get_logger()

SerializedTagValue = Union[int, float, str, bytes, List[int], List[float], None]
SerializedTagDict = Dict[str, SerializedTagValue]


def convert_types(
    exif_tags: Dict[str, Union[IfdTag, bytes]],
) -> SerializedTagDict:
    """
    Convert Exif IfdTags to built-in Python types for easier serialization and programmatic use.

    - If the printable value of the IfdTag is relevant (e.g. enum type), it is preserved.
    - Otherwise, values are processed based on their field type, with some cleanups applied.
    - Single-element lists are unpacked to return the item directly.
    """

    output: SerializedTagDict = {}

    for tag_name, ifd_tag in exif_tags.items():
        # JPEGThumbnail and TIFFThumbnail are the only values
        # in Exif Tags dict that do not have the IfdTag type.
        if isinstance(ifd_tag, bytes):
            output[tag_name] = ifd_tag
            continue

        convert_func: Callable[[IfdTag, str], SerializedTagValue]

        if ifd_tag.prefer_printable:
            # Prioritize the printable value if prefer_printable is set
            convert_func = convert_proprietary

        else:
            # Get the conversion function based on field type
            try:
                convert_func = conversion_map[ifd_tag.field_type]
            except KeyError:
                logger.error(
                    "Type conversion for field type %s not explicitly supported",
                    ifd_tag.field_type,
                )
                convert_func = convert_proprietary  # Fallback to printable

        output[tag_name] = convert_func(ifd_tag, tag_name)

        # Useful only for testing
        # logger.warning(
        #     f"{convert_func.__name__}: {ifd_tag.field_type} to {type(output[tag_name]).__name__}\n"
        #     f"{tag_name} --> {str(output[tag_name])[:30]!r}"
        # )

    return output


def convert_ascii(ifd_tag: IfdTag, tag_name: str) -> Union[str, bytes, None]:
    """
    Handle ASCII conversion, including special date formats.

    Returns:
    - str
    - bytes for rare ascii sequences that aren't Unicode
    - None for empty values
    """

    out = ifd_tag.values

    # Handle DateTime formatting; often formatted in a way that cannot
    # be parsed by Python dateutil (%Y:%m:%d %H:%M:%S).
    if "DateTime" in tag_name and len(out) == 19 and out.count(":") == 4:
        out = out.replace(":", "-", 2)

    # Handle GPSDate formatting; these are proper dates with the wrong
    # delimiter (':' rather than '-'). Invalid values have been found
    # in test images: '' and '2014:09:259'
    elif tag_name == "GPS GPSDate" and len(out) == 10 and out.count(":") == 2:
        out = out.replace(":", "-")

    # Strip occasional trailing whitespaces
    out = out.strip()

    if not out:
        return None

    # Attempt to decode bytes if unicode
    if isinstance(out, bytes):
        try:
            return out.decode()
        except UnicodeDecodeError:
            pass

    return out


def convert_undefined(ifd_tag: IfdTag, _tag_name: str) -> Union[bytes, str, int, None]:
    """
    Handle Undefined type conversion.

    Returns:
    - bytes if not Unicode such as Exif MakerNote
    - str for Unicode
    - int for rare MakerNote Tags containing a single value
    - None for empty values such as some MakerNote Tags
    """

    out = ifd_tag.values

    if len(out) == 1:
        # Return integer from single-element list
        return out[0]

    # These contain bytes represented as a list of integers, sometimes with surrounded by spaces and/or null bytes
    out = bytes(out).strip(b" \x00")

    if not out:
        return None

    # Empty byte sequences or Unicode values should be decoded as strings
    try:
        return out.decode()
    except UnicodeDecodeError:
        return out


def convert_numeric(ifd_tag: IfdTag, _tag_name: str) -> Union[int, List[int], None]:
    """
    Handle numeric types conversion.

    Returns:
    - int in most cases
    - list of int
    - None for empty values such as some MakerNote Tags

    Note: All Floating Point tags seen were empty.
    """

    out = ifd_tag.values

    if not out:  # Empty lists, seen in floating point numbers
        return None

    return out[0] if len(out) == 1 else out


def convert_ratio(
    ifd_tag: IfdTag, _tag_name: str
) -> Union[int, float, List[int], List[float], None]:
    """
    Handle Ratio and Signed Ratio conversion.

    Returns:
    - int when the denominator is 1 or unused
    - float otherwise
    - a list of int or float, such as GPS Latitude/Longitude/TimeStamp
    - None for empty values such as some MakerNote Tags

    Ratios can be re-created with `Ratio(float_value).limit_denominator()`.
    """

    out = []

    for ratio in ifd_tag.values:
        # Prevent division by 0. Sometimes, EXIF data is full of 0s when a feature is unused.
        if ratio.denominator == 0:
            ratio = ratio.numerator

        ratio = float(ratio)

        if ratio.is_integer():
            ratio = int(ratio)

        out.append(ratio)

    if not out:
        return None

    return out[0] if len(out) == 1 else out


def convert_bytes(ifd_tag: IfdTag, tag_name: str) -> Union[bytes, str, int, None]:
    """
    Handle Byte and Signed Byte conversion.

    Returns:
    - bytes
    - str for Unicode such as GPSVersionID and Image ApplicationNotes (XML)
    - int for single byte values such as GPSAltitudeRef or some MakerNote fields
    - None for empty values such as some MakerNote Tags
    """

    out = ifd_tag.values

    if len(out) == 1:
        # Byte can be a single integer, such as GPSAltitudeRef (0 or 1)
        return out[0]

    if tag_name == "GPS GPSVersionID":
        return ".".join(map(str, out))  # e.g. [2, 3, 0, 0] --> '2.3.0.0'

    # Byte sequences are often surrounded by or only composed of spaces and/or null bytes
    out = bytes(out).strip(b" \x00")

    if not out:
        return None

    # Unicode values should be decoded as strings (e.g. XML)
    try:
        return out.decode()
    except UnicodeDecodeError:
        return out


def convert_proprietary(ifd_tag: IfdTag, _tag_name: str) -> Union[str, None]:
    """
    Handle Proprietary type conversion.

    Returns:
    - str as all tags of this made-up type (e.g. enums) prefer printable
    - None for very rare empty printable values
    """

    out = ifd_tag.printable
    if not out or out == "[]":
        return None

    return out


# Mapping of field type to conversion function
conversion_map: Dict[FieldType, Callable] = {
    FieldType.PROPRIETARY: convert_proprietary,
    FieldType.BYTE: convert_bytes,
    FieldType.ASCII: convert_ascii,
    FieldType.SHORT: convert_numeric,
    FieldType.LONG: convert_numeric,
    FieldType.RATIO: convert_ratio,
    FieldType.SIGNED_BYTE: convert_numeric,
    FieldType.UNDEFINED: convert_undefined,
    FieldType.SIGNED_SHORT: convert_numeric,
    FieldType.SIGNED_LONG: convert_numeric,
    FieldType.SIGNED_RATIO: convert_ratio,
    FieldType.FLOAT_32: convert_numeric,
    FieldType.FLOAT_64: convert_numeric,
    FieldType.IFD: convert_bytes,
}
