"""
Base classes.
"""

import re
import struct
from typing import Any, BinaryIO, Dict, List, Optional, Tuple, Union

from exifread.core.exceptions import ExifError
from exifread.core.ifd_tag import IfdTag
from exifread.core.xmp import xmp_bytes_to_str
from exifread.exif_log import get_logger
from exifread.tags import (
    DEFAULT_STOP_TAG,
    IGNORE_TAGS,
    IfdDictValue,
    SubIfdTagDict,
    SubIfdTagDictValue,
)
from exifread.tags.exif import EXIF_TAGS
from exifread.tags.fields import (
    FIELD_DEFINITIONS,
    FLOAT_FIELD_TYPES,
    RATIO_FIELD_TYPES,
    SIGNED_FIELD_TYPES,
    FieldType,
)
from exifread.tags.makernote import (
    apple,
    canon,
    casio,
    dji,
    fujifilm,
    nikon,
    olympus,
    sony,
)
from exifread.utils import Ratio

logger = get_logger()


class ExifHeader:
    """
    Handle an EXIF header.
    """

    def __init__(
        self,
        file_handle: BinaryIO,
        endian: str,
        offset: int,
        fake_exif: int,
        strict: bool,
        debug=False,
        detailed=True,
        truncate_tags=True,
    ) -> None:
        self.file_handle = file_handle
        self.endian = endian
        self.offset = offset
        self.fake_exif = fake_exif
        self.strict = strict
        self.debug = debug
        self.detailed = detailed
        self.truncate_tags = truncate_tags
        self.tags: Dict[str, Any] = {}

    def s2n(self, offset: int, length: int, signed=False) -> int:
        """
        Convert slice to integer, based on sign and endian flags.

        Usually this offset is assumed to be relative to the beginning of the
        start of the EXIF information.
        For some cameras that use relative tags, this offset may be relative
        to some other starting point.
        """
        # Little-endian if Intel, big-endian if Motorola
        fmt = "<" if self.endian == "I" else ">"
        # Construct a format string from the requested length and signedness;
        # raise a ValueError if length is something silly like 3
        try:
            fmt += {
                (1, False): "B",
                (1, True): "b",
                (2, False): "H",
                (2, True): "h",
                (4, False): "I",
                (4, True): "i",
                (8, False): "L",
                (8, True): "l",
            }[(length, signed)]
        except KeyError as err:
            raise ValueError("unexpected unpacking length: %d" % length) from err
        self.file_handle.seek(self.offset + offset)
        buf = self.file_handle.read(length)

        if buf:
            # Make sure the buffer is the proper length.
            # Allows bypassing of corrupt slices.
            if len(buf) != length:
                logger.warning("Unexpected slice length: %d", len(buf))
                return 0
            return struct.unpack(fmt, buf)[0]
        return 0

    def n2b(self, offset: int, length: int) -> bytes:
        """Convert offset to bytes."""
        s = b""
        for _ in range(length):
            if self.endian == "I":
                s += bytes([offset & 0xFF])
            else:
                s = bytes([offset & 0xFF]) + s
            offset = offset >> 8
        return s

    def _first_ifd(self) -> int:
        """Return first IFD."""
        return self.s2n(4, 4)

    def _next_ifd(self, ifd: int) -> int:
        """Return the pointer to next IFD."""
        entries = self.s2n(ifd, 2)
        next_ifd = self.s2n(ifd + 2 + 12 * entries, 4)
        if next_ifd == ifd:
            return 0
        return next_ifd

    def list_ifd(self) -> List[int]:
        """Return the list of IFDs in the header."""
        i = self._first_ifd()
        ifds = []
        set_ifds = set()
        while i:
            if i in set_ifds:
                logger.warning("IFD loop detected.")
                break
            set_ifds.add(i)
            ifds.append(i)
            i = self._next_ifd(i)
        return ifds

    def _process_field(
        self,
        tag_name: str,
        count: int,
        field_type: int,
        type_length: int,
        offset: int,
    ) -> list:
        values: List[Any] = []
        signed = field_type in SIGNED_FIELD_TYPES
        # TODO: investigate
        # some entries get too big to handle could be malformed
        # file or problem with self.s2n
        if count < 1000:
            for _ in range(count):
                if field_type in RATIO_FIELD_TYPES:
                    # a ratio
                    ratio_value = Ratio(
                        self.s2n(offset, 4, signed), self.s2n(offset + 4, 4, signed)
                    )
                    values.append(ratio_value)
                elif field_type in FLOAT_FIELD_TYPES:
                    # a float or double
                    unpack_format = ""
                    if self.endian == "I":
                        unpack_format += "<"
                    else:
                        unpack_format += ">"
                    if field_type == FieldType.FLOAT_32:
                        unpack_format += "f"
                    else:
                        unpack_format += "d"
                    self.file_handle.seek(self.offset + offset)
                    byte_str = self.file_handle.read(type_length)
                    try:
                        values.append(struct.unpack(unpack_format, byte_str))
                    except struct.error:
                        logger.warning("Possibly corrupted field %s", tag_name)

                else:
                    value = self.s2n(offset, type_length, signed)
                    values.append(value)

                offset = offset + type_length
        # The test above causes problems with tags that are
        # supposed to have long values! Fix up one important case.
        elif tag_name in ("MakerNote", canon.CAMERA_INFO_TAG_NAME):
            for _ in range(count):
                value = self.s2n(offset, type_length, signed)
                values.append(value)
                offset = offset + type_length
        return values

    def _process_ascii_field(
        self, ifd_name: str, tag_name: str, count: int, offset: int
    ):
        values: Union[str, bytes] = ""
        # special case: null-terminated ASCII string
        # XXX investigate
        # sometimes gets too big to fit in int value
        if count != 0:  # and count < (2**31):  # 2E31 is hardware dependent. --gd
            file_position = self.offset + offset
            try:
                self.file_handle.seek(file_position)
                values = self.file_handle.read(count)

                # Drop any garbage after a null.
                values = values.split(b"\x00", 1)[0]
                if isinstance(values, bytes):
                    try:
                        values = values.decode("utf-8")
                    except UnicodeDecodeError:
                        logger.warning(
                            "Possibly corrupted field %s in %s IFD", tag_name, ifd_name
                        )
            except OverflowError:
                logger.warning(
                    "OverflowError at position: %s, length: %s", file_position, count
                )
                values = ""
            except MemoryError:
                logger.warning(
                    "MemoryError at position: %s, length: %s", file_position, count
                )
                values = ""
        return values

    def _get_printable_for_field(
        self,
        count: int,
        values: Union[str, list],
        field_type: FieldType,
        tag_entry: IfdDictValue,
        stop_tag: str,
    ) -> Tuple[str, bool]:
        # TODO: use only one type
        if count == 1 and field_type != FieldType.ASCII:
            printable = str(values[0])
        elif count > 50 and len(values) > 20 and not isinstance(values, str):
            if self.truncate_tags:
                printable = str(values[0:20])[0:-1] + ", ... ]"
            else:
                printable = str(values[0:-1])
        else:
            printable = str(values)

        prefer_printable = False

        # compute printable version of values
        if tag_entry:
            # optional 2nd tag element is present
            if tag_entry[1] is not None:
                prefer_printable = True

                # call mapping function
                if callable(tag_entry[1]):
                    printable = tag_entry[1](values)
                # handle sub-ifd
                elif isinstance(tag_entry[1], tuple):
                    ifd_info = tag_entry[1]
                    try:
                        logger.debug("%s SubIFD at offset %d:", ifd_info[0], values[0])
                        self.dump_ifd(
                            ifd=values[0],  # type: ignore
                            stop_tag=stop_tag,
                            ifd_name=ifd_info[0],
                            tag_dict=ifd_info[1],
                        )
                    except IndexError:
                        logger.warning("No values found for %s SubIFD", ifd_info[0])
                else:
                    printable = ""
                    for val in values:
                        # use lookup table for this tag
                        printable += tag_entry[1].get(val, repr(val))

        return printable, prefer_printable

    def _process_tag(
        self,
        ifd: int,
        ifd_name: str,
        tag_entry: SubIfdTagDictValue,
        entry: int,
        tag: int,
        tag_name: str,
        relative: bool,
        stop_tag: str,
    ) -> None:
        field_type_id = self.s2n(entry + 2, 2)
        try:
            field_type = FieldType(field_type_id)
        except ValueError as err:
            if self.strict:
                raise ValueError(
                    "Unknown field type %d in tag 0x%04X" % (field_type_id, tag)
                ) from err
            return

        # Not handled
        if field_type == FieldType.PROPRIETARY:
            if self.strict:
                raise ValueError(
                    "Unknown field type %d in tag 0x%04X" % (field_type_id, tag)
                )
            return

        type_length = FIELD_DEFINITIONS[field_type][0]
        count = self.s2n(entry + 4, 4)
        # Adjust for tag id/type/count (2+2+4 bytes)
        # Now we point at either the data or the 2nd level offset
        offset = entry + 8

        # If the value fits in 4 bytes, it is inlined, else we
        # need to jump ahead again.
        if count * type_length > 4:
            # offset is not the value; it's a pointer to the value
            # if relative we set things up so s2n will seek to the right
            # place when it adds self.offset.  Note that this 'relative'
            # is for the Nikon type 3 makernote.  Other cameras may use
            # other relative offsets, which would have to be computed here
            # slightly differently.
            if relative:
                tmp_offset = self.s2n(offset, 4)
                offset = tmp_offset + ifd - 8
                if self.fake_exif:
                    offset += 18
            else:
                offset = self.s2n(offset, 4)

        field_offset = offset
        if field_type == FieldType.ASCII:
            values = self._process_ascii_field(ifd_name, tag_name, count, offset)
        else:
            values = self._process_field(
                tag_name, count, field_type, type_length, offset
            )

        printable, prefer_printable = self._get_printable_for_field(
            count, values, field_type, tag_entry, stop_tag
        )

        self.tags[ifd_name + " " + tag_name] = IfdTag(
            printable,
            tag,
            field_type,
            values,
            field_offset,
            count * type_length,
            prefer_printable,
        )
        tag_value = repr(self.tags[ifd_name + " " + tag_name])
        logger.debug(" %s: %s", tag_name, tag_value)

    def dump_ifd(
        self,
        ifd: int,
        ifd_name: str,
        tag_dict=None,
        relative=0,
        stop_tag=DEFAULT_STOP_TAG,
    ) -> None:
        """Return a list of entries in the given IFD."""

        # make sure we can process the entries
        if tag_dict is None:
            tag_dict = EXIF_TAGS
        try:
            entries = self.s2n(ifd, 2)
        except TypeError:
            logger.warning("Possibly corrupted IFD: %s", ifd_name)
            return

        for i in range(entries):
            # entry is index of start of this IFD in the file
            entry = ifd + 2 + 12 * i
            tag = self.s2n(entry, 2)

            # get tag name early to avoid errors, help debug
            tag_entry = tag_dict.get(tag)
            if tag_entry:
                tag_name = tag_entry[0]
            else:
                tag_name = f"Tag 0x{tag:04X}"

            # ignore certain tags for faster processing
            if not (not self.detailed and tag in IGNORE_TAGS):
                self._process_tag(
                    ifd, ifd_name, tag_entry, entry, tag, tag_name, relative, stop_tag
                )

            if tag_name == stop_tag:
                break

    def extract_tiff_thumbnail(self, thumb_ifd: int) -> None:
        """
        Extract uncompressed TIFF thumbnail.

        Take advantage of the pre-existing layout in the thumbnail IFD as
        much as possible
        """
        thumb = self.tags.get("Thumbnail Compression")
        if not thumb or thumb.printable != "Uncompressed TIFF":
            return

        entries = self.s2n(thumb_ifd, 2)
        # this is header plus offset to IFD ...
        if self.endian == "M":
            tiff = b"MM\x00*\x00\x00\x00\x08"
        else:
            tiff = b"II*\x00\x08\x00\x00\x00"
            # ... plus thumbnail IFD data plus a null "next IFD" pointer
        self.file_handle.seek(self.offset + thumb_ifd)
        tiff += self.file_handle.read(entries * 12 + 2) + b"\x00\x00\x00\x00"

        # fix up large value offset pointers into data area
        for i in range(entries):
            entry = thumb_ifd + 2 + 12 * i
            tag = self.s2n(entry, 2)
            field_type = FieldType(self.s2n(entry + 2, 2))
            type_length = FIELD_DEFINITIONS[field_type][0]
            count = self.s2n(entry + 4, 4)
            old_offset = self.s2n(entry + 8, 4)
            # start of the 4-byte pointer area in entry
            ptr = i * 12 + 18

            # remember strip offsets
            strip_len = 0
            if tag == 0x0111:
                strip_off = ptr
                strip_len = count * type_length
                # is it in the data area?
            if count * type_length > 4:
                # update offset pointer (nasty "strings are immutable" crap)
                # should be able to say "tiff[ptr:ptr+4]=newoff"
                newoff = len(tiff)
                tiff = tiff[:ptr] + self.n2b(newoff, 4) + tiff[ptr + 4 :]
                # remember strip offsets location
                if tag == 0x0111:
                    strip_off = newoff
                    strip_len = 4
                # get original data and store it
                self.file_handle.seek(self.offset + old_offset)
                tiff += self.file_handle.read(count * type_length)

        # add pixel strips and update strip offset info
        old_offsets = self.tags["Thumbnail StripOffsets"].values
        old_counts = self.tags["Thumbnail StripByteCounts"].values
        for i, old_offset in enumerate(old_offsets):
            # update offset pointer (more nasty "strings are immutable" crap)
            offset = self.n2b(len(tiff), strip_len)
            tiff = tiff[:strip_off] + offset + tiff[strip_off + strip_len :]
            strip_off += strip_len
            # add pixel strip to end
            self.file_handle.seek(self.offset + old_offset)
            tiff += self.file_handle.read(old_counts[i])

        self.tags["TIFFThumbnail"] = tiff

    def extract_jpeg_thumbnail(self) -> None:
        """
        Extract JPEG thumbnail.

        (Thankfully the JPEG data is stored as a unit.)
        """
        thumb_offset = self.tags.get("Thumbnail JPEGInterchangeFormat")
        thumb_length = self.tags.get("Thumbnail JPEGInterchangeFormatLength")
        if thumb_offset and thumb_length:
            self.file_handle.seek(self.offset + thumb_offset.values[0])
            size = thumb_length.values[0]
            self.tags["JPEGThumbnail"] = self.file_handle.read(size)

        # Sometimes in a TIFF file, a JPEG thumbnail is hidden in the MakerNote
        # since it's not allowed in a uncompressed TIFF IFD
        if "JPEGThumbnail" not in self.tags:
            thumb_offset = self.tags.get("MakerNote JPEGThumbnail")
            if thumb_offset:
                self.file_handle.seek(self.offset + thumb_offset.values[0])
                self.tags["JPEGThumbnail"] = self.file_handle.read(
                    thumb_offset.field_length
                )

    def decode_maker_note(self) -> None:
        """
        Decode all the camera-specific MakerNote formats

        Note is the data that comprises this MakerNote.
        The MakerNote will likely have pointers in it that point to other
        parts of the file. We'll use self.offset as the starting point for
        most of those pointers, since they are relative to the beginning
        of the file.
        If the MakerNote is in a newer format, it may use relative addressing
        within the MakerNote. In that case we'll use relative addresses for
        the pointers.
        As an aside: it's not just to be annoying that the manufacturers use
        relative offsets.  It's so that if the makernote has to be moved by the
        picture software all of the offsets don't have to be adjusted.  Overall,
        this is probably the right strategy for makernotes, though the spec is
        ambiguous.
        The spec does not appear to imagine that makernotes would
        follow EXIF format internally.  Once they did, it's ambiguous whether
        the offsets should be from the header at the start of all the EXIF info,
        or from the header at the start of the makernote.

        TODO: look into splitting this up
        """
        note = self.tags["EXIF MakerNote"]

        # Some apps use MakerNote tags but do not use a format for which we
        # have a description, so just do a raw dump for these.
        make = self.tags["Image Make"].printable

        # Nikon
        # The maker note usually starts with the word Nikon, followed by the
        # type of the makernote (1 or 2, as a short).  If the word Nikon is
        # not at the start of the makernote, it's probably type 2, since some
        # cameras work that way.
        if "NIKON" in make:
            if note.values[0:7] == [78, 105, 107, 111, 110, 0, 1]:
                logger.debug("Looks like a type 1 Nikon MakerNote.")
                self.dump_ifd(
                    ifd=note.field_offset + 8,
                    ifd_name="MakerNote",
                    tag_dict=nikon.TAGS_OLD,
                )
            elif note.values[0:7] == [78, 105, 107, 111, 110, 0, 2]:
                logger.debug("Looks like a labeled type 2 Nikon MakerNote")
                if note.values[12:14] != [0, 42] and note.values[12:14] != [42, 0]:
                    raise ValueError("Missing marker tag 42 in MakerNote.")
                    # skip the Makernote label and the TIFF header
                self.dump_ifd(
                    ifd=note.field_offset + 10 + 8,
                    ifd_name="MakerNote",
                    tag_dict=nikon.TAGS_NEW,
                    relative=1,
                )
            else:
                # E99x or D1
                logger.debug("Looks like an unlabeled type 2 Nikon MakerNote")
                self.dump_ifd(
                    ifd=note.field_offset, ifd_name="MakerNote", tag_dict=nikon.TAGS_NEW
                )
            return

        # Olympus
        if make.startswith("OLYMPUS"):
            self.dump_ifd(
                ifd=note.field_offset + 8, ifd_name="MakerNote", tag_dict=olympus.TAGS
            )
            # TODO
            # for i in (('MakerNote Tag 0x2020', makernote.OLYMPUS_TAG_0x2020),):
            #    self.decode_olympus_tag(self.tags[i[0]].values, i[1])
            # return

        # Casio
        if "CASIO" in make or "Casio" in make:
            self.dump_ifd(
                ifd=note.field_offset, ifd_name="MakerNote", tag_dict=casio.TAGS
            )
            return

        if "SONY" in make:
            self.dump_ifd(
                ifd=note.field_offset, ifd_name="MakerNote", tag_dict=sony.TAGS
            )
            return

        # Fujifilm
        if make == "FUJIFILM":
            # bug: everything else is "Motorola" endian, but the MakerNote
            # is "Intel" endian
            endian = self.endian
            self.endian = "I"
            # bug: IFD offsets are from beginning of MakerNote, not
            # beginning of file header
            offset = self.offset
            self.offset += note.field_offset
            # process note with bogus values (note is actually at offset 12)
            self.dump_ifd(ifd=12, ifd_name="MakerNote", tag_dict=fujifilm.TAGS)
            # reset to correct values
            self.endian = endian
            self.offset = offset
            return

        # Apple
        if make == "Apple" and note.values[0:10] == [
            65,
            112,
            112,
            108,
            101,
            32,
            105,
            79,
            83,
            0,
        ]:
            offset = self.offset
            self.offset += note.field_offset + 14
            self.dump_ifd(ifd=0, ifd_name="MakerNote", tag_dict=apple.TAGS)
            self.offset = offset
            return

        if make == "DJI":
            endian = self.endian
            self.endian = "I"
            offset = self.offset
            self.offset += note.field_offset
            self.dump_ifd(ifd=0, ifd_name="MakerNote", tag_dict=dji.TAGS)
            self.offset = offset
            self.endian = endian
            return

        # Canon
        if make == "Canon":
            self.dump_ifd(
                ifd=note.field_offset, ifd_name="MakerNote", tag_dict=canon.TAGS
            )
            for tag_id, tags_dict in canon.OFFSET_TAGS.items():
                tag_str = f"MakerNote Tag 0x{tag_id:04X}"
                if tag_str in self.tags:
                    logger.debug("Canon %s", tag_str)
                    self._canon_decode_tag(self.tags[tag_str].values, tags_dict)
                    del self.tags[tag_str]
            if canon.CAMERA_INFO_TAG_NAME in self.tags:
                tag = self.tags[canon.CAMERA_INFO_TAG_NAME]
                logger.debug("Canon CameraInfo")
                self._canon_decode_camera_info(tag)
                del self.tags[canon.CAMERA_INFO_TAG_NAME]
            return

    #    TODO Decode Olympus MakerNote tag based on offset within tag.
    #    def _olympus_decode_tag(self, value, mn_tags):
    #        pass

    def _canon_decode_tag(self, value, mn_tags: SubIfdTagDict) -> None:
        """
        Decode Canon MakerNote tag based on offset within tag.

        See http://www.burren.cx/david/canon.html by David Burren
        """
        for tag_idx in range(1, len(value)):
            tag_name, tag_format = mn_tags.get(tag_idx, ("Unknown", None))
            if tag_format is not None:
                if callable(tag_format):
                    val = tag_format(value[tag_idx])
                elif isinstance(tag_format, dict):
                    val = tag_format.get(value[tag_idx], "Unknown")
                else:
                    raise ExifError(f"Invalid tag type for Canon: {type(tag_format)}")
            else:
                val = value[tag_idx]
            try:
                logger.debug(" %s %s %s", tag_idx, tag_name, hex(value[tag_idx]))
            except TypeError:
                logger.debug(" %s %s %s", tag_idx, tag_name, value[tag_idx])

            # It's not a real IFD Tag, but we fake one to make everybody happy.
            # This will have a "proprietary" type
            self.tags["MakerNote " + tag_name] = IfdTag(
                printable=str(val),
                tag=0,
                field_type=FieldType.PROPRIETARY,
                values=val,
                field_offset=0,
                field_length=0,
            )

    def _canon_decode_camera_info(self, camera_info_tag: IfdTag) -> None:
        """
        Decode the variable length encoded camera info section.
        """
        model_tag: Optional[IfdTag] = self.tags.get("Image Model", None)
        if model_tag is None:
            return
        model = model_tag.printable

        for model_name_re, tag_desc in canon.CAMERA_INFO_MODEL_MAP.items():
            if re.search(model_name_re, model):
                camera_info_tags = tag_desc
                break
        else:
            return

        # We are assuming here that these are all unsigned bytes
        if camera_info_tag.field_type not in (FieldType.BYTE, FieldType.UNDEFINED):
            return
        camera_info = struct.pack(
            "<%dB" % len(camera_info_tag.values), *camera_info_tag.values
        )

        # Look for each data value and decode it appropriately.
        for offset, tag in camera_info_tags.items():
            tag_name, tag_format, tag_func = tag
            tag_size = struct.calcsize(tag_format)
            if len(camera_info) < offset + tag_size:
                continue
            packed_tag_value = camera_info[offset : offset + tag_size]
            tag_value = tag_func(struct.unpack(tag_format, packed_tag_value)[0])

            logger.debug(" %s %s", tag_name, tag_value)

            self.tags["MakerNote " + tag_name] = IfdTag(
                printable=str(tag_value),
                tag=0,
                field_type=FieldType.PROPRIETARY,
                values=tag_value,
                field_offset=0,
                field_length=0,
            )

    def parse_xmp(self, xmp_bytes: bytes):
        """Adobe's Extensible Metadata Platform, just dump the pretty XML."""

        logger.debug("XMP cleaning data")
        self.tags["Image ApplicationNotes"] = IfdTag(
            xmp_bytes_to_str(xmp_bytes), 0, FieldType.BYTE, xmp_bytes, 0, 0
        )
