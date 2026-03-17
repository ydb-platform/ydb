# Copyright (c) 2020-2021, Manfred Moitzi
# License: MIT License
from typing import Iterable, Tuple
import struct

from ezdxf.tools.binarydata import BitStream
from ezdxf.entities import DXFClass

from .const import *
from .crc import crc8
from .fileheader import FileHeader
from .header_section import DwgSectionLoader


def load_classes_section(specs: FileHeader, data: Bytes, crc_check=False):
    if specs.version <= ACAD_2000:
        return DwgClassesSectionR2000(specs, data, crc_check)
    else:
        return DwgClassesSectionR2004(specs, data, crc_check)


class DwgClassesSectionR2000(DwgSectionLoader):
    def load_data_section(self, data: Bytes) -> Bytes:
        if self.specs.version > ACAD_2000:
            raise DwgVersionError(self.specs.version)
        seeker, section_size = self.specs.sections[CLASSES_ID]
        return data[seeker : seeker + section_size]

    def load_classes(self) -> Iterable[Tuple[int, DXFClass]]:
        sentinel = self.data[:SENTINEL_SIZE]
        if (
            sentinel
            != b"\x8D\xA1\xC4\xB8\xC4\xA9\xF8\xC5\xC0\xDC\xF4\x5F\xE7\xCF\xB6\x8A"
        ):
            raise DwgCorruptedClassesSection(
                "Sentinel for start of CLASSES section not found."
            )
        start_index = SENTINEL_SIZE
        bs = BitStream(
            self.data[start_index:],
            dxfversion=self.specs.version,
            encoding=self.specs.encoding,
        )
        class_data_size = bs.read_unsigned_long()  # data size in bytes
        end_sentinel_index = SENTINEL_SIZE + 6 + class_data_size
        end_index = end_sentinel_index - 2
        end_bit_index = (3 + class_data_size) << 3

        while bs.bit_index < end_bit_index:
            class_num = bs.read_bit_short()
            dxfattribs = {
                "flags": bs.read_bit_short(),  # version?
                "app_name": bs.read_text(),
                "cpp_class_name": bs.read_text(),
                "name": bs.read_text(),
                "was_a_proxy": bs.read_bit(),
                "is_an_entity": int(bs.read_bit_short() == 0x1F2),
            }
            yield class_num, DXFClass.new(dxfattribs=dxfattribs)

        if self.crc_check and False:
            check = struct.unpack_from("<H", self.data, end_index)[0]
            # TODO: classes crc check
            # Which data should be checked? This is not correct:
            crc = crc8(self.data[start_index:end_index])
            if check != crc:
                raise CRCError("CRC error in classes section.")
        sentinel = self.data[
            end_sentinel_index : end_sentinel_index + SENTINEL_SIZE
        ]
        if (
            sentinel
            != b"\x72\x5E\x3B\x47\x3B\x56\x07\x3A\x3F\x23\x0B\xA0\x18\x30\x49\x75"
        ):
            raise DwgCorruptedClassesSection(
                "Sentinel for end of CLASSES section not found."
            )


class DwgClassesSectionR2004(DwgClassesSectionR2000):
    def load_data(self, data: Bytes) -> Bytes:
        raise NotImplementedError()
