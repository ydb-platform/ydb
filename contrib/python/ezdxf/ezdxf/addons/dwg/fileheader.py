# Copyright (c) 2020-2021, Manfred Moitzi
# License: MIT License
from typing import Dict, Tuple
import struct
from .const import *
from .crc import crc8

codepage_to_encoding = {
    37: "cp874",  # Thai,
    38: "cp932",  # Japanese
    39: "gbk",  # UnifiedChinese
    40: "cp949",  # Korean
    41: "cp950",  # TradChinese
    28: "cp1250",  # CentralEurope
    29: "cp1251",  # Cyrillic
    30: "cp1252",  # WesternEurope
    32: "cp1253",  # Greek
    33: "cp1254",  # Turkish
    34: "cp1255",  # Hebrew
    35: "cp1256",  # Arabic
    36: "cp1257",  # Baltic
}

FILE_HEADER_MAGIC = {
    3: 0xA598,
    4: 0x8101,
    5: 0x3CC4,
    6: 0x8461,
}


class FileHeader:
    def __init__(self, data: Bytes, crc_check=False):
        self.crc_check = crc_check
        if len(data) < 6:
            raise DwgVersionError("Not a DWG file.")
        ver = data[:6].decode(errors="ignore")  # type: ignore
        if ver not in SUPPORTED_VERSIONS:
            raise DwgVersionError(
                f"Not a DWG file or unsupported DWG version, signature: {ver}."
            )
        self.version: str = ver
        codepage: int = struct.unpack_from("<h", data, 0x13)[0]
        self.encoding = codepage_to_encoding.get(codepage, "cp1252")
        self.maintenance_release_version = data[0xB]
        self.sections: Dict[int, Tuple[int, int]] = dict()
        if self.version <= ACAD_2000:
            self.r2000_header(data)
        else:
            raise DwgVersionError(self.version)

    def r2000_header(self, data: Bytes):
        index = 0x15
        section_count: int = struct.unpack_from("<L", data, index)[0]
        index += 4
        fmt = "<BLL"
        record_size = struct.calcsize(fmt)
        for record in range(section_count):
            # 0: HEADER_ID
            # 1: CLASSES_ID
            # 2: OBJECTS_ID
            num, seeker, size = struct.unpack_from(fmt, data, index)
            index += record_size
            self.sections[num] = (seeker, size)

        if self.crc_check:
            # CRC from first byte of file until start of crc value
            check = (
                crc8(data[:index], seed=0)
                ^ FILE_HEADER_MAGIC[len(self.sections)]
            )
            crc = struct.unpack_from("<H", data, index)[0]
            if crc != check:
                raise CRCError("CRC error in file header.")

        index += 2
        sentinel = data[index : index + SENTINEL_SIZE]
        if (
            sentinel
            != b"\x95\xA0\x4E\x28\x99\x82\x1A\xE5\x5E\x41\xE0\x5F\x9D\x3A\x4D\x00"
        ):
            raise DwgCorruptedFileHeader(
                "Corrupted DXF R13/14/2000 file header."
            )

    def print(self):
        print(f"DWG version: {self.version}")
        print(f"encoding: {self.encoding}")
        print(f"Records: {len(self.sections)}")
        print("Header: seeker {0[0]} size: {0[1]}".format(self.sections[0]))
        print("Classes: seeker {0[0]} size: {0[1]}".format(self.sections[1]))
        print("Objects: seeker {0[0]} size: {0[1]}".format(self.sections[2]))
