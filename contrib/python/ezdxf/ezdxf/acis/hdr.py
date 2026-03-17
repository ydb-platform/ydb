#  Copyright (c) 2022-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime
import struct
from . import const

# ACIS versions exported by BricsCAD:
# R2000/AC1015: 400, "ACIS 4.00 NT", text length has no prefix "@"
# R2004/AC1018: 20800 @ "ACIS 208.00 NT", text length has "@" prefix ??? weird
# R2007/AC1021: 700 @ "ACIS 32.0 NT", text length has "@" prefix
# R2010/AC1024: 700 @ "ACIS 32.0 NT", text length has "@" prefix

# A test showed that R2000 files that contains ACIS v700/32.0 or v20800/208.0
# data can be opened by Autodesk TrueView, BricsCAD and Allplan, so exporting
# only v700/32.0 for all DXF versions should be OK!
# test script: exploration/acis/transplant_acis_data.py


def encode_str(s: str) -> bytes:
    b = s.encode("utf8", errors="ignore")
    return struct.pack("<BB", const.Tags.STR, len(b)) + b


def encode_double(value: float) -> bytes:
    return struct.pack("<Bd", const.Tags.DOUBLE, value)


@dataclass
class AcisHeader:
    """Represents an ACIS file header."""

    version: int = const.MIN_EXPORT_VERSION
    n_records: int = 0  # can be 0
    n_entities: int = 0
    flags: int = 0
    product_id: str = const.EZDXF_BUILDER_ID
    acis_version: str = const.ACIS_VERSION[const.MIN_EXPORT_VERSION]
    creation_date: datetime = field(default_factory=datetime.now)
    units_in_mm: float = 1.0
    asm_version: str = ""
    asm_end_marker: bool = False  # depends on DXF version: R2013, RT2018

    @property
    def has_asm_header(self) -> bool:
        return self.asm_version != ""

    def dumps(self) -> list[str]:
        """Returns the SAT file header as list of strings."""
        return [
            f"{self.version} {self.n_records} {self.n_entities} {self.flags} ",
            self._header_str(),
            f"{self.units_in_mm:g} 9.9999999999999995e-007 1e-010 ",
        ]

    def dumpb(self) -> bytes:
        """Returns the SAB file header as bytes."""
        buffer: list[bytes] = []
        if self.version > 21800:
            buffer.append(const.ASM_SIGNATURE)
        else:
            buffer.append(const.ACIS_SIGNATURE)
        data = struct.pack(
            "<iiii", self.version, self.n_records, self.n_entities, self.flags
        )
        buffer.append(data)
        buffer.append(encode_str(self.product_id))
        buffer.append(encode_str(self.acis_version))
        buffer.append(encode_str(self.creation_date.ctime()))
        buffer.append(encode_double(self.units_in_mm))
        buffer.append(encode_double(const.RES_TOL))
        buffer.append(encode_double(const.NOR_TOL))
        return b"".join(buffer)

    def _header_str(self) -> str:
        p_len = len(self.product_id)
        a_len = len(self.acis_version)
        date = self.creation_date.ctime()
        if self.version > 400:
            return f"@{p_len} {self.product_id} @{a_len} {self.acis_version} @{len(date)} {date} "
        else:
            return f"{p_len} {self.product_id} {a_len} {self.acis_version} {len(date)} {date} "

    def set_version(self, version: int) -> None:
        """Sets the ACIS version as an integer value and updates the version
        string accordingly.
        """
        try:
            self.acis_version = const.ACIS_VERSION[version]
            self.version = version
        except KeyError:
            raise ValueError(f"invalid ACIS version number {version}")
        self.asm_version = const.ASM_VERSION.get(version, "")

    def asm_header(self):
        from .entities import AsmHeader
        return AsmHeader(self.asm_version)

    def sat_end_marker(self) -> str:
        if self.asm_end_marker:
            return const.END_OF_ASM_DATA_SAT + " "
        else:
            return const.END_OF_ACIS_DATA_SAT + " "

    def sab_end_marker(self) -> bytes:
        if self.asm_end_marker:
            return const.END_OF_ASM_DATA_SAB
        else:
            return const.END_OF_ACIS_DATA_SAB
