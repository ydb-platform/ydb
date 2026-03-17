# Copyright (c) 2020-2021, Manfred Moitzi
# License: MIT License
from typing import Union

ACAD_13 = "AC1012"
ACAD_14 = "AC1014"
ACAD_2000 = "AC1015"
ACAD_2004 = "AC1018"
ACAD_2007 = "AC1021"
ACAD_2010 = "AC1024"
ACAD_2013 = "AC1027"
ACAD_2018 = "AC1032"
ACAD_LATEST = ACAD_2018

SUPPORTED_VERSIONS = [ACAD_13, ACAD_14, ACAD_2000]
HEADER_ID = 0
CLASSES_ID = 1
OBJECTS_ID = 2
SENTINEL_SIZE = 16

Bytes = Union[bytes, bytearray, memoryview]


class DwgError(Exception):
    pass


class DwgVersionError(DwgError):
    pass


class DwgCorruptedFileHeader(DwgError):
    pass


class DwgCorruptedClassesSection(DwgError):
    pass


class DwgCorruptedHeaderSection(DwgError):
    pass


class CRCError(DwgError):
    pass
