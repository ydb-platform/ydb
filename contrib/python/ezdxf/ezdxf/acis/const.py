#  Copyright (c) 2022-2024, Manfred Moitzi
#  License: MIT License
import enum
from ezdxf.version import __version__

# SAT Export Requirements for Autodesk Products
# ---------------------------------------------
# Script to create test files:
# examples/acistools/create_3dsolid_cube.py

# DXF R2000, R2004, R2007, R2010: OK, tested with TrueView 2022
# ACIS version 700
# ACIS version string: "ACIS 32.0 NT"
# record count: 0, not required
# body count: 1, required
# ASM header: no
# end-marker: "End-of-ACIS-data"

# DXF R2004, R2007, R2010: OK, tested with TrueView 2022
# ACIS version 20800
# ACIS version string: "ACIS 208.00 NT"
# record count: 0, not required
# body count: n + 1 (asm-header), required
# ASM header: "208.0.4.7009"
# end-marker: "End-of-ACIS-data"

# SAB Export Requirements for Autodesk Products
# ---------------------------------------------
# DXF R2013, R2018: OK, tested with TrueView 2022
# ACIS version 21800
# ACIS version string: "ACIS 208.00 NT"
# record count: 0, not required
# body count: n + 1  (asm-header), required
# ASM header: "208.0.4.7009"
# end-marker: "End-of-ASM-data"

ACIS_VERSION = {
    400: "ACIS 4.00 NT",  # DXF R2000, no asm header - only R2000
    700: "ACIS 32.0 NT",  # DXF R2000-R2010, no asm header
    20800: "ACIS 208.00 NT",  # DXF R2013 with asm-header, asm-end-marker
    21800: "ACIS 218.00 NT",  # DXF R2013 with asm-header, asm-end-marker
    22300: "ACIS 223.00 NT",  # DXF R2018 with asm-header, asm-end-marker
}
ASM_VERSION = {
    20800: "208.0.4.7009",  # DXF R2004, R2007, R2010
    21800: "208.0.4.7009",  # DXF R2013, default version for R2013 and R2018
    22300: "222.0.0.1700",  # DXF R2018
}
EZDXF_BUILDER_ID = f"ezdxf v{__version__} ACIS Builder"
MIN_EXPORT_VERSION = 700

# ACIS version 700 is the default version for DXF R2000, R2004, R2007 and R2010 (SAT)
# ACIS version 21800 is the default version for DXF R2013 and R2018 (SAB)
DEFAULT_SAT_VERSION = 700
DEFAULT_SAB_VERSION = 21800

DATE_FMT = "%a %b %d %H:%M:%S %Y"
END_OF_ACIS_DATA_SAT = "End-of-ACIS-data"
END_OF_ACIS_DATA_SAB = b"\x0e\x03End\x0e\x02of\x0e\x04ACIS\x0d\x04data"
END_OF_ASM_DATA_SAT = "End-of-ASM-data"
END_OF_ASM_DATA_SAB = b"\x0e\x03End\x0e\x02of\x0e\x03ASM\x0d\x04data"
BEGIN_OF_ACIS_HISTORY_DATA = "Begin-of-ACIS-History-data"
END_OF_ACIS_HISTORY_DATA = "End-of-ACIS-History-data"
DATA_END_MARKERS = (
    END_OF_ACIS_DATA_SAT,
    BEGIN_OF_ACIS_HISTORY_DATA,
    END_OF_ASM_DATA_SAT,
)
NULL_PTR_NAME = "null-ptr"
NONE_ENTITY_NAME = "none-entity"
NOR_TOL = 1e-10
RES_TOL = 9.9999999999999995e-7

BOOL_SPECIFIER = {
    "forward": True,
    "forward_v": True,
    "reversed": False,
    "reversed_v": False,
    "single": True,
    "double": False,
}

ACIS_SIGNATURE = b"ACIS BinaryFile"  # DXF R2013/R2018
ASM_SIGNATURE = b"ASM BinaryFile4"  # DXF R2018
SIGNATURES = [ACIS_SIGNATURE, ASM_SIGNATURE]


def is_valid_export_version(version: int):
    return version >= MIN_EXPORT_VERSION and version in ACIS_VERSION


class Tags(enum.IntEnum):
    NO_TYPE = 0x00
    BYTE = 0x01  # not used in files!
    CHAR = 0x02  # not used in files!
    SHORT = 0x03  # not used in files!
    INT = 0x04  # 32-bit signed integer
    FLOAT = 0x05  # not used in files!
    DOUBLE = 0x06  # 64-bit double precision floating point value
    STR = 0x07  # count is the following 8-bit uchar
    STR2 = 0x08  # not used in files!
    STR3 = 0x09  # not used in files!

    # bool value for reversed, double, I - depends on context
    BOOL_TRUE = 0x0A

    # bool value forward, single, forward_v - depends on context
    BOOL_FALSE = 0x0B
    POINTER = 0x0C
    ENTITY_TYPE = 0x0D
    ENTITY_TYPE_EX = 0x0E
    SUBTYPE_START = 0x0F
    SUBTYPE_END = 0x10
    RECORD_END = 0x11
    LITERAL_STR = 0x12  # count ia a 32-bit uint, see transform entity
    LOCATION_VEC = 0x13  # vector (3 doubles)
    DIRECTION_VEC = 0x14  # vector (3 doubles)

    # Enumeration are stored as strings in SAT and ints in SAB.
    # It's not possible to translate SAT enums (strings) to SAB enums (int) and
    # vice versa without knowing the implementation details. Each enumeration
    # is specific to the class where it is used.
    ENUM = 0x15
    # 0x16: ???
    UNKNOWN_0x17 = 0x17  # double


# entity type structure:
# 0x0D 0x04 (char count of) "body" = SAT "body"
# 0x0E 0x05 "plane" 0x0D 0x07 "surface" = SAT "plane-surface"
# 0x0E 0x06 "ref_vt" 0x0E 0x03 "eye" 0x0D 0x06 "attrib" = SAT "ref_vt-eye-attrib"


class Flags(enum.IntFlag):
    HAS_HISTORY = 1


class AcisException(Exception):
    pass


class InvalidLinkStructure(AcisException):
    pass


class ParsingError(AcisException):
    pass


class ExportError(AcisException):
    pass


class EndOfAcisData(AcisException):
    pass


class Features:
    LAW_SPL = 400
    CONE_SCALING = 400
    LOFT_LAW = 400
    REF_MIN_UV_GRID = 400
    VBLEND_AUTO = 400
    BL_ENV_SF = 400
    ELLIPSE_OFFSET = 500
    TOL_MODELING = 500
    APPROX_SUMMARY = 500
    TAPER_SCALING = 500
    LAZY_B_SPLINE = 500
    DM_MULTI_SURF = 500
    GA_COPY_ACTION = 600
    DM_MULTI_SURF_COLOR = 600
    RECAL_SKIN_ERROR = 520
    TAPER_U_RULED = 600
    DM_60 = 600
    LOFT_PCURVE = 600
    EELIST_OWNER = 600
    ANNO_HOOKED = 700
    PATTERN = 700
    ENTITY_TAGS = 700
    AT = 700
    NET_LAW = 700
    STRINGLESS_HISTORY = 700
