# NOTE: Changes in the global settings might not immediately affect
# the functions in utils.py due to numba compilation and caching!
from typing import Dict, List, Tuple

import numpy as np

# SHORTCUT SETTINGS
# h3 library
SHORTCUT_H3_RES: int = 3
SHORTCUT_FILE = "shortcuts.bin"

OCEAN_TIMEZONE_PREFIX = r"Etc/GMT"

# DATA FILES
# BINARY
BINARY_FILE_ENDING = ".bin"

POLY_ZONE_IDS = "poly_zone_ids"
POLY_COORD_AMOUNT = "poly_coord_amount"
POLY_ADR2DATA = "poly_adr2data"
POLY_MAX_VALUES = "poly_bounds"
POLY_DATA = "poly_data"
POLY_NR2ZONE_ID = "poly_nr2zone_id"

HOLE_COORD_AMOUNT = "hole_coord_amount"
HOLE_ADR2DATA = "hole_adr2data"
HOLE_DATA = "hole_data"

BINARY_DATA_ATTRIBUTES = [
    POLY_ZONE_IDS,
    POLY_COORD_AMOUNT,
    POLY_ADR2DATA,
    POLY_MAX_VALUES,
    POLY_DATA,
    POLY_NR2ZONE_ID,
    HOLE_COORD_AMOUNT,
    HOLE_ADR2DATA,
    HOLE_DATA,
]

# JSON
JSON_FILE_ENDING = ".json"
HOLE_REGISTRY = "hole_registry"
TIMEZONE_NAMES_FILE = "timezone_names" + JSON_FILE_ENDING
HOLE_REGISTRY_FILE = HOLE_REGISTRY + JSON_FILE_ENDING

DATA_ATTRIBUTE_NAMES = BINARY_DATA_ATTRIBUTES + [HOLE_REGISTRY]

# all data files that should be included in the build:
ALL_BINARY_FILES = [
    specifier + BINARY_FILE_ENDING for specifier in BINARY_DATA_ATTRIBUTES
]
ALL_JSON_FILES = [TIMEZONE_NAMES_FILE, HOLE_REGISTRY_FILE]
PACKAGE_DATA_FILES = ALL_BINARY_FILES + ALL_JSON_FILES

# TODO create variables for used dtype for each type of data (polygon address, coordinate...)
# B = unsigned char (1byte = 8bit Integer)
NR_BYTES_B = 1
DTYPE_FORMAT_B = b"<B"
DTYPE_FORMAT_B_NUMPY = "<i1"
THRES_DTYPE_B = 2 ** (NR_BYTES_B * 8)

# H = unsigned short (2 byte integer)
NR_BYTES_H = 2
DTYPE_FORMAT_H = b"<H"
DTYPE_FORMAT_H_NUMPY = "<u2"
THRES_DTYPE_H = 2 ** (NR_BYTES_H * 8)  # = 65536

# value to write for representing an invalid zone (e.g. no shortcut polygon)
# = 65535 = highest possible value with H (2 byte unsigned integer)
INVALID_VALUE_DTYPE_H = THRES_DTYPE_H - 1

# i = signed 4byte integer
NR_BYTES_I = 4
DTYPE_FORMAT_SIGNED_I = b"<i"
DTYPE_FORMAT_SIGNED_I_NUMPY = "<i4"
THRES_DTYPE_SIGNED_I_UPPER = 2 ** ((NR_BYTES_I * 8) - 1)
THRES_DTYPE_SIGNED_I_LOWER = -THRES_DTYPE_SIGNED_I_UPPER

# I = unsigned 4byte integer
DTYPE_FORMAT_I = b"<I"
THRES_DTYPE_I = 2 ** (NR_BYTES_I * 8)

# Q = unsigned 8byte integer
NR_BYTES_Q = 8
DTYPE_FORMAT_Q = b"<Q"

# f = 8byte signed float
DTYPE_FORMAT_F_NUMPY = "<f8"

# IMPORTANT: all values between -180 and 180 degree must fit into the domain of i4!
# is the same as testing if 360 fits into the domain of I4 (unsigned!)
MAX_ALLOWED_COORD_VAL = 2 ** (8 * NR_BYTES_I - 1)

# from math import floor,log10
# DECIMAL_PLACES_SHIFT = floor(log10(MAX_ALLOWED_COORD_VAL/180.0)) # == 7
DECIMAL_PLACES_SHIFT = 7
INT2COORD_FACTOR = 10 ** (-DECIMAL_PLACES_SHIFT)
COORD2INT_FACTOR = 10**DECIMAL_PLACES_SHIFT
MAX_LNG_VAL = 180.0
MAX_LAT_VAL = 90.0
MAX_LNG_VAL_INT = int(MAX_LNG_VAL * COORD2INT_FACTOR)
MAX_LAT_VAL_INT = int(MAX_LAT_VAL * COORD2INT_FACTOR)
MAX_INT_VAL = MAX_LNG_VAL_INT
assert MAX_INT_VAL < MAX_ALLOWED_COORD_VAL

# TYPES

# hexagon id to list of polygon ids
ShortcutMapping = Dict[int, np.ndarray]
CoordPairs = List[Tuple[float, float]]
CoordLists = List[List[float]]
IntLists = List[List[int]]
