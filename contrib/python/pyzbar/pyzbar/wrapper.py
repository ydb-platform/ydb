"""Low-level wrapper around zbar's interface
"""
from ctypes import (
    c_ubyte, c_char_p, c_int, c_uint, c_ulong, c_void_p, Structure,
    CFUNCTYPE, POINTER
)
from enum import IntEnum, unique

from . import zbar_library


__all__ = [
    'EXTERNAL_DEPENDENCIES', 'LIBZBAR', 'ZBarConfig', 'ZBarSymbol', 'ZBarOrientation',
    'zbar_image_create', 'zbar_image_destroy', 'zbar_image_first_symbol',
    'zbar_image_scanner_create', 'zbar_image_scanner_destroy',
    'zbar_image_scanner_set_config', 'zbar_image_set_data',
    'zbar_image_set_format', 'zbar_image_set_size', 'zbar_scan_image',
    'zbar_symbol_get_data_length', 'zbar_symbol_get_data',
    'zbar_symbol_get_loc_size', 'zbar_symbol_get_loc_x',
    'zbar_symbol_get_loc_y', 'zbar_symbol_next',
    'zbar_symbol_get_orientation', 'zbar_symbol_get_quality',
]

# Globals populated in load_libzbar
LIBZBAR = None
"""ctypes.CDLL
"""

EXTERNAL_DEPENDENCIES = []
"""List of instances of ctypes.CDLL. Helpful when freezing.
"""

# Types
c_ubyte_p = POINTER(c_ubyte)
c_uint_p = POINTER(c_uint)
c_ulong_p = POINTER(c_ulong)
"""unsigned char* type
"""


# Defines and enums
@unique
class ZBarSymbol(IntEnum):
    NONE = 0          # /**< no symbol decoded */
    PARTIAL = 1       # /**< intermediate status */
    EAN2 = 2          # /**< GS1 2-digit add-on */
    EAN5 = 5          # /**< GS1 5-digit add-on */
    EAN8 = 8          # /**< EAN-8 */
    UPCE = 9          # /**< UPC-E */
    ISBN10 = 10       # /**< ISBN-10 (from EAN-13). @since 0.4 */
    UPCA = 12         # /**< UPC-A */
    EAN13 = 13        # /**< EAN-13 */
    ISBN13 = 14       # /**< ISBN-13 (from EAN-13). @since 0.4 */
    COMPOSITE = 15    # /**< EAN/UPC composite */
    I25 = 25          # /**< Interleaved 2 of 5. @since 0.4 */
    DATABAR = 34      # /**< GS1 DataBar (RSS). @since 0.11 */
    DATABAR_EXP = 35  # /**< GS1 DataBar Expanded. @since 0.11 */
    CODABAR = 38      # /**< Codabar. @since 0.11 */
    CODE39 = 39       # /**< Code 39. @since 0.4 */
    PDF417 = 57       # /**< PDF417. @since 0.6 */
    QRCODE = 64       # /**< QR Code. @since 0.10 */
    SQCODE = 80       # /**< SQ Code. @since 0.20.1 */
    CODE93 = 93       # /**< Code 93. @since 0.11 */
    CODE128 = 128     # /**< Code 128 */


@unique
class ZBarConfig(IntEnum):
    CFG_ENABLE = 0          # /**< enable symbology/feature */
    CFG_ADD_CHECK = 1       # /**< enable check digit when optional */
    CFG_EMIT_CHECK = 2      # /**< return check digit when present */
    CFG_ASCII = 3           # /**< enable full ASCII character set */
    CFG_NUM = 4             # /**< number of boolean decoder configs */

    CFG_MIN_LEN = 0x20      # /**< minimum data length for valid decode */
    CFG_MAX_LEN = 0x21      # /**< maximum data length for valid decode */

    CFG_UNCERTAINTY = 0x40  # /**< required video consistency frames */

    CFG_POSITION = 0x80     # /**< enable scanner to collect position data */

    CFG_X_DENSITY = 0x100   # /**< image scanner vertical scan density */
    CFG_Y_DENSITY = 0x101   # /**< image scanner horizontal scan density */


@unique
class ZBarOrientation(IntEnum):
    UNKNOWN = -1   # /**< unable to determine orientation */
    UP = 0         # /**< upright, read left to right */
    RIGHT = 1      # /**< sideways, read top to bottom */
    DOWN = 2       # /**< upside-down, read right to left */
    LEFT = 3       # /**< sideways, read bottom to top */


# Structs
class zbar_image_scanner(Structure):
    """Opaque C++ class with private implementation
    """
    pass


class zbar_image(Structure):
    """Opaque C++ class with private implementation
    """
    pass


class zbar_symbol(Structure):
    """Opaque C++ class with private implementation

    The first item in the structure is an integeger value in the ZBarSymbol
    enumeration.
    """
    _fields_ = [
        ('type', c_int),
    ]


def load_libzbar():
    """Loads the zbar shared library and its dependencies.

    Populates the globals LIBZBAR and EXTERNAL_DEPENDENCIES.
    """
    global LIBZBAR
    global EXTERNAL_DEPENDENCIES
    if not LIBZBAR:
        libzbar, dependencies = zbar_library.load()
        LIBZBAR = libzbar
        EXTERNAL_DEPENDENCIES = [LIBZBAR] + dependencies

    return LIBZBAR


# Function signatures
def zbar_function(fname, restype, *args):
    """Returns a foreign function exported by `zbar`.

    Args:
        fname (:obj:`str`): Name of the exported function as string.
        restype (:obj:): Return type - one of the `ctypes` primitive C data
        types.
        *args: Arguments - a sequence of `ctypes` primitive C data types.

    Returns:
        cddl.CFunctionType: A wrapper around the function.
    """
    prototype = CFUNCTYPE(restype, *args)
    return prototype((fname, load_libzbar()))


zbar_version = zbar_function(
    'zbar_version',
    c_int,
    c_uint_p,    # major,
    c_uint_p,    # minor
)

zbar_set_verbosity = zbar_function(
    'zbar_set_verbosity',
    None,
    c_int
)

zbar_image_scanner_create = zbar_function(
    'zbar_image_scanner_create',
    POINTER(zbar_image_scanner)
)

zbar_image_scanner_destroy = zbar_function(
    'zbar_image_scanner_destroy',
    None,
    POINTER(zbar_image_scanner)
)

zbar_parse_config = zbar_function(
    'zbar_parse_config',
    c_int,
    c_char_p,          # config_string,
    POINTER(c_int),    # symbology - values in ZBarSymbol
    POINTER(c_int),    # config - values in ZBarConfig
    POINTER(c_int),    # value
)

zbar_image_scanner_set_config = zbar_function(
    'zbar_image_scanner_set_config',
    c_int,
    POINTER(zbar_image_scanner),  # scanner
    c_int,                        # symbology - values in ZBarSymbol
    c_int,                        # config - values in ZBarConfig
    c_int                         # value
)

zbar_image_create = zbar_function(
    'zbar_image_create',
    POINTER(zbar_image)
)

zbar_image_destroy = zbar_function(
    'zbar_image_destroy',
    None,
    POINTER(zbar_image)
)

zbar_image_set_format = zbar_function(
    'zbar_image_set_format',
    None,
    POINTER(zbar_image),
    c_uint
)

zbar_image_set_size = zbar_function(
    'zbar_image_set_size',
    None,
    POINTER(zbar_image),
    c_uint,     # width
    c_uint      # height
)

zbar_image_set_data = zbar_function(
    'zbar_image_set_data',
    None,
    POINTER(zbar_image),
    c_void_p,   # data
    c_ulong,    # raw_image_data_length
    c_void_p    # A function pointer(!)
)

zbar_scan_image = zbar_function(
    'zbar_scan_image',
    c_int,
    POINTER(zbar_image_scanner),
    POINTER(zbar_image)
)

zbar_image_first_symbol = zbar_function(
    'zbar_image_first_symbol',
    POINTER(zbar_symbol),
    POINTER(zbar_image)
)

zbar_symbol_get_data_length = zbar_function(
    'zbar_symbol_get_data_length',
    c_uint,
    POINTER(zbar_symbol)
)

zbar_symbol_get_data = zbar_function(
    'zbar_symbol_get_data',
    c_ubyte_p,
    POINTER(zbar_symbol)
)

zbar_symbol_get_loc_size = zbar_function(
    'zbar_symbol_get_loc_size',
    c_uint,
    POINTER(zbar_symbol)
)

zbar_symbol_get_loc_x = zbar_function(
    'zbar_symbol_get_loc_x',
    c_int,
    POINTER(zbar_symbol),
    c_uint
)

zbar_symbol_get_loc_y = zbar_function(
    'zbar_symbol_get_loc_y',
    c_int,
    POINTER(zbar_symbol),
    c_uint
)


try:
    zbar_symbol_get_orientation = zbar_function(
        'zbar_symbol_get_orientation',
        c_uint,
        POINTER(zbar_symbol)
    )
except AttributeError:
    # This function not present in the original pre-20
    zbar_symbol_get_orientation = None


zbar_symbol_next = zbar_function(
    'zbar_symbol_next',
    POINTER(zbar_symbol),
    POINTER(zbar_symbol)
)

zbar_symbol_get_quality = zbar_function(
    'zbar_symbol_get_quality',
    c_int,
    POINTER(zbar_symbol)
)
