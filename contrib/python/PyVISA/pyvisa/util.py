# -*- coding: utf-8 -*-
"""General utility functions.

This file is part of PyVISA.

:copyright: 2014-2024 by PyVISA Authors, see AUTHORS for more details.
:license: MIT, see LICENSE for more details.

"""

import functools
import inspect
import io
import math
import os
import platform
import struct
import subprocess
import sys
import warnings
from collections import OrderedDict
from enum import Enum
from pathlib import Path
from types import ModuleType
from typing import (
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
    overload,
)

from typing_extensions import Literal

from . import constants, logger

np: Optional[ModuleType]
try:
    import numpy

    np = numpy
except ImportError:
    np = None


#: Length of the header found before a binary block (ieee or hp) that will
#: trigger a warning to alert the user of a possibly incorrect answer from the
#: instrument. In general binary block are not prefixed by a header and finding
#: a long one may mean that we picked up a # in the bulk of the message.
DEFAULT_LENGTH_BEFORE_BLOCK = 25


def _use_numpy_routines(container: Union[type, Callable]) -> bool:
    """Should optimized numpy routines be used to extract data."""
    if np is None or container in (tuple, list):
        return False

    if container is np.array or (
        inspect.isclass(container) and issubclass(container, np.ndarray)  # type: ignore
    ):
        return True

    return False


def read_user_library_path() -> Optional[str]:
    """Return the library path stored in one of the following configuration files:

        <sys prefix>/share/pyvisa/.pyvisarc
        ~/.pyvisarc

    <sys prefix> is the site-specific directory prefix where the platform
    independent Python files are installed.

    Example configuration file:

        [Paths]
        visa library=/my/path/visa.so
        dll_extra_paths=/my/otherpath/;/my/otherpath2

    Return `None` if  configuration files or keys are not present.

    """
    from configparser import ConfigParser, NoOptionError, NoSectionError

    config_parser = ConfigParser()
    files = config_parser.read(
        [
            os.path.join(sys.prefix, "share", "pyvisa", ".pyvisarc"),
            os.path.join(os.path.expanduser("~"), ".pyvisarc"),
        ]
    )

    if not files:
        logger.debug("No user defined library files")
        return None

    logger.debug("User defined library files: %s" % files)
    try:
        return config_parser.get("Paths", "visa library")
    except (NoOptionError, NoSectionError):
        logger.debug("NoOptionError or NoSectionError while reading config file")
        return None


_ADDED_DLL_PATHS: Set[str] = set()


def add_user_dll_extra_paths() -> Optional[List[str]]:
    """Add paths to search for .dll dependencies on Windows.

    The configuration files are expected to be stored in one of the following location:

        <sys prefix>/share/pyvisa/.pyvisarc
        ~/.pyvisarc

    <sys prefix> is the site-specific directory prefix where the platform
    independent Python files are installed.

    Example configuration file:

        [Paths]
        visa library=/my/path/visa.so
        dll_extra_paths=/my/otherpath/;/my/otherpath2

    """
    from configparser import ConfigParser, NoOptionError, NoSectionError

    if sys.platform == "win32":
        config_parser = ConfigParser()
        files = config_parser.read(
            [
                os.path.join(sys.prefix, "share", "pyvisa", ".pyvisarc"),
                os.path.join(os.path.expanduser("~"), ".pyvisarc"),
            ]
        )

        if not files:
            logger.debug("No user defined configuration")
            return None

        logger.debug("User defined configuration files: %s" % files)

        try:
            dll_extra_paths = config_parser.get("Paths", "dll_extra_paths").split(";")
            for path in dll_extra_paths:
                if path not in _ADDED_DLL_PATHS:
                    os.add_dll_directory(path)
                    _ADDED_DLL_PATHS.add(path)
                else:
                    logger.debug("Path %r has already been added; skipping" % path)
            return dll_extra_paths
        except (NoOptionError, NoSectionError):
            logger.debug(
                "NoOptionError or NoSectionError while reading config file for"
                " dll_extra_paths."
            )
            return None
    else:
        logger.debug("Not loading dll_extra_paths because we are not on Windows")
        return None


class PEMachineType(Enum):
    UNKNOWN = 0
    I386 = 0x014C
    R3000 = 0x0162
    R4000 = 0x0166
    R10000 = 0x0168
    WCEMIPSV2 = 0x0169
    ALPHA = 0x0184
    SH3 = 0x01A2
    SH3DSP = 0x01A3
    SH3E = 0x01A4
    SH4 = 0x01A6
    SH5 = 0x01A8
    ARM = 0x01C0
    AARCH64 = 0xAA64
    THUMB = 0x01C2
    ARMNT = 0x01C4
    AM33 = 0x01D3
    POWERPC = 0x01F0
    POWERPCFP = 0x01F1
    IA64 = 0x0200
    MIPS16 = 0x0266
    ALPHA64 = 0x0284
    MIPSFPU = 0x0366
    MIPSFPU16 = 0x0466
    TRICORE = 0x0520
    CEF = 0x0CEF
    EBC = 0x0EBC
    AMD64 = 0x8664
    M32R = 0x9041
    CEE = 0xC0EE


class ArchitectureType(Enum):
    I386 = ("x86", 32)
    X86_64 = ("x86", 64)
    AARCH64 = ("arm", 64)

    @classmethod
    def from_platform_machine(cls, machine: str) -> Optional["ArchitectureType"]:
        if machine == "i386" or machine == "i686":
            return cls.I386
        elif machine == "x86_64" or machine == "amd64":
            return cls.X86_64
        elif machine == "arm64" or machine == "aarch64":
            return cls.AARCH64

        return None


class LibraryPath(str):
    """Object encapsulating information about a VISA dynamic library."""

    #: Path with which the object was created
    path: str

    #: Detection method employed to locate the library
    found_by: str

    #: Architectural information
    _arch: Optional[List[ArchitectureType]] = None

    def __new__(
        cls: Type["LibraryPath"], path: str, found_by: str = "auto"
    ) -> "LibraryPath":
        obj = super(LibraryPath, cls).__new__(cls, path)  # type: ignore
        obj.path = path
        obj.found_by = found_by

        return obj

    @property
    def arch(self) -> List[ArchitectureType]:
        """Architecture of the library."""
        if self._arch is None:
            try:
                self._arch = get_arch(Path(self.path))
            except Exception:
                self._arch = []

        return self._arch


def cleanup_timeout(timeout: Optional[Union[int, float]]) -> int:
    """Turn a timeout expressed as a float into in interger or the proper constant."""
    if timeout is None or math.isinf(timeout):
        timeout = constants.VI_TMO_INFINITE

    elif timeout < 1:
        timeout = constants.VI_TMO_IMMEDIATE

    elif not (1 <= timeout <= 4294967294):
        raise ValueError("timeout value is invalid")

    else:
        timeout = int(timeout)

    return timeout


_converters: Dict[str, Callable[[str], Any]] = {
    "s": str,
    "b": functools.partial(int, base=2),
    "c": ord,
    "d": int,
    "o": functools.partial(int, base=8),
    "x": functools.partial(int, base=16),
    "X": functools.partial(int, base=16),
    "h": functools.partial(int, base=16),
    "H": functools.partial(int, base=16),
    "e": float,
    "E": float,
    "f": float,
    "F": float,
    "g": float,
    "G": float,
}

_np_converters = {
    "d": "i",
    "e": "d",
    "E": "d",
    "f": "d",
    "F": "d",
    "g": "d",
    "G": "d",
}


ASCII_CONVERTER = Union[
    Literal["s", "b", "c", "d", "o", "x", "X", "e", "E", "f", "F", "g", "G"],
    Callable[[str], Any],
]


def from_ascii_block(
    ascii_data: str,
    converter: ASCII_CONVERTER = "f",
    separator: Union[str, Callable[[str], Iterable[str]]] = ",",
    container: Callable[
        [Iterable[Union[int, float]]], Sequence[Union[int, float]]
    ] = list,
) -> Sequence:
    """Parse ascii data and return an iterable of numbers.

    Parameters
    ----------
    ascii_data : str
        Data to be parsed.
    converter : ASCII_CONVERTER, optional
        Str format of function to convert each value. Default to "f".
    separator : Union[str, Callable[[str], Iterable[str]]]
        str or callable used to split the data into individual elements.
        If a str is given, data.split(separator) is used. Default to ",".
    container : Union[Type, Callable[[Iterable], Sequence]], optional
        Container type to use for the output data. Possible values are: list,
        tuple, np.ndarray, etc, Default to list.

    Returns
    -------
    Sequence
        Parsed data.

    """
    if (
        _use_numpy_routines(container)
        and isinstance(converter, str)
        and isinstance(separator, str)
        and converter in _np_converters
    ):
        assert np  # for typing
        return np.fromstring(ascii_data, _np_converters[converter], sep=separator)

    if isinstance(converter, str):
        try:
            converter = _converters[converter]
        except KeyError:
            raise ValueError(
                "Invalid code for converter: %s not in %s"
                % (converter, str(tuple(_converters.keys())))
            )

    data: Iterable[str]
    if isinstance(separator, str):
        data = ascii_data.split(separator)
        if not data[-1]:
            data = data[:-1]
    else:
        data = separator(ascii_data)

    return container([converter(raw_value) for raw_value in data])


def to_ascii_block(
    iterable: Iterable[Any],
    converter: Union[str, Callable[[Any], str]] = "f",
    separator: Union[str, Callable[[Iterable[str]], str]] = ",",
) -> str:
    """Turn an iterable of numbers in an ascii block of data.

    Parameters
    ----------
    iterable : Iterable[Any]
        Data to be formatted.
    converter : Union[str, Callable[[Any], str]]
        String formatting code or function used to convert each value.
        Default to "f".
    separator : Union[str, Callable[[Iterable[str]], str]]
        str or callable that join individual elements into a str.
        If a str is given, separator.join(data) is used.

    """
    if isinstance(separator, str):
        separator = separator.join

    if isinstance(converter, str):
        converter = "%" + converter
        block = separator(converter % val for val in iterable)
    else:
        block = separator(converter(val) for val in iterable)
    return block


#: Valid binary header when reading/writing binary block of data from an instrument
BINARY_HEADERS = Literal["ieee", "hp", "rs", "empty"]

#: Valid datatype for binary block. See Python standard library struct module for more
#: details.
BINARY_DATATYPES = Literal[
    "s", "b", "B", "h", "H", "i", "I", "l", "L", "q", "Q", "f", "d"
]

#: Valid output containers for storing the parsed binary data
BINARY_CONTAINERS = Union[type, Callable]


def parse_ieee_block_header(
    block: Union[bytes, bytearray],
    length_before_block: Optional[int] = None,
    raise_on_late_block: bool = False,
) -> Tuple[int, int]:
    """Parse the header of a IEEE block.

    Definite Length Arbitrary Block:
    #<header_length><data_length><data>

    The header_length specifies the size of the data_length field.
    And the data_length field specifies the size of the data.

    Indefinite Length Arbitrary Block:
    #0<data>

    In this case the data length returned will be 0. The actual length can be
    deduced from the block and the offset.

    Parameters
    ----------
    block : Union[bytes, bytearray]
        IEEE formatted block of data.
    length_before_block : Optional[int], optional
        Number of bytes before the actual start of the block. Default to None,
        which means that number will be inferred.
    raise_on_late_block : bool, optional
        Raise an error in the beginning of the block is not found before
        DEFAULT_LENGTH_BEFORE_BLOCK, if False use a warning. Default to False.

    Returns
    -------
    int
        Offset at which the actual data starts
    int
        Length of the data in bytes.

    """
    begin = block.find(b"#")
    if begin < 0:
        raise ValueError(
            'Could not find hash sign ("#") indicating the start of the block. '
            "The block begins with %r" % block[:25]
        )

    length_before_block = (
        DEFAULT_LENGTH_BEFORE_BLOCK
        if length_before_block is None
        else length_before_block
    )
    if begin > length_before_block:
        msg = (
            "The beginning of the block has been found at %d which "
            "is an unexpectedly large value. The actual block may "
            "have been missing a beginning marker but the block "
            "contained one:\n%s"
        ) % (begin, repr(block[: begin + 25]))
        if raise_on_late_block:
            raise RuntimeError(msg)
        else:
            warnings.warn(msg, UserWarning)

    try:
        # int(block[begin+1]) != int(block[begin+1:begin+2]) in Python 3
        header_length = int(block[begin + 1 : begin + 2], base=16)
    except ValueError:
        header_length = 0

    offset = begin + 2 + header_length

    if header_length > 0:
        # #3100DATA
        # 012345

        if header_length == 10 and len(block[begin:]) < (2**16) + 4:
            # Detect an HP formatted block, which starts with "A"
            msg = (
                "Header length in IEEE format was indicated as 0xA (10d) but the "
                "block length was less than 64 KiB. It appears the block may be "
                "using the HP format instead of IEEE. If so, you will need to use "
                'the `header_fmt = "hp"` argument.'
            )
            raise ValueError(msg)

        data_length = int(block[begin + 2 : offset])

    else:
        # #0DATA
        # 012
        data_length = -1

    return offset, data_length


def parse_rs_block_header(
    block: Union[bytes, bytearray],
    length_before_block: Optional[int] = None,
    raise_on_late_block: bool = False,
) -> Tuple[int, int]:
    """Parse the header of a Rohde & Schwarz extended-length block. This is
    used for data transfers 1 GB or larger on R&S instruments.

    Definite Length Arbitrary Block:
    #(<data_length>)<data>

    The header_length specifies the size of the data_length field.
    And the data_length field specifies the size of the data.

    Parameters
    ----------
    block : Union[bytes, bytearray]
        R&S formatted block of data
    length_before_block : Optional[int], optional
        Maximum number of bytes before the actual start of the block before a warning
        is issued (or an exception is raised, if raise_on_late_block is True). Default
        to None, which means the DEFAULT_LENGTH_BEFORE_BLOCK constant will be used..
    raise_on_late_block : bool, optional
        Raise an error in the beginning of the block is not found before
        length_before_block, if False use a warning. Default to False.

    Returns
    -------
    int
        Offset at which the actual data starts
    int
        Length of the data in bytes.

    """
    begin = block.find(b"#(")
    if begin < 0:
        raise ValueError(
            'Could not find the standard block header ("#(") indicating the start '
            "of the block. The block begins with %r" % block[:25]
        )

    length_before_block = (
        DEFAULT_LENGTH_BEFORE_BLOCK
        if length_before_block is None
        else length_before_block
    )
    if begin > length_before_block:
        msg = (
            "The beginning of the block has been found at %d which "
            "is an unexpectedly large value. The actual block may "
            "have been missing a beginning marker but the block "
            "contained one:\n%s"
        ) % (begin, repr(block[: begin + 25]))
        if raise_on_late_block:
            raise RuntimeError(msg)
        else:
            warnings.warn(msg, UserWarning)

    # Rohde & Schwarz uses the format #(length_digits)DATA when the number of
    # bytes exceeds what can be represented with 9 decimal digits (≥1 GB).
    # The length character is no longer used, and instead parentheses are used
    # to delimit the length, allowing an arbitrary number of length digits.
    header_length = (block.find(b")", begin)) - (begin + 2)
    if header_length < 0:
        msg = (
            "Block length is indicated using parentheses syntax, but no "
            "closing parenthesis was found."
        )
        raise RuntimeError(msg)
    elif header_length > 18:
        # ≥1 exabyte of data seems quite unlikely
        msg = (
            "Unexpectedly large block length indicated using parentheses "
            f"syntax. Indicated length was {header_length} digits long."
        )
        raise RuntimeError(msg)

    """
    #(12345678901234567890123)DATA
    ^ ^                     ^ ^
    | |<-- header_length -->| |
    | |                       `--- offset
    | `--- begin + 2
    `--- begin
    """
    offset = begin + 2 + header_length + 1
    data_length = int(block[begin + 2 : offset - 1])

    return offset, data_length


def parse_ieee_or_rs_block_header(
    block: Union[bytes, bytearray],
    length_before_block: Optional[int] = None,
    raise_on_late_block: bool = False,
) -> Tuple[int, int]:
    """Parse the header of a IEEE block.

    Definite Length Arbitrary Block:
    #<header_length><data_length><data>

    The header_length specifies the size of the data_length field.
    And the data_length field specifies the size of the data.

    Indefinite Length Arbitrary Block:
    #0<data>

    In this case the data length returned will be 0. The actual length can be
    deduced from the block and the offset.

    Parameters
    ----------
    block : Union[bytes, bytearray]
        IEEE formatted block of data.
    length_before_block : Optional[int], optional
        Maximum number of bytes before the actual start of the block before a warning
        is issued (or an exception is raised, if raise_on_late_block is True). Default
        to None, which means the DEFAULT_LENGTH_BEFORE_BLOCK constant will be used..
    raise_on_late_block : bool, optional
        Raise an error in the beginning of the block is not found before
        length_before_block, if False use a warning. Default to False.

    Returns
    -------
    int
        Offset at which the actual data starts
    int
        Length of the data in bytes.

    """
    begin = block.find(b"#")
    if begin < 0:
        raise ValueError(
            'Could not find hash sign ("#") indicating the start of the block. '
            "The first 25 characters of the block: %r" % block[:25]
        )

    if block[begin + 1] == ord("("):
        return parse_rs_block_header(block, length_before_block, raise_on_late_block)
    else:
        return parse_ieee_block_header(block, length_before_block, raise_on_late_block)


def parse_hp_block_header(
    block: Union[bytes, bytearray],
    is_big_endian: bool,
    length_before_block: Optional[int] = None,
    raise_on_late_block: bool = False,
) -> Tuple[int, int]:
    """Parse the header of a HP block.

    Definite Length Arbitrary Block:
    #A<data_length><data>

    The header ia always 4 bytes long.
    The data_length field specifies the size of the data.

    Parameters
    ----------
    block : Union[bytes, bytearray]
        HP formatted block of data.
    is_big_endian : bool
        Is the header in big or little endian order.
    length_before_block : Optional[int], optional
        Number of bytes before the actual start of the block. Default to None,
        which means that number will be inferred.
    raise_on_late_block : bool, optional
        Raise an error in the beginning of the block is not found before
        DEFAULT_LENGTH_BEFORE_BLOCK, if False use a warning. Default to False.

    Returns
    -------
    int
        Offset at which the actual data starts
    int
        Length of the data in bytes.

    """
    begin = block.find(b"#A")
    if begin < 0:
        raise ValueError(
            'Could not find the standard block header ("#A") indicating the start '
            "of the block. The block begins with %r" % block[:25]
        )

    length_before_block = (
        DEFAULT_LENGTH_BEFORE_BLOCK
        if length_before_block is None
        else length_before_block
    )
    if begin > length_before_block:
        msg = (
            "The beginning of the block has been found at %d which "
            "is an unexpectedly large value. The actual block may "
            "have been missing a beginning marker but the block "
            "contained one:\n%s"
        ) % (begin, repr(block))
        if raise_on_late_block:
            raise RuntimeError(msg)
        else:
            warnings.warn(msg, UserWarning)

    offset = begin + 4

    data_length = int.from_bytes(
        block[begin + 2 : offset], byteorder="big" if is_big_endian else "little"
    )

    return offset, data_length


def from_ieee_block(
    block: Union[bytes, bytearray],
    datatype: BINARY_DATATYPES = "f",
    is_big_endian: bool = False,
    container: Callable[
        [Iterable[Union[int, float]]], Sequence[Union[int, float]]
    ] = list,
) -> Sequence[Union[int, float]]:
    """Convert a block in the IEEE format into an iterable of numbers.

    Definite Length Arbitrary Block:
    #<header_length><data_length><data>

    The header_length specifies the size of the data_length field.
    And the data_length field specifies the size of the data.

    Indefinite Length Arbitrary Block:
    #0<data>

    Parameters
    ----------
    block : Union[bytes, bytearray]
        IEEE formatted block of data.
    datatype : BINARY_DATATYPES, optional
        Format string for a single element. See struct module. 'f' by default.
    is_big_endian : bool, optional
        Are the data in big or little endian order.
    container : Union[Type, Callable[[Iterable], Sequence]], optional
        Container type to use for the output data. Possible values are: list,
        tuple, np.ndarray, etc, Default to list.

    Returns
    -------
    Sequence[Union[int, float]]
        Parsed data.

    """
    offset, data_length = parse_ieee_block_header(block)

    # If the data length is not reported takes all the data and do not make
    # any assumption about the termination character
    if data_length == -1:
        data_length = len(block) - offset

    if len(block) < offset + data_length:
        raise ValueError(
            "Binary data is incomplete. The header states %d data"
            " bytes, but %d were received." % (data_length, len(block) - offset)
        )

    return from_binary_block(
        block, offset, data_length, datatype, is_big_endian, container
    )


def from_rs_block(
    block: Union[bytes, bytearray],
    datatype: BINARY_DATATYPES = "f",
    is_big_endian: bool = False,
    container: Callable[
        [Iterable[Union[int, float]]], Sequence[Union[int, float]]
    ] = list,
) -> Sequence[Union[int, float]]:
    """Convert a block in the Rohde & Schwarz format into an iterable of numbers.

    Definite Length Arbitrary Block:
    #(<data_length>)<data>

    The header_length specifies the size of the data_length field.
    The data_length field specifies the size of the data.

    Parameters
    ----------
    block : Union[bytes, bytearray]
        R&S formatted block of data.
    datatype : BINARY_DATATYPES, optional
        Format string for a single element. See struct module. 'f' by default.
    is_big_endian : bool, optional
        Are the data in big or little endian order.
    container : Union[Type, Callable[[Iterable], Sequence]], optional
        Container type to use for the output data. Possible values are: list,
        tuple, np.ndarray, etc, Default to list.

    Returns
    -------
    Sequence[Union[int, float]]
        Parsed data.

    """
    offset, data_length = parse_rs_block_header(block)

    if len(block) < offset + data_length:
        raise ValueError(
            "Binary data is incomplete. The header states %d data"
            " bytes, but %d were received." % (data_length, len(block) - offset)
        )

    return from_binary_block(
        block, offset, data_length, datatype, is_big_endian, container
    )


def from_ieee_or_rs_block(
    block: Union[bytes, bytearray],
    datatype: BINARY_DATATYPES = "f",
    is_big_endian: bool = False,
    container: Callable[
        [Iterable[Union[int, float]]], Sequence[Union[int, float]]
    ] = list,
) -> Sequence[Union[int, float]]:
    """Convert a block in either the IEEE or Rohde & Schwarz format into an iterable of numbers.

    Definite Length Arbitrary Block (IEEE):
    #<header_length><data_length><data>

    Definite Length Arbitrary Block (R&S):
    #(<data_length>)<data>

    The header_length specifies the size of the data_length field.
    The data_length field specifies the size of the data.

    Parameters
    ----------
    block : Union[bytes, bytearray]
        R&S formatted block of data.
    datatype : BINARY_DATATYPES, optional
        Format string for a single element. See struct module. 'f' by default.
    is_big_endian : bool, optional
        Are the data in big or little endian order.
    container : Union[Type, Callable[[Iterable], Sequence]], optional
        Container type to use for the output data. Possible values are: list,
        tuple, np.ndarray, etc, Default to list.

    Returns
    -------
    Sequence[Union[int, float]]
        Parsed data.

    """
    offset, data_length = parse_ieee_or_rs_block_header(block)

    # If the data length is not reported takes all the data and do not make
    # any assumption about the termination character
    if data_length == -1:
        data_length = len(block) - offset

    if len(block) < offset + data_length:
        raise ValueError(
            "Binary data is incomplete. The header states %d data"
            " bytes, but %d were received." % (data_length, len(block) - offset)
        )

    return from_binary_block(
        block, offset, data_length, datatype, is_big_endian, container
    )


def from_hp_block(
    block: Union[bytes, bytearray],
    datatype: BINARY_DATATYPES = "f",
    is_big_endian: bool = False,
    container: Callable[
        [Iterable[Union[int, float]]], Sequence[Union[int, float]]
    ] = list,
) -> Sequence[Union[int, float]]:
    """Convert a block in the HP format into an iterable of numbers.

    Definite Length Arbitrary Block:
    #A<data_length><data>

    The header ia always 4 bytes long.
    The data_length field specifies the size of the data.

    Parameters
    ----------
    block : Union[bytes, bytearray]
        HP formatted block of data.
    datatype : BINARY_DATATYPES, optional
        Format string for a single element. See struct module. 'f' by default.
    is_big_endian : bool, optional
        Are the data in big or little endian order.
    container : Union[Type, Callable[[Iterable], Sequence]], optional
        Container type to use for the output data. Possible values are: list,
        tuple, np.ndarray, etc, Default to list.

    Returns
    -------
    Sequence[Union[int, float]]
        Parsed data.

    """
    offset, data_length = parse_hp_block_header(block, is_big_endian)

    if len(block) < offset + data_length:
        raise ValueError(
            "Binary data is incomplete. The header states %d data"
            " bytes, but %d were received." % (data_length, len(block) - offset)
        )

    return from_binary_block(
        block, offset, data_length, datatype, is_big_endian, container
    )


def from_binary_block(
    block: Union[bytes, bytearray],
    offset: int = 0,
    data_length: Optional[int] = None,
    datatype: BINARY_DATATYPES = "f",
    is_big_endian: bool = False,
    container: Callable[
        [Iterable[Union[int, float]]], Sequence[Union[int, float]]
    ] = list,
) -> Sequence[Union[int, float]]:
    """Convert a binary block into an iterable of numbers.


    Parameters
    ----------
    block : Union[bytes, bytearray]
        HP formatted block of data.
    offset : int
        Offset at which the actual data starts
    data_length : int
        Length of the data in bytes.
    datatype : BINARY_DATATYPES, optional
        Format string for a single element. See struct module. 'f' by default.
    is_big_endian : bool, optional
        Are the data in big or little endian order.
    container : Union[Type, Callable[[Iterable], Sequence]], optional
        Container type to use for the output data. Possible values are: list,
        tuple, np.ndarray, etc, Default to list.

    Returns
    -------
    Sequence[Union[int, float]]
        Parsed data.

    """
    if data_length is None:
        data_length = len(block) - offset

    element_length = struct.calcsize(datatype)
    array_length = int(data_length / element_length)

    endianess = ">" if is_big_endian else "<"

    if _use_numpy_routines(container):
        assert np  # for typing
        return np.frombuffer(block, endianess + datatype, array_length, offset)

    fullfmt = "%s%d%s" % (endianess, array_length, datatype)

    try:
        raw_data = struct.unpack_from(fullfmt, block, offset)
    except struct.error:
        raise ValueError("Binary data was malformed")

    if datatype in "sp":
        raw_data = raw_data[0]

    return container(raw_data)


def to_binary_block(
    iterable: Union[bytes, bytearray, Sequence[Union[int, float]]],
    header: Union[str, bytes],
    datatype: BINARY_DATATYPES,
    is_big_endian: bool,
) -> bytes:
    """Convert an iterable of numbers into a block of data with a given header.

    Parameters
    ----------
    iterable : Sequence[Union[int, float]]
        Sequence of numbers to pack into a block.
    header : Union[str, bytes]
        Header which should prefix the binary block
    datatype : BINARY_DATATYPES
        Format string for a single element. See struct module.
    is_big_endian : bool
        Are the data in big or little endian order.

    Returns
    -------
    bytes
        Binary block of data preceded by the specified header

    """
    if isinstance(header, str):
        header = header.encode("ascii")

    if isinstance(iterable, (bytes, bytearray)):
        if datatype not in "sbB":
            warnings.warn(
                "Using the formats 's', 'p', 'b' or 'B' is more efficient when "
                "directly writing bytes",
                UserWarning,
            )
        else:
            return header + iterable

    endianess = ">" if is_big_endian else "<"

    if _use_numpy_routines(type(iterable)):
        assert np and isinstance(iterable, np.ndarray)  # For typing
        return header + iterable.astype(endianess + datatype).tobytes()

    array_length = len(iterable)
    fullfmt = "%s%d%s" % (endianess, array_length, datatype)

    if datatype in ("s", "p"):
        block = struct.pack(fullfmt, iterable)

    else:
        block = struct.pack(fullfmt, *iterable)

    return header + block


def to_ieee_block(
    iterable: Sequence[Union[int, float]],
    datatype: BINARY_DATATYPES = "f",
    is_big_endian: bool = False,
) -> bytes:
    """Convert an iterable of numbers into a block of data in the IEEE format.

    Parameters
    ----------
    iterable : Sequence[Union[int, float]]
        Sequence of numbers to pack into a block.
    datatype : BINARY_DATATYPES, optional
        Format string for a single element. See struct module. Default to 'f'.
    is_big_endian : bool, optional
        Are the data in big or little endian order. Default to False.

    Returns
    -------
    bytes
        Binary block of data preceded by the specified header

    """
    array_length = len(iterable)
    element_length = struct.calcsize(datatype)
    data_length = array_length * element_length

    number_of_digits_in_data_length = f"{len(str(data_length)):X}"

    if len(number_of_digits_in_data_length) > 1:
        msg = (
            "Block length in bytes cannot be greater than or equal to 1 PB "
            "(it must be representable with 15 decimal digits), but the "
            f"block length was {data_length} bytes, which requires "
            f"{len(str(data_length))} digits to represent."
        )
        raise OverflowError(msg)

    header = f"#{number_of_digits_in_data_length}{data_length:d}"

    # header = "%d" % data_length
    # header = "#%d%s" % (len(header), header)

    return to_binary_block(iterable, header, datatype, is_big_endian)


def to_rs_block(
    iterable: Sequence[Union[int, float]],
    datatype: BINARY_DATATYPES = "f",
    is_big_endian: bool = False,
) -> bytes:
    """Convert an iterable of numbers into a block of data in the Rohde & Schwarz
    format for extended block lengths. This is used by R&S for blocks greater
    than or equal to 1 GB.

    Parameters
    ----------
    iterable : Sequence[Union[int, float]]
        Sequence of numbers to pack into a block.
    datatype : BINARY_DATATYPES, optional
        Format string for a single element. See struct module. Default to 'f'.
    is_big_endian : bool, optional
        Are the data in big or little endian order. Default to False.

    Returns
    -------
    bytes
        Binary block of data preceded by the specified header

    """
    array_length = len(iterable)
    element_length = struct.calcsize(datatype)
    data_length = array_length * element_length

    header = f"#({data_length:d})"

    return to_binary_block(iterable, header, datatype, is_big_endian)


def to_hp_block(
    iterable: Sequence[Union[int, float]],
    datatype: BINARY_DATATYPES = "f",
    is_big_endian: bool = False,
) -> bytes:
    """Convert an iterable of numbers into a block of data in the HP format.

    Parameters
    ----------
    iterable : Sequence[Union[int, float]]
        Sequence of numbers to pack into a block.
    datatype : BINARY_DATATYPES, optional
        Format string for a single element. See struct module. Default to 'f'.
    is_big_endian : bool, optional
        Are the data in big or little endian order. Default to False.

    Returns
    -------
    bytes
        Binary block of data preceded by the specified header

    """
    array_length = len(iterable)
    element_length = struct.calcsize(datatype)
    data_length = array_length * element_length

    if data_length >= 2**16:
        msg = (
            "Block length in bytes cannot be greater than or equal to 64 KiB "
            "(it must be representable with a 16-bit unsigned int), but the "
            f"block length was {data_length} bytes."
        )
        raise OverflowError(msg)

    header = b"#A" + (
        int.to_bytes(data_length, 2, "big" if is_big_endian else "little")
    )

    return to_binary_block(iterable, header, datatype, is_big_endian)


# The actual value would be:
# DebugInfo = Union[List[str], Dict[str, Union[str, DebugInfo]]]
DebugInfo = Union[List[str], Dict[str, Any]]


def get_system_details(
    backends: bool = True,
) -> Dict[str, Union[str, Dict[str, DebugInfo]]]:
    """Return a dictionary with information about the system."""
    buildno, builddate = platform.python_build()
    if sys.maxunicode == 65535:
        # UCS2 build (standard)
        unitype = "UCS2"
    else:
        # UCS4 build (most recent Linux distros)
        unitype = "UCS4"
    machine_info = platform.machine()
    maybe_architecture = ArchitectureType.from_platform_machine(machine_info)
    if maybe_architecture:
        architecture_str = str(maybe_architecture.value)
    else:
        architecture_str = machine_info

    from . import __version__

    backend_details: Dict[str, DebugInfo] = OrderedDict()
    d: Dict[str, Union[str, dict]] = {
        "platform": platform.platform(),
        "processor": platform.processor(),
        "executable": sys.executable,
        "implementation": getattr(platform, "python_implementation", lambda: "n/a")(),
        "python": platform.python_version(),
        "compiler": platform.python_compiler(),
        "buildno": buildno,
        "builddate": builddate,
        "unicode": unitype,
        "architecture": architecture_str,
        "pyvisa": __version__,
        "backends": backend_details,
    }

    if backends:
        from . import highlevel

        for backend in highlevel.list_backends():
            if backend.startswith("pyvisa-"):
                backend = backend[7:]

            try:
                cls = highlevel.get_wrapper_class(backend)
            except Exception as e:
                backend_details[backend] = [
                    "Could not instantiate backend",
                    "-> %s" % str(e),
                ]
                continue

            try:
                backend_details[backend] = cls.get_debug_info()
            except Exception as e:
                backend_details[backend] = [
                    "Could not obtain debug info",
                    "-> %s" % str(e),
                ]

    return d


def system_details_to_str(
    d: Dict[str, Union[str, Dict[str, DebugInfo]]], indent: str = ""
) -> str:
    """Convert the system details to a str.

    System details can be obtained by `get_system_details`.

    """

    details = [
        "Machine Details:",
        "   Platform ID:    %s" % d.get("platform", "n/a"),
        "   Processor:      %s" % d.get("processor", "n/a"),
        "",
        "Python:",
        "   Implementation: %s" % d.get("implementation", "n/a"),
        "   Executable:     %s" % d.get("executable", "n/a"),
        "   Version:        %s" % d.get("python", "n/a"),
        "   Compiler:       %s" % d.get("compiler", "n/a"),
        "   Architecture:   %s" % d.get("architecture", "n/a"),
        "   Build:          %s (#%s)"
        % (d.get("builddate", "n/a"), d.get("buildno", "n/a")),
        "   Unicode:        %s" % d.get("unicode", "n/a"),
        "",
        "PyVISA Version: %s" % d.get("pyvisa", "n/a"),
        "",
    ]

    def _to_list(key, value, indent_level=0):
        sp = " " * indent_level * 3

        if isinstance(value, str):
            if key:
                return ["%s%s: %s" % (sp, key, value)]
            else:
                return ["%s%s" % (sp, value)]

        elif isinstance(value, dict):
            if key:
                al = ["%s%s:" % (sp, key)]
            else:
                al = []

            for k, v in value.items():
                al.extend(_to_list(k, v, indent_level + 1))
            return al

        elif isinstance(value, (tuple, list)):
            if key:
                al = ["%s%s:" % (sp, key)]
            else:
                al = []

            for v in value:
                al.extend(_to_list(None, v, indent_level + 1))

            return al

        else:
            return ["%s" % value]

    details.extend(_to_list("Backends", d["backends"]))

    joiner = "\n" + indent
    return indent + joiner.join(details) + "\n"


@overload
def get_debug_info(to_screen: Literal[True] = True) -> None:
    pass


@overload
def get_debug_info(to_screen: Literal[False]) -> str:
    pass


def get_debug_info(to_screen: bool = True):
    """Get the PyVISA debug information."""
    out = system_details_to_str(get_system_details())
    if not to_screen:
        return out
    print(out)


def get_shared_library_arch(filename: Union[str, Path]) -> PEMachineType:
    """Get the architecture of shared library."""
    with io.open(filename, "rb") as fp:
        dos_headers = fp.read(64)
        _ = fp.read(4)

        magic, _skip, offset = struct.unpack("=2s58sl", dos_headers)

        if magic != b"MZ":
            raise Exception("Not an executable")

        fp.seek(offset, io.SEEK_SET)
        pe_header = fp.read(6)

        sig, _skip, machine = struct.unpack(str("=2s2sH"), pe_header)

        if sig != b"PE":
            raise Exception("Not a PE executable")

        try:
            return PEMachineType(machine)
        except ValueError:
            return PEMachineType.UNKNOWN


def get_arch(filename: Union[str, Path]) -> List[ArchitectureType]:
    """Get the architecture of the platform."""
    this_platform = sys.platform
    if this_platform.startswith("win"):
        machine_type = get_shared_library_arch(filename)
        if machine_type == PEMachineType.I386:
            return [ArchitectureType.I386]
        elif machine_type == PEMachineType.AMD64:
            return [ArchitectureType.X86_64]
        elif machine_type == PEMachineType.AARCH64:
            return [ArchitectureType.AARCH64]
        else:
            return []
    elif this_platform not in ("linux", "darwin"):
        raise OSError("Unsupported platform: %s" % this_platform)
    res = subprocess.run(["file", filename], capture_output=True)
    out = res.stdout.decode("ascii")

    if this_platform.startswith("linux"):
        # Example outputs:
        #   i386:
        #       /usr/bin/python: ELF 32-bit LSB executable, Intel 80386, version 1 (SYSV)
        #   x86_64:
        #       /usr/bin/python3.10: ELF 64-bit LSB pie executable, x86-64, version 1 (SYSV), dynamically linked
        #   aarch64:
        #       /usr/bin/python3.9: ELF 64-bit LSB executable, ARM aarch64, version 1 (SYSV), dynamically linked
        if "80386" in out:
            return [ArchitectureType.I386]
        if "x86-64" in out:
            return [ArchitectureType.X86_64]
        if "aarch64" in out:
            return [ArchitectureType.AARCH64]

        return []
    else:  # darwin
        # universal binary, i386 and x86_64:
        #   /usr/bin/grep: Mach-O universal binary with 2 architectures
        #   /usr/bin/grep (for architecture x86_64):    Mach-O 64-bit executable x86_64
        #   /usr/bin/grep (for architecture i386):      Mach-O executable i386
        # universal binary, x86_64 and aarch64:
        #   /usr/bin/grep: Mach-O universal binary with 2 architectures: [x86_64:Mach-O 64-bit executable x86_64] [arm64e:Mach-O 64-bit executable arm64e]
        #   /usr/bin/grep (for architecture x86_64):	Mach-O 64-bit executable x86_64
        #   /usr/bin/grep (for architecture arm64e):	Mach-O 64-bit executable arm64e
        # single-arch binary, aarch64:
        #   /opt/homebrew/bin/rg: Mach-O 64-bit executable arm64
        archs: List[ArchitectureType] = []

        if "executable i386" in out:
            archs.append(ArchitectureType.I386)
        if "executable x86_64" in out:
            archs.append(ArchitectureType.X86_64)
        if "executable arm64" in out:
            archs.append(ArchitectureType.AARCH64)

        return archs


def message_size(
    num_points: int,
    datatype: BINARY_DATATYPES = "f",
    header_format: Literal["ieee", "hp", "empty"] = "ieee",
) -> int:
    """Helper function to calculate a message size, including the header, in bytes.

    Parameters
    ----------
    num_points : int
        Number of data points to transfer
    datatype : BINARY_DATATYPES, optional
            The format string for a single element. See struct module.
    header_format : str
        Specify "ieee" or "hp" or "empty" header format

    Returns
    -------
    int
        The total message size in bytes

    """
    data_length = num_points * struct.calcsize(datatype)
    if header_format == "ieee":
        header_length = len(f"{data_length}") + 2
    elif header_format == "hp":
        header_length = 4
    elif header_format == "empty":
        header_length = 0
    else:
        raise ValueError("Unsupported header_fmt: %s" % header_format)
    result = data_length + header_length + 1  # 1 for the term char
    return result
