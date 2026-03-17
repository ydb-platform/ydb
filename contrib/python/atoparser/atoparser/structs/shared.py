"""Shared structs and definitions used serialize/deserialize Atop data directly from log files.

Structs are declared in a way that will help provide as close to a 1 to 1 match as possible for debuggability
and maintenance. The _fields_ of every struct match their original name, however the struct names have been updated
to match python CamelCase standards. Each struct includes the following to help identify the original source:
    C Name: utsname
    C Location: sys/utsname.h
"""

import ctypes

# Disable the following pylint warnings to allow the variables and classes to match the style from the C.
# This helps with maintainability and cross-referencing.
# pylint: disable=invalid-name

# Definitions from time.h
time_t = ctypes.c_long

# Definitions from atop.h
count_t = ctypes.c_longlong

# Definitions from sys/types.h
off_t = ctypes.c_long
pid_t = ctypes.c_int


class HeaderMixin:
    """Shared logic for top level struct describing information contained in the log file.

    Attributes:
        supported_version: The version of Atop that this header is compatible with as <major.<minor>.
    """

    supported_version: str
    Record: ctypes.Structure
    SStat: ctypes.Structure
    TStat: ctypes.Structure
    CStat: ctypes.Structure
    CGChainer: ctypes.Structure

    def check_compatibility(self) -> None:
        """Verify if the loaded values are compatible with this header version.

        Raises:
            ValueError if not compatible.
        """
        sizes = [
            ("Header", self.rawheadlen, ctypes.sizeof(self.__class__)),
            ("Record", self.rawreclen, ctypes.sizeof(self.Record)),
            ("SStat", self.sstatlen, ctypes.sizeof(self.SStat)),
        ]
        if self.major_version >= 2 and self.minor_version >= 3:
            sizes.append(("TStat", self.tstatlen, ctypes.sizeof(self.TStat)))
        else:
            sizes.append(("PStat", self.pstatlen, ctypes.sizeof(self.TStat)))
        if self.major_version >= 2 and self.minor_version >= 11:
            sizes.append(("CStat", self.cstatlen, ctypes.sizeof(self.CStat)))
        if any(size[1] != size[2] for size in sizes):
            raise ValueError(
                f"File has incompatible Atop format. Struct length evaluations (type, found, expected): {sizes}"
            )

    @property
    def major_version(self) -> int:
        """The major version from the semantic version."""
        self.semantic_version  # Call the primary property to ensure population. pylint: disable=pointless-statement
        return self._major_version

    @property
    def minor_version(self) -> int:
        """The minor version from the semantic version."""
        self.semantic_version  # Call the primary property to ensure population. pylint: disable=pointless-statement
        return self._minor_version

    @property
    def semantic_version(self) -> str:
        """The semantic version as major.minor.

        Atop releases have "maintenance" versions, but they do not impact the header or file structure.
        i.e., 2.3.1 is the same as 2.3.
        """
        # Use a general getattr() call to ensure the instance can always set the attribute even on first call.
        # C structs have various ways of creating instances, so __init__ is not always called to set up attributes.
        if not getattr(self, "_semantic_version", None):
            # pylint: disable=attribute-defined-outside-init
            self._major_version = (self.aversion >> 8) & 0x7F
            self._minor_version = self.aversion & 0xFF
            self._semantic_version = f"{self._major_version}.{self._minor_version}"
        return self._semantic_version


class UTSName(ctypes.Structure):
    """Struct to describe basic system information.

    C Name: utsname
    C Location: sys/utsname.h
    """

    _fields_ = [
        # Standard GNU length is 64 characters + null terminator
        ("sysname", ctypes.c_char * 65),
        ("nodename", ctypes.c_char * 65),
        ("release", ctypes.c_char * 65),
        ("version", ctypes.c_char * 65),
        ("machine", ctypes.c_char * 65),
        ("domain", ctypes.c_char * 65),
    ]
