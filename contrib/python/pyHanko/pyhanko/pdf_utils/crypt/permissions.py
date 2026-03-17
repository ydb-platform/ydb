import operator
import struct
from enum import Flag
from functools import reduce

__all__ = ['PdfPermissions', 'StandardPermissions', 'PubKeyPermissions']


class PdfPermissions(Flag):
    """
    Utility mixin for PDF permission flags.
    """

    # We purposefully do not inherit from IntFlag since
    # PDF uses 32-bit twos complement to treat flags as ints,
    # which doesn't jive well with what IntFlag would do,
    # so it's hard to detect backwards compatibility issues.

    @classmethod
    def allow_everything(cls):
        """
        Set all permissions.
        """

        return reduce(operator.or_, cls.__members__.values())

    @classmethod
    def from_uint(cls, uint_flags: int):
        """
        Convert a 32-bit unsigned integer into PDF permission flags.
        """

        result = cls(0)
        for flag in cls:
            if uint_flags & flag.value:
                result |= flag
        return result

    @classmethod
    def from_bytes(cls, flags: bytes):
        """
        Convert a string of 4 bytes into PDF permission flags.
        """

        uint_flags = struct.unpack('>I', flags)[0]
        return cls.from_uint(uint_flags)

    @classmethod
    def from_sint32(cls, sint32_flags: int):
        """
        Convert a 32-bit signed integer into PDF permission flags.
        """

        return cls.from_uint(sint32_flags & 0xFFFFFFFF)

    def as_uint32(self) -> int:
        """
        Convert a set of PDF permission flags to their 32-bit
        unsigned integer representation.

        This will already take into account some conventions
        in the PDF specification, i.e. to set as-yet undefined
        permission flags to 'Allow'.
        """

        raise NotImplementedError

    def as_bytes(self) -> bytes:
        """
        Convert a set of PDF permission flags to their binary
        representation.
        """
        return struct.pack('>I', self.as_uint32())

    def as_sint32(self) -> int:
        """
        Convert a set of PDF permission flags to their
        signed integer representation.
        """
        return struct.unpack('>i', self.as_bytes())[0]

    def mac_required(self) -> bool:
        raise NotImplementedError


class StandardPermissions(PdfPermissions, Flag):
    """
    Permission flags for the standard security handler.

    See Table 22 in ISO 32000-2:2020.
    """

    ALLOW_PRINTING = 4
    ALLOW_MODIFICATION_GENERIC = 8
    ALLOW_CONTENT_EXTRACTION = 16
    ALLOW_ANNOTS_FORM_FILLING = 32
    ALLOW_FORM_FILLING = 256
    ALLOW_ASSISTIVE_TECHNOLOGY = 512
    ALLOW_REASSEMBLY = 1024
    ALLOW_HIGH_QUALITY_PRINTING = 2048
    TOLERATE_MISSING_PDF_MAC = 4096

    def as_uint32(self) -> int:
        return sum(x.value for x in self.__class__ if x in self) | 0xFFFFE0C0

    def mac_required(self) -> bool:
        return StandardPermissions.TOLERATE_MISSING_PDF_MAC not in self


class PubKeyPermissions(PdfPermissions, Flag):
    """
    Permission flags for the public-key security handler.

    See Table 24 in ISO 32000-2:2020.
    """

    ALLOW_ENCRYPTION_CHANGE = 2
    ALLOW_PRINTING = 4
    ALLOW_MODIFICATION_GENERIC = 8
    ALLOW_CONTENT_EXTRACTION = 16
    ALLOW_ANNOTS_FORM_FILLING = 32
    ALLOW_FORM_FILLING = 256
    ALLOW_ASSISTIVE_TECHNOLOGY = 512
    ALLOW_REASSEMBLY = 1024
    ALLOW_HIGH_QUALITY_PRINTING = 2048
    TOLERATE_MISSING_PDF_MAC = 4096

    def as_uint32(self) -> int:
        # ensure the first bit is set for compatibility with Acrobat
        return sum(x.value for x in self.__class__ if x in self) | 0xFFFFE0C1

    def mac_required(self) -> bool:
        return PubKeyPermissions.TOLERATE_MISSING_PDF_MAC not in self
