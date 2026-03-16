#
# (C) Copyright 2017- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities
# granted to it by virtue of its status as an intergovernmental organisation nor
# does it submit to any jurisdiction.
#

"""
Exception class hierarchy
"""

from .bindings import ENC, ffi, lib


class GribInternalError(Exception):
    """
    @brief Wrap errors coming from the C API in a Python exception object.

    Base class for all exceptions
    """

    def __init__(self, value):
        # Call the base class constructor with the parameters it needs
        Exception.__init__(self, value)
        if isinstance(value, int):
            self.msg = ffi.string(lib.grib_get_error_message(value)).decode(ENC)
        else:
            self.msg = value

    def __str__(self):
        return self.msg


class FunctionalityNotEnabledError(GribInternalError):
    """Functionality not enabled."""


class WrongBitmapSizeError(GribInternalError):
    """Size of bitmap is incorrect."""


class OutOfRangeError(GribInternalError):
    """Value out of coding range."""


class UnsupportedEditionError(GribInternalError):
    """Edition not supported.."""


class AttributeNotFoundError(GribInternalError):
    """Attribute not found.."""


class TooManyAttributesError(GribInternalError):
    """Too many attributes. Increase MAX_ACCESSOR_ATTRIBUTES."""


class AttributeClashError(GribInternalError):
    """Attribute is already present, cannot add."""


class NullPointerError(GribInternalError):
    """Null pointer."""


class MissingBufrEntryError(GribInternalError):
    """Missing BUFR table entry for descriptor."""


class WrongConversionError(GribInternalError):
    """Wrong type conversion."""


class StringTooSmallError(GribInternalError):
    """String is smaller than requested."""


class InvalidKeyValueError(GribInternalError):
    """Invalid key value."""


class ValueDifferentError(GribInternalError):
    """Value is different."""


class DifferentEditionError(GribInternalError):
    """Edition of two messages is different."""


class InvalidBitsPerValueError(GribInternalError):
    """Invalid number of bits per value."""


class CorruptedIndexError(GribInternalError):
    """Index is corrupted."""


class MessageMalformedError(GribInternalError):
    """Message malformed."""


class UnderflowError(GribInternalError):
    """Underflow."""


class SwitchNoMatchError(GribInternalError):
    """Switch unable to find a matching case."""


class ConstantFieldError(GribInternalError):
    """Constant field."""


class MessageTooLargeError(GribInternalError):
    """Message is too large for the current architecture."""


class InternalArrayTooSmallError(GribInternalError):
    """An internal array is too small."""


class PrematureEndOfFileError(GribInternalError):
    """End of resource reached when reading message."""


class NullIndexError(GribInternalError):
    """Null index."""


class EndOfIndexError(GribInternalError):
    """End of index reached."""


class WrongGridError(GribInternalError):
    """Grid description is wrong or inconsistent."""


class NoValuesError(GribInternalError):
    """Unable to code a field without values."""


class EndError(GribInternalError):
    """End of resource."""


class WrongTypeError(GribInternalError):
    """Wrong type while packing."""


class NoDefinitionsError(GribInternalError):
    """Definitions files not found."""


class HashArrayNoMatchError(GribInternalError):
    """Hash array no match."""


class ConceptNoMatchError(GribInternalError):
    """Concept no match."""


class OutOfAreaError(GribInternalError):
    """The point is out of the grid area."""


class MissingKeyError(GribInternalError):
    """Missing a key from the fieldset."""


class InvalidOrderByError(GribInternalError):
    """Invalid order by."""


class InvalidNearestError(GribInternalError):
    """Invalid nearest id."""


class InvalidKeysIteratorError(GribInternalError):
    """Invalid keys iterator id."""


class InvalidIteratorError(GribInternalError):
    """Invalid iterator id."""


class InvalidIndexError(GribInternalError):
    """Invalid index id."""


class InvalidGribError(GribInternalError):
    """Invalid GRIB id."""


class InvalidFileError(GribInternalError):
    """Invalid file id."""


class WrongStepUnitError(GribInternalError):
    """Wrong units for step (step must be integer)."""


class WrongStepError(GribInternalError):
    """Unable to set step."""


class InvalidTypeError(GribInternalError):
    """Invalid key type."""


class WrongLengthError(GribInternalError):
    """Wrong message length."""


class ValueCannotBeMissingError(GribInternalError):
    """Value cannot be missing."""


class InvalidSectionNumberError(GribInternalError):
    """Invalid section number."""


class NullHandleError(GribInternalError):
    """Null handle."""


class InvalidArgumentError(GribInternalError):
    """Invalid argument."""


class ReadOnlyError(GribInternalError):
    """Value is read only."""


class MemoryAllocationError(GribInternalError):
    """Memory allocation error."""


class GeocalculusError(GribInternalError):
    """Problem with calculation of geographic attributes."""


class NoMoreInSetError(GribInternalError):
    """Code cannot unpack because of string too small."""


class EncodingError(GribInternalError):
    """Encoding invalid."""


class DecodingError(GribInternalError):
    """Decoding invalid."""


class MessageInvalidError(GribInternalError):
    """Message invalid."""


class IOProblemError(GribInternalError):
    """Input output problem."""


class KeyValueNotFoundError(GribInternalError):
    """Key/value not found."""


class WrongArraySizeError(GribInternalError):
    """Array size mismatch."""


class CodeNotFoundInTableError(GribInternalError):
    """Code not found in code table."""


class FileNotFoundError(GribInternalError):
    """File not found."""


class ArrayTooSmallError(GribInternalError):
    """Passed array is too small."""


class MessageEndNotFoundError(GribInternalError):
    """Missing 7777 at end of message."""


class FunctionNotImplementedError(GribInternalError):
    """Function not yet implemented."""


class BufferTooSmallError(GribInternalError):
    """Passed buffer is too small."""


class InternalError(GribInternalError):
    """Internal error."""


class EndOfFileError(GribInternalError):
    """End of resource reached."""


ERROR_MAP = {
    -67: FunctionalityNotEnabledError,
    -66: WrongBitmapSizeError,
    -65: OutOfRangeError,
    -64: UnsupportedEditionError,
    -63: AttributeNotFoundError,
    -62: TooManyAttributesError,
    -61: AttributeClashError,
    -60: NullPointerError,
    -59: MissingBufrEntryError,
    -58: WrongConversionError,
    -57: StringTooSmallError,
    -56: InvalidKeyValueError,
    -55: ValueDifferentError,
    -54: DifferentEditionError,
    -53: InvalidBitsPerValueError,
    -52: CorruptedIndexError,
    -51: MessageMalformedError,
    -50: UnderflowError,
    -49: SwitchNoMatchError,
    -48: ConstantFieldError,
    -47: MessageTooLargeError,
    -46: InternalArrayTooSmallError,
    -45: PrematureEndOfFileError,
    -44: NullIndexError,
    -43: EndOfIndexError,
    -42: WrongGridError,
    -41: NoValuesError,
    -40: EndError,
    -39: WrongTypeError,
    -38: NoDefinitionsError,
    -37: HashArrayNoMatchError,
    -36: ConceptNoMatchError,
    -35: OutOfAreaError,
    -34: MissingKeyError,
    -33: InvalidOrderByError,
    -32: InvalidNearestError,
    -31: InvalidKeysIteratorError,
    -30: InvalidIteratorError,
    -29: InvalidIndexError,
    -28: InvalidGribError,
    -27: InvalidFileError,
    -26: WrongStepUnitError,
    -25: WrongStepError,
    -24: InvalidTypeError,
    -23: WrongLengthError,
    -22: ValueCannotBeMissingError,
    -21: InvalidSectionNumberError,
    -20: NullHandleError,
    -19: InvalidArgumentError,
    -18: ReadOnlyError,
    -17: MemoryAllocationError,
    -16: GeocalculusError,
    -15: NoMoreInSetError,
    -14: EncodingError,
    -13: DecodingError,
    -12: MessageInvalidError,
    -11: IOProblemError,
    -10: KeyValueNotFoundError,
    -9: WrongArraySizeError,
    -8: CodeNotFoundInTableError,
    -7: FileNotFoundError,
    -6: ArrayTooSmallError,
    -5: MessageEndNotFoundError,
    -4: FunctionNotImplementedError,
    -3: BufferTooSmallError,
    -2: InternalError,
    -1: EndOfFileError,
}


def raise_grib_error(errid):
    """
    Raise the GribInternalError corresponding to ``errid``.
    """
    raise ERROR_MAP[errid](errid)
