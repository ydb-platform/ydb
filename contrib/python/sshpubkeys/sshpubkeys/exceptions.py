# pylint:disable=line-too-long,too-many-ancestors
"""Exceptions for sshpubkeys."""


class InvalidKeyException(Exception):
    """Invalid key - something is wrong with the key, and it should not be accepted, as OpenSSH will not work with it."""


class InvalidKeyError(InvalidKeyException):
    """Invalid key - something is wrong with the key, and it should not be accepted, as OpenSSH will not work with it."""


class InvalidKeyLengthException(InvalidKeyError):
    """Invalid key length - either too short or too long.

    See also TooShortKeyException and TooLongKeyException."""


class InvalidKeyLengthError(InvalidKeyError):
    """Invalid key length - either too short or too long.

    See also TooShortKeyException and TooLongKeyException."""


class TooShortKeyException(InvalidKeyLengthError):
    """Key is shorter than what the specification allow."""


class TooShortKeyError(TooShortKeyException):
    """Key is shorter than what the specification allows."""


class TooLongKeyException(InvalidKeyLengthError):
    """Key is longer than what the specification allows."""


class TooLongKeyError(TooLongKeyException):
    """Key is longer than what the specification allows."""


class InvalidTypeException(InvalidKeyError):
    """Key type is invalid or unrecognized."""


class InvalidTypeError(InvalidTypeException):
    """Key type is invalid or unrecognized."""


class MalformedDataException(InvalidKeyError):
    """The key is invalid - unable to parse the data. The data may be corrupted, truncated, or includes extra content that is not allowed."""


class MalformedDataError(MalformedDataException):
    """The key is invalid - unable to parse the data. The data may be corrupted, truncated, or includes extra content that is not allowed."""


class InvalidOptionsException(MalformedDataError):
    """Options string is invalid: it contains invalid characters, unrecognized options, or is otherwise malformed."""


class InvalidOptionsError(InvalidOptionsException):
    """Options string is invalid: it contains invalid characters, unrecognized options, or is otherwise malformed."""


class InvalidOptionNameException(InvalidOptionsError):
    """Invalid option name (contains disallowed characters, or is unrecognized.)."""


class InvalidOptionNameError(InvalidOptionNameException):
    """Invalid option name (contains disallowed characters, or is unrecognized.)."""


class UnknownOptionNameException(InvalidOptionsError):
    """Unrecognized option name."""


class UnknownOptionNameError(UnknownOptionNameException):
    """Unrecognized option name."""


class MissingMandatoryOptionValueException(InvalidOptionsError):
    """Mandatory option value is missing."""


class MissingMandatoryOptionValueError(MissingMandatoryOptionValueException):
    """Mandatory option value is missing."""
