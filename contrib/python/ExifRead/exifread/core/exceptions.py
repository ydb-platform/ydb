"""Exception classes."""


class ExifError(Exception):
    """Base class for all errors."""


class InvalidExif(ExifError):
    """The EXIF is invalid."""


class ExifNotFound(ExifError):
    """The EXIF could not be found."""
