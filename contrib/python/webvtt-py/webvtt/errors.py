
__all__ = ['MalformedFileError', 'MalformedCaptionError', 'InvalidCaptionsError', 'MissingFilenameError']


class MalformedFileError(Exception):
    """Error raised when the file is not well formatted"""


class MalformedCaptionError(Exception):
    """Error raised when a caption is not well formatted"""


class InvalidCaptionsError(Exception):
    """Error raised when passing wrong captions to the segmenter"""


class MissingFilenameError(Exception):
    """Error raised when saving a file without filename."""
