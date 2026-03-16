"""
This module defines custom exceptions that are raised in the `numcodecs` codebase.
"""


class UnknownCodecError(ValueError):
    """
    An exception that is raised when trying to receive a codec that has not been registered.

    Parameters
    ----------
    codec_id : str
        Codec identifier.

    Examples
    ----------
    >>> import numcodecs
    >>> numcodecs.get_codec({"codec_id": "unknown"})
    Traceback (most recent call last):
        ...
    UnknownCodecError: codec not available: 'unknown'
    """

    def __init__(self, codec_id: str):
        self.codec_id = codec_id
        super().__init__(f"codec not available: '{codec_id}'")
