# -*- coding: utf-8 -*-
"""
Exceptions for pyproj
"""


class ProjError(RuntimeError):
    """Raised when a Proj error occurs."""

    internal_proj_error = None

    def __init__(self, error_message):
        if self.internal_proj_error is not None:
            error_message = (
                "{error_message}: (Internal Proj Error: {internal_proj_error})"
            ).format(
                error_message=error_message,
                internal_proj_error=self.internal_proj_error,
            )
            ProjError.clear()
        super(ProjError, self).__init__(error_message)

    @staticmethod
    def clear():
        """
        This will clear the internal PROJ erro message.
        """
        ProjError.internal_proj_error = None


class CRSError(ProjError):
    """Raised when a CRS error occurs."""


class GeodError(RuntimeError):
    """Raised when a Geod error occurs."""


class DataDirError(RuntimeError):
    """Raised when a the data directory was not found."""
