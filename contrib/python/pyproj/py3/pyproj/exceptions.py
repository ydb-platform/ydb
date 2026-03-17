"""
Exceptions for pyproj
"""

from pyproj._context import _clear_proj_error, _get_proj_error


class ProjError(RuntimeError):
    """Raised when a Proj error occurs."""

    def __init__(self, error_message: str) -> None:
        proj_error = _get_proj_error()
        if proj_error is not None:
            error_message = f"{error_message}: (Internal Proj Error: {proj_error})"
            _clear_proj_error()
        super().__init__(error_message)


class CRSError(ProjError):
    """Raised when a CRS error occurs."""


class GeodError(RuntimeError):
    """Raised when a Geod error occurs."""


class DataDirError(RuntimeError):
    """Raised when a the data directory was not found."""
