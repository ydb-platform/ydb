"""Metadata for the Project."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, metadata, version

__all__ = ("__project__", "__version__")

try:
    __version__ = version("litestar_htmx")
    """Version of the project."""
    __project__ = metadata("litestar_htmx")["Name"]
    """Name of the project."""
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"
    __project__ = "Litestar HTMX"
finally:
    del version, PackageNotFoundError, metadata
