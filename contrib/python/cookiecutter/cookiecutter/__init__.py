"""Main package for Cookiecutter."""

from pathlib import Path


def _get_version() -> str:
    """Read VERSION.txt and return its contents."""
    path = Path(__file__).parent.resolve()
    version_file = path / "VERSION.txt"
    import pkgutil
    return pkgutil.get_data(__package__, "VERSION.txt").strip().decode("utf-8")


__version__ = _get_version()
