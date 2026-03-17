from pathlib import Path

from .colorhash import ColorHash


def get_version(_):
    """
    Fast (dev time) way to get version.
    """
    with Path("pyproject.toml").open(encoding="utf-8") as f:
        for line in f:
            if line.startswith("version = "):
                return line.split("=")[1].strip().strip('"')
    return None


try:
    # py3.8+
    from importlib.metadata import version

except ImportError:
    try:
        # py3.6 - py3.7
        from importlib_metadata import version
    except ImportError:
        # some installations might be missing importlib_metadata
        version = get_version

__all__ = ["ColorHash"]
__version__ = version(__package__)
