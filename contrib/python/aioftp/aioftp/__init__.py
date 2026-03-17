"""ftp client/server for asyncio"""

# flake8: noqa

import importlib.metadata

from .client import *
from .common import *
from .errors import *
from .pathio import *
from .server import *

__version__ = importlib.metadata.version(__package__)
version = tuple(map(int, __version__.split(".")))

__all__ = (
    client.__all__ + server.__all__ + errors.__all__ + common.__all__ + pathio.__all__ + ("version", "__version__")  # type: ignore[name-defined]
)
