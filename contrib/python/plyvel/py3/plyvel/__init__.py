"""
Plyvel, a fast and feature-rich Python interface to LevelDB.
"""

# Only import the symbols that are part of the public API
from ._plyvel import (  # noqa
    __leveldb_version__,
    DB,
    repair_db,
    destroy_db,
    Error,
    IOError,
    CorruptionError,
    IteratorInvalidError,
)

from ._version import __version__  # noqa
