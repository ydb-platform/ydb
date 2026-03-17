__author__ = 'Peter Wang'
__license__ = 'BSD'
__version__ = '1.1.0'

# Relative imports assuming the current package structure
from .exceptions import Empty, Full  # noqa: F401
from .queue import Queue  # noqa: F401
import logging
log = logging.getLogger(__name__)

# Async queue imports
try:
    from .async_queue import AsyncQueue  # noqa: F401
    from .async_sqlqueue import (  # noqa: F401
        AsyncSQLiteQueue,
        AsyncFIFOSQLiteQueue,
        AsyncFILOSQLiteQueue,
        AsyncUniqueQ
    )
except ImportError as e:
    # If async dependencies are not available, log info
    log.info("Async queues not available, may need to install "
             "aiofiles and aiosqlite: %s", e)

# Attempt to import optional components, logging if not found.
try:
    from .pdict import PDict  # noqa: F401
    from .sqlqueue import (  # noqa: F401
        SQLiteQueue,
        FIFOSQLiteQueue,
        FILOSQLiteQueue,
        UniqueQ
    )
    from .priorityqueue import PriorityQueue
    from .sqlackqueue import (  # noqa: F401
        SQLiteAckQueue,
        FIFOSQLiteAckQueue,
        FILOSQLiteAckQueue,
        UniqueAckQ,
        AckStatus
    )
except ImportError:
    # If sqlite3 is not available, log a message.
    log.info("No sqlite3 module found, sqlite3 based queues are not available")

try:
    from .mysqlqueue import MySQLQueue  # noqa: F401
except ImportError:
    # failed due to DBUtils not installed via extra-requirements.txt
    log.info("DBUtils may not be installed, install "
             "via 'pip install persist-queue[extra]'")

# Define what symbols are exported by the module.
__all__ = [
    "Queue",
    "SQLiteQueue",
    "FIFOSQLiteQueue",
    "FILOSQLiteQueue",
    "UniqueQ",
    "PDict",
    "SQLiteAckQueue",
    "FIFOSQLiteAckQueue",
    "FILOSQLiteAckQueue",
    "UniqueAckQ",
    "AckStatus",
    "MySQLQueue",
    "PriorityQueue",
    "AsyncQueue",
    "AsyncSQLiteQueue",
    "AsyncFIFOSQLiteQueue",
    "AsyncFILOSQLiteQueue",
    "AsyncUniqueQ",
    "Empty",
    "Full",
    "__author__",
    "__license__",
    "__version__"
]
