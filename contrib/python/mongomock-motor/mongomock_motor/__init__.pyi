from contextlib import contextmanager
from typing import Iterator

from motor.motor_asyncio import (
    AsyncIOMotorClient as AsyncMongoMockClient,
)
from motor.motor_asyncio import (
    AsyncIOMotorCollection as AsyncMongoMockCollection,
)
from motor.motor_asyncio import AsyncIOMotorCommandCursor as AsyncCommandCursor
from motor.motor_asyncio import AsyncIOMotorCursor as AsyncCursor
from motor.motor_asyncio import (
    AsyncIOMotorDatabase as AsyncMongoMockDatabase,
)
from motor.motor_asyncio import (
    AsyncIOMotorLatentCommandCursor as AsyncLatentCommandCursor,
)

@contextmanager
def enabled_gridfs_integration() -> Iterator[None]: ...

__all__: list[str] = [
    'AsyncCommandCursor',
    'AsyncCursor',
    'AsyncLatentCommandCursor',
    'AsyncMongoMockClient',
    'AsyncMongoMockCollection',
    'AsyncMongoMockDatabase',
    'enabled_gridfs_integration',
]
