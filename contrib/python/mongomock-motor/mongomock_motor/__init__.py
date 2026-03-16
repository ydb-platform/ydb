import asyncio
import importlib
from asyncio.events import AbstractEventLoop
from contextlib import ExitStack, contextmanager
from functools import wraps
from typing import Any, List, Optional, Union
from unittest.mock import patch

from mongomock.collection import Collection as MongoMockCollection
from mongomock.collection import Cursor as MongoMockCursor
from mongomock.command_cursor import CommandCursor as MongoMockCommandCursor
from mongomock.database import Database as MongoMockDatabase
from mongomock.gridfs import _create_grid_out_cursor
from mongomock.mongo_client import MongoClient as MongoMockMongoClient
from pymongo.database import Database as PyMongoDatabase
from typing_extensions import Self

from .patches import _patch_client_internals, _patch_collection_internals
from .typing import BuildInfo, DocumentType


def masquerade_class(name: str):
    module_name, target_name = name.rsplit('.', 1)

    try:
        target = getattr(importlib.import_module(module_name), target_name)
    except Exception:
        return lambda cls: cls

    def decorator(cls):
        @wraps(target, updated=())
        class Wrapper(cls):
            @property
            def __class__(self):
                return target

        return Wrapper

    return decorator


def with_async_methods(source: str, async_methods: List[str]):
    def decorator(cls):
        for method_name in async_methods:

            def make_wrapper(method_name: str):
                async def wrapper(self, *args, **kwargs):
                    proxy_source = self.__dict__.get(f'_{cls.__name__}{source}')
                    return getattr(proxy_source, method_name)(*args, **kwargs)

                return wrapper

            setattr(cls, method_name, make_wrapper(method_name))

        return cls

    return decorator


def with_cursor_chaining_methods(source: str, chaining_methods: List[str]):
    def decorator(cls):
        for method_name in chaining_methods:

            def make_wrapper(method_name: str):
                def wrapper(self, *args, **kwargs):
                    proxy_source = self.__dict__.get(f'_{cls.__name__}{source}')
                    getattr(proxy_source, method_name)(*args, **kwargs)
                    return self

                return wrapper

            setattr(cls, method_name, make_wrapper(method_name))

        return cls

    return decorator


@masquerade_class('motor.motor_asyncio.AsyncIOMotorCursor')
@with_cursor_chaining_methods(
    '__cursor',
    [
        'add_option',
        'allow_disk_use',
        'batch_size',
        'collation',
        'comment',
        'hint',
        'limit',
        'max_await_time_ms',
        'max_scan',
        'max_time_ms',
        'max',
        'min',
        'remove_option',
        'skip',
        'sort',
        'where',
    ],
)
@with_async_methods(
    '__cursor',
    [
        'distinct',
        'close',
    ],
)
class AsyncCursor:
    def __init__(self, cursor: MongoMockCursor) -> None:
        self.__cursor = cursor

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__cursor, name)

    def __aiter__(self) -> Self:
        return self

    async def next(self) -> Any:
        try:
            return next(self.__cursor)
        except StopIteration:
            raise StopAsyncIteration()

    __anext__ = next

    def clone(self) -> 'AsyncCursor':
        return AsyncCursor(self.__cursor.clone())

    async def to_list(self, *args, **kwargs) -> List:
        return list(self.__cursor)


@masquerade_class('motor.motor_asyncio.AsyncIOMotorCommandCursor')
@with_cursor_chaining_methods(
    '__cursor',
    [
        'batch_size',
    ],
)
@with_async_methods(
    '__cursor',
    [
        'close',
    ],
)
class AsyncCommandCursor:
    def __init__(self, cursor: MongoMockCommandCursor) -> None:
        self.__cursor = cursor

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__cursor, name)

    def __aiter__(self) -> Self:
        return self

    async def next(self) -> DocumentType:
        try:
            return next(self.__cursor)
        except StopIteration:
            raise StopAsyncIteration()

    __anext__ = next

    async def to_list(self, *args, **kwargs) -> List[DocumentType]:
        return list(self.__cursor)


@masquerade_class('motor.motor_asyncio.AsyncIOMotorLatentCommandCursor')
@with_async_methods(
    '__cursor',
    [
        'close',
    ],
)
class AsyncLatentCommandCursor:
    def __init__(self, cursor: MongoMockCommandCursor) -> None:
        self.__cursor = cursor

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__cursor, name)

    def __aiter__(self) -> Self:
        return self

    async def next(self) -> DocumentType:
        try:
            return next(self.__cursor)
        except StopIteration:
            raise StopAsyncIteration()

    __anext__ = next

    async def to_list(self, *args, **kwargs) -> List[DocumentType]:
        return list(self.__cursor)


@masquerade_class('motor.motor_asyncio.AsyncIOMotorCollection')
@with_async_methods(
    '__collection',
    [
        'bulk_write',
        'count_documents',
        'count',  # deprecated
        'create_index',
        'create_indexes',
        'delete_many',
        'delete_one',
        'drop_index',
        'drop_indexes',
        'drop',
        'distinct',
        'ensure_index',
        'estimated_document_count',
        'find_and_modify',  # deprecated
        'find_one_and_delete',
        'find_one_and_replace',
        'find_one_and_update',
        'find_one',
        'index_information',
        'inline_map_reduce',
        'insert_many',
        'insert_one',
        'map_reduce',
        'options',
        'reindex',
        'rename',
        'replace_one',
        'save',
        'update_many',
        'update_one',
    ],
)
class AsyncMongoMockCollection:
    def __init__(
        self, database: 'AsyncMongoMockDatabase', collection: MongoMockCollection
    ) -> None:
        self.database = database
        self.__collection = _patch_collection_internals(collection)

    def get_io_loop(self) -> AbstractEventLoop:
        return self.database.get_io_loop()

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AsyncMongoMockCollection):
            return NotImplemented
        return self.__collection == other.__collection

    def __getattr__(self, name: str) -> Any:
        return getattr(self.__collection, name)

    def __hash__(self) -> int:
        return hash(self.__collection)

    def find(self, *args, **kwargs) -> AsyncCursor:
        return AsyncCursor(self.__collection.find(*args, **kwargs))

    def aggregate(self, *args, **kwargs) -> AsyncLatentCommandCursor:
        return AsyncLatentCommandCursor(self.__collection.aggregate(*args, **kwargs))

    def list_indexes(self, *args, **kwargs) -> AsyncCommandCursor:
        return AsyncCommandCursor(
            MongoMockCommandCursor(
                list(self.__collection.list_indexes(*args, **kwargs))
            )
        )


@masquerade_class('motor.motor_asyncio.AsyncIOMotorDatabase')
@with_async_methods(
    '__database',
    [
        'create_collection',
        'dereference',
        'drop_collection',
        'list_collection_names',
        'validate_collection',
    ],
)
class AsyncMongoMockDatabase:
    def __init__(
        self,
        client: 'AsyncMongoMockClient',
        database: MongoMockDatabase,
        mock_build_info: Optional[BuildInfo] = None,
    ) -> None:
        self.client = client
        self.__database = database
        self.__build_info = mock_build_info or {
            'ok': 1.0,
            'version': '5.0.5',
            'versionArray': [5, 0, 5],
        }

    @property
    def delegate(self) -> MongoMockDatabase:
        return self.__database

    def get_io_loop(self) -> AbstractEventLoop:
        return self.client.get_io_loop()

    def get_collection(self, *args, **kwargs) -> AsyncMongoMockCollection:
        return AsyncMongoMockCollection(
            self,
            self.__database.get_collection(*args, **kwargs),
        )

    def aggregate(self, *args, **kwargs) -> AsyncLatentCommandCursor:
        return AsyncLatentCommandCursor(self.__database.aggregate(*args, **kwargs))

    async def command(self, *args, **kwargs) -> Union[DocumentType, BuildInfo]:
        try:
            return getattr(self.__database, 'command')(*args, **kwargs)
        except NotImplementedError:
            if not args:
                raise
            if isinstance(args[0], str) and args[0].lower() == 'buildinfo':
                return self.__build_info
            if (
                isinstance(args[0], dict)
                and args[0]
                and list(args[0])[0].lower() == 'buildinfo'
            ):
                return self.__build_info
            raise

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AsyncMongoMockDatabase):
            return NotImplemented
        return self.__database == other.__database

    def __getitem__(self, name: str) -> AsyncMongoMockCollection:
        return self.get_collection(name)

    def __getattr__(self, name: str) -> Union[AsyncMongoMockCollection, Any]:
        if name in dir(self.__database):
            return getattr(self.__database, name)

        return self.get_collection(name)

    def __hash__(self) -> int:
        return hash(self.__database)


@masquerade_class('motor.motor_asyncio.AsyncIOMotorClient')
@with_async_methods(
    '__client',
    [
        'drop_database',
        'list_database_names',
        'list_databases',
        'server_info',
    ],
)
class AsyncMongoMockClient:
    def __init__(
        self,
        *args,
        mock_build_info: Optional[BuildInfo] = None,
        mock_mongo_client: Optional[MongoMockMongoClient] = None,
        mock_io_loop: Optional[AbstractEventLoop] = None,
        **kwargs,
    ) -> None:
        self.__client = _patch_client_internals(
            mock_mongo_client or MongoMockMongoClient(*args, **kwargs)
        )
        self.__build_info = mock_build_info
        self.__io_loop = mock_io_loop

    def get_io_loop(self) -> AbstractEventLoop:
        return self.__io_loop or asyncio.get_event_loop()

    def get_database(self, *args, **kwargs) -> AsyncMongoMockDatabase:
        return AsyncMongoMockDatabase(
            self,
            self.__client.get_database(*args, **kwargs),
            mock_build_info=self.__build_info,
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AsyncMongoMockClient):
            return NotImplemented
        return self.__client == other.__client

    def __getitem__(self, name) -> AsyncMongoMockDatabase:
        return self.get_database(name)

    def __getattr__(self, name: str) -> Union[Any, AsyncMongoMockDatabase]:
        if name in dir(self.__client):
            return getattr(self.__client, name)

        return self.get_database(name)

    def __hash__(self) -> int:
        return hash(self.__client)


@contextmanager
def enabled_gridfs_integration():
    Database = (PyMongoDatabase, MongoMockDatabase)
    Collection = (PyMongoDatabase, MongoMockCollection)

    with ExitStack() as stack:
        try:
            stack.enter_context(
                patch(
                    'gridfs.synchronous.grid_file.Database',
                    Database,
                )
            )
            stack.enter_context(
                patch(
                    'gridfs.synchronous.grid_file.Collection',
                    Collection,
                )
            )
            stack.enter_context(
                patch(
                    'gridfs.synchronous.grid_file.GridOutCursor',
                    _create_grid_out_cursor,
                )
            )
        except (AttributeError, ModuleNotFoundError):
            stack.enter_context(
                patch(
                    'gridfs.Database',
                    Database,
                )
            )
            stack.enter_context(
                patch(
                    'gridfs.grid_file.Collection',
                    Collection,
                )
            )
            stack.enter_context(
                patch(
                    'gridfs.GridOutCursor',
                    _create_grid_out_cursor,
                )
            )

        yield
