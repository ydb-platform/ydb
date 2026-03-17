import asyncio
from typing import Any, Callable, Coroutine
from uuid import uuid4

from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    AsyncEngine,
    AsyncSession,
)

EngineCreatorFunc = Callable[[str], AsyncEngine]
SessionMakerCreatorFunc = Callable[
    [AsyncEngine], async_sessionmaker[AsyncSession]
]
AsyncFunc = Callable[["DBConnect"], Coroutine[Any, Any, None]]


class DBConnect:
    """stores the database connection parameters"""

    def __init__(
        self,
        engine_creator: EngineCreatorFunc,
        session_maker_creator: SessionMakerCreatorFunc,
        host: str | None = None,
        before_create_session_handler: AsyncFunc | None = None,
    ) -> None:
        """
        engine_creator: Specify a function that will return the
            configured AsyncEngine

        session_maker_creator: Specify a function that will return the
            configured async_sessionmaker

        host: Specify the host to connect to. The connection is lazy.

        before_create_session_handler: You can specify a handler function that
            will be triggered before attempting to create a new session.
            You can, for example, check whether the host is alive and that
            it's the master and call change_host to change the host if
            necessary.
        """
        self.context_key = str(uuid4())

        self.host = host
        self._engine_creator = engine_creator
        self._session_maker_creator = session_maker_creator
        self._before_create_session_handler = before_create_session_handler

        self._engine: AsyncEngine | None = None
        self._session_maker: async_sessionmaker[AsyncSession] | None = None
        self._lock = asyncio.Lock()

    async def connect(self, host: str) -> None:
        """initiates engine and session maker"""
        assert host
        async with self._lock:
            await self._connect(host)

    async def change_host(self, host: str) -> None:
        """Renews the connection if a host needs to be changed"""
        assert host
        async with self._lock:
            if host != self.host:
                await self._connect(host)

    async def create_session(self) -> AsyncSession:
        """Creates a new session"""
        maker = await self.session_maker()
        return maker()

    async def session_maker(self) -> async_sessionmaker[AsyncSession]:
        """Gets the session maker"""
        if self._before_create_session_handler:
            await self._before_create_session_handler(self)
        if not self._session_maker:
            assert self.host
            await self.connect(self.host)

        assert self._session_maker
        return self._session_maker

    async def close(self) -> None:
        if self._engine:
            await self._engine.dispose()
        self._engine = None
        self._session_maker = None

    async def _connect(self, host: str) -> None:
        self.host = host
        await self.close()
        self._engine = self._engine_creator(host)
        self._session_maker = self._session_maker_creator(self._engine)
