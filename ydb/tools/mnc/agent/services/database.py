import aiosqlite
import logging
from typing import Optional, Coroutine
from pathlib import Path
from ydb.tools.mnc.agent import config


class DatabaseService:
    """Simple async SQLite wrapper service."""

    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path or str(Path(config.mnc_home) / "mnc_agent.db")
        self.connection: Optional[aiosqlite.Connection] = None
        self.logger = logging.getLogger(__name__)
        self._init_tasks = []

    def add_init_task(self, task: Coroutine):
        self._init_tasks.append(task)

    async def connect(self):
        """Connect to the database."""
        try:
            self.connection = await aiosqlite.connect(self.db_path)
            self.logger.info(f"Connected to database: {self.db_path}")
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            raise
        for task in self._init_tasks:
            await task

    async def disconnect(self):
        """Disconnect from the database."""
        if self.connection:
            await self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from database")

    async def execute(self, sql: str, parameters: tuple = ()) -> aiosqlite.Cursor:
        """Execute SQL query."""
        if not self.connection:
            raise RuntimeError("Database not connected")

        try:
            cursor = await self.connection.execute(sql, parameters)
            return cursor
        except Exception as e:
            self.logger.error(f"Database execution error: {e}")
            raise

    async def execute_many(self, sql: str, parameters_list: list) -> aiosqlite.Cursor:
        """Execute SQL query with multiple parameter sets."""
        if not self.connection:
            raise RuntimeError("Database not connected")

        try:
            cursor = await self.connection.executemany(sql, parameters_list)
            return cursor
        except Exception as e:
            self.logger.error(f"Database execution error: {e}")
            raise

    async def fetch_one(self, sql: str, parameters: tuple = ()) -> Optional[tuple]:
        """Fetch one row from database."""
        cursor = await self.execute(sql, parameters)
        row = await cursor.fetchone()
        await cursor.close()
        return row

    async def fetch_all(self, sql: str, parameters: tuple = ()) -> list:
        """Fetch all rows from database."""
        cursor = await self.execute(sql, parameters)
        rows = await cursor.fetchall()
        await cursor.close()
        return rows

    async def commit(self):
        """Commit current transaction."""
        if self.connection:
            await self.connection.commit()

    async def rollback(self):
        """Rollback current transaction."""
        if self.connection:
            await self.connection.rollback()


# Global database service instance
database_service = DatabaseService()
