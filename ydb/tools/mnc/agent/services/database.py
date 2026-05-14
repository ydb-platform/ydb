import asyncio
import logging
import sqlite3
from pathlib import Path
from typing import Optional, Coroutine

from ydb.tools.mnc.agent import config


class DatabaseService:
    def __init__(self, db_path: Optional[str] = None):
        self.db_path = db_path
        self.connection: Optional[sqlite3.Connection] = None
        self.logger = logging.getLogger(__name__)
        self._init_tasks: list[Coroutine] = []
        self._lock = asyncio.Lock()

    def add_init_task(self, task: Coroutine):
        self._init_tasks.append(task)

    def _ensure_connection(self) -> sqlite3.Connection:
        if not self.connection:
            raise RuntimeError("Database not connected")
        return self.connection

    async def connect(self):
        try:
            if self.db_path is None:
                self.db_path = str(Path(config.mnc_home) / "mnc_agent.db")
            self.connection = sqlite3.connect(self.db_path, check_same_thread=False)
            self.connection.row_factory = sqlite3.Row
            self.logger.info("Connected to database: %s", self.db_path)
        except Exception as exc:
            self.logger.error("Failed to connect to database: %s", exc)
            raise
        for task in self._init_tasks:
            await task

    async def disconnect(self):
        if self.connection:
            self.connection.close()
            self.connection = None
            self.logger.info("Disconnected from database")

    async def execute(self, sql: str, parameters: tuple = ()) -> None:
        conn = self._ensure_connection()
        async with self._lock:
            await asyncio.to_thread(conn.execute, sql, parameters)

    async def execute_many(self, sql: str, parameters_list: list) -> None:
        conn = self._ensure_connection()
        async with self._lock:
            await asyncio.to_thread(conn.executemany, sql, parameters_list)

    async def fetch_one(self, sql: str, parameters: tuple = ()) -> Optional[tuple]:
        conn = self._ensure_connection()

        def _fetch_one():
            cursor = conn.execute(sql, parameters)
            row = cursor.fetchone()
            cursor.close()
            return tuple(row) if row is not None else None

        async with self._lock:
            return await asyncio.to_thread(_fetch_one)

    async def fetch_all(self, sql: str, parameters: tuple = ()) -> list:
        conn = self._ensure_connection()

        def _fetch_all():
            cursor = conn.execute(sql, parameters)
            rows = cursor.fetchall()
            cursor.close()
            return [tuple(row) for row in rows]

        async with self._lock:
            return await asyncio.to_thread(_fetch_all)

    async def commit(self):
        conn = self._ensure_connection()
        async with self._lock:
            await asyncio.to_thread(conn.commit)

    async def rollback(self):
        conn = self._ensure_connection()
        async with self._lock:
            await asyncio.to_thread(conn.rollback)


database_service = DatabaseService()
