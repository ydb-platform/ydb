import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from ydb.tests.oss.ydb_sdk_import import ydb


class TestAsyncSessionPoolKeepAlive(unittest.IsolatedAsyncioTestCase):
    async def test_failed_keep_alive_releases_capacity(self):
        for error in (ydb.BadSession, ydb.ConnectionLost, asyncio.CancelledError):
            with self.subTest(error=error.__name__):
                await self.check_failed_keep_alive(error)

    async def check_failed_keep_alive(self, error):
        async def create_session(session, settings=None):
            session._state.set_id("test-session-{}".format(id(session)))
            return session

        async def delete_session(session, settings=None):
            session.reset()

        driver = SimpleNamespace(wait=AsyncMock())
        driver.table_client = ydb.aio.table.TableClient(driver)
        with patch.object(ydb.aio.table.Session, "create", create_session), \
                patch.object(ydb.aio.table.Session, "delete", delete_session):
            pool = ydb.aio.SessionPool(driver, 1)
            try:
                session = await pool.acquire(timeout=1)
                deletions = []

                async def fail_keep_alive(settings):
                    raise error("KeepAlive failed")

                async def record_delete(settings):
                    deletions.append(settings)
                    session.reset()

                session.keep_alive = fail_keep_alive
                session.delete = record_delete
                await asyncio.gather(pool._send_keep_alive(session), return_exceptions=True)

                self.assertEqual(deletions, [pool._req_settings])
                self.assertFalse(session.initialized())
                self.assertEqual(pool._active_count, 0)
                self.assertTrue(pool._active_queue.empty())

                replacement = await pool.acquire(timeout=1)
                self.assertIsNot(replacement, session)
                await pool.release(replacement)
            finally:
                await pool.stop()
