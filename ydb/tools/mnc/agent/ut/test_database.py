import os
import tempfile
import unittest
from unittest import mock

from ydb.tools.mnc.agent import config
from ydb.tools.mnc.agent.services import database as database_module
from ydb.tools.mnc.agent.services.database import DatabaseService
from ydb.tools.mnc.agent.services.nodes import NodeServicePersistent


class DatabaseServiceTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.service = DatabaseService(db_path=":memory:")
        await self.service.connect()

    async def asyncTearDown(self):
        await self.service.disconnect()

    async def test_execute_before_connect_raises(self):
        fresh = DatabaseService(db_path=":memory:")
        with self.assertRaises(RuntimeError):
            await fresh.execute("CREATE TABLE t (x INTEGER)")
        with self.assertRaises(RuntimeError):
            await fresh.fetch_one("SELECT 1")
        with self.assertRaises(RuntimeError):
            await fresh.fetch_all("SELECT 1")
        with self.assertRaises(RuntimeError):
            await fresh.commit()
        with self.assertRaises(RuntimeError):
            await fresh.rollback()

    async def test_execute_and_fetch_round_trip(self):
        await self.service.execute("CREATE TABLE t (x INTEGER, y TEXT)")
        await self.service.execute("INSERT INTO t VALUES (?, ?)", (1, "a"))
        await self.service.execute("INSERT INTO t VALUES (?, ?)", (2, "b"))
        await self.service.commit()

        row = await self.service.fetch_one("SELECT x, y FROM t WHERE x = ?", (1,))
        self.assertEqual(row, (1, "a"))

        rows = await self.service.fetch_all("SELECT x, y FROM t ORDER BY x")
        self.assertEqual(rows, [(1, "a"), (2, "b")])

    async def test_fetch_one_returns_none_for_missing_row(self):
        await self.service.execute("CREATE TABLE t (x INTEGER)")
        await self.service.commit()
        self.assertIsNone(await self.service.fetch_one("SELECT x FROM t WHERE x = ?", (42,)))

    async def test_execute_many_inserts_all_rows(self):
        await self.service.execute("CREATE TABLE t (x INTEGER)")
        await self.service.execute_many("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
        await self.service.commit()
        rows = await self.service.fetch_all("SELECT x FROM t ORDER BY x")
        self.assertEqual(rows, [(1,), (2,), (3,)])

    async def test_rollback_discards_uncommitted_changes(self):
        await self.service.execute("CREATE TABLE t (x INTEGER)")
        await self.service.commit()
        await self.service.execute("INSERT INTO t VALUES (?)", (1,))
        await self.service.rollback()
        rows = await self.service.fetch_all("SELECT x FROM t")
        self.assertEqual(rows, [])

    async def test_connect_runs_init_tasks_in_order(self):
        order = []
        service = DatabaseService(db_path=":memory:")

        async def first():
            order.append("first")

        async def second():
            order.append("second")

        service.add_init_task(first())
        service.add_init_task(second())
        await service.connect()
        try:
            self.assertEqual(order, ["first", "second"])
        finally:
            await service.disconnect()

    async def test_disconnect_is_idempotent(self):
        service = DatabaseService(db_path=":memory:")
        await service.connect()
        await service.disconnect()
        # Second disconnect should not raise.
        await service.disconnect()
        self.assertIsNone(service.connection)

    async def test_default_db_path_uses_mnc_home(self):
        old_home = config.mnc_home
        with tempfile.TemporaryDirectory() as tmp:
            config.mnc_home = tmp
            try:
                service = DatabaseService()
                await service.connect()
                try:
                    self.assertEqual(service.db_path, os.path.join(tmp, "mnc_agent.db"))
                    self.assertTrue(os.path.exists(service.db_path))
                finally:
                    await service.disconnect()
            finally:
                config.mnc_home = old_home

    async def test_connect_propagates_sqlite_errors(self):
        service = DatabaseService(db_path=":memory:")
        with mock.patch.object(database_module.sqlite3, "connect", side_effect=RuntimeError("boom")):
            with self.assertRaises(RuntimeError):
                await service.connect()
        self.assertIsNone(service.connection)


class NodeServicePersistentTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.db = DatabaseService(db_path=":memory:")
        await self.db.connect()
        self.persistent = NodeServicePersistent(self.db)
        await self.persistent.init_tables()

    async def asyncTearDown(self):
        await self.db.disconnect()

    async def test_init_tables_creates_node_info_with_enabled_column(self):
        rows = await self.db.fetch_all("SELECT name FROM pragma_table_info('node_info')")
        names = {row[0] for row in rows}
        self.assertIn("node", names)
        self.assertIn("pid", names)
        self.assertIn("enabled", names)
        self.assertIn("updated_at", names)

    async def test_init_tables_is_idempotent(self):
        await self.persistent.init_tables()
        await self.persistent.init_tables()
        # Should still have a single row count after re-init.
        await self.persistent.save_node_info("n1", 42, True)
        rows = await self.db.fetch_all("SELECT node FROM node_info")
        self.assertEqual(rows, [("n1",)])

    async def test_save_and_load_node_info_round_trip(self):
        await self.persistent.save_node_info("n1", 123, True)
        loaded = await self.persistent.load_node_info("n1")
        self.assertIsNotNone(loaded)
        self.assertEqual(loaded.node, "n1")
        self.assertEqual(loaded.pid, 123)
        self.assertTrue(loaded.enabled)

    async def test_save_node_info_upserts(self):
        await self.persistent.save_node_info("n1", 1, True)
        await self.persistent.save_node_info("n1", 2, False)
        loaded = await self.persistent.load_node_info("n1")
        self.assertEqual(loaded.pid, 2)
        self.assertFalse(loaded.enabled)

    async def test_load_node_info_returns_none_for_missing(self):
        self.assertIsNone(await self.persistent.load_node_info("missing"))

    async def test_load_all_node_info_returns_all_rows(self):
        await self.persistent.save_node_info("n1", 1, True)
        await self.persistent.save_node_info("n2", None, False)
        loaded = await self.persistent.load_all_node_info()
        by_node = {info.node: info for info in loaded}
        self.assertEqual(set(by_node), {"n1", "n2"})
        self.assertEqual(by_node["n1"].pid, 1)
        self.assertTrue(by_node["n1"].enabled)
        self.assertIsNone(by_node["n2"].pid)
        self.assertFalse(by_node["n2"].enabled)

    async def test_set_and_get_node_enabled(self):
        await self.persistent.save_node_info("n1", 1, True)
        await self.persistent.set_node_enabled("n1", False)
        self.assertFalse(await self.persistent.get_node_enabled("n1"))
        await self.persistent.set_node_enabled("n1", True)
        self.assertTrue(await self.persistent.get_node_enabled("n1"))

    async def test_get_node_enabled_defaults_to_true_for_unknown_node(self):
        self.assertTrue(await self.persistent.get_node_enabled("unknown"))

    async def test_delete_node_info_removes_only_target(self):
        await self.persistent.save_node_info("n1", 1, True)
        await self.persistent.save_node_info("n2", 2, True)
        await self.persistent.delete_node_info("n1")
        loaded = await self.persistent.load_all_node_info()
        self.assertEqual([info.node for info in loaded], ["n2"])

    async def test_clear_all_node_info_removes_everything(self):
        await self.persistent.save_node_info("n1", 1, True)
        await self.persistent.save_node_info("n2", 2, True)
        await self.persistent.clear_all_node_info()
        self.assertEqual(await self.persistent.load_all_node_info(), [])

    async def test_migration_adds_enabled_column_when_missing(self):
        legacy_db = DatabaseService(db_path=":memory:")
        await legacy_db.connect()
        try:
            await legacy_db.execute(
                "CREATE TABLE node_info (node TEXT PRIMARY KEY, pid INTEGER, updated_at TIMESTAMP)"
            )
            await legacy_db.commit()
            persistent = NodeServicePersistent(legacy_db)
            await persistent.init_tables()
            rows = await legacy_db.fetch_all("SELECT name FROM pragma_table_info('node_info')")
            names = {row[0] for row in rows}
            self.assertIn("enabled", names)
        finally:
            await legacy_db.disconnect()

    async def test_migration_failure_propagates(self):
        async def boom(*args, **kwargs):
            raise RuntimeError("migration failed")

        with mock.patch.object(self.db, "fetch_one", boom):
            with self.assertRaises(RuntimeError):
                await self.persistent._run_migrations()
