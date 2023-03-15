# -*- coding: utf-8 -*-
import logging
import os
import asyncio

import pytest

from ydb.tests.oss.ydb_sdk_import import ydb
from ydb.tests.library.common.types import Erasure, from_bytes
from ydb.tests.library.harness.util import LogLevels

from common import (
    async_execute_serializable_job,
    async_execute_stale_ro_job,
    async_scheme_job,
    async_repeat_n_times,
)

logger = logging.getLogger(__name__)

SIMPLE_TABLE_TEMPLATE = (r"""
CREATE TABLE `{table}` (
    `id` Uint64,
    `value` Utf8,
    PRIMARY KEY (`id`)
)
""")

INDEXED_TABLE_TEMPLATE = (r"""
CREATE TABLE `{table}` (
    `id` Uint64,
    `value` Utf8,
    PRIMARY KEY (`id`),
    INDEX value_index GLOBAL ON (`value`)
)
""")

INDEXED_ASYNC_TABLE_TEMPLATE = (r"""
CREATE TABLE `{table}` (
    `id` Uint64,
    `value` Utf8,
    PRIMARY KEY (`id`),
    INDEX value_index GLOBAL ASYNC ON (`value`)
)
""")

DROP_TABLE_TEMPLATE = (r"""
DROP TABLE `{table}`
""")


async def create_simple_table(pool, path):
    await async_scheme_job(pool, SIMPLE_TABLE_TEMPLATE.format(table=path))


async def create_indexed_table(pool, path):
    await async_scheme_job(pool, INDEXED_TABLE_TEMPLATE.format(table=path))


async def create_indexed_async_table(pool, path):
    await async_scheme_job(pool, INDEXED_ASYNC_TABLE_TEMPLATE.format(table=path))


async def replace_table(pool, src, dst, *args):
    async with pool.checkout() as session:
        await session.rename_tables(
            [
                ydb.table.RenameItem(
                    source_path=src,
                    destination_path=dst,
                    replace_destination=True,
                )
            ]
        )


async def substitute_table(pool, src, dst, backup):
    try:
        await async_scheme_job(pool, DROP_TABLE_TEMPLATE.format(table=backup))
    except ydb.issues.SchemeError:
        pass

    with pool.async_checkout() as async_session:
        session = await async_session
        await session.rename_tables(
            [
                ydb.table.RenameItem(
                    source_path=dst,
                    destination_path=backup,
                    replace_destination=False,
                ),
                ydb.table.RenameItem(
                    source_path=src,
                    destination_path=dst,
                    replace_destination=False,
                )
            ]
        )


class Simple:
    def __init__(self, driver_configs, create_method, replace_method, select_from_index=False):
        self._driver_configs = driver_configs
        self._database = driver_configs.database

        self._create_method = create_method
        self._replace_method = replace_method

        self._select_from_index = select_from_index

    async def driver_init_async(self):
        self._driver = ydb.aio.Driver(self._driver_configs)
        await self._driver.wait(timeout=15)
        self._pool = ydb.aio.SessionPool(driver=self._driver, size=10)

    upsert_table_template = (
        r"""
        DECLARE $key AS Uint64;
        DECLARE $value AS Utf8;

        UPSERT INTO `{table}`
        (id, value)
        VALUES ($key, $value);
        """
    )

    select_table_template = (
        r"""
        DECLARE $key AS Uint64;
        SELECT value FROM `{table}` WHERE id = $key;
        """
    )

    select_index_table_template = (
        r"""
        DECLARE $value AS Utf8;
        SELECT id FROM `{table}` VIEW `value_index` WHERE value = $value;
        """
    )

    tables = ["table_main", "table_tmp", "table_backup"]

    async def prepare(self):
        await self.async_prepare("table_main")

    @staticmethod
    def _value_for_table_row(table, key):
        if key % 2 == 0:
            return from_bytes("{table}-{idx}".format(table=table, idx=key))
        else:
            return from_bytes("value-{idx}".format(idx=key))

    async def async_prepare(self, table):
        logger.info("begin")

        await self._create_method(self._pool, table)
        logger.info("create_method done")

        coros = []
        query = self.upsert_table_template.format(table=table)
        for idx in range(30):
            parameters = {
                '$key': idx,
                '$value': self._value_for_table_row(table, idx)
            }
            coros.append(asyncio.create_task(async_execute_serializable_job(self._pool, query, parameters)))

        logger.info("wait coros")
        await asyncio.wait(coros)

    async def move(self):
        await self.async_prepare("table_tmp")

        coros = []

        write_query = self.upsert_table_template.format(table="table_main")
        for idx in range(20):
            parameters = {
                '$key': idx,
                '$value': self._value_for_table_row("table_main", idx)
            }
            coros.append(asyncio.create_task(async_repeat_n_times(async_execute_serializable_job, 20, self._pool, write_query, parameters)))

        read_query = self.select_table_template.format(table="table_main")
        for idx in range(20):
            parameters = {
                '$key': 10+idx,
            }
            coros.append(asyncio.create_task(async_repeat_n_times(async_execute_serializable_job, 15, self._pool, read_query, parameters)))

        if self._select_from_index:
            read_index_query = self.select_index_table_template.format(table="table_main")
            for idx in range(20):
                parameters = {
                    '$value': self._value_for_table_row("table_main", idx)
                }
                coros.append(asyncio.create_task(async_repeat_n_times(async_execute_stale_ro_job, 15, self._pool, read_index_query, parameters)))

        coros.append(
            asyncio.create_task(
                self._replace_method(
                    self._pool,
                    os.path.join(self._database, "table_tmp"),
                    os.path.join(self._database, "table_main"),
                    os.path.join(self._database, "table_backup")
                )
            )
        )

        async def calle():
            await asyncio.wait(coros)

        await calle()


# local configuration for the ydb cluster (fetched by ydb_cluster_configuration fixture)
CLUSTER_CONFIG = dict(
    erasure=Erasure.NONE,
    nodes=3,
    n_to_select=1,
    additional_log_configs={
        'FLAT_TX_SCHEMESHARD': LogLevels.DEBUG,
        'SCHEME_BOARD_POPULATOR': LogLevels.WARN,
        'SCHEME_BOARD_SUBSCRIBER': LogLevels.WARN,
        'TX_DATASHARD': LogLevels.DEBUG,
        'CHANGE_EXCHANGE': LogLevels.DEBUG,
    },
)


@pytest.mark.parametrize("create_method,select_from_index", [
    (create_simple_table, False),
    (create_indexed_table, True),
    (create_indexed_async_table, True),
])
@pytest.mark.parametrize("replace_method", [
    replace_table,
    substitute_table,
])
def test_client_gets_retriable_errors_when_rename(create_method, select_from_index, replace_method, ydb_database, ydb_endpoint):
    async def test():
        database = ydb_database
        logger.info(" database is %s", database)

        driver_configs = ydb.DriverConfig(
            ydb_endpoint,
            database
        )

        scenario = Simple(driver_configs, create_method, replace_method, select_from_index)
        await scenario.driver_init_async()

        logger.info(" database is %s: PREPARE", database)
        await scenario.prepare()

        logger.info(" database is %s: MOVE 1", database)
        await scenario.move()

        logger.info(" database is %s: MOVE 2", database)
        await scenario.move()

    asyncio.run(test())
