# -*- coding: utf-8 -*-
import pytest
from ydb.tests.library.serializability.checker import SerializabilityError, SerializabilityChecker
from ydb.tests.tools.ydb_serializable.lib import DatabaseChecker, DatabaseCheckerOptions


def test_order_conflict():
    checker = SerializabilityChecker()
    checker.prepare_keys(4)

    # w1 writes to keys 1 and 2
    w1 = checker.new_node()
    checker.ensure_write_value(w1, 1)
    checker.ensure_write_value(w1, 2)

    # w2 writes to keys 2 and 3
    w2 = checker.new_node()
    checker.ensure_write_value(w2, 2)
    checker.ensure_write_value(w2, 3)

    # r1 shows that w1 happens before w2
    r1 = checker.new_node()
    checker.ensure_read_value(r1, 1, w1.value)
    checker.ensure_read_value(r1, 2, w2.value)

    # r2 shows that w2 happens before w1
    r2 = checker.new_node()
    checker.ensure_read_value(r2, 2, w1.value)
    checker.ensure_read_value(r2, 3, w2.value)

    # commit everything except r2
    checker.commit(w1)
    checker.commit(w2)
    checker.commit(r1)

    # without r2 in committed set verify must succeed
    checker.verify()

    # commit r2
    checker.commit(r2)

    # with r2 committed checker must find a contradiction
    with pytest.raises(SerializabilityError):
        checker.verify()


def test_missing_value():
    checker = SerializabilityChecker()
    checker.prepare_keys(2)

    # w1 writes to keys 1 and 2
    w1 = checker.new_node()
    checker.ensure_write_value(w1, 1)
    checker.ensure_write_value(w1, 2)

    # r1 reads from keys 1 and 2
    r1 = checker.new_node()
    checker.ensure_read_value(r1, 1, 0)
    checker.ensure_read_value(r1, 2, w1.value)

    # commit both w1 and r1
    checker.commit(w1)
    checker.commit(r1)

    # verification must fail (read missed correct value for key 1)
    with pytest.raises(SerializabilityError):
        checker.verify()


def test_unexpected_value():
    checker = SerializabilityChecker()
    checker.prepare_keys(2)

    # w1 writes to keys 1 and 2
    w1 = checker.new_node()
    checker.ensure_write_value(w1, 1)
    checker.ensure_write_value(w1, 2)

    # r1 reads from key 1
    r1 = checker.new_node()
    checker.ensure_read_value(r1, 1, w1.value)

    # abort w1, but commit r1
    checker.abort(w1)
    checker.commit(r1)

    # verification must fail (reading a value that has not been committed)
    with pytest.raises(SerializabilityError):
        checker.verify()


def test_local():
    with open('ydb_endpoint.txt') as r:
        endpoint = r.read()

    with open('ydb_database.txt') as r:
        database = r.read()

    async def async_wrapper():
        async with DatabaseChecker(endpoint, database) as checker:
            options = DatabaseCheckerOptions()

            options.read_table_ranges = False
            await checker.async_run(options)

            options.read_table_ranges = True
            await checker.async_run(options)

    import asyncio
    asyncio.run(async_wrapper())
