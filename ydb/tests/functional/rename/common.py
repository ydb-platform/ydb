# -*- coding: utf-8 -*-
import logging
import six
import asyncio

from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


robust_retries = ydb.RetrySettings() \
    .with_fast_backoff(ydb.BackoffSettings(ceiling=10, slot_duration=0.05, uncertain_ratio=0.1)) \
    .with_slow_backoff(ydb.BackoffSettings(ceiling=10, slot_duration=1.00, uncertain_ratio=0.2))


async def async_retry_operation(callee, retry_settings=None, *args, **kwargs):
    opt_generator = ydb.retry_operation_impl(
        callee, retry_settings, *args, **kwargs)
    for next_opt in opt_generator:
        if isinstance(next_opt, ydb.YdbRetryOperationSleepOpt):
            await asyncio.sleep(next_opt.timeout)
        else:
            try:
                return await next_opt.result
            except ydb.GenericError as e:
                if "Cannot find table" in e.message:
                    next_opt.set_exception(ydb.Unavailable(e.message))
            except Exception as e:
                next_opt.set_exception(e)


async def async_execute_serializable_job(pool: ydb.aio.SessionPool, query, parameters):
    async def calle(pool, query, parameters):
        async with pool.checkout() as session:
            prepared_query = await session.prepare(query)
            with session.transaction(ydb.SerializableReadWrite()) as tx:
                result = await tx.execute(prepared_query, parameters=parameters, commit_tx=True)
                return result
    return await async_retry_operation(calle, robust_retries, pool, query, parameters)


async def async_execute_stale_ro_job(pool: ydb.aio.SessionPool, query, parameters):
    async def calle(pool, query, parameters):
        async with pool.checkout() as session:
            prepared_query = await session.prepare(query)
            with session.transaction(ydb.StaleReadOnly()) as tx:
                result = await tx.execute(
                    prepared_query,
                    parameters=parameters,
                    commit_tx=True
                )
                return result
    return await async_retry_operation(calle, robust_retries, pool, query, parameters)


async def async_scheme_job(pool: ydb.SessionPool, query):
    async def calle(pool, query):
        async with pool.checkout() as session:
            result = await session.execute_scheme(query)
            return result
    return await async_retry_operation(calle, robust_retries, pool, query)


async def async_repeat_n_times(calle, count, *args, **kwargs):
    for _ in six.moves.range(count + 1):
        await calle(*args, **kwargs)
