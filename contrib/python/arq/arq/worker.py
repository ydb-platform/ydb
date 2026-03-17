import asyncio
import contextlib
import inspect
import logging
import signal
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from functools import partial
from signal import Signals
from time import time
from typing import TYPE_CHECKING, Any, Callable, Optional, Union, cast

from redis.exceptions import ResponseError, WatchError

from arq.cron import CronJob
from arq.jobs import Deserializer, JobResult, SerializationError, Serializer, deserialize_job_raw, serialize_result

from .connections import ArqRedis, RedisSettings, create_pool, log_redis_info
from .constants import (
    abort_job_max_age,
    abort_jobs_ss,
    default_queue_name,
    expires_extra_ms,
    health_check_key_suffix,
    in_progress_key_prefix,
    job_key_prefix,
    keep_cronjob_progress,
    result_key_prefix,
    retry_key_prefix,
)
from .utils import (
    args_to_string,
    import_string,
    ms_to_datetime,
    poll,
    timestamp_ms,
    to_ms,
    to_seconds,
    to_unix_ms,
    truncate,
)

if TYPE_CHECKING:
    from .typing import SecondsTimedelta, StartupShutdown, WorkerCoroutine, WorkerSettingsType

logger = logging.getLogger('arq.worker')
no_result = object()


@dataclass
class Function:
    name: str
    coroutine: 'WorkerCoroutine'
    timeout_s: Optional[float]
    keep_result_s: Optional[float]
    keep_result_forever: Optional[bool]
    max_tries: Optional[int]


def func(
    coroutine: Union[str, Function, 'WorkerCoroutine'],
    *,
    name: Optional[str] = None,
    keep_result: Optional['SecondsTimedelta'] = None,
    timeout: Optional['SecondsTimedelta'] = None,
    keep_result_forever: Optional[bool] = None,
    max_tries: Optional[int] = None,
) -> Function:
    """
    Wrapper for a job function which lets you configure more settings.

    :param coroutine: coroutine function to call, can be a string to import
    :param name: name for function, if None, ``coroutine.__qualname__`` is used
    :param keep_result: duration to keep the result for, if 0 the result is not kept
    :param keep_result_forever: whether to keep results forever, if None use Worker default, wins over ``keep_result``
    :param timeout: maximum time the job should take
    :param max_tries: maximum number of tries allowed for the function, use 1 to prevent retrying
    """
    if isinstance(coroutine, Function):
        return coroutine

    if isinstance(coroutine, str):
        name = name or coroutine
        coroutine_: WorkerCoroutine = import_string(coroutine)
    else:
        coroutine_ = coroutine

    if not asyncio.iscoroutinefunction(coroutine_):
        raise RuntimeError(f'{coroutine_} is not a coroutine function')
    timeout = to_seconds(timeout)
    keep_result = to_seconds(keep_result)

    return Function(name or coroutine_.__qualname__, coroutine_, timeout, keep_result, keep_result_forever, max_tries)


class Retry(RuntimeError):
    """
    Special exception to retry the job (if ``max_tries`` hasn't been reached).

    :param defer: duration to wait before rerunning the job
    """

    def __init__(self, defer: Optional['SecondsTimedelta'] = None):
        self.defer_score: Optional[int] = to_ms(defer)

    def __repr__(self) -> str:
        return f'<Retry defer {(self.defer_score or 0) / 1000:0.2f}s>'

    def __str__(self) -> str:
        return repr(self)


class JobExecutionFailed(RuntimeError):
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, JobExecutionFailed):
            return self.args == other.args
        return False


class FailedJobs(RuntimeError):
    def __init__(self, count: int, job_results: list[JobResult]):
        self.count = count
        self.job_results = job_results

    def __str__(self) -> str:
        if self.count == 1 and self.job_results:
            exc = self.job_results[0].result
            return f'1 job failed {exc!r}'
        else:
            return f'{self.count} jobs failed:\n' + '\n'.join(repr(r.result) for r in self.job_results)

    def __repr__(self) -> str:
        return f'<{str(self)}>'


class RetryJob(RuntimeError):
    pass


class Worker:
    """
    Main class for running jobs.

    :param functions: list of functions to register, can either be raw coroutine functions or the
      result of :func:`arq.worker.func`.
    :param queue_name: queue name to get jobs from
    :param cron_jobs:  list of cron jobs to run, use :func:`arq.cron.cron` to create them
    :param redis_settings: settings for creating a redis connection
    :param redis_pool: existing redis pool, generally None
    :param burst: whether to stop the worker once all jobs have been run
    :param on_startup: coroutine function to run at startup
    :param on_shutdown: coroutine function to run at shutdown
    :param on_job_start: coroutine function to run on job start
    :param on_job_end: coroutine function to run on job end
    :param after_job_end: coroutine function to run after job has ended and results have been recorded
    :param handle_signals: default true, register signal handlers,
      set to false when running inside other async framework
    :param job_completion_wait: time to wait before cancelling tasks after a signal.
      Useful together with ``terminationGracePeriodSeconds`` in kubernetes,
      when you want to make the pod complete jobs before shutting down.
      The worker will not pick new tasks while waiting for shut down.
    :param max_jobs: maximum number of jobs to run at a time
    :param job_timeout: default job timeout (max run time)
    :param keep_result: default duration to keep job results for
    :param keep_result_forever: whether to keep results forever
    :param poll_delay: duration between polling the queue for new jobs
    :param queue_read_limit: the maximum number of jobs to pull from the queue each time it's polled. By default it
                             equals ``max_jobs`` * 5, or 100; whichever is higher.
    :param max_tries: default maximum number of times to retry a job
    :param health_check_interval: how often to set the health check key
    :param health_check_key: redis key under which health check is set
    :param ctx: dictionary to hold extra user defined state
    :param retry_jobs: whether to retry jobs on Retry or CancelledError or not
    :param allow_abort_jobs: whether to abort jobs on a call to :func:`arq.jobs.Job.abort`
    :param max_burst_jobs: the maximum number of jobs to process in burst mode (disabled with negative values)
    :param job_serializer: a function that serializes Python objects to bytes, defaults to pickle.dumps
    :param job_deserializer: a function that deserializes bytes into Python objects, defaults to pickle.loads
    :param expires_extra_ms: the default length of time from when a job is expected to start
     after which the job expires, defaults to 1 day in ms.
    :param timezone: timezone used for evaluation of cron schedules,
        defaults to system timezone
    :param log_results: when set to true (default) results for successful jobs
      will be logged
    """

    def __init__(
        self,
        functions: Sequence[Union[Function, 'WorkerCoroutine']] = (),
        *,
        queue_name: Optional[str] = default_queue_name,
        cron_jobs: Optional[Sequence[CronJob]] = None,
        redis_settings: Optional[RedisSettings] = None,
        redis_pool: Optional[ArqRedis] = None,
        burst: bool = False,
        on_startup: Optional['StartupShutdown'] = None,
        on_shutdown: Optional['StartupShutdown'] = None,
        on_job_start: Optional['StartupShutdown'] = None,
        on_job_end: Optional['StartupShutdown'] = None,
        after_job_end: Optional['StartupShutdown'] = None,
        handle_signals: bool = True,
        job_completion_wait: int = 0,
        max_jobs: int = 10,
        job_timeout: 'SecondsTimedelta' = 300,
        keep_result: 'SecondsTimedelta' = 3600,
        keep_result_forever: bool = False,
        poll_delay: 'SecondsTimedelta' = 0.5,
        queue_read_limit: Optional[int] = None,
        max_tries: int = 5,
        health_check_interval: 'SecondsTimedelta' = 3600,
        health_check_key: Optional[str] = None,
        ctx: Optional[dict[Any, Any]] = None,
        retry_jobs: bool = True,
        allow_abort_jobs: bool = False,
        max_burst_jobs: int = -1,
        job_serializer: Optional[Serializer] = None,
        job_deserializer: Optional[Deserializer] = None,
        expires_extra_ms: int = expires_extra_ms,
        timezone: Optional[timezone] = None,
        log_results: bool = True,
    ):
        self.functions: dict[str, Union[Function, CronJob]] = {f.name: f for f in map(func, functions)}
        if queue_name is None:
            if redis_pool is not None:
                queue_name = redis_pool.default_queue_name
            else:
                raise ValueError('If queue_name is absent, redis_pool must be present.')
        self.queue_name = queue_name
        self.cron_jobs: list[CronJob] = []
        if cron_jobs is not None:
            if not all(isinstance(cj, CronJob) for cj in cron_jobs):
                raise RuntimeError('cron_jobs, must be instances of CronJob')
            self.cron_jobs = list(cron_jobs)
            self.functions.update({cj.name: cj for cj in self.cron_jobs})
        if len(self.functions) == 0:
            raise RuntimeError('at least one function or cron_job must be registered')
        self.burst = burst
        self.on_startup = on_startup
        self.on_shutdown = on_shutdown
        self.on_job_start = on_job_start
        self.on_job_end = on_job_end
        self.after_job_end = after_job_end

        self.max_jobs = max_jobs
        self.sem = asyncio.BoundedSemaphore(max_jobs + 1)
        self.job_counter: int = 0

        self.job_timeout_s = to_seconds(job_timeout)
        self.keep_result_s = to_seconds(keep_result)
        self.keep_result_forever = keep_result_forever
        self.poll_delay_s = to_seconds(poll_delay)
        self.queue_read_limit = queue_read_limit or max(max_jobs * 5, 100)
        self._queue_read_offset = 0
        self.max_tries = max_tries
        self.health_check_interval = to_seconds(health_check_interval)
        if health_check_key is None:
            self.health_check_key = self.queue_name + health_check_key_suffix
        else:
            self.health_check_key = health_check_key
        self._pool = redis_pool
        if self._pool is None:
            self.redis_settings: Optional[RedisSettings] = redis_settings or RedisSettings()
        else:
            self.redis_settings = None
        # self.tasks holds references to run_job coroutines currently running
        self.tasks: dict[str, asyncio.Task[Any]] = {}
        # self.job_tasks holds references the actual jobs running
        self.job_tasks: dict[str, asyncio.Task[Any]] = {}
        self.main_task: Optional[asyncio.Task[None]] = None
        self.loop = asyncio.get_event_loop()
        self.ctx = ctx or {}
        max_timeout = max(f.timeout_s or self.job_timeout_s for f in self.functions.values())
        self.in_progress_timeout_s = (max_timeout or 0) + 10
        self.jobs_complete = 0
        self.jobs_retried = 0
        self.jobs_failed = 0
        self._last_health_check: float = 0
        self._last_health_check_log: Optional[str] = None
        self._handle_signals = handle_signals
        self._job_completion_wait = job_completion_wait
        if self._handle_signals:
            if self._job_completion_wait:
                self._add_signal_handler(signal.SIGINT, self.handle_sig_wait_for_completion)
                self._add_signal_handler(signal.SIGTERM, self.handle_sig_wait_for_completion)
            else:
                self._add_signal_handler(signal.SIGINT, self.handle_sig)
                self._add_signal_handler(signal.SIGTERM, self.handle_sig)
        self.on_stop: Optional[Callable[[Signals], None]] = None
        # whether or not to retry jobs on Retry and CancelledError
        self.retry_jobs = retry_jobs
        self.allow_abort_jobs = allow_abort_jobs
        self.allow_pick_jobs: bool = True
        self.aborting_tasks: set[str] = set()
        self.max_burst_jobs = max_burst_jobs
        self.job_serializer = job_serializer
        self.job_deserializer = job_deserializer
        self.expires_extra_ms = expires_extra_ms
        self.log_results = log_results

        # default to system timezone
        self.timezone = datetime.now().astimezone().tzinfo if timezone is None else timezone

    def run(self) -> None:
        """
        Sync function to run the worker, finally closes worker connections.
        """
        self.main_task = self.loop.create_task(self.main())
        try:
            self.loop.run_until_complete(self.main_task)
        except asyncio.CancelledError:  # pragma: no cover
            # happens on shutdown, fine
            pass
        finally:
            self.loop.run_until_complete(self.close())

    async def async_run(self) -> None:
        """
        Asynchronously run the worker, does not close connections. Useful when testing.
        """
        self.main_task = self.loop.create_task(self.main())
        await self.main_task

    async def run_check(self, retry_jobs: Optional[bool] = None, max_burst_jobs: Optional[int] = None) -> int:
        """
        Run :func:`arq.worker.Worker.async_run`, check for failed jobs and raise :class:`arq.worker.FailedJobs`
        if any jobs have failed.

        :return: number of completed jobs
        """
        if retry_jobs is not None:
            self.retry_jobs = retry_jobs
        if max_burst_jobs is not None:
            self.max_burst_jobs = max_burst_jobs
        await self.async_run()
        if self.jobs_failed:
            failed_job_results = [r for r in await self.pool.all_job_results() if not r.success]
            raise FailedJobs(self.jobs_failed, failed_job_results)
        else:
            return self.jobs_complete

    @property
    def pool(self) -> ArqRedis:
        return cast(ArqRedis, self._pool)

    async def main(self) -> None:
        if self._pool is None:
            self._pool = await create_pool(
                self.redis_settings,
                job_deserializer=self.job_deserializer,
                job_serializer=self.job_serializer,
                default_queue_name=self.queue_name,
                expires_extra_ms=self.expires_extra_ms,
            )

        logger.info('Starting worker for %d functions: %s', len(self.functions), ', '.join(self.functions))
        await log_redis_info(self.pool, logger.info)
        self.ctx['redis'] = self.pool
        if self.on_startup:
            await self.on_startup(self.ctx)

        async for _ in poll(self.poll_delay_s):
            await self._poll_iteration()

            if self.burst:
                if 0 <= self.max_burst_jobs <= self._jobs_started():
                    await asyncio.gather(*self.tasks.values())
                    return None
                queued_jobs = await self.pool.zcard(self.queue_name)
                if queued_jobs == 0:
                    await asyncio.gather(*self.tasks.values())
                    return None

    async def _poll_iteration(self) -> None:
        """
        Get ids of pending jobs from the main queue sorted-set data structure and start those jobs, remove
        any finished tasks from self.tasks.
        """
        count = self.queue_read_limit
        if self.burst and self.max_burst_jobs >= 0:
            burst_jobs_remaining = self.max_burst_jobs - self._jobs_started()
            if burst_jobs_remaining < 1:
                return
            count = min(burst_jobs_remaining, count)
        if self.allow_pick_jobs:
            if self.job_counter < self.max_jobs:
                now = timestamp_ms()
                job_ids = await self.pool.zrangebyscore(
                    self.queue_name, min=float('-inf'), start=self._queue_read_offset, num=count, max=now
                )

                await self.start_jobs(job_ids)

        if self.allow_abort_jobs:
            await self._cancel_aborted_jobs()

        for job_id, t in list(self.tasks.items()):
            if t.done():
                del self.tasks[job_id]
                # required to make sure errors in run_job get propagated
                t.result()

        await self.heart_beat()

    async def _cancel_aborted_jobs(self) -> None:
        """
        Go through job_ids in the abort_jobs_ss sorted set and cancel those tasks.
        """
        async with self.pool.pipeline(transaction=True) as pipe:
            pipe.zrange(abort_jobs_ss, start=0, end=-1)
            pipe.zremrangebyscore(abort_jobs_ss, min=timestamp_ms() + abort_job_max_age, max=float('inf'))
            abort_job_ids, _ = await pipe.execute()

        aborted: set[str] = set()
        for job_id_bytes in abort_job_ids:
            job_id = job_id_bytes.decode()
            try:
                task = self.job_tasks[job_id]
            except KeyError:
                pass
            else:
                aborted.add(job_id)
                task.cancel()

        if aborted:
            self.aborting_tasks.update(aborted)
            await self.pool.zrem(abort_jobs_ss, *aborted)

    def _release_sem_dec_counter_on_complete(self) -> None:
        self.job_counter = self.job_counter - 1
        self.sem.release()

    async def start_jobs(self, job_ids: list[bytes]) -> None:
        """
        For each job id, get the job definition, check it's not running and start it in a task
        """
        for job_id_b in job_ids:
            await self.sem.acquire()

            if self.job_counter >= self.max_jobs:
                self.sem.release()
                return None

            self.job_counter = self.job_counter + 1

            job_id = job_id_b.decode()
            in_progress_key = in_progress_key_prefix + job_id
            async with self.pool.pipeline(transaction=True) as pipe:
                await pipe.watch(in_progress_key)
                ongoing_exists = await pipe.exists(in_progress_key)
                score = await pipe.zscore(self.queue_name, job_id)
                if ongoing_exists or not score or score > timestamp_ms():
                    # job already started elsewhere, or already finished and removed from queue
                    # if score > ts_now,
                    # it means probably the job was re-enqueued with a delay in another worker
                    self.job_counter = self.job_counter - 1
                    self.sem.release()
                    logger.debug('job %s already running elsewhere', job_id)
                    continue

                pipe.multi()
                pipe.psetex(in_progress_key, int(self.in_progress_timeout_s * 1000), b'1')
                try:
                    await pipe.execute()
                except (ResponseError, WatchError):
                    # job already started elsewhere since we got 'existing'
                    self.job_counter = self.job_counter - 1
                    self.sem.release()
                    logger.debug('multi-exec error, job %s already started elsewhere', job_id)
                else:
                    t = self.loop.create_task(self.run_job(job_id, int(score)))
                    t.add_done_callback(lambda _: self._release_sem_dec_counter_on_complete())
                    self.tasks[job_id] = t

    async def run_job(self, job_id: str, score: int) -> None:  # noqa: C901
        start_ms = timestamp_ms()
        async with self.pool.pipeline(transaction=True) as pipe:
            pipe.get(job_key_prefix + job_id)
            pipe.incr(retry_key_prefix + job_id)
            pipe.expire(retry_key_prefix + job_id, 88400)
            if self.allow_abort_jobs:
                pipe.zrem(abort_jobs_ss, job_id)
                v, job_try, _, abort_job = await pipe.execute()
            else:
                v, job_try, _ = await pipe.execute()
                abort_job = False

        function_name, enqueue_time_ms = '<unknown>', 0
        args: tuple[Any, ...] = ()
        kwargs: dict[Any, Any] = {}

        async def job_failed(exc: BaseException) -> None:
            self.jobs_failed += 1
            result_data_ = serialize_result(
                function=function_name,
                args=args,
                kwargs=kwargs,
                job_try=job_try,
                enqueue_time_ms=enqueue_time_ms,
                success=False,
                result=exc,
                start_ms=start_ms,
                finished_ms=timestamp_ms(),
                ref=f'{job_id}:{function_name}',
                serializer=self.job_serializer,
                queue_name=self.queue_name,
                job_id=job_id,
            )
            await asyncio.shield(self.finish_failed_job(job_id, result_data_))

        if not v:
            logger.warning('job %s expired', job_id)
            return await job_failed(JobExecutionFailed('job expired'))

        try:
            function_name, args, kwargs, enqueue_job_try, enqueue_time_ms = deserialize_job_raw(
                v, deserializer=self.job_deserializer
            )
        except SerializationError as e:
            logger.exception('deserializing job %s failed', job_id)
            return await job_failed(e)

        if abort_job:
            t = (timestamp_ms() - enqueue_time_ms) / 1000
            logger.info('%6.2fs ⊘ %s:%s aborted before start', t, job_id, function_name)
            return await job_failed(asyncio.CancelledError())

        try:
            function: Union[Function, CronJob] = self.functions[function_name]
        except KeyError:
            logger.warning('job %s, function %r not found', job_id, function_name)
            return await job_failed(JobExecutionFailed(f'function {function_name!r} not found'))

        if hasattr(function, 'next_run'):
            # cron_job
            ref = function_name
            keep_in_progress: Optional[float] = keep_cronjob_progress
        else:
            ref = f'{job_id}:{function_name}'
            keep_in_progress = None

        if enqueue_job_try and enqueue_job_try > job_try:
            job_try = enqueue_job_try
            await self.pool.setex(retry_key_prefix + job_id, 88400, str(job_try))

        max_tries = self.max_tries if function.max_tries is None else function.max_tries
        if job_try > max_tries:
            t = (timestamp_ms() - enqueue_time_ms) / 1000
            logger.warning('%6.2fs ! %s max retries %d exceeded', t, ref, max_tries)
            self.jobs_failed += 1
            result_data = serialize_result(
                function_name,
                args,
                kwargs,
                job_try,
                enqueue_time_ms,
                False,
                JobExecutionFailed(f'max {max_tries} retries exceeded'),
                start_ms,
                timestamp_ms(),
                ref,
                self.queue_name,
                job_id=job_id,
                serializer=self.job_serializer,
            )
            return await asyncio.shield(self.finish_failed_job(job_id, result_data))

        result = no_result
        exc_extra = None
        finish = False
        timeout_s = self.job_timeout_s if function.timeout_s is None else function.timeout_s
        incr_score: Optional[int] = None
        job_ctx = {
            'job_id': job_id,
            'job_try': job_try,
            'enqueue_time': ms_to_datetime(enqueue_time_ms),
            'score': score,
        }
        ctx = {**self.ctx, **job_ctx}

        if self.on_job_start:
            await self.on_job_start(ctx)

        start_ms = timestamp_ms()
        success = False
        try:
            s = args_to_string(args, kwargs)
            extra = f' try={job_try}' if job_try > 1 else ''
            if (start_ms - score) > 1200:
                extra += f' delayed={(start_ms - score) / 1000:0.2f}s'
            logger.info('%6.2fs → %s(%s)%s', (start_ms - enqueue_time_ms) / 1000, ref, s, extra)
            self.job_tasks[job_id] = task = self.loop.create_task(function.coroutine(ctx, *args, **kwargs))

            # run repr(result) and extra inside try/except as they can raise exceptions
            try:
                result = await asyncio.wait_for(task, timeout_s)
            except (Exception, asyncio.CancelledError) as e:
                exc_extra = getattr(e, 'extra', None)
                if callable(exc_extra):
                    exc_extra = exc_extra()
                raise
            else:
                result_str = '' if result is None or not self.log_results else truncate(repr(result))
            finally:
                del self.job_tasks[job_id]

        except (Exception, asyncio.CancelledError) as e:
            finished_ms = timestamp_ms()
            t = (finished_ms - start_ms) / 1000
            if self.retry_jobs and isinstance(e, Retry):
                incr_score = e.defer_score
                logger.info('%6.2fs ↻ %s retrying job in %0.2fs', t, ref, (e.defer_score or 0) / 1000)
                if e.defer_score:
                    incr_score = e.defer_score + (timestamp_ms() - score)
                self.jobs_retried += 1
            elif job_id in self.aborting_tasks and isinstance(e, asyncio.CancelledError):
                logger.info('%6.2fs ⊘ %s aborted', t, ref)
                result = e
                finish = True
                self.aborting_tasks.remove(job_id)
                self.jobs_failed += 1
            elif self.retry_jobs and isinstance(e, (asyncio.CancelledError, RetryJob)):
                logger.info('%6.2fs ↻ %s cancelled, will be run again', t, ref)
                self.jobs_retried += 1
            else:
                logger.exception(
                    '%6.2fs ! %s failed, %s: %s', t, ref, e.__class__.__name__, e, extra={'extra': exc_extra}
                )
                result = e
                finish = True
                self.jobs_failed += 1
        else:
            success = True
            finished_ms = timestamp_ms()
            logger.info('%6.2fs ← %s ● %s', (finished_ms - start_ms) / 1000, ref, result_str)
            finish = True
            self.jobs_complete += 1

        keep_result_forever = (
            self.keep_result_forever if function.keep_result_forever is None else function.keep_result_forever
        )
        result_timeout_s = self.keep_result_s if function.keep_result_s is None else function.keep_result_s
        result_data = None
        if result is not no_result and (keep_result_forever or result_timeout_s > 0):
            result_data = serialize_result(
                function_name,
                args,
                kwargs,
                job_try,
                enqueue_time_ms,
                success,
                result,
                start_ms,
                finished_ms,
                ref,
                self.queue_name,
                job_id=job_id,
                serializer=self.job_serializer,
            )

        if self.on_job_end:
            await self.on_job_end(ctx)

        await asyncio.shield(
            self.finish_job(
                job_id,
                finish,
                result_data,
                result_timeout_s,
                keep_result_forever,
                incr_score,
                keep_in_progress,
            )
        )

        if self.after_job_end:
            await self.after_job_end(ctx)

    async def finish_job(
        self,
        job_id: str,
        finish: bool,
        result_data: Optional[bytes],
        result_timeout_s: Optional[float],
        keep_result_forever: bool,
        incr_score: Optional[int],
        keep_in_progress: Optional[float],
    ) -> None:
        async with self.pool.pipeline(transaction=True) as tr:
            delete_keys = []
            in_progress_key = in_progress_key_prefix + job_id
            if keep_in_progress is None:
                delete_keys += [in_progress_key]
            else:
                tr.pexpire(in_progress_key, to_ms(keep_in_progress))

            if finish:
                if result_data:
                    expire = None if keep_result_forever else result_timeout_s
                    tr.set(result_key_prefix + job_id, result_data, px=to_ms(expire))
                delete_keys += [retry_key_prefix + job_id, job_key_prefix + job_id]
                tr.zrem(abort_jobs_ss, job_id)
                tr.zrem(self.queue_name, job_id)
            elif incr_score:
                tr.zincrby(self.queue_name, incr_score, job_id)
            if delete_keys:
                tr.delete(*delete_keys)
            await tr.execute()

    async def finish_failed_job(self, job_id: str, result_data: Optional[bytes]) -> None:
        async with self.pool.pipeline(transaction=True) as tr:
            tr.delete(
                retry_key_prefix + job_id,
                in_progress_key_prefix + job_id,
                job_key_prefix + job_id,
            )
            tr.zrem(abort_jobs_ss, job_id)
            tr.zrem(self.queue_name, job_id)
            # result_data would only be None if serializing the result fails
            keep_result = self.keep_result_forever or self.keep_result_s > 0
            if result_data is not None and keep_result:  # pragma: no branch
                expire = 0 if self.keep_result_forever else self.keep_result_s
                tr.set(result_key_prefix + job_id, result_data, px=to_ms(expire))
            await tr.execute()

    async def heart_beat(self) -> None:
        now = datetime.now(tz=self.timezone)
        await self.record_health()

        cron_window_size = max(self.poll_delay_s, 0.5)  # Clamp the cron delay to 0.5
        await self.run_cron(now, cron_window_size)

    async def run_cron(self, n: datetime, delay: float, num_windows: int = 2) -> None:
        job_futures = set()

        cron_delay = timedelta(seconds=delay * num_windows)

        this_hb_cutoff = n + cron_delay

        for cron_job in self.cron_jobs:
            if cron_job.next_run is None:
                if cron_job.run_at_startup:
                    cron_job.next_run = n
                else:
                    cron_job.calculate_next(n)
                    # This isn't getting run this iteration in any case.
                    continue

            # We queue up the cron if the next execution time is in the next
            # delay * num_windows (by default 0.5 * 2 = 1 second).
            if cron_job.next_run < this_hb_cutoff:
                if cron_job.job_id:
                    job_id: Optional[str] = cron_job.job_id
                else:
                    job_id = f'{cron_job.name}:{to_unix_ms(cron_job.next_run)}' if cron_job.unique else None
                job_futures.add(
                    self.pool.enqueue_job(
                        cron_job.name,
                        _job_id=job_id,
                        _queue_name=self.queue_name,
                        _defer_until=(
                            cron_job.next_run if cron_job.next_run > datetime.now(tz=self.timezone) else None
                        ),
                    )
                )
                cron_job.calculate_next(cron_job.next_run)

        job_futures and await asyncio.gather(*job_futures)

    async def record_health(self) -> None:
        now_ts = time()
        if (now_ts - self._last_health_check) < self.health_check_interval:
            return
        self._last_health_check = now_ts
        pending_tasks = sum(not t.done() for t in self.tasks.values())
        queued = await self.pool.zcard(self.queue_name)
        info = (
            f'{datetime.now():%b-%d %H:%M:%S} j_complete={self.jobs_complete} j_failed={self.jobs_failed} '
            f'j_retried={self.jobs_retried} j_ongoing={pending_tasks} queued={queued}'
        )
        await self.pool.psetex(  # type: ignore[no-untyped-call]
            self.health_check_key, int((self.health_check_interval + 1) * 1000), info.encode()
        )
        log_suffix = info[info.index('j_complete=') :]
        if self._last_health_check_log and log_suffix != self._last_health_check_log:
            logger.info('recording health: %s', info)
            self._last_health_check_log = log_suffix
        elif not self._last_health_check_log:
            self._last_health_check_log = log_suffix

    def _add_signal_handler(self, signum: Signals, handler: Callable[[Signals], None]) -> None:
        try:
            self.loop.add_signal_handler(signum, partial(handler, signum))
        except NotImplementedError:  # pragma: no cover
            logger.debug('Windows does not support adding a signal handler to an eventloop')

    def _jobs_started(self) -> int:
        return self.jobs_complete + self.jobs_retried + self.jobs_failed + len(self.tasks)

    async def _sleep_until_tasks_complete(self) -> None:
        """
        Sleeps until all tasks are done. Used together with asyncio.wait_for()
        """
        while len(self.tasks):
            await asyncio.sleep(0.1)

    async def _wait_for_tasks_to_complete(self, signum: Signals) -> None:
        """
        Wait for tasks to complete, until `wait_for_job_completion_on_signal_second` has been reached.
        """
        with contextlib.suppress(asyncio.TimeoutError):
            await asyncio.wait_for(
                self._sleep_until_tasks_complete(),
                self._job_completion_wait,
            )
        logger.info(
            'shutdown on %s, wait complete ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d ongoing to cancel',
            signum.name,
            self.jobs_complete,
            self.jobs_failed,
            self.jobs_retried,
            sum(not t.done() for t in self.tasks.values()),
        )
        for t in self.tasks.values():
            if not t.done():
                t.cancel()
        self.main_task and self.main_task.cancel()
        self.on_stop and self.on_stop(signum)

    def handle_sig_wait_for_completion(self, signum: Signals) -> None:
        """
        Alternative signal handler that allow tasks to complete within a given time before shutting down the worker.
        Time can be configured using `wait_for_job_completion_on_signal_second`.
        The worker will stop picking jobs when signal has been received.
        """
        sig = Signals(signum)
        logger.info('Setting allow_pick_jobs to `False`')
        self.allow_pick_jobs = False
        logger.info(
            'shutdown on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d to be completed',
            sig.name,
            self.jobs_complete,
            self.jobs_failed,
            self.jobs_retried,
            len(self.tasks),
        )
        self.loop.create_task(self._wait_for_tasks_to_complete(signum=sig))

    def handle_sig(self, signum: Signals) -> None:
        sig = Signals(signum)
        logger.info(
            'shutdown on %s ◆ %d jobs complete ◆ %d failed ◆ %d retries ◆ %d ongoing to cancel',
            sig.name,
            self.jobs_complete,
            self.jobs_failed,
            self.jobs_retried,
            len(self.tasks),
        )
        for t in self.tasks.values():
            if not t.done():
                t.cancel()
        self.main_task and self.main_task.cancel()
        self.on_stop and self.on_stop(sig)

    async def close(self) -> None:
        if not self._handle_signals:
            self.handle_sig(signal.SIGUSR1)
        if not self._pool:
            return
        await asyncio.gather(*self.tasks.values())
        await self.pool.delete(self.health_check_key)
        if self.on_shutdown:
            await self.on_shutdown(self.ctx)
        await self.pool.close(close_connection_pool=True)
        self._pool = None

    def __repr__(self) -> str:
        return (
            f'<Worker j_complete={self.jobs_complete} j_failed={self.jobs_failed} j_retried={self.jobs_retried} '
            f'j_ongoing={sum(not t.done() for t in self.tasks.values())}>'
        )


def get_kwargs(settings_cls: 'WorkerSettingsType') -> dict[str, NameError]:
    worker_args = set(inspect.signature(Worker).parameters.keys())
    d = settings_cls if isinstance(settings_cls, dict) else settings_cls.__dict__
    return {k: v for k, v in d.items() if k in worker_args}


def create_worker(settings_cls: 'WorkerSettingsType', **kwargs: Any) -> Worker:
    return Worker(**{**get_kwargs(settings_cls), **kwargs})


def run_worker(settings_cls: 'WorkerSettingsType', **kwargs: Any) -> Worker:
    worker = create_worker(settings_cls, **kwargs)
    worker.run()
    return worker


async def async_check_health(
    redis_settings: Optional[RedisSettings], health_check_key: Optional[str] = None, queue_name: Optional[str] = None
) -> int:
    redis_settings = redis_settings or RedisSettings()
    redis: ArqRedis = await create_pool(redis_settings)
    queue_name = queue_name or default_queue_name
    health_check_key = health_check_key or (queue_name + health_check_key_suffix)

    data = await redis.get(health_check_key)
    if not data:
        logger.warning('Health check failed: no health check sentinel value found')
        r = 1
    else:
        logger.info('Health check successful: %s', data)
        r = 0
    await redis.close(close_connection_pool=True)
    return r


def check_health(settings_cls: 'WorkerSettingsType') -> int:
    """
    Run a health check on the worker and return the appropriate exit code.
    :return: 0 if successful, 1 if not
    """
    cls_kwargs = get_kwargs(settings_cls)
    redis_settings = cast(Optional[RedisSettings], cls_kwargs.get('redis_settings'))
    health_check_key = cast(Optional[str], cls_kwargs.get('health_check_key'))
    queue_name = cast(Optional[str], cls_kwargs.get('queue_name'))
    return asyncio.run(async_check_health(redis_settings, health_check_key, queue_name))
