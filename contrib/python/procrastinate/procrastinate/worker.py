from __future__ import annotations

import asyncio
import contextlib
import functools
import inspect
import logging
import time
from collections.abc import Awaitable, Callable, Iterable
from typing import Any

from procrastinate import (
    app,
    exceptions,
    job_context,
    jobs,
    periodic,
    retry,
    signals,
    tasks,
    types,
    utils,
)

logger = logging.getLogger(__name__)

WORKER_NAME = "worker"
WORKER_CONCURRENCY = 1  # maximum number of parallel jobs
FETCH_JOB_POLLING_INTERVAL = 5.0  # seconds
ABORT_JOB_POLLING_INTERVAL = 5.0  # seconds


class Worker:
    def __init__(
        self,
        app: app.App,
        queues: Iterable[str] | None = None,
        name: str | None = WORKER_NAME,
        concurrency: int = WORKER_CONCURRENCY,
        wait: bool = True,
        fetch_job_polling_interval: float = FETCH_JOB_POLLING_INTERVAL,
        abort_job_polling_interval: float = ABORT_JOB_POLLING_INTERVAL,
        shutdown_graceful_timeout: float | None = None,
        listen_notify: bool = True,
        delete_jobs: str | jobs.DeleteJobCondition | None = None,
        additional_context: dict[str, Any] | None = None,
        install_signal_handlers: bool = True,
        update_heartbeat_interval: float = 10.0,
        stalled_worker_timeout: float = 30.0,
    ):
        self.app = app
        self.queues = queues
        self.worker_name = name
        self.concurrency = concurrency
        self.wait = wait
        self.fetch_job_polling_interval = fetch_job_polling_interval
        self.abort_job_polling_interval = abort_job_polling_interval
        self.listen_notify = listen_notify
        self.delete_jobs = (
            jobs.DeleteJobCondition(delete_jobs)
            if isinstance(delete_jobs, str)
            else delete_jobs
        ) or jobs.DeleteJobCondition.NEVER
        self.additional_context = additional_context
        self.install_signal_handlers = install_signal_handlers
        self.update_heartbeat_interval = update_heartbeat_interval
        self.stalled_worker_timeout = stalled_worker_timeout

        if self.worker_name:
            self.logger = logger.getChild(self.worker_name)
        else:
            self.logger = logger

        self.worker_id: int | None = None

        self._loop_task: asyncio.Future[Any] | None = None
        self._new_job_event = asyncio.Event()
        self._running_jobs: dict[asyncio.Task[Any], job_context.JobContext] = {}
        self._job_semaphore = asyncio.Semaphore(self.concurrency)
        self._stop_event = asyncio.Event()
        self.shutdown_graceful_timeout = shutdown_graceful_timeout
        self._job_ids_to_abort: dict[int, job_context.AbortReason] = dict()
        self.run_task: asyncio.Task[Any] | None = None

    def stop(self):
        if self._stop_event.is_set():
            return
        self.logger.info(
            "Stop requested",
            extra=self._log_extra(
                context=None, action="stopping_worker", job_result=None
            ),
        )

        self._stop_event.set()

    async def _periodic_deferrer(self):
        deferrer = periodic.PeriodicDeferrer(
            registry=self.app.periodic_registry,
            **self.app.periodic_defaults,
        )
        return await deferrer.worker()

    def find_task(self, task_name: str) -> tasks.Task[Any, Any, Any]:
        try:
            return self.app.tasks[task_name]
        except KeyError as exc:
            raise exceptions.TaskNotFound from exc

    def _log_extra(
        self,
        action: str,
        context: job_context.JobContext | None,
        job_result: job_context.JobResult | None,
        **kwargs: Any,
    ) -> types.JSONDict:
        worker: types.JSONDict = {
            "name": self.worker_name,
            "worker_id": self.worker_id,
            "job_id": context.job.id if context else None,
            "queues": list(self.queues or []),
        }
        extra: types.JSONDict = {
            "action": action,
            "worker": worker,
        }
        if context:
            extra["job"] = context.job.log_context()

        return {
            **extra,
            **(job_result.as_dict() if job_result else {}),
            **kwargs,
        }

    async def _persist_job_status(
        self,
        job: jobs.Job,
        status: jobs.Status,
        retry_decision: retry.RetryDecision | None,
        context: job_context.JobContext,
        job_result: job_context.JobResult | None,
    ):
        if retry_decision:
            await self.app.job_manager.retry_job(
                job=job,
                retry_at=retry_decision.retry_at,
                lock=retry_decision.lock,
                priority=retry_decision.priority,
                queue=retry_decision.queue,
            )
        else:
            delete_job = {
                jobs.DeleteJobCondition.ALWAYS: True,
                jobs.DeleteJobCondition.NEVER: False,
                jobs.DeleteJobCondition.SUCCESSFUL: status == jobs.Status.SUCCEEDED,
            }[self.delete_jobs]
            await self.app.job_manager.finish_job(
                job=job, status=status, delete_job=delete_job
            )

        assert job.id
        self._job_ids_to_abort.pop(job.id, None)

        self.logger.debug(
            f"Acknowledged job completion {job.call_string}",
            extra=self._log_extra(
                action="finish_task",
                context=context,
                status=status,
                job_result=job_result,
            ),
        )

    def _log_job_outcome(
        self,
        status: jobs.Status,
        context: job_context.JobContext,
        job_result: job_context.JobResult | None,
        job_retry: exceptions.JobRetry | None,
        exc_info: bool | BaseException = False,
    ):
        if status == jobs.Status.SUCCEEDED:
            log_action, log_title = "job_success", "Success"
        elif status == jobs.Status.ABORTED and not job_retry:
            log_action, log_title = "job_aborted", "Aborted"
        elif status == jobs.Status.ABORTED and job_retry:
            log_action, log_title = "job_aborted_retry", "Aborted, to retry"
        elif job_retry:
            log_action, log_title = "job_error_retry", "Error, to retry"
        else:
            log_action, log_title = "job_error", "Error"

        text = f"Job {context.job.call_string} ended with status: {log_title}, "
        # in practice we should always have a start and end timestamp here
        # but in theory the JobResult class allows it to be None
        if job_result and job_result.start_timestamp and job_result.end_timestamp:
            duration = job_result.end_timestamp - job_result.start_timestamp
            text += f"lasted {duration:.3f} s"
        if job_result and job_result.result:
            text += f" - Result: {job_result.result}"[:250]

        extra = self._log_extra(
            context=context, action=log_action, job_result=job_result
        )
        log_level = (
            logging.ERROR
            if status == jobs.Status.FAILED and not job_retry
            else logging.INFO
        )
        logger.log(log_level, text, extra=extra, exc_info=exc_info)

    async def _process_job(self, context: job_context.JobContext):
        """
        Processes a given job and persists its status
        """
        task = self.app.tasks.get(context.job.task_name)
        job_retry = None
        exc_info = False
        retry_decision = None
        job = context.job

        job_result = job_context.JobResult(start_timestamp=context.start_timestamp)

        try:
            if not task:
                raise exceptions.TaskNotFound

            self.logger.debug(
                f"Loaded job info, about to start job {job.call_string}",
                extra=self._log_extra(
                    context=context, action="loaded_job_info", job_result=job_result
                ),
            )

            self.logger.info(
                f"Starting job {job.call_string}",
                extra=self._log_extra(
                    context=context, action="start_job", job_result=job_result
                ),
            )

            exc_info: bool | BaseException = False

            async def ensure_async() -> Callable[..., Awaitable[Any]]:
                await_func: Callable[..., Awaitable[Any]]
                if inspect.iscoroutinefunction(task.func):
                    await_func = task
                else:
                    await_func = functools.partial(utils.sync_to_async, task)

                job_args = [context] if task.pass_context else []
                task_result = await await_func(*job_args, **job.task_kwargs)
                # In some cases, the task function might be a synchronous function
                # that returns an awaitable without actually being a
                # coroutinefunction. In that case, in the await above, we haven't
                # actually called the task, but merely generated the awaitable that
                # implements the task. In that case, we want to wait this awaitable.
                # It's easy enough to be in that situation that the best course of
                # action is probably to await the awaitable.
                # It's not even sure it's worth emitting a warning
                if inspect.isawaitable(task_result):
                    task_result = await task_result

                return task_result

            job_result.result = await ensure_async()

        except BaseException as e:
            exc_info = e

            # aborted job can be retried if it is caused by a shutdown.
            if not (isinstance(e, exceptions.JobAborted)) or (
                context.abort_reason() == job_context.AbortReason.SHUTDOWN
            ):
                job_retry = (
                    task.get_retry_exception(exception=e, job=job) if task else None
                )
                retry_decision = job_retry.retry_decision if job_retry else None
                if isinstance(e, exceptions.TaskNotFound):
                    self.logger.exception(
                        f"Task was not found: {e}",
                        extra=self._log_extra(
                            context=context,
                            action="task_not_found",
                            exception=str(e),
                            job_result=job_result,
                        ),
                    )
        finally:
            job_result.end_timestamp = time.time()

            if isinstance(exc_info, exceptions.JobAborted) or isinstance(
                exc_info, asyncio.CancelledError
            ):
                status = jobs.Status.ABORTED
            elif exc_info:
                status = jobs.Status.FAILED
            else:
                status = jobs.Status.SUCCEEDED

            self._log_job_outcome(
                status=status,
                context=context,
                job_result=job_result,
                job_retry=job_retry,
                exc_info=exc_info,
            )

            persist_job_status_task = asyncio.create_task(
                self._persist_job_status(
                    job=job,
                    status=status,
                    retry_decision=retry_decision,
                    context=context,
                    job_result=job_result,
                )
            )
            try:
                await asyncio.shield(persist_job_status_task)
            except asyncio.CancelledError:
                await persist_job_status_task
                raise

    async def _fetch_and_process_jobs(self):
        """Fetch and process jobs until there is no job left or asked to stop"""
        while not self._stop_event.is_set():
            acquire_sem_task = asyncio.create_task(self._job_semaphore.acquire())
            job = None
            try:
                await utils.wait_any(acquire_sem_task, self._stop_event.wait())
                if self._stop_event.is_set():
                    break

                assert self.worker_id is not None
                job = await self.app.job_manager.fetch_job(
                    queues=self.queues, worker_id=self.worker_id
                )
            finally:
                if (not job or self._stop_event.is_set()) and acquire_sem_task.done():
                    self._job_semaphore.release()
                self._new_job_event.clear()

            if not job:
                break

            job_id = job.id

            context = job_context.JobContext(
                app=self.app,
                worker_name=self.worker_name,
                worker_queues=self.queues,
                additional_context=self.additional_context.copy()
                if self.additional_context
                else {},
                job=job,
                abort_reason=lambda: self._job_ids_to_abort.get(job_id)
                if job_id
                else None,
                start_timestamp=time.time(),
            )
            job_task = asyncio.create_task(
                self._process_job(context),
                name=f"process job {job.task_name}[{job.id}]",
            )
            self._running_jobs[job_task] = context

            def on_job_complete(task: asyncio.Task):
                del self._running_jobs[task]
                self._job_semaphore.release()

            job_task.add_done_callback(on_job_complete)

    async def run(self):
        """
        Run the worker
        This will run forever until asked to stop/cancelled, or until no more job is available is configured not to wait
        """
        logger.debug("Pruning stalled workers with old heartbeats")
        pruned_workers = await self.app.job_manager.prune_stalled_workers(
            self.stalled_worker_timeout
        )
        if pruned_workers:
            logger.debug(f"Pruned stalled workers: {', '.join(str(pruned_workers))}")

        self.worker_id = await self.app.job_manager.register_worker()
        logger.debug(f"Registered worker {self.worker_id} in the database")

        self.run_task = asyncio.current_task()
        loop_task = asyncio.create_task(self._run_loop(), name="worker loop")

        try:
            # shield the loop task from cancellation
            # instead, a stop event is set to enable graceful shutdown
            await asyncio.shield(loop_task)
        except asyncio.CancelledError:
            # worker.run is cancelled, usually by cancelling app.run_worker_async
            self.stop()
            await loop_task
            raise

    async def _handle_notification(
        self, *, channel: str, notification: jobs.Notification
    ):
        if notification["type"] == "job_inserted":
            self._new_job_event.set()
        elif notification["type"] == "abort_job_requested":
            self._handle_abort_jobs_requested([notification["job_id"]])

    async def _update_heartbeat(self):
        while True:
            logger.debug(
                f"Waiting for {self.update_heartbeat_interval}s before updating worker heartbeat"
            )
            await asyncio.sleep(self.update_heartbeat_interval)

            logger.debug(f"Updating heartbeat of worker {self.worker_id}")
            assert self.worker_id is not None
            await self.app.job_manager.update_heartbeat(self.worker_id)

    async def _poll_jobs_to_abort(self):
        while True:
            logger.debug(
                f"waiting for {self.abort_job_polling_interval}s before querying jobs to abort"
            )
            await asyncio.sleep(self.abort_job_polling_interval)
            if not self._running_jobs:
                logger.debug("Not querying jobs to abort because no job is running")
                continue
            try:
                job_ids = await self.app.job_manager.list_jobs_to_abort_async()
                self._handle_abort_jobs_requested(job_ids)
            except Exception as error:
                logger.exception(
                    f"poll_jobs_to_abort error: {error!r}",
                    exc_info=error,
                    extra={
                        "action": "poll_jobs_to_abort_error",
                    },
                )
                # recover from errors and continue polling

    def _handle_abort_jobs_requested(self, job_ids: Iterable[int]):
        running_job_ids = {c.job.id for c in self._running_jobs.values() if c.job.id}
        new_job_ids_to_abort = (running_job_ids & set(job_ids)) - set(
            self._job_ids_to_abort
        )

        for process_job_task, context in self._running_jobs.items():
            if context.job.id in new_job_ids_to_abort:
                self._abort_job(
                    process_job_task, context, job_context.AbortReason.USER_REQUEST
                )

    def _abort_job(
        self,
        process_job_task: asyncio.Task[Any],
        context: job_context.JobContext,
        reason: job_context.AbortReason,
    ):
        assert context.job.id
        self._job_ids_to_abort[context.job.id] = reason

        log_message: str
        task = self.app.tasks.get(context.job.task_name)
        if not task:
            log_message = "Received a request to abort a job but the job has no associated task. No action to perform"
        elif not inspect.iscoroutinefunction(task.func):
            log_message = "Received a request to abort a synchronous job. Job is responsible for aborting by checking context.should_abort"
        else:
            log_message = "Received a request to abort an asynchronous job. Cancelling asyncio task"
            process_job_task.cancel()

        self.logger.debug(
            log_message,
            extra=self._log_extra(action="abort_job", context=context, job_result=None),
        )

    async def _shutdown(self, side_tasks: list[asyncio.Task[Any]]):
        """
        Gracefully shutdown the worker by cancelling side tasks
        and waiting for all pending jobs.
        """
        await utils.cancel_and_capture_errors(side_tasks)

        now = time.time()
        for context in self._running_jobs.values():
            duration = now - context.start_timestamp
            self.logger.info(
                f"Waiting for job to finish: worker: {context.job.call_string} (started {duration:.3f} s ago)",
                extra=self._log_extra(
                    context=None, action="ending_job", job_result=None
                ),
            )

        if self._running_jobs:
            await asyncio.wait(
                self._running_jobs, timeout=self.shutdown_graceful_timeout
            )

        # As a reminder, tasks have a done callback that
        # removes them from the self._running_jobs dict,
        # so as the tasks stop, this dict will shrink.
        if self._running_jobs:
            self.logger.info(
                f"{len(self._running_jobs)} jobs still running after graceful timeout. Aborting them",
                extra=self._log_extra(
                    action="stop_worker",
                    queues=self.queues,
                    context=None,
                    job_result=None,
                ),
            )
            await self._abort_running_jobs()

        assert self.worker_id is not None
        await self.app.job_manager.unregister_worker(self.worker_id)
        logger.debug(f"Unregistered finished worker {self.worker_id} from the database")
        self.worker_id = None

        self.logger.info(
            f"Stopped worker on {utils.queues_display(self.queues)}",
            extra=self._log_extra(
                action="stop_worker", queues=self.queues, context=None, job_result=None
            ),
        )

    async def _abort_running_jobs(self):
        for task, context in self._running_jobs.items():
            self._abort_job(task, context, job_context.AbortReason.SHUTDOWN)

        await asyncio.gather(*self._running_jobs, return_exceptions=True)

    def _start_side_tasks(self) -> list[asyncio.Task[Any]]:
        """Start side tasks such as periodic deferrer and notification listener"""
        side_tasks = [
            asyncio.create_task(self._update_heartbeat(), name="update_heartbeats"),
            asyncio.create_task(self._periodic_deferrer(), name="deferrer"),
            asyncio.create_task(self._poll_jobs_to_abort(), name="poll_jobs_to_abort"),
        ]
        if self.listen_notify:
            listener_coro = self.app.job_manager.listen_for_jobs(
                on_notification=self._handle_notification,
                queues=self.queues,
            )
            side_tasks.append(asyncio.create_task(listener_coro, name="listener"))
        return side_tasks

    async def _monitor_side_tasks(self, side_tasks: list[asyncio.Task[Any]]):
        """Monitor side tasks and stop the worker if any task fails"""
        try:
            done, _pending = await asyncio.wait(
                side_tasks, return_when=asyncio.FIRST_COMPLETED
            )
            for task in done:
                if exc := task.exception():
                    self.logger.error(
                        f"Side task {task.get_name()} failed with exception: {exc}, stopping worker",
                        extra=self._log_extra(
                            action="side_task_failed",
                            context=None,
                            job_result=None,
                            task_name=task.get_name(),
                            exception=str(exc),
                        ),
                        exc_info=exc,
                    )
                    self.stop()
                    return
        except Exception as exc:
            self.logger.exception(
                f"Side task monitor failed: {exc}",
                extra=self._log_extra(
                    action="side_task_monitor_failed",
                    context=None,
                    job_result=None,
                    exception=str(exc),
                ),
            )
            raise

    async def _run_loop(self):
        """
        Run all side coroutines, then start fetching/processing jobs in a loop
        """
        self.logger.info(
            f"Starting worker on {utils.queues_display(self.queues)}",
            extra=self._log_extra(
                action="start_worker", context=None, queues=self.queues, job_result=None
            ),
        )
        self._new_job_event.clear()
        self._stop_event.clear()
        self._running_jobs = {}
        self._job_semaphore = asyncio.Semaphore(self.concurrency)
        side_tasks = self._start_side_tasks()
        side_tasks_monitor = asyncio.create_task(
            self._monitor_side_tasks(side_tasks), name="side_tasks_monitor"
        )

        context = (
            signals.on_stop(self.stop)
            if self.install_signal_handlers
            else contextlib.nullcontext()
        )

        try:
            with context:
                await self._fetch_and_process_jobs()
                if not self.wait:
                    self.logger.info(
                        "No job found. Stopping worker because wait=False",
                        extra=self._log_extra(
                            context=None,
                            action="stop_worker",
                            queues=self.queues,
                            job_result=None,
                        ),
                    )
                    self._stop_event.set()

                while not self._stop_event.is_set():
                    # wait for a new job notification, a stop event or the next polling interval
                    await utils.wait_any(
                        self._new_job_event.wait(),
                        asyncio.sleep(self.fetch_job_polling_interval),
                        self._stop_event.wait(),
                    )
                    await self._fetch_and_process_jobs()
        finally:
            if not side_tasks_monitor.done():
                side_tasks_monitor.cancel()
            await self._shutdown(side_tasks=side_tasks)
