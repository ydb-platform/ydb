import asyncio
import dataclasses
import enum
import faulthandler
import inspect
import multiprocessing as mp
import os
import pickle
import platform
import queue
import re
import signal
import sys
import tempfile
import time
import traceback
import warnings
from typing import Any, Callable, List, Optional, Type
from uuid import uuid4

from contextlog import get_logger

import annet
from annet import tracing
from annet.connectors import Connector
from annet.lib import catch_ctrl_c, find_exc_in_stack
from annet.output import capture_output
from annet.tracing import tracing_connector


class PoolWorkerTaskType(enum.Enum):
    INVOKE = "invoke"
    STOP = "stop"


@dataclasses.dataclass(frozen=True)
class PoolWorkerTask:
    type: PoolWorkerTaskType
    payload: Optional[Any] = None


class _PickleSafeTracebackFormatterConnector(Connector[Callable[[BaseException], str]]):
    name = "PickleSafeTraceBackFormatter"
    ep_name = "pickle_safe_traceback_formatter"

    def _get_default(self) -> Callable[[], Callable[[BaseException], str]]:
        def wrapper():
            return lambda x: "".join(traceback.format_exception(x))

        return wrapper


pickle_safe_traceback_formatter_connector = _PickleSafeTracebackFormatterConnector()


class PickleSafeException(Exception):
    """An exception that can be safely pickled and passed between processes."""

    def __init__(self, orig_exc_cls: Type[Exception], orig_exc_msg: str, device_id: str, formatted_output: str) -> None:
        self.orig_exc_cls = orig_exc_cls
        self.orig_exc_msg = orig_exc_msg
        self.device_id = device_id
        self.formatted_output = formatted_output
        super().__init__(orig_exc_cls, orig_exc_msg, device_id, formatted_output)

    def __repr__(self) -> str:
        orig_cls_name = self.orig_exc_cls.__name__
        orig_msg_repr = repr(self.orig_exc_msg)
        return f"PickleSafeException<{orig_cls_name}({orig_msg_repr})>"

    def __str__(self) -> str:
        pretty_lines: List[str] = []
        pretty_lines.append(f"An exception for device {self.device_id}:")
        pretty_lines.append(self.formatted_output)
        return "\n".join(pretty_lines)

    @classmethod
    def from_exc(cls, orig_exc: Exception, device_id: str) -> "PickleSafeException":
        return PickleSafeException(
            orig_exc.__class__, str(orig_exc), device_id, pickle_safe_traceback_formatter_connector.get()(orig_exc)
        )


@catch_ctrl_c
def pool_worker(pool, index, task_queue, done_queue, context_carrier):
    faulthandler.register(signal.SIGUSR1)

    tracing_connector.get().attach_context(tracing_connector.get().extract_context(context_carrier))

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=DeprecationWarning)
        try:
            asyncio.get_event_loop().close()
        except Exception:
            pass
        asyncio.set_event_loop(asyncio.new_event_loop())

    return _pool_worker(pool, index, task_queue, done_queue)


@tracing.function(flush=True)
def _pool_worker(pool, index, task_queue, done_queue):
    worker_id = uuid4().hex

    pool_span = tracing_connector.get().get_current_span()
    if pool_span:
        pool_span.update_name("pool_worker")
        pool_span.set_attribute("worker.id", worker_id)
        pool_span.set_attribute("worker.index", index)

    cap_stdout = tempfile.TemporaryFile(mode="w+") if pool.capture_output else None
    cap_stderr = tempfile.TemporaryFile(mode="w+") if pool.capture_output else None

    _logger = get_logger()
    tasks_done = 0
    device_ids = []
    worker_name = mp.current_process().name

    while True:
        task: PoolWorkerTask = task_queue.get()
        if task.type == PoolWorkerTaskType.STOP:
            _logger.debug("I received STOP, terminating...")
            return

        device_id: Optional[str] = None
        if isinstance(task.payload, tuple):
            if len(task.payload) > 0:
                match = re.search(r"([^/]+).cfg", task.payload[0])
                if match:
                    device_id = match.group(1)
        else:
            device_id = str(task.payload)

        if device_id:
            device_ids.append(device_id)

        if pool_span:
            pool_span.set_attribute("device.ids", device_ids)

        task_result = TaskResult(worker_name, task.payload)
        task_result.extra["start_time"] = time.monotonic()
        ret_exc = None

        try:
            with tracing_connector.get().start_as_current_span(
                "pool_worker.invoke",
            ) as span:
                if device_id:
                    span.set_attribute("device.id", device_id)

                name = "invoke"
                invoke_span_ctx = tracing_connector.get().start_as_linked_span(name, tracer_name=__name__)
                capture_output_ctx = capture_output(cap_stdout, cap_stderr)

                with invoke_span_ctx as invoke_span, capture_output_ctx as _:
                    invoke_span.set_attribute("func", pool.func.__name__)
                    invoke_span.set_attribute("worker.id", worker_id)
                    if device_id:
                        invoke_span.set_attribute("device.id", device_id)

                    _logger.warning("Worker-%d start invoke %s", index, device_id)
                    task_result.result = invoke_retry(
                        pool.func, pool.net_retry, task.payload, *pool.args, **pool.kwargs
                    )
                    _logger.warning("Worker-%d finish invoke %s", index, device_id)

                    # Check if the result is picklable and throw an exception if not.
                    # Otherwise the exception will be thrown inside the multiprocessing
                    # code and we won't be able to handle it.
                    pickle.dumps(task_result.result)
        except KeyboardInterrupt:  # pylint: disable=try-except-raise
            raise
        except Exception as exc:
            safe_exc = PickleSafeException.from_exc(exc, task.payload)
            ret_exc = safe_exc
            task_result.exc = safe_exc
            task_result.result = None
        if pool.capture_output:
            task_result.extra["cap_stdout"] = cap_stdout.read()
            task_result.extra["cap_stderr"] = cap_stderr.read()

        results = list(pool._run_callbacks(task_result, in_thread=True))  # pylint: disable=protected-access
        done_queue.put((worker_name, task, results, ret_exc))

        tasks_done += 1
        if pool.max_tasks and tasks_done >= pool.max_tasks:
            _logger.debug("Maximum tasks limit reached. Now I can retire")
            tracing_connector.get().force_flush()
            sys.exit(9)


class TaskResult:
    def __init__(self, worker_name, device_id, result=None, exc=None):
        self.worker_name = worker_name
        self.device_id = device_id
        self.result = result
        self.exc = exc
        self.extra = {}

    def __repr__(self):
        return "TaskResult(worker_name=%s, device_id=%s, result=%s, exc=%s, extra=%s)" % (
            self.worker_name,
            self.device_id,
            self.result,
            self.exc,
            self.extra,
        )


class Parallel:
    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.callbacks = []
        self.in_thread_callbacks = []
        self.parallel = mp.cpu_count()
        self.task_timeout = 1800  # maximum seconds to wait for next task done
        self.max_tasks = 25
        self.tasks_done = 0
        self.net_retry = 3
        self.capture_output = False

    def tune(self, **kwargs):
        for kw, arg in kwargs.items():
            if not hasattr(self, kw):
                raise ValueError("Can not tune Parallel.%s: attribute doesn't exist" % kw)
            setattr(self, kw, arg)
        return self

    def tune_args(self, args):
        for kw in args._enum_args():  # pylint: disable=protected-access
            if hasattr(self, kw):
                setattr(self, kw, getattr(args, kw))
        return self

    # func prototype: func(parallel_object, worker_name, ret) -> ret(s)
    def add_callback(self, func, in_thread=False):
        if not callable(func):
            raise annet.ExecError("callback must be a callable object or function")
        if in_thread:
            self.in_thread_callbacks.append(func)
        else:
            self.callbacks.append(func)
        return self

    def _run_callbacks(self, task_result: TaskResult, in_thread=False):
        task_results = [task_result]
        cbs = self.in_thread_callbacks if in_thread else self.callbacks
        for cb in cbs:
            task_results = [cb_result for result in task_results for cb_result in self._cb_wrapper(cb, result)]
        yield from task_results

    def _cb_wrapper(self, cb, task_result: TaskResult):
        task_results = []
        try:
            if inspect.isgeneratorfunction(cb):
                future = cb(self, task_result)
                while True:
                    cb_result = next(future)
                    if isinstance(cb_result, TaskResult):
                        task_results.append(cb_result)
            else:
                cb_result = cb(self, task_result)
                if isinstance(cb_result, TaskResult):
                    task_results = [cb_result]
        except StopIteration:
            pass
        except Exception as exc:
            exc.device_id = task_result.device_id
            exc.formatted_output = traceback.format_exc()
            task_result.exc = exc
            task_results = [task_result]
        yield from task_results

    @tracing.function
    def run(self, device_ids, tolerate_fails=True, strict_error_code=False):
        success, fail = {}, {}
        for task_result in self.irun(device_ids, tolerate_fails):
            if task_result.exc is not None:
                fail[task_result.device_id] = task_result.exc
            else:
                success[task_result.device_id] = task_result.result
        if strict_error_code and fail:
            raise RuntimeError("failed for %d/%d devices" % (len(fail), len(device_ids)))
        return success, fail

    def irun(self, device_ids, tolerate_fails=True):
        _logger = get_logger()
        self.tasks_done = 0
        pool_size = self.parallel if len(device_ids) > self.parallel else len(device_ids)

        span = tracing_connector.get().get_current_span()
        if span:
            span.set_attribute("pool_size", pool_size)

        # single process way
        if pool_size == 1:
            cap_stdout = tempfile.TemporaryFile(mode="w+") if self.capture_output else None
            cap_stderr = tempfile.TemporaryFile(mode="w+") if self.capture_output else None
            worker_name = mp.current_process().name

            for device_id in device_ids:
                task_result = TaskResult(worker_name, device_id)
                task_result.extra["start_time"] = time.monotonic()
                try:
                    with capture_output(cap_stdout, cap_stderr):
                        task_result.result = invoke_retry(
                            self.func, self.net_retry, device_id, *self.args, **self.kwargs
                        )
                except Exception as exc:
                    safe_exc = PickleSafeException.from_exc(exc, device_id)
                    if not tolerate_fails:
                        raise safe_exc
                    task_result.exc = safe_exc
                if self.capture_output:
                    task_result.extra["cap_stdout"] = cap_stdout.read()
                    task_result.extra["cap_stderr"] = cap_stderr.read()
                self.tasks_done += 1

                yield from [
                    result
                    for in_thread_result in self._run_callbacks(task_result, in_thread=True)
                    for result in self._run_callbacks(in_thread_result)
                ]
        else:
            # multiple processes way
            _logger.info("creating process pool with %d workers", pool_size)
            task_queue = mp.Queue()
            done_queue = mp.Queue()
            for device_id in device_ids:
                task_queue.put(PoolWorkerTask(type=PoolWorkerTaskType.INVOKE, payload=device_id))

            context_carrier = {}
            tracing_connector.get().inject_context(context_carrier)

            pool = {}
            for index in range(pool_size):
                task_queue.put(PoolWorkerTask(type=PoolWorkerTaskType.STOP))
                worker_name = "Worker-%d" % index
                worker_args = (self, index, task_queue, done_queue, context_carrier)

                worker = mp.Process(name=worker_name, target=pool_worker, args=worker_args)
                pool[worker_name] = worker
                worker.start()
                _logger.debug("Worker '%s' has been created with PID %d", worker_name, worker.pid)

            last_task_ts = time.monotonic()
            while True:
                exc = None
                worker_name = None
                in_thread_results = None

                queue_empty = False
                try:
                    worker_name, _, in_thread_results, exc = done_queue.get(True, 1)
                    last_task_ts = time.monotonic()
                except queue.Empty:
                    queue_empty = True

                retired_workers, failed_workers = self._check_children(pool)

                terminate_by_timeout = False
                terminate_exc = None
                if queue_empty:
                    if time.monotonic() - last_task_ts > self.task_timeout:
                        # timeout hit
                        terminate_by_timeout = True
                        terminate_exc = annet.ExecError()

                if not tolerate_fails:
                    if exc is not None:
                        # worker returned exception
                        terminate_exc = exc
                    elif failed_workers:
                        # some workers exited with non-zero (and non-9) code
                        terminate_exc = annet.ExecError(f"Workers {failed_workers} exited with error")

                if terminate_exc is not None:
                    for name, worker in pool.items():
                        if worker.exitcode is None:
                            if terminate_by_timeout:
                                os.kill(worker.pid, signal.SIGUSR1)  # force dump stacktrace
                                time.sleep(10)
                            worker.terminate()
                            _logger.warning("Worker '%s' (PID: %d) has been terminated", name, worker.pid)
                        worker.join()
                    raise terminate_exc

                if not queue_empty:
                    self.tasks_done += 1

                    qsize = "unknown"
                    if platform.system() != "Darwin":
                        # Not implemented for macOS
                        qsize = str(done_queue.qsize())

                    _logger.debug("Got a result from worker '%s', qsize is %s", worker_name, qsize)

                    yield from [
                        result
                        for in_thread_result in in_thread_results
                        for result in self._run_callbacks(in_thread_result)
                    ]

                if not pool:
                    break

                for name in retired_workers:
                    _logger.debug("Worker '%s' has retired. Restart it", name)
                    pool[name] = mp.Process(name=name, target=pool_worker, args=worker_args)
                    pool[name].start()
            task_queue.close()
            done_queue.close()

    def _check_children(self, pool):
        _logger = get_logger()
        retired_workers = []
        failed_workers = []
        for name in list(pool.keys()):
            exitcode = pool[name].exitcode
            if exitcode is None:
                continue
            if exitcode == 9:
                retired_workers.append(name)
            elif exitcode != 0:
                _logger.error(
                    "Worker '%s' (PID: %d) has exited with non-zero exit code %d", name, pool[name].pid, exitcode
                )
                failed_workers.append(name)
            else:
                _logger.debug("Worker '%s' (PID: %d) has been reaped with exitcode %d", name, pool[name].pid, exitcode)
            pool[name].join()
            if exitcode != 9:
                del pool[name]
        return retired_workers, failed_workers


def invoke_retry(func, net_retry, *args, **kwargs):
    attempt = 0
    while True:
        try:
            result = func(*args, **kwargs)
            if inspect.isgenerator(result):
                result = list(result)
            return result
        except Exception as exc:
            if not find_exc_in_stack(exc, (BrokenPipeError, ConnectionResetError)):
                raise
            if attempt >= net_retry:
                raise
            attempt += 1
