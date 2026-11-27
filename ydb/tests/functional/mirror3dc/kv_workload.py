from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import hashlib
import logging
from queue import Queue
import re
import threading
import time

import grpc

from ydb.public.api.grpc.ydb_keyvalue_v1_pb2_grpc import KeyValueServiceStub
from ydb.public.api.protos import ydb_keyvalue_pb2 as kv_api
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


logger = logging.getLogger(__name__)
RPC_TIMEOUT = 30
RETRY_TIMEOUT = 120
# BSQueue's 10s backoff plus configuration/session establishment time.
BSQUEUE_RECONNECT_GRACE = 20


def invoke(request, stopped, *, retryable_grpc_codes=(grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED),
           retry_response=None):
    deadline = time.monotonic() + RETRY_TIMEOUT
    while True:
        try:
            response = request()
            if response.operation.status not in (
                StatusIds.UNAVAILABLE, StatusIds.OVERLOADED, StatusIds.TIMEOUT,
                StatusIds.ABORTED, StatusIds.UNDETERMINED,
            ) and (retry_response is None or not retry_response(response)):
                return response
            error = str(response)
        except grpc.RpcError as exc:
            if exc.code() not in retryable_grpc_codes:
                raise
            error = str(exc)
        if time.monotonic() >= deadline or stopped.wait(0.1):
            raise AssertionError(f'KV request did not recover: {error}')


def make_value(key, size):
    return hashlib.shake_256(key.encode()).digest(size)


class BsQueueReconnectGrace:
    """Retry the target group's DSPE4 responses until an explicitly armed deadline."""

    def __init__(self, event_logger=None):
        self._events = event_logger or logger
        self._lock = threading.Lock()
        self._deadline = 0
        self._group_id = None
        self._retries = 0

    @property
    def retries(self):
        with self._lock:
            return self._retries

    def expect_reconnect(self, timeout, *, group_id):
        if timeout <= 0:
            raise ValueError('Reconnect grace timeout must be positive')
        with self._lock:
            self._group_id = int(group_id)
            self._deadline = time.monotonic() + timeout
            deadline = self._deadline
        self._events.info('BSQUEUE_RECONNECT_GRACE_START group=%s timeout=%s deadline=%s', group_id, timeout, deadline)

    def retry_response(self, response):
        if response.operation.status != StatusIds.INTERNAL_ERROR:
            return False
        pending = list(response.operation.issues)
        messages = []
        while pending:
            issue = pending.pop()
            messages.append(issue.message)
            pending.extend(issue.issues)
        with self._lock:
            remaining = self._deadline - time.monotonic()
            if remaining <= 0:
                return False
            group_id = self._group_id
            group_pattern = rf'\bGroupId#\s*{group_id}(?!\d)'
            if not any('(DSPE4)' in message and re.search(group_pattern, message) for message in messages):
                return False
            self._retries += 1
            retries = self._retries
        self._events.warning('BSQUEUE_RECONNECT_RETRY group=%s status=%s issues=%s remaining=%.3f retries=%s',
                             group_id, response.operation.status, messages, remaining, retries)
        return True


class KvWorkloadHandle:
    """Observe live workers without pausing writes or waiting on unfinished futures."""

    def __init__(self, progress, lock, tasks, reconnect_grace=None):
        self._progress = progress
        self._lock = lock
        self._tasks = tasks
        self._reconnect_grace = reconnect_grace or BsQueueReconnectGrace()

    def stats(self):
        with self._lock:
            return dict(self._progress, bsqueue_retries=self._reconnect_grace.retries)

    def expect_reconnect(self, timeout, *, group_id):
        self._reconnect_grace.expect_reconnect(timeout, group_id=group_id)

    def check_errors(self):
        for task in tuple(self._tasks):
            if task.done():
                task.result()

    def wait_progress(self, timeout=30, min_writes=1, min_reads=1):
        """Wait for successful operations beyond the counters at this call."""
        if timeout <= 0 or min_writes < 0 or min_reads < 0:
            raise ValueError('Timeout must be positive and progress increments non-negative')
        before = self.stats()
        deadline = time.monotonic() + timeout
        while True:
            self.check_errors()
            current = self.stats()
            if (current['writes'] - before['writes'] >= min_writes
                    and current['validated_reads'] - before['validated_reads'] >= min_reads):
                return current
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(
                    f'KV workload did not progress: before={before}, current={current}, '
                    f'required writes={min_writes}, reads={min_reads}'
                )
            time.sleep(min(0.05, remaining))


def run_kv_workload(endpoints, path, partitions=3, writers=3, readers=3, writes_per_thread=100, value_size=1024,
                    background_action=None, before_background_action=None, workload_started=None, event_logger=None):
    """Write unique keys and read every acknowledged value in parallel.

    ``workload_started(handle)`` runs in the background-action thread after the
    first validated read, before ``before_background_action`` and its writer barrier.
    """
    events = event_logger or logger
    acknowledged = Queue()
    stopped = threading.Event()
    load_started = threading.Event()
    action_finished = threading.Event()
    writers_ready = threading.Barrier(writers + 1, timeout=RETRY_TIMEOUT)
    writers_resume = threading.Event()
    if before_background_action is not None and background_action is None:
        raise ValueError('before_background_action requires background_action')
    if workload_started is not None and background_action is None:
        raise ValueError('workload_started requires background_action')
    written_keys = []
    keys_lock = threading.Lock()
    progress = dict(writes=0, validated_reads=0)
    worker_tasks = []
    reconnect_grace = BsQueueReconnectGrace(events)
    handle = KvWorkloadHandle(progress, keys_lock, worker_tasks, reconnect_grace)
    write_locks = [threading.Lock() for _ in range(writers)]
    if background_action is None:
        action_finished.set()

    @contextmanager
    def pause_writes():
        """Wait for in-flight writes, then prevent new writes during a snapshot."""
        with ExitStack() as stack:
            for lock in write_locks:
                stack.enter_context(lock)
            yield len(written_keys)

    def run_action():
        try:
            deadline = time.monotonic() + RETRY_TIMEOUT
            while not load_started.wait(timeout=0.1):
                handle.check_errors()
                if time.monotonic() >= deadline:
                    raise AssertionError('KV workload did not start before the background action')
            handle.check_errors()
            if workload_started is not None:
                workload_started(handle)
            if before_background_action is not None:
                while writers_ready.n_waiting < writers:
                    handle.check_errors()
                    if time.monotonic() >= deadline:
                        raise AssertionError('Writers did not finish the initial batch')
                    time.sleep(0.05)
                writers_ready.wait()
                before_background_action()
                writers_resume.set()
            background_action(pause_writes)
        finally:
            action_finished.set()
            writers_resume.set()

    def write_loop(worker):
        count = 0
        with grpc.insecure_channel(endpoints[worker % len(endpoints)]) as channel:
            stub = KeyValueServiceStub(channel)
            seq = 0
            while seq < writes_per_thread or not action_finished.is_set():
                if stopped.is_set():
                    break
                partition = (worker + seq) % partitions
                key = f'writer-{worker}-{seq}'
                request = kv_api.ExecuteTransactionRequest(path=path, partition_id=partition)
                # MAIN forces even small values into separate BlobStorage blobs.
                request.commands.add().write.CopyFrom(kv_api.ExecuteTransactionRequest.Command.Write(
                    key=key, value=make_value(key, value_size), storage_channel=2,
                ))
                with write_locks[worker]:
                    events.info('write partition=%s key=%s', partition, key)
                    try:
                        response = invoke(lambda: stub.ExecuteTransaction(request, timeout=RPC_TIMEOUT), stopped,
                                          retry_response=reconnect_grace.retry_response)
                        if response.operation.status != StatusIds.SUCCESS:
                            raise AssertionError(
                                f'Write failed for partition {partition}, key {key}: status={response.operation.status}'
                            )
                    except BaseException:
                        events.exception('write_failed partition=%s key=%s', partition, key)
                        raise
                    events.info('write_ok partition=%s key=%s', partition, key)
                    acknowledged.put((partition, key))
                    with keys_lock:
                        written_keys.append((partition, key))
                        progress['writes'] += 1
                count += 1
                seq += 1
                if before_background_action is not None and seq == writes_per_thread:
                    writers_ready.wait()
                    if not writers_resume.wait(timeout=RETRY_TIMEOUT):
                        raise AssertionError('Writers were not resumed after the baseline snapshot')
                stopped.wait(0.1)
        return count

    def read_loop(worker):
        count = 0
        with grpc.insecure_channel(endpoints[(writers + worker) % len(endpoints)]) as channel:
            stub = KeyValueServiceStub(channel)
            while not stopped.is_set():
                item = acknowledged.get()
                if item is None:
                    break
                partition, key = item
                request = kv_api.ReadRequest(path=path, partition_id=partition, key=key)
                events.info('read partition=%s key=%s', partition, key)
                try:
                    response = invoke(lambda: stub.Read(request, timeout=RPC_TIMEOUT), stopped,
                                      retry_response=reconnect_grace.retry_response)
                    if response.operation.status != StatusIds.SUCCESS:
                        raise AssertionError(
                            f'Read failed for partition {partition}, key {key}: status={response.operation.status}'
                        )
                    result = kv_api.ReadResult()
                    if not response.operation.result.Unpack(result) or result.is_overrun:
                        raise AssertionError(f'Invalid read result for partition {partition}, key {key}')
                    if result.value != make_value(key, value_size):
                        raise AssertionError(f'Corrupted value for partition {partition}, key {key}')
                except BaseException:
                    events.exception('read_failed partition=%s key=%s', partition, key)
                    raise
                events.info('read_ok partition=%s key=%s', partition, key)
                count += 1
                with keys_lock:
                    progress['validated_reads'] += 1
                load_started.set()
        return count

    events.info('WORKLOAD_START writers=%s readers=%s partitions=%s writes_per_thread=%s value_size=%s',
                writers, readers, partitions, writes_per_thread, value_size)
    with ThreadPoolExecutor(max_workers=writers + readers + 1, thread_name_prefix='mirror3dc-kv') as executor:
        read_tasks = [executor.submit(read_loop, worker) for worker in range(readers)]
        write_tasks = [executor.submit(write_loop, worker) for worker in range(writers)]
        worker_tasks.extend(read_tasks + write_tasks)
        action_task = executor.submit(run_action) if background_action is not None else None
        try:
            if action_task is not None:
                action_task.result()
            written = sum(task.result() for task in write_tasks)
            # Re-read every acknowledged write after the background action has completed.
            events.info('WORKLOAD_REREAD keys=%s', written)
            for item in written_keys:
                acknowledged.put(item)
            for _ in range(readers):
                acknowledged.put(None)
            validated = sum(task.result() for task in read_tasks)
            if written < writers * writes_per_thread or validated != 2 * written:
                raise AssertionError(f'Incomplete KV workload: {written} writes, {validated} validated reads')
            events.info('WORKLOAD_PASSED writes=%s validated_reads=%s value_size=%s bsqueue_retries=%s',
                        written, validated, value_size, reconnect_grace.retries)
        except BaseException:
            events.exception('WORKLOAD_FAILED progress=%s', handle.stats())
            raise
        finally:
            stopped.set()
            writers_ready.abort()
            writers_resume.set()
            for _ in range(readers):
                acknowledged.put(None)
    return written
