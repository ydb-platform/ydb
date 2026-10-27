from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack, contextmanager
import hashlib
import logging
from queue import Queue
import threading
import time

import grpc

from ydb.public.api.grpc.ydb_keyvalue_v1_pb2_grpc import KeyValueServiceStub
from ydb.public.api.protos import ydb_keyvalue_pb2 as kv_api
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds


logger = logging.getLogger(__name__)
RPC_TIMEOUT = 30
RETRY_TIMEOUT = 120


def invoke(request, stopped):
    deadline = time.monotonic() + RETRY_TIMEOUT
    while True:
        try:
            response = request()
            if response.operation.status not in (
                StatusIds.UNAVAILABLE, StatusIds.OVERLOADED, StatusIds.TIMEOUT,
                StatusIds.ABORTED, StatusIds.UNDETERMINED,
            ):
                return response
            error = str(response)
        except grpc.RpcError as exc:
            if exc.code() not in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED):
                raise
            error = str(exc)
        if time.monotonic() >= deadline or stopped.wait(0.1):
            raise AssertionError(f'KV request did not recover: {error}')


def make_value(key, size):
    return hashlib.shake_256(key.encode()).digest(size)


def run_kv_workload(endpoints, path, partitions=3, writers=3, readers=3, writes_per_thread=100, value_size=1024,
                    background_action=None, before_background_action=None):
    """Write unique keys and read every acknowledged value in parallel."""
    acknowledged = Queue()
    stopped = threading.Event()
    load_started = threading.Event()
    action_finished = threading.Event()
    writers_ready = threading.Barrier(writers + 1, timeout=RETRY_TIMEOUT)
    writers_resume = threading.Event()
    if before_background_action is not None and background_action is None:
        raise ValueError('before_background_action requires background_action')
    written_keys = []
    keys_lock = threading.Lock()
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
            if not load_started.wait(timeout=RETRY_TIMEOUT):
                raise AssertionError('KV workload did not start before the background action')
            if before_background_action is not None:
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
                    response = invoke(lambda: stub.ExecuteTransaction(request, timeout=RPC_TIMEOUT), stopped)
                    if response.operation.status != StatusIds.SUCCESS:
                        raise AssertionError(f'Write failed for partition {partition}, key {key}: {response}')
                    acknowledged.put((partition, key))
                    with keys_lock:
                        written_keys.append((partition, key))
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
                response = invoke(lambda: stub.Read(request, timeout=RPC_TIMEOUT), stopped)
                if response.operation.status != StatusIds.SUCCESS:
                    raise AssertionError(f'Read failed for partition {partition}, key {key}: {response}')
                result = kv_api.ReadResult()
                if not response.operation.result.Unpack(result) or result.is_overrun:
                    raise AssertionError(f'Invalid read result for partition {partition}, key {key}: {response}')
                if result.value != make_value(key, value_size):
                    raise AssertionError(f'Corrupted value for partition {partition}, key {key}: {result.value[:64]!r}')
                count += 1
                load_started.set()
        return count

    with ThreadPoolExecutor(max_workers=writers + readers + 1, thread_name_prefix='mirror3dc-kv') as executor:
        read_tasks = [executor.submit(read_loop, worker) for worker in range(readers)]
        write_tasks = [executor.submit(write_loop, worker) for worker in range(writers)]
        action_task = executor.submit(run_action) if background_action is not None else None
        try:
            written = sum(task.result() for task in write_tasks)
            if action_task is not None:
                action_task.result()
            # Re-read every acknowledged write after the background action has completed.
            for item in written_keys:
                acknowledged.put(item)
            for _ in range(readers):
                acknowledged.put(None)
            validated = sum(task.result() for task in read_tasks)
            if written < writers * writes_per_thread or validated != 2 * written:
                raise AssertionError(f'Incomplete KV workload: {written} writes, {validated} validated reads')
            logger.info('KV workload completed: %d writes, %d validated reads, value size %d', written, validated, value_size)
        finally:
            stopped.set()
            for _ in range(readers):
                acknowledged.put(None)
