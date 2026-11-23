from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
import logging
import random
import threading

import grpc

from ydb.public.api.grpc.ydb_keyvalue_v1_pb2_grpc import KeyValueServiceStub
from ydb.public.api.protos import ydb_keyvalue_pb2 as kv_api
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds
from ydb.tests.functional.mirror3dc.kv_workload import RPC_TIMEOUT, RETRY_TIMEOUT, invoke, make_value


logger = logging.getLogger(__name__)


@dataclass
class KeyEntry:
    key: str
    active: bool = True
    lock: object = field(default_factory=threading.Lock)


class KvChurnWorkload:
    """Keep target_keys..target_keys+batch_size live keys while validating reads.

    One mutator alternates batches of unique writes and deletions. A per-key lock
    serializes readers with deletion, while unrelated keys remain concurrent.
    Every key is validated before deletion or during the final live-key check.
    """

    OPERATION_INTERVAL = 0.01

    def __init__(self, endpoints, path, *, seed, target_keys=100, batch_size=20, readers=3, value_size=1024, event_logger=None):
        self.endpoints = endpoints
        self.path = path
        self.seed = seed
        self.target_keys = target_keys
        self.batch_size = batch_size
        self.readers = readers
        self.value_size = value_size
        self.current = {}
        self.lock = threading.Lock()
        self.ready = threading.Event()
        self.stopped = threading.Event()
        self.failed = threading.Event()
        self.writes = 0
        self.deletes = 0
        self.reads = 0
        self.high_watermark = 0
        self.executor = None
        self.tasks = []
        self.events = event_logger or logger

    def stats(self):
        with self.lock:
            return dict(writes=self.writes, deletes=self.deletes, reads=self.reads,
                        live=len(self.current), max_live=self.high_watermark)

    def check_errors(self):
        for task in self.tasks:
            if task.done():
                task.result()

    def _guard(self, action, *args):
        try:
            action(*args)
        except BaseException:
            self.events.exception('WORKER_FAILED action=%s', action.__name__)
            self.failed.set()
            self.stopped.set()
            raise

    def _invoke(self, request):
        # A deliberately stopped server can cancel an already accepted RPC.
        return invoke(request, self.failed, retryable_grpc_codes=(
            grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED, grpc.StatusCode.CANCELLED,
        ))

    def _execute(self, stub, request, key):
        response = self._invoke(lambda: stub.ExecuteTransaction(request, timeout=RPC_TIMEOUT))
        if response.operation.status != StatusIds.SUCCESS:
            raise AssertionError(f'KV mutation failed: key={key}: {response}')

    def _read(self, stub, entry):
        self.events.info('read key=%s', entry.key)
        request = kv_api.ReadRequest(path=self.path, partition_id=0, key=entry.key)
        response = self._invoke(lambda: stub.Read(request, timeout=RPC_TIMEOUT))
        if response.operation.status != StatusIds.SUCCESS:
            raise AssertionError(f'KV read failed: {entry.key}: {response}')
        result = kv_api.ReadResult()
        if not response.operation.result.Unpack(result) or result.is_overrun:
            raise AssertionError(f'Invalid KV read result: {entry.key}: {response}')
        if result.value != make_value(entry.key, self.value_size):
            raise AssertionError(f'Corrupted KV value: {entry.key}')
        with self.lock:
            self.reads += 1
        self.events.info('read_ok key=%s', entry.key)

    def _write(self, stub):
        entry = KeyEntry(key=str(self.writes))
        self.events.info('write key=%s', entry.key)
        request = kv_api.ExecuteTransactionRequest(path=self.path, partition_id=0)
        request.commands.add().write.CopyFrom(kv_api.ExecuteTransactionRequest.Command.Write(
            key=entry.key, value=make_value(entry.key, self.value_size), storage_channel=2,
        ))
        self._execute(stub, request, entry.key)
        with self.lock:
            self.current[entry.key] = entry
            self.writes += 1
            self.high_watermark = max(self.high_watermark, len(self.current))
            if len(self.current) > self.target_keys + self.batch_size:
                raise AssertionError('KV working set exceeded its configured bound')
        self.events.info('write_ok key=%s', entry.key)

    def _delete(self, stub):
        with self.lock:
            entry = next(iter(self.current.values()))
        with entry.lock:
            self._read(stub, entry)
            self.events.info('delete key=%s', entry.key)
            request = kv_api.ExecuteTransactionRequest(path=self.path, partition_id=0)
            bounds = request.commands.add().delete_range.range
            bounds.from_key_inclusive = entry.key
            bounds.to_key_inclusive = entry.key
            self._execute(stub, request, entry.key)
            with self.lock:
                del self.current[entry.key]
                entry.active = False
                self.deletes += 1
            self.events.info('delete_ok key=%s', entry.key)

    def _mutate(self):
        with grpc.insecure_channel(self.endpoints[0]) as channel:
            stub = KeyValueServiceStub(channel)
            for _ in range(self.target_keys):
                if self.stopped.is_set():
                    return
                self._write(stub)
            self.ready.set()
            while not self.stopped.is_set():
                for action in (self._write, self._delete):
                    for _ in range(self.batch_size):
                        if self.stopped.is_set():
                            return
                        action(stub)
                        self.stopped.wait(self.OPERATION_INTERVAL)

    def _read_loop(self, worker):
        rng = random.Random(self.seed + worker + 1)
        with grpc.insecure_channel(self.endpoints[(worker + 1) % len(self.endpoints)]) as channel:
            stub = KeyValueServiceStub(channel)
            while not self.stopped.is_set():
                with self.lock:
                    entries = list(self.current.values())
                if entries:
                    entry = rng.choice(entries)
                    with entry.lock:
                        if entry.active:
                            self._read(stub, entry)
                self.stopped.wait(self.OPERATION_INTERVAL)

    def __enter__(self):
        self.events.info('WORKLOAD_START target_keys=%s batch_size=%s readers=%s',
                         self.target_keys, self.batch_size, self.readers)
        self.executor = ThreadPoolExecutor(max_workers=self.readers + 1, thread_name_prefix='mirror3dc-churn')
        self.tasks = [self.executor.submit(self._guard, self._mutate)]
        self.tasks += [self.executor.submit(self._guard, self._read_loop, worker) for worker in range(self.readers)]
        try:
            # Poll errors as well as readiness so a failed initial fill cannot hang.
            for _ in range(RETRY_TIMEOUT * 10):
                self.check_errors()
                if self.ready.wait(0.1):
                    return self
            raise AssertionError('KV churn did not fill its initial working set')
        except BaseException:
            self.stopped.set()
            self.executor.shutdown(wait=True)
            raise

    def __exit__(self, exc_type, exc_value, traceback):
        self.stopped.set()
        self.executor.shutdown(wait=True)
        self.events.info('WORKLOAD_STOPPED')
        logger.info('KV churn finished: %s', self.stats())
        if exc_type is None:
            self.check_errors()
            # The mutation thread has stopped; validate every remaining key.
            with grpc.insecure_channel(self.endpoints[0]) as channel:
                stub = KeyValueServiceStub(channel)
                for entry in self.current.values():
                    self._read(stub, entry)
            stats = self.stats()
            if not (self.target_keys <= stats['live'] <= self.target_keys + self.batch_size):
                raise AssertionError(f'Unexpected KV working set size: {stats}')
            if not stats['deletes'] or not stats['reads']:
                raise AssertionError(f'Incomplete churn workload: {stats}')
            logger.info('KV churn final validation passed: %s', stats)
