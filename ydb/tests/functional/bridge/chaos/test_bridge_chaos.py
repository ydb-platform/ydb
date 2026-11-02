# -*- coding: utf-8 -*-
"""
Randomized life of the cluster becoming the bridged one.

The ordinary 8-node cluster starts with config v1, it is migrated to distconf, converted into the single-pile bridged
cluster, and gets the second pile; every restart on the way is kill -9 of a node. Then, until the deadline, random pile
operations (switchover, failover, disconnection and rejoin, removal of the pile and its addition again) mix with random
failures (one or two nodes of a pile, the node running BSC, more nodes than the failure model allows, the whole cluster,
a rolling restart of a pile). Finally the cluster keeps only one of its piles, the original (inline) one or the added one.
KV and table workloads run all the time.

The test fails when an acknowledged write is lost or reads back with another value, when a node dies without being
killed, or when an operation doesn't finish in time once the failures are healed.

Environment:
    BRIDGE_CHAOS_SEED -- seed of the random choices of the run; it is logged, so the run can be repeated
    BRIDGE_CHAOS_SECONDS -- length of the random phase, 900 by default
    BRIDGE_CHAOS_READD -- hosts of the pile added again after its removal: fresh (formatted ones, the default), same,
        or random; the same hosts never get synchronized yet, as their cluster state history diverges from the cluster's
    BRIDGE_CHAOS_TABLE -- run the table workload, 1 by default
    BRIDGE_CHAOS_PDISK_DIR -- directory for the drive files (it needs O_DIRECT, so not tmpfs), the test's temporary
        directory by default; on a busy rotational disk the drives stall long enough to be declared broken
    BRIDGE_CHAOS_LOG -- extra log levels of the nodes, as COMPONENT=LEVEL[,COMPONENT=LEVEL...], e.g. KEYVALUE=DEBUG
"""
import hashlib
import logging
import os
import random
import socket
import tempfile
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import grpc
import requests
import yaml
import ydb

from ydb.public.api.grpc import ydb_config_v1_pb2_grpc as config_grpc
from ydb.public.api.grpc import ydb_keyvalue_v1_pb2_grpc as kv_grpc
from ydb.public.api.grpc.draft import ydb_dynamic_config_v1_pb2_grpc as dynconfig_grpc
from ydb.public.api.protos import ydb_config_pb2 as config_api
from ydb.public.api.protos import ydb_keyvalue_pb2 as kv_api
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge_api
from ydb.public.api.protos.draft import ydb_dynamic_config_pb2 as dynconfig_api
from ydb.public.api.protos.ydb_bridge_common_pb2 import PileState
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

from ydb.tests.library.clients.kikimr_bridge_client import BridgeClient
from ydb.tests.library.clients.kikimr_keyvalue_client import KeyValueClient
from ydb.tests.library.common.types import Erasure
from ydb.tests.library.harness import daemon
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.library.harness.util import LogLevels

logger = logging.getLogger(__name__)

PRIMARY = PileState.PRIMARY
PROMOTED = PileState.PROMOTED
SYNC = PileState.SYNCHRONIZED
UNSYNC = PileState.NOT_SYNCHRONIZED
SUSPENDED = PileState.SUSPENDED
DISCONNECTED = PileState.DISCONNECTED
INACTIVE = PileState.INACTIVE

ORIGINAL_PILE = "r1"
ADDED_PILE = "r2"
PILE_NODES = 8  # block-4-2 needs 8 fail domains in every pile

KV_PATH = "/Root/mydb/chaos_kv"
KV_PARTITIONS = 4
KV_HOT_KEY = "hot"  # overwritten all the time, unlike the other keys written once
KV_VALUE_SIZES = (64, 1024, 16 * 1024, 128 * 1024, 520 * 1024)
KV_VALUE_SIZE_WEIGHTS = (3, 3, 2, 1, 0.3)
TABLE_PATH = "/Root/chaos_rows"

TOKEN = "root@builtin"
RPC_TIMEOUT = 10
RECOVERY_TIMEOUT = 300  # for the cluster to serve again once the failures are healed
SYNC_TIMEOUT = 600  # for the pile to get synchronized
TRANSIENT_STATUSES = (StatusIds.INTERNAL_ERROR, StatusIds.UNAVAILABLE, StatusIds.TIMEOUT, StatusIds.OVERLOADED,
                      StatusIds.UNDETERMINED, StatusIds.SESSION_EXPIRED)
GRPC_OPTIONS = [
    ('grpc.max_receive_message_length', 64 * 10 ** 6),
    ('grpc.max_send_message_length', 64 * 10 ** 6),
]


class ChaosFailure(Exception):
    """Loss of acknowledged data, unexpected death of a node, operation not finished in time: the test fails at once."""


class NotYet(Exception):
    """The condition being waited for doesn't hold yet."""


def state_names(states):
    return {name: PileState.State.Name(state) for name, state in sorted(states.items())}


def unpack(response, result):
    response.operation.result.Unpack(result)
    return result


def ranges(numbers):
    """Sorted numbers as a compact list of ranges, like 1-5,7,9-10."""
    parts = []
    for number in numbers:
        if parts and parts[-1][1] + 1 == number:
            parts[-1][1] = number
        else:
            parts.append([number, number])
    return ",".join(str(a) if a == b else "{}-{}".format(a, b) for a, b in parts)


########################################################################################################################
# KV workload


def value_header(key, seq, size):
    return "{}:{}:{};".format(key, seq, size).encode()


def make_value(key, seq, size):
    header = value_header(key, seq, size)
    return header + hashlib.shake_256(header).digest(size)


def value_seq(key, value):
    """Sequence number of the write the value comes from; the value has to be exactly the one this write has written."""
    header, separator, _ = value.partition(b";")
    try:
        written_key, seq, size = header.decode().rsplit(":", 2)
        seq, size = int(seq), int(size)
    except (UnicodeDecodeError, ValueError):
        raise ChaosFailure("corrupted value of KV key {}: {!r}".format(key, value[:64]))
    if not separator or written_key != key or make_value(key, seq, size) != value:
        raise ChaosFailure("KV key {} has value {!r} of size {}, which is not the one written".format(
            key, value[:64], len(value)))
    return seq


class KvAccess(object):
    """KV requests with deadlines through the nodes chosen by the caller."""

    def __init__(self):
        self.channels = {}

    def stub(self, endpoint):
        channel = self.channels.get(endpoint)
        if channel is None:
            channel = self.channels[endpoint] = grpc.insecure_channel(endpoint, options=GRPC_OPTIONS)
        return kv_grpc.KeyValueServiceStub(channel)

    def write(self, endpoint, partition, key, value):
        request = kv_api.ExecuteTransactionRequest(path=KV_PATH, partition_id=partition)
        write = request.commands.add().write
        write.key = key
        write.value = value
        return self.stub(endpoint).ExecuteTransaction(request, timeout=RPC_TIMEOUT).operation.status

    def read(self, endpoint, partition, key):
        request = kv_api.ReadRequest(path=KV_PATH, partition_id=partition, key=key)
        response = self.stub(endpoint).Read(request, timeout=RPC_TIMEOUT)
        if response.operation.status != StatusIds.SUCCESS:
            return response.operation.status, None
        result = unpack(response, kv_api.ReadResult())
        if result.is_overrun:
            return StatusIds.UNAVAILABLE, None
        return StatusIds.SUCCESS, result.value

    def list_keys(self, endpoint, partition):
        """Sizes of the values of all the keys of the partition."""
        keys = {}
        last_key = None
        while True:
            request = kv_api.ListRangeRequest(path=KV_PATH, partition_id=partition)
            if last_key is not None:
                request.range.from_key_exclusive = last_key
            response = self.stub(endpoint).ListRange(request, timeout=RPC_TIMEOUT)
            if response.operation.status != StatusIds.SUCCESS:
                return response.operation.status, None
            result = unpack(response, kv_api.ListRangeResult())
            for item in result.key:
                keys[item.key] = item.value_size
            if not result.is_overrun or not result.key:
                return StatusIds.SUCCESS, keys
            last_key = result.key[-1].key

    def close(self):
        for channel in self.channels.values():
            channel.close()


class KvWorkload(object):
    """
    Writes values of unique keys and overwrites the hot key of every partition through random nodes, reading
    acknowledged values back meanwhile. The write is acknowledged when it succeeds; its outcome is unknown otherwise.
    """

    def __init__(self, endpoints, seed):
        self.endpoints = endpoints  # returns endpoints of the nodes requests may go through
        self.seed = seed
        self.lock = threading.Lock()
        self.seq = 0
        self.acked = {}  # (partition, key) -> (seq, size) of the acknowledged write of the unique key
        self.acked_keys = []  # the same keys, for random choice
        self.uncertain = {}  # (partition, key) -> seq of the write of the unique key with unknown outcome
        self.hot_acked = [None] * KV_PARTITIONS  # seq of the latest acknowledged write of the hot key
        self.hot_acked_seqs = [set() for _ in range(KV_PARTITIONS)]
        self.hot_written = [set() for _ in range(KV_PARTITIONS)]  # seqs of all the writes of the hot key
        self.failures = []
        self.num_writes = 0
        self.num_reads = 0
        self.stop_event = threading.Event()
        self.threads = []

    def start(self):
        for loop in (self.write_loop, self.read_loop):
            thread = threading.Thread(target=self.run, args=(loop,), name=loop.__name__, daemon=True)
            thread.start()
            self.threads.append(thread)

    def stop(self):
        self.stop_event.set()
        for thread in self.threads:
            thread.join(timeout=2 * RPC_TIMEOUT + 10)
        self.threads = []

    def run(self, loop):
        kv = KvAccess()
        try:
            loop(kv)
        except Exception as e:
            logger.exception("KV workload thread has failed")
            with self.lock:
                self.failures.append(e if isinstance(e, ChaosFailure) else ChaosFailure("KV workload: {!r}".format(e)))
        finally:
            kv.close()

    def write(self, kv, endpoint, partition, hot, size):
        """Writes the hot key or a new unique one, records the outcome; returns whether the write is acknowledged."""
        with self.lock:
            self.seq += 1
            seq = self.seq
            key = KV_HOT_KEY if hot else "u{:09d}".format(seq)
            if hot:
                self.hot_written[partition].add(seq)
        try:
            acked = kv.write(endpoint, partition, key, make_value(key, seq, size)) == StatusIds.SUCCESS
        except grpc.RpcError:
            acked = False
        with self.lock:
            self.num_writes += 1
            if hot:
                if acked:
                    self.hot_acked[partition] = seq
                    self.hot_acked_seqs[partition].add(seq)
            elif acked:
                self.acked[partition, key] = (seq, size)
                self.acked_keys.append((partition, key))
            else:
                self.uncertain[partition, key] = seq
        return acked

    def write_loop(self, kv):
        rng = random.Random(self.seed)
        while not self.stop_event.is_set():
            endpoints = self.endpoints()
            if endpoints:
                partition = rng.randrange(KV_PARTITIONS)
                hot = rng.random() < 0.25
                size = rng.choices(KV_VALUE_SIZES, KV_VALUE_SIZE_WEIGHTS)[0]
                self.write(kv, rng.choice(endpoints), partition, hot, size)
            self.stop_event.wait(0.1)

    def read_loop(self, kv):
        rng = random.Random(self.seed + 1)
        while not self.stop_event.is_set():
            endpoints = self.endpoints()
            with self.lock:
                acked_keys = len(self.acked_keys)
                if rng.random() < 0.3:
                    partition = rng.randrange(KV_PARTITIONS)
                    key, floor = KV_HOT_KEY, self.hot_acked[partition]
                elif acked_keys:
                    (partition, key), floor = self.acked_keys[rng.randrange(acked_keys)], None
                else:
                    key = None
            if endpoints and key is not None:
                try:
                    status, value = kv.read(rng.choice(endpoints), partition, key)
                except grpc.RpcError:
                    status, value = None, None
                failure = self.check_read(partition, key, status, value, floor)
                with self.lock:
                    self.num_reads += 1
                    if failure:
                        logger.error("KV read check has failed: %s", failure)
                        self.failures.append(failure)
            self.stop_event.wait(0.05)

    def check_read(self, partition, key, status, value, floor):
        """
        Returns ChaosFailure when the read contradicts the writes acknowledged before it has been sent (floor is the
        latest acknowledged write of the hot key then), None otherwise; errors are fine, the cluster may not serve.
        """
        hot = key == KV_HOT_KEY
        if status == StatusIds.NOT_FOUND:
            if not hot or floor is not None:
                return ChaosFailure("acknowledged KV key {} of partition {} is not found".format(key, partition))
            return None
        elif status != StatusIds.SUCCESS:
            return None
        try:
            seq = value_seq(key, value)
        except ChaosFailure as e:
            return e
        with self.lock:
            if not hot:
                expected = self.acked.get((partition, key), (self.uncertain.get((partition, key)), None))[0]
                if seq != expected:
                    return ChaosFailure("KV key {} of partition {} has value of write {}, but write {} has made it".format(
                        key, partition, seq, expected))
            elif seq not in self.hot_written[partition]:
                return ChaosFailure("hot KV key of partition {} has value of write {}, which has been made to another"
                                    " partition".format(partition, seq))
            elif floor is not None and seq < floor and seq in self.hot_acked_seqs[partition]:
                return ChaosFailure("hot KV key of partition {} has got back to acknowledged write {} from acknowledged"
                                    " write {}".format(partition, seq, floor))
        return None

    def probe(self):
        """Writes a new key to every partition; returns whether every write is acknowledged."""
        kv = KvAccess()
        try:
            for partition in range(KV_PARTITIONS):
                endpoints = self.endpoints()
                if not endpoints or not self.write(kv, random.choice(endpoints), partition, False, 1024):
                    raise NotYet("KV partition {} doesn't take writes".format(partition))
            return True
        finally:
            kv.close()

    def verify(self, full, timeout):
        """
        Checks that every key acknowledged so far is there with its value; reads every value when full, a sample of
        them otherwise.
        """
        with self.lock:
            acked = dict(self.acked)
            hot_acked = list(self.hot_acked)

        def call(request):
            last = None
            deadline = time.monotonic() + timeout
            while time.monotonic() < deadline:
                endpoints = self.endpoints()
                if endpoints:
                    kv = KvAccess()
                    try:
                        status, result = request(kv, random.choice(endpoints))
                    except grpc.RpcError as e:
                        status, result = None, e
                    finally:
                        kv.close()
                    if status in (StatusIds.SUCCESS, StatusIds.NOT_FOUND):
                        return status, result
                    last = (status, result)
                time.sleep(1)
            raise ChaosFailure("KV is not serving for {} seconds, last response: {}".format(timeout, last))

        for partition in range(KV_PARTITIONS):
            status, keys = call(lambda kv, endpoint: kv.list_keys(endpoint, partition))
            assert status == StatusIds.SUCCESS
            for (key_partition, key), (seq, size) in acked.items():
                length = len(value_header(key, seq, size)) + size
                if key_partition != partition:
                    continue
                elif key not in keys:
                    raise ChaosFailure("acknowledged KV key {} of partition {} is not listed".format(key, partition))
                elif keys[key] != length:
                    raise ChaosFailure("acknowledged KV key {} of partition {} has value of size {}, {} has been"
                                       " written".format(key, partition, keys[key], length))
            if hot_acked[partition] is not None and KV_HOT_KEY not in keys:
                raise ChaosFailure("acknowledged hot KV key of partition {} is not listed".format(partition))

        items = [(partition, key, None) for partition, key in acked]
        if not full:
            items = random.sample(items, min(len(items), 200))
        items += [(partition, KV_HOT_KEY, floor) for partition, floor in enumerate(hot_acked) if floor is not None]

        def check(item):
            partition, key, floor = item
            status, value = call(lambda kv, endpoint: kv.read(endpoint, partition, key))
            failure = self.check_read(partition, key, status, value, floor)
            if failure:
                raise failure

        with ThreadPoolExecutor(max_workers=8) as executor:
            list(executor.map(check, items))
        logger.info("KV verified: %d keys listed, %d values read, %d writes made, %d reads made by workload",
                    len(acked), len(items), self.num_writes, self.num_reads)


########################################################################################################################
# Table workload


def stop_driver(driver):
    # the same workaround of SDK lock inversion as bridge/common.py has: drivers without discovery need cleanup() only
    cache = driver._store

    def cleanup():
        for connection in cache.values():
            connection.close()

    cache.cleanup = cleanup
    driver.stop()


class TableClient(object):
    """Session pool of the driver pinned to the node."""

    def __init__(self, endpoint):
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint="grpc://" + endpoint, database="/Root", disable_discovery=True))
        self.pool = ydb.SessionPool(self.driver, size=1)

    def execute(self, query, scheme=False):
        settings = ydb.BaseRequestSettings().with_timeout(RPC_TIMEOUT)

        def operation(session):
            if scheme:
                return session.execute_scheme(query, settings=settings)
            return session.transaction(ydb.SerializableReadWrite()).execute(query, commit_tx=True, settings=settings)

        return self.pool.retry_operation_sync(operation, retry_settings=ydb.RetrySettings(
            max_retries=0, get_session_client_timeout=5))

    def close(self):
        try:
            self.pool.stop(timeout=5)
        finally:
            stop_driver(self.driver)


class TableWorkload(object):
    """Upserts rows with new ids through random nodes; the upsert is acknowledged when it succeeds."""

    def __init__(self, endpoints, seed):
        self.endpoints = endpoints
        self.seed = seed
        self.lock = threading.Lock()
        self.next_id = 0
        self.acked = set()
        self.failures = []
        self.stop_event = threading.Event()
        self.thread = None

    @staticmethod
    def row_value(row_id):
        return hashlib.sha256(b"row %d" % row_id).hexdigest()

    def run_with_client(self, action, timeout=None):
        """
        Runs action(client) through a random node, retrying on errors until timeout; without timeout it makes one
        attempt, and an error makes NotYet.
        """
        deadline = time.monotonic() + (timeout or 0)
        while True:
            endpoints = self.endpoints()
            if endpoints:
                client = None
                try:
                    client = TableClient(random.choice(endpoints))
                    return action(client)
                except ChaosFailure:
                    raise
                except Exception as e:
                    if timeout is None:
                        raise NotYet("table request has failed: {!r}".format(e))
                    logger.debug("Table request has failed: %r", e)
                finally:
                    if client:
                        client.close()
            elif timeout is None:
                raise NotYet("no node to send table request to")
            if time.monotonic() >= deadline:
                raise ChaosFailure("table is not serving for {} seconds".format(timeout))
            time.sleep(1)

    def create(self, timeout):
        # the request is repeated when its response is lost
        self.run_with_client(lambda client: client.execute(
            "CREATE TABLE IF NOT EXISTS `{}` (id Uint64, value Utf8, PRIMARY KEY (id));".format(TABLE_PATH),
            scheme=True), timeout)

    def write_batch(self, client, size):
        with self.lock:
            ids = range(self.next_id, self.next_id + size)
            self.next_id += size
        client.execute("UPSERT INTO `{}` (id, value) VALUES {};".format(TABLE_PATH, ",".join(
            '({}u, "{}")'.format(row_id, self.row_value(row_id)) for row_id in ids)))
        with self.lock:
            self.acked.update(ids)

    def start(self):
        self.thread = threading.Thread(target=self.write_loop, name="table_writer", daemon=True)
        self.thread.start()

    def stop(self):
        self.stop_event.set()
        if self.thread:
            self.thread.join(timeout=4 * RPC_TIMEOUT)
            self.thread = None

    def write_loop(self):
        client = None
        try:
            while not self.stop_event.is_set():
                endpoints = self.endpoints()
                if endpoints:
                    try:
                        client = client or TableClient(random.choice(endpoints))
                        self.write_batch(client, 10)
                    except Exception as e:
                        # the outcome of the upsert is unknown, its rows may be there or not
                        logger.debug("Table write has failed: %r", e)
                        if client:
                            client.close()
                        client = None
                self.stop_event.wait(0.5)
        except Exception as e:
            logger.exception("Table workload thread has failed")
            with self.lock:
                self.failures.append(ChaosFailure("table workload: {!r}".format(e)))
        finally:
            if client:
                client.close()

    def probe(self):
        self.run_with_client(lambda client: self.write_batch(client, 1))
        return True

    def verify(self, timeout):
        with self.lock:
            acked = set(self.acked)
        present = set()

        count = []

        def read_all(client):
            present.clear()
            count.clear()
            condition = ""
            while True:
                result = client.execute("SELECT id, value FROM `{}`{} ORDER BY id LIMIT 500;".format(TABLE_PATH, condition))
                rows = result[0].rows
                for row in rows:
                    if row.value != self.row_value(row.id):
                        raise ChaosFailure("row {} has value {}, which is not the one written".format(row.id, row.value))
                    present.add(row.id)
                if len(rows) < 500 and not result[0].truncated:
                    break
                condition = " WHERE id > {}ul".format(rows[-1].id)
            count.append(client.execute("SELECT COUNT(*) AS rows FROM `{}`;".format(TABLE_PATH))[0].rows[0].rows)

        self.run_with_client(read_all, timeout)
        missing = sorted(acked - present)
        if missing:
            raise ChaosFailure("{} of {} acknowledged rows are missing ({} rows read, COUNT(*) {}): {}".format(
                len(missing), len(acked), len(present), count[0], ranges(missing)))
        logger.info("Table verified: %d acknowledged rows, %d rows present, COUNT(*) %d", len(acked), len(present),
                    count[0])


########################################################################################################################
# Test


class TestBridgeChaos(object):

    def setup_method(self):
        seed = os.getenv("BRIDGE_CHAOS_SEED")
        self.seed = int(seed) if seed else random.randrange(1 << 32)
        logger.info("BRIDGE_CHAOS_SEED=%d", self.seed)
        self.rng = random.Random(self.seed)  # chaos decisions only, so that the run is repeated by the seed
        self.chaos_seconds = float(os.getenv("BRIDGE_CHAOS_SECONDS", "900"))
        self.readd_mode = os.getenv("BRIDGE_CHAOS_READD", "fresh")
        assert self.readd_mode in ("fresh", "same", "random"), self.readd_mode
        log_configs = {
            'BS_NODE': LogLevels.DEBUG,
            'BS_BRIDGE_SYNC': LogLevels.DEBUG,
            'BS_CONTROLLER': LogLevels.INFO,
            'BS_PROXY_BRIDGE': LogLevels.NOTICE,
        }
        for item in filter(None, os.getenv("BRIDGE_CHAOS_LOG", "").split(",")):
            component, level = item.split("=")
            log_configs[component] = LogLevels[level]

        self.configurator = KikimrConfigGenerator(
            Erasure.BLOCK_4_2,
            nodes=PILE_NODES,
            use_in_memory_pdisks=False,
            pdisk_store_path=os.getenv("BRIDGE_CHAOS_PDISK_DIR") or None,
            use_self_management=False,
            use_config_store=False,
            separate_node_configs=True,
            simple_config=True,
            explicit_hosts_and_host_configs=True,
            extra_grpc_services=['bridge', 'config'],
            additional_log_configs=log_configs,
        )
        self.cluster = KiKiMR(configurator=self.configurator)
        self.cluster.start()

        self.piles = {ORIGINAL_PILE: sorted(self.cluster.nodes)}
        self.pdisks = {}  # node id -> [(path, size)] of its drives
        for pdisk in self.configurator.pdisks_info:
            self.pdisks.setdefault(pdisk['node_id'], []).append((pdisk['pdisk_path'], pdisk['disk_size']))
        self.pdisk_files = []  # temporary files of the drives of the nodes added later, kept until the end
        self.states = None  # pile states the cluster has to be in, None until it gets bridged
        self.down = set()  # nodes stopped on purpose
        self.held_ports = {}  # node id -> listening sockets keeping the ports of the stopped node, see hold_ports()
        self.incarnations = {}  # node id -> number of its restarts by the test
        self.endpoints = ()
        self.refresh_endpoints()
        self.kv = KvWorkload(lambda: self.endpoints, self.seed)
        self.table = TableWorkload(lambda: self.endpoints, self.seed) if os.getenv("BRIDGE_CHAOS_TABLE", "1") == "1" \
            else None

    def teardown_method(self):
        try:
            for workload in self.workloads():
                workload.stop()
        finally:
            for node_id in list(self.held_ports):
                self.release_ports(node_id)
            self.cluster.stop()

    def workloads(self):
        return [workload for workload in (self.kv, self.table) if workload]

    ####################################################################################################################
    # Waiting and checking

    def check_health(self):
        for node_id, node in self.cluster.nodes.items():
            if node_id not in self.down and not node.is_alive():
                raise ChaosFailure("node {} has died without being killed{}".format(node_id, self.stderr_tail(node)))
        for workload in self.workloads():
            if workload.failures:
                raise workload.failures[0]

    @staticmethod
    def stderr_tail(node, lines=40):
        try:
            with open(node.stderr_file_name, errors="replace") as f:
                return ", stderr:\n" + "".join(f.readlines()[-lines:])
        except (OSError, TypeError):
            return ""

    def eventually(self, action, what, timeout):
        """Repeats the action until it returns something true; NotYet and request errors make it repeat."""
        deadline = time.monotonic() + timeout
        last_error = None
        while True:
            self.check_health()
            try:
                result = action()
                if result:
                    return result
            except (NotYet, grpc.RpcError, ydb.Error) as e:
                last_error = e
            if time.monotonic() >= deadline:
                raise ChaosFailure("{} has not happened in {} seconds, last error: {}".format(what, timeout, last_error))
            time.sleep(1)

    def pause(self, low, high):
        duration = self.rng.uniform(low, high)
        logger.info("Pausing for %.1f seconds", duration)
        deadline = time.monotonic() + duration
        while time.monotonic() < deadline:
            self.check_health()
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))

    def wait_serving(self, timeout=RECOVERY_TIMEOUT):
        for workload in self.workloads():
            self.eventually(workload.probe, "{} serving".format(type(workload).__name__), timeout)

    def verify(self, full=False):
        self.check_health()
        self.kv.verify(full, RECOVERY_TIMEOUT)
        if self.table:
            self.table.verify(RECOVERY_TIMEOUT)
        self.check_health()

    ####################################################################################################################
    # Nodes

    def serving_piles(self):
        if self.states is None:
            return list(self.piles)
        return [name for name, state in self.states.items() if state in (PRIMARY, SYNC, UNSYNC)]

    def serving_nodes(self, piles=None):
        return [node_id for name in (piles or self.serving_piles()) for node_id in self.piles[name]
                if node_id not in self.down]

    def random_serving_node(self, piles=None):
        node_ids = self.serving_nodes(piles)
        if not node_ids:
            raise NotYet("no node to send the request to")
        return random.choice(node_ids)

    def refresh_endpoints(self):
        self.endpoints = tuple(self.cluster.nodes[node_id].endpoint for node_id in self.serving_nodes())

    def kill(self, node_ids):
        node_ids = [node_id for node_id in node_ids if node_id not in self.down]
        logger.info("Killing nodes %s", node_ids)
        self.down.update(node_ids)
        self.refresh_endpoints()
        if node_ids:
            with ThreadPoolExecutor(max_workers=len(node_ids)) as executor:
                list(executor.map(lambda node_id: daemon.Daemon.kill(self.cluster.nodes[node_id]), node_ids))
        for node_id in node_ids:
            self.hold_ports(node_id)

    def hold_ports(self, node_id):
        """Listens on the ports of the stopped node until it starts again. Port managers of tests run by other users of
        the host don't know of the ports of this cluster, and they take the ones nobody listens on."""
        node = self.cluster.nodes[node_id]
        sockets = []
        for port in {node.ic_port, node.grpc_port, node.mon_port, node.grpc_ssl_port} - {None}:
            sock = socket.socket(socket.AF_INET6, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, 0)
            try:
                sock.bind(("::", port))
                sock.listen(1)
                sockets.append(sock)
            except OSError as e:
                sock.close()
                logger.warning("Can't hold port %d of node %d: %s", port, node_id, e)
        self.held_ports[node_id] = sockets

    def release_ports(self, node_id):
        for sock in self.held_ports.pop(node_id, []):
            sock.close()

    def start(self, node_ids):
        node_ids = [node_id for node_id in node_ids if node_id in self.down]
        logger.info("Starting nodes %s", node_ids)
        for node_id in node_ids:
            node = self.cluster.nodes[node_id]
            self.release_ports(node_id)
            # every run of the node writes its own log, as the node truncates the log file it starts with
            self.incarnations[node_id] = self.incarnations.get(node_id, 0) + 1
            node.set_log_file_prefix("logfile_{:03d}_".format(self.incarnations[node_id]))
            node.start()
            self.down.discard(node_id)
        self.refresh_endpoints()

    def rolling_restart(self, node_ids):
        """kill -9 of every node, one or two at a time, which block-4-2 survives"""
        order = list(node_ids)
        self.rng.shuffle(order)
        while order:
            batch = order[:self.rng.choice((1, 2))]
            order = order[len(batch):]
            self.kill(batch)
            self.pause(0, 3)
            self.start(batch)
            self.wait_serving()

    def wipe(self, node_ids):
        """Gives the stopped nodes fresh drives and the current config, as if they were new hosts."""
        cfg = self.fetch_config()
        for node_id in node_ids:
            # the node keeps the config it has got read-only
            config_dir = os.path.join(self.cluster.config_path, "node_{}".format(node_id))
            for name in os.listdir(config_dir):
                os.chmod(os.path.join(config_dir, name), 0o644)
        self.cluster.overwrite_configs(cfg, node_ids=node_ids)
        for node_id in node_ids:
            for path, size in self.pdisks[node_id]:
                self.cluster.nodes[node_id].format_pdisk(path, size)

    def bsc_node(self):
        """The node running BSC, or a random one when it can't be found."""
        for node_id in random.sample(self.serving_nodes(), len(self.serving_nodes())):
            try:
                response = requests.get("http://localhost:{}/viewer/json/tabletinfo".format(
                    self.cluster.nodes[node_id].mon_port), params={"filter": "Type=BSController"}, timeout=5)
                tablets = [tablet for tablet in response.json().get("TabletStateInfo", [])
                           if tablet.get("State") == "Active" and tablet.get("NodeId") in self.cluster.nodes]
                if tablets:
                    return max(tablets, key=lambda tablet: tablet.get("Generation", 0))["NodeId"]
            except (requests.RequestException, ValueError):
                pass
        return self.rng.choice(self.serving_nodes())

    def restart_bsc(self):
        node_id = self.bsc_node()
        logger.info("Restarting node %d running BSC", node_id)
        self.kill([node_id])
        self.pause(0, 5)
        self.start([node_id])

    ####################################################################################################################
    # Config

    def call(self, stub_class, method, request, what, timeout=120):
        """Sends the request through a random serving node until it succeeds; transient errors are retried."""
        def attempt():
            node = self.cluster.nodes[self.random_serving_node()]
            channel = grpc.insecure_channel(node.endpoint, options=GRPC_OPTIONS)
            try:
                response = getattr(stub_class(channel), method)(request, metadata=[('x-ydb-auth-ticket', TOKEN)],
                                                                timeout=60)
            finally:
                channel.close()
            if response.operation.status == StatusIds.SUCCESS:
                return response
            elif response.operation.status in TRANSIENT_STATUSES:
                raise NotYet(str(response.operation))
            raise ChaosFailure("{} has failed: {}".format(what, response.operation))
        return self.eventually(attempt, what, timeout)

    def fetch_config(self):
        request = config_api.FetchConfigRequest()
        request.all.SetInParent()
        result = unpack(self.call(config_grpc.ConfigServiceStub, 'FetchConfig', request, "config fetch"),
                        config_api.FetchConfigResult())
        assert len(result.config) == 1, result
        return yaml.safe_load(result.config[0].config)

    def replace_config(self, cfg):
        """Replaces the config with the next version of it, waiting until running nodes store it."""
        cfg["metadata"]["version"] += 1
        version = cfg["metadata"]["version"]
        logger.info("Replacing config with version %d", version)
        text = yaml.dump(cfg)
        attempted = []

        def attempt():
            # the request might have succeeded while its response has been lost
            if attempted and self.fetch_config()["metadata"]["version"] == version:
                return True
            attempted.append(True)
            request = config_api.ReplaceConfigRequest(replace=text)
            node = self.cluster.nodes[self.random_serving_node()]
            channel = grpc.insecure_channel(node.endpoint, options=GRPC_OPTIONS)
            try:
                response = config_grpc.ConfigServiceStub(channel).ReplaceConfig(
                    request, metadata=[('x-ydb-auth-ticket', TOKEN)], timeout=60)
            finally:
                channel.close()
            if response.operation.status in TRANSIENT_STATUSES:
                raise NotYet(str(response.operation))
            elif response.operation.status != StatusIds.SUCCESS:
                raise ChaosFailure("config replacement has failed: {}".format(response.operation))
            return True
        self.eventually(attempt, "config replacement", 120)

        def stored():
            for node_id in self.serving_nodes():
                try:
                    if self.cluster.nodes[node_id].read_node_config()["metadata"]["version"] != version:
                        raise NotYet("node {} doesn't have the config yet".format(node_id))
                except (KeyError, OSError, TypeError, yaml.YAMLError) as e:
                    raise NotYet("node {} config: {!r}".format(node_id, e))
            return True
        self.eventually(stored, "config storing", 120)

    def dynconfig(self, method, request, result=None):
        response = self.call(dynconfig_grpc.DynamicConfigServiceStub, method, request, "dynconfig " + method)
        return unpack(response, result) if result is not None else None

    ####################################################################################################################
    # Bridge

    def bridge(self, action, what, timeout=120, piles=None, rpc_timeout=30):
        """Runs action(client) through a random serving node until it returns something true."""
        def attempt():
            node_ids = self.serving_nodes(piles)
            if not node_ids:
                raise NotYet("no node to send bridge request to")
            node = self.cluster.nodes[random.choice(node_ids)]
            client = BridgeClient(node.host, node.port, timeout=rpc_timeout)
            client.set_auth_token(TOKEN)
            try:
                return action(client)
            finally:
                client.close()
        return self.eventually(attempt, what, timeout)

    @staticmethod
    def cluster_state(client):
        response = client.get_cluster_state()
        if response.operation.status != StatusIds.SUCCESS:
            raise NotYet(str(response.operation))
        result = unpack(response, bridge_api.GetClusterStateResult())
        return {pile.pile_name: pile.state for pile in result.pile_states}, result

    def primary(self):
        return next(name for name, state in self.states.items() if state == PRIMARY)

    def other(self, name):
        return next(other for other in self.piles if other != name)

    def wait_states(self, what=None, timeout=120):
        expected = dict(self.states)

        def check(client):
            states, result = self.cluster_state(client)
            if states != expected or result.HasField("pending_pile_operation") or result.conversion_in_progress:
                raise NotYet("states {}, pending operation {}, conversion {}".format(
                    state_names(states), result.pending_pile_operation, result.conversion_in_progress))
            return True
        self.bridge(check, what or "pile states {}".format(state_names(expected)), timeout)
        self.refresh_endpoints()

    def update(self, changes, piles=None):
        completed = {PROMOTED: PRIMARY, SUSPENDED: DISCONNECTED, UNSYNC: SYNC}
        names = [(name, PileState.State.Name(state)) for name, state in changes]
        logger.info("Updating pile states: %s", names)
        attempted = []

        def attempt(client):
            if attempted:
                # the request might have succeeded while its response has been lost
                states, _ = self.cluster_state(client)
                if all(states.get(name) in (state, completed.get(state, state)) for name, state in changes):
                    return True
            attempted.append(True)
            response = client.update_cluster_state([PileState(pile_name=name, state=state) for name, state in changes])
            if response.operation.status == StatusIds.SUCCESS:
                return True
            elif response.operation.status in TRANSIENT_STATUSES:
                raise NotYet(str(response.operation))
            raise ChaosFailure("update of pile states {} is refused: {}".format(names, response.operation))
        self.bridge(attempt, "update of pile states {}".format(names), piles=piles)

    def pile_operation(self, name, add):
        what = "{} of pile {}".format("addition" if add else "removal", name)
        logger.info("Starting %s", what)

        def attempt(client):
            states, result = self.cluster_state(client)
            done = (states.get(name) != INACTIVE) if add else (states.get(name) == INACTIVE)
            if done and not result.HasField("pending_pile_operation"):
                return True
            elif result.HasField("pending_pile_operation"):
                raise NotYet("pending operation {}".format(result.pending_pile_operation))
            response = client.add_pile(name) if add else client.remove_pile(name)
            issues = str(response.operation.issues)
            if response.operation.status in (StatusIds.SUCCESS,) + TRANSIENT_STATUSES or \
                    "this is retried until it succeeds" in issues or \
                    "group allocation failed" in issues:  # BSC learns about the hosts just listed asynchronously
                raise NotYet(str(response.operation))  # the next attempt checks the result
            raise ChaosFailure("{} is refused: {}".format(what, response.operation))
        self.bridge(attempt, what, timeout=300, rpc_timeout=120)
        self.states[name] = DISCONNECTED if add else INACTIVE
        self.wait_states()

    def rejoin(self, name, restart_bsc=False):
        self.update([(name, UNSYNC)])
        self.states[name] = UNSYNC
        self.refresh_endpoints()
        if restart_bsc:
            self.pause(0, 3)
            self.restart_bsc()
        self.states[name] = SYNC
        self.wait_states("synchronization of pile {}".format(name), SYNC_TIMEOUT)

    def disconnect(self, name, graceful=False, new_primary=None, piles=None):
        changes = [(name, SUSPENDED if graceful else DISCONNECTED)]
        if new_primary:
            changes.append((new_primary, PRIMARY))
            self.states[new_primary] = PRIMARY
        self.states[name] = DISCONNECTED
        self.refresh_endpoints()
        self.update(changes, piles=piles)
        self.wait_states()

    ####################################################################################################################
    # Phases

    def migrate_to_v2(self):
        logger.info("Migrating the cluster to config v2 and distconf")
        nodes = sorted(self.cluster.nodes)
        result = self.dynconfig('GetConfig', dynconfig_api.GetConfigRequest(), dynconfig_api.GetConfigResult())
        in_console = bool(result.config and result.config[0])
        if in_console:
            cfg = yaml.safe_load(result.config[0])
        else:
            result = self.dynconfig('FetchStartupConfig', dynconfig_api.FetchStartupConfigRequest(),
                                    dynconfig_api.FetchStartupConfigResult())
            cfg = {"metadata": {"version": 0, "cluster": "", "kind": "MainConfig"}, "config": yaml.safe_load(result.config)}
        cfg["config"].setdefault("feature_flags", {})["switch_to_config_v2"] = True
        if in_console:
            # Console overwrites runtime flags on node restart, so the flag gets there first
            self.dynconfig('ReplaceConfig', dynconfig_api.ReplaceConfigRequest(config=yaml.dump(cfg)))
            result = self.dynconfig('GetConfig', dynconfig_api.GetConfigRequest(), dynconfig_api.GetConfigResult())
            cfg = yaml.safe_load(result.config[0])

        self.cluster.overwrite_configs(cfg)
        self.cluster.enable_config_dir()
        self.rolling_restart(nodes)

        # the config gets to the config v2 storage as is, then self-management is enabled
        cfg["metadata"]["version"] -= 1  # replace_config makes the next version, while this one is the initial one
        self.replace_config(cfg)
        cfg = self.fetch_config()
        cfg["config"]["self_management_config"] = {"enabled": True}
        # drives of the test are ROT ones; distconf places static groups only on the drives of the default type (SSD
        # unless set), and so do the parts of static groups in the piles added later
        cfg["config"]["default_disk_type"] = "ROT"
        self.replace_config(cfg)
        self.rolling_restart(nodes)

        # sections managed by distconf from now on are dropped
        cfg = self.fetch_config()
        section = cfg["config"]
        section["security_config"] = section["domains_config"].pop("security_config")
        section.pop("domains_config")
        section.pop("blob_storage_config")
        if "static_erasure" in section:
            section["erasure"] = section.pop("static_erasure")
        self.replace_config(cfg)
        self.rolling_restart(nodes)
        self.verify()

    def convert(self):
        logger.info("Converting the cluster into the bridged one")
        nodes = self.piles[ORIGINAL_PILE]

        # every node has to run with single-pile bridge config before the conversion
        cfg = self.fetch_config()
        cfg["config"]["bridge_config"] = {"piles": [{"name": ORIGINAL_PILE}]}
        self.replace_config(cfg)
        self.rolling_restart(nodes)

        def convert(client):
            if client.get_cluster_state().operation.status == StatusIds.SUCCESS:
                return True  # converted already, maybe by the request which response has been lost
            response = client.convert_to_bridge()
            if response.operation.status == StatusIds.SUCCESS:
                return True
            raise NotYet(str(response.operation))
        self.bridge(convert, "conversion")
        self.states = {ORIGINAL_PILE: PRIMARY}
        self.wait_states("conversion of BSC groups")
        self.verify()

        # nodes of the converted cluster get the name of their pile before other piles get listed in the config
        cfg = self.fetch_config()
        for host in cfg["config"]["hosts"]:
            host.setdefault("location", {})["bridge_pile_name"] = ORIGINAL_PILE
        self.replace_config(cfg)
        self.rolling_restart(nodes)

    def list_added_pile(self):
        """Lists the pile in the config along with its hosts (each one with its own drive), and starts its nodes."""
        logger.info("Listing pile %s in the config", ADDED_PILE)
        cfg = self.fetch_config()
        section = cfg["config"]
        section["bridge_config"]["piles"].append({"name": ADDED_PILE})
        first_node_id = max(self.cluster.nodes) + 1
        node_ids = list(range(first_node_id, first_node_id + PILE_NODES))
        drive_dir = os.path.dirname(self.pdisks[1][0][0])
        for node_id in node_ids:
            pdisk_file = tempfile.NamedTemporaryFile(prefix="pdisk_{}_".format(node_id), suffix=".data", dir=drive_dir)
            self.pdisk_files.append(pdisk_file)
            self.pdisks[node_id] = [(pdisk_file.name, self.configurator.static_pdisk_size)]
            host_config_id = max(host_config["host_config_id"] for host_config in section["host_configs"]) + 1
            section["host_configs"].append({
                "host_config_id": host_config_id,
                "drive": [{"path": pdisk_file.name, "type": "ROT"}],
            })
            section["hosts"].append({
                "host": "localhost",
                "port": self.configurator.port_allocator.get_node_port_allocator(node_id).ic_port,
                "host_config_id": host_config_id,
                "location": {"bridge_pile_name": ADDED_PILE},
            })

        # config of the cluster migrated from v1 binds system tablets to the nodes of the original pile; they have to run
        # in the added pile too, otherwise nothing serves once the original pile is gone (nothing checks this)
        system_tablets = section.get("system_tablets", {})
        if "default_node" in system_tablets:
            system_tablets["default_node"] = system_tablets["default_node"] + node_ids
        for tablets in system_tablets.values():
            for tablet in tablets if isinstance(tablets, list) else []:
                if isinstance(tablet, dict) and "node" in tablet:
                    tablet["node"] = tablet["node"] + node_ids

        # new nodes start with the config that lists them, with the version it gets when replaced
        self.configurator.full_config = yaml.safe_load(yaml.dump(cfg))
        self.configurator.full_config["metadata"]["version"] += 1
        for node_id in node_ids:
            node = self.cluster.prepare_node(self.configurator)
            assert node.node_id == node_id, (node.node_id, node_id)
            for path, size in self.pdisks[node_id]:
                node.format_pdisk(path, size)
            self.down.add(node_id)
        self.piles[ADDED_PILE] = node_ids
        self.replace_config(cfg)
        self.start(node_ids)
        self.states[ADDED_PILE] = INACTIVE
        self.wait_states()

    def add_pile(self):
        self.list_added_pile()
        self.pile_operation(ADDED_PILE, add=True)
        self.rejoin(ADDED_PILE)
        self.verify()

    def chaos(self):
        actions = [
            (3, self.act_kill_nodes),
            (2, self.act_restart_bsc),
            (2, self.act_switchover),
            (2, self.act_disconnect_and_rejoin),
            (1, self.act_failover),
            (1, self.act_beyond_failure_model),
            (1, self.act_remove_and_add),
            (1, self.act_rolling_restart),
        ]
        deadline = time.monotonic() + self.chaos_seconds
        step = 0
        while time.monotonic() < deadline:
            step += 1
            action = self.rng.choices([action for _, action in actions], [weight for weight, _ in actions])[0]
            logger.info("Chaos step %d: %s, states %s", step, action.__name__, state_names(self.states))
            action()
            # every action leaves the cluster with all its piles synchronized and its nodes running
            assert not self.down, self.down
            assert sorted(self.states.values()) == sorted([PRIMARY, SYNC]), state_names(self.states)
            self.verify()

    def shrink(self):
        survivor = self.rng.choice((ORIGINAL_PILE, ADDED_PILE))
        removed = self.other(survivor)
        logger.info("Keeping pile %s only", survivor)
        if self.primary() != survivor:
            self.act_switchover()
        self.disconnect(removed)
        self.kill(self.piles[removed])
        self.pile_operation(removed, add=False)
        self.verify(full=True)  # the survivor serves alone
        self.rolling_restart(self.piles[survivor])
        self.verify()

    ####################################################################################################################
    # Chaos actions; each one leaves the cluster with all its piles synchronized and its nodes running

    def act_kill_nodes(self):
        # one or two nodes of a pile, which block-4-2 survives
        victims = self.rng.sample(self.piles[self.rng.choice(sorted(self.piles))], self.rng.choice((1, 2)))
        self.kill(victims)
        self.pause(1, 30)
        self.start(victims)
        self.wait_serving()

    def act_restart_bsc(self):
        self.restart_bsc()
        self.wait_serving()

    def act_switchover(self):
        target = self.other(self.primary())
        self.update([(target, PROMOTED)])
        self.states[self.primary()] = SYNC
        self.states[target] = PRIMARY
        self.wait_states()

    def act_disconnect_and_rejoin(self):
        name = self.other(self.primary())
        self.disconnect(name, graceful=self.rng.random() < 0.3)
        variant = self.rng.random()
        if variant < 0.4:
            victims = list(self.piles[name])  # the pile is gone completely
        elif variant < 0.7:
            victims = self.rng.sample(self.piles[name], self.rng.randint(1, 4))
        else:
            victims = []
        self.kill(victims)
        self.verify(full=True)  # the primary serves alone
        self.pause(5, 60)
        self.start(victims)
        self.rejoin(name, restart_bsc=self.rng.random() < 0.4)

    def act_failover(self):
        failed = self.primary()
        survivor = self.other(failed)
        self.kill(self.piles[failed])
        self.disconnect(failed, new_primary=survivor, piles=[survivor])
        self.wait_serving()
        # the survivor serves alone, so it has to keep every acknowledged write: this is what being synchronized means
        self.verify(full=True)
        self.pause(5, 40)
        self.start(self.piles[failed])
        self.rejoin(failed, restart_bsc=self.rng.random() < 0.3)

    def act_beyond_failure_model(self):
        # the cluster may stop serving, but it has to serve again once the nodes are back
        variant = self.rng.random()
        if variant < 0.4:
            victims = self.rng.sample(self.piles[self.rng.choice(sorted(self.piles))], self.rng.randint(3, PILE_NODES))
        elif variant < 0.8:
            victims = [node_id for name in sorted(self.piles) for node_id in self.rng.sample(self.piles[name], 3)]
        else:
            victims = [node_id for name in sorted(self.piles) for node_id in self.piles[name]]  # the whole cluster
        self.kill(victims)
        self.pause(5, 30)
        self.start(victims)
        self.wait_serving()

    def act_remove_and_add(self):
        name = self.other(self.primary())
        self.disconnect(name)
        self.kill(self.piles[name])
        self.pile_operation(name, add=False)
        self.verify(full=True)  # the primary serves alone
        mode = self.readd_mode if self.readd_mode != "random" else self.rng.choice(("fresh", "same"))
        logger.info("Adding pile %s again with %s hosts", name, mode)
        if mode == "fresh":
            self.wipe(self.piles[name])
        self.start(self.piles[name])
        self.pile_operation(name, add=True)
        self.rejoin(name, restart_bsc=self.rng.random() < 0.3)

    def act_rolling_restart(self):
        self.rolling_restart(self.piles[self.rng.choice(sorted(self.piles))])

    ####################################################################################################################

    def test_bridge_chaos(self):
        response = KeyValueClient(self.cluster.nodes[1].host, self.cluster.nodes[1].grpc_port, cluster=self.cluster) \
            .create_tablets(KV_PARTITIONS, KV_PATH)
        assert response.operation.status == StatusIds.SUCCESS, response
        if self.table:
            self.table.create(RECOVERY_TIMEOUT)
        self.wait_serving()
        for workload in self.workloads():
            workload.start()

        self.migrate_to_v2()
        self.convert()
        self.add_pile()
        self.chaos()
        self.shrink()

        for workload in self.workloads():
            workload.stop()
        self.verify(full=True)
