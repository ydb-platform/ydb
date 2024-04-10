# -*- coding: utf-8 -*-
import argparse
import copy
import os
import random
import time
import logging
import string
import threading
import collections
import itertools
import six
from six.moves import queue
import ydb
from library.python.monlib.metric_registry import MetricRegistry
import socket

ydb.interceptor.monkey_patch_event_handler()

logger = logging.getLogger(__name__)

BLOB_MIN_SIZE = 128 * 1024
WINDOW_SIZE = 5000


def random_string(size):
    return ''.join([random.choice(string.ascii_lowercase) for _ in six.moves.range(size)])


def generate_blobs(count=32):
    return [random_string(idx * BLOB_MIN_SIZE) for idx in range(1, count + 1)]


class EventKind(object):
    ALTER_TABLE = 'alter_table'
    COPY_TABLE = 'copy_table'
    DROP_TABLE = 'drop_table'
    START_READ_TABLE = 'start_read_table'
    READ_TABLE_CHUNK = 'read_table_chunk'

    WRITE = 'write'
    REMOVE_OUTDATED = 'remove_outdated'
    FIND_OUTDATED = 'find_outdated'

    SCAN_QUERY_CHUNK = 'scan_query_chunk'
    START_SCAN_QUERY = 'start_scan_query'

    @classmethod
    def periodic_tasks(cls):
        return (
            cls.START_READ_TABLE,
            cls.START_SCAN_QUERY,
        )

    @classmethod
    def rare(cls):
        return (
            cls.ALTER_TABLE,
            cls.DROP_TABLE,
            cls.COPY_TABLE,
            cls.START_READ_TABLE,
            cls.START_SCAN_QUERY,
        )

    @classmethod
    def list(cls):
        return (
            cls.COPY_TABLE,
            cls.DROP_TABLE,
            cls.ALTER_TABLE,

            cls.START_READ_TABLE,
            cls.READ_TABLE_CHUNK,

            cls.FIND_OUTDATED,
            cls.REMOVE_OUTDATED,
            cls.WRITE,
            cls.START_SCAN_QUERY,
            cls.SCAN_QUERY_CHUNK,
        )


def get_table_description():
    return (
        ydb.TableDescription()
        .with_primary_keys('key')
        .with_key_bloom_filter(ydb.FeatureFlag.ENABLED)
        .with_read_replicas_settings(ydb.ReadReplicasSettings().with_any_az_read_replicas_count(1))
        .with_column_families(
            ydb.ColumnFamily()
            .with_compression(ydb.Compression.LZ4)
            .with_name('lz4_family')
        )
        .with_indexes(
            ydb.TableIndex('by_timestamp').with_index_columns('timestamp')
        )
        .with_ttl(
            ydb.TtlSettings().with_date_type_column('timestamp', expire_after_seconds=240)
        )
        .with_partitioning_settings(
            ydb.PartitioningSettings()
            .with_partition_size_mb(128)
            .with_partitioning_by_load(ydb.FeatureFlag.ENABLED)
            .with_partitioning_by_size(ydb.FeatureFlag.ENABLED)
        )
        .with_columns(
            ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
            ydb.Column('timestamp', ydb.OptionalType(ydb.PrimitiveType.Timestamp)),
            ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Utf8), family='lz4_family'),
        )
    )


def timestamp():
    return int(1000 * time.time())


def extract_keys(response):
    try:
        result_sets = response.result()
        return result_sets[0].rows
    except Exception:
        return []


def status_code_to_label(status=None):
    if status is None:
        return 'Success'
    return status.name.lower().capitalize()


class WorkloadStats(object):
    def __init__(self, *evs):
        self.lock = threading.Lock()
        self.registry = MetricRegistry()
        self.by_events_stats = {}
        for ev in evs:
            self.init_kind(ev)

    def int_gauge(self, sensor, **labels):
        all_labels = copy.deepcopy(labels)
        all_labels.update({'sensor': sensor})
        return self.registry.int_gauge(all_labels)

    def rate(self, sensor, **labels):
        all_labels = copy.deepcopy(labels)
        all_labels.update({'sensor': sensor})
        return self.registry.rate(all_labels)

    def init_kind(self, ev_kind):
        self.by_events_stats[ev_kind] = {}
        for status in list(ydb.StatusCode):
            if status == ydb.StatusCode.STATUS_CODE_UNSPECIFIED:
                continue

            label = status_code_to_label(status)
            self.by_events_stats[ev_kind][label] = self.rate(
                label, event=ev_kind)

    def save_event(self, ev_kind, details=None):
        label = status_code_to_label(details)
        if ev_kind not in self.by_events_stats:
            return

        self.by_events_stats[ev_kind][label].inc()

    def print_stats(self):
        report = ["=" * 120]
        for event_kind, stats in six.iteritems(self.by_events_stats):
            for response_kind, responses_count in six.iteritems(stats):
                value = responses_count.get()
                if value > 0:
                    report.append(
                        "EventKind: {event_kind}, {response_kind} responses count: {responses_count}".format(
                            event_kind=event_kind,
                            response_kind=response_kind,
                            responses_count=value
                        )
                    )
        report.append("=" * 120)
        print("\n".join(report))


class YdbQueue(object):
    def __init__(self, idx, database, stats, driver, pool):
        self.working_dir = os.path.join(database, socket.gethostname().split('.')[0].replace('-', '_') + "_" + str(idx))
        self.copies_dir = os.path.join(self.working_dir, 'copies')
        self.table_name = self.table_name_with_timestamp()
        self.queries = {}
        self.pool = pool
        self.driver = driver
        self.stats = stats
        self.blobs = generate_blobs(16)
        self.blobs_iter = itertools.cycle(self.blobs)
        self.outdated_period = 60 * 2
        self.database = database
        self.ops = ydb.BaseRequestSettings().with_operation_timeout(19).with_timeout(20)
        self.prepare_test()
        self.initialize_queries()
        # a queue with tables to drop
        self.drop_queue = collections.deque()
        # a set with keys that are ready to be removed
        self.outdated_keys = collections.deque()
        # a number that stores largest outdated key. That helps avoiding duplicates in deque.
        self.largest_outdated_key = 0
        self.outdated_keys_max_size = 50

    def table_name_with_timestamp(self, working_dir=None):
        if working_dir is not None:
            return os.path.join(working_dir, "queue_" + str(timestamp()))
        return os.path.join(self.working_dir, "queue_" + str(timestamp()))

    def prepare_test(self):
        self.driver.scheme_client.make_directory(self.working_dir)
        self.driver.scheme_client.make_directory(self.copies_dir)
        print("Working dir %s" % self.working_dir)
        f = self.prepare_new_queue(self.table_name)
        f.result()

    def prepare_new_queue(self, table_name=None):
        session = self.pool.acquire()
        table_name = self.table_name_with_timestamp() if table_name is None else table_name
        f = session.async_create_table(table_name, get_table_description(), settings=self.ops)
        f.add_done_callback(lambda x: self.on_received_response(session, x, 'create'))
        return f

    def initialize_queries(self):
        self.queries = {
            # use reverse iteration here
            EventKind.WRITE: ydb.DataQuery(
                """
                --!syntax_v1
                DECLARE $key as Uint64;
                DECLARE $value as Utf8;
                DECLARE $timestamp as Uint64;
                UPSERT INTO `{}` (`key`, `timestamp`, `value`) VALUES ($key, CAST($timestamp as Timestamp), $value);
                """.format(self.table_name), {
                    '$key': ydb.PrimitiveType.Uint64.proto,
                    '$value': ydb.PrimitiveType.Utf8.proto,
                    '$timestamp': ydb.PrimitiveType.Uint64.proto,
                }
            ),
            EventKind.FIND_OUTDATED: ydb.DataQuery(
                """
                --!syntax_v1
                DECLARE $key as Uint64;
                SELECT `key` FROM `{}`
                WHERE `key` <= $key
                ORDER BY `key`
                LIMIT 50;
                """.format(self.table_name), {
                    '$key': ydb.PrimitiveType.Uint64.proto
                }
            ),
            EventKind.REMOVE_OUTDATED: ydb.DataQuery(
                """
                --!syntax_v1
                DECLARE $keys as List<Struct<key: Uint64>>;
                DELETE FROM `{}` ON SELECT `key` FROM AS_TABLE($keys);
                """.format(self.table_name), {
                    '$keys': ydb.ListType(ydb.StructType().add_member('key', ydb.PrimitiveType.Uint64)).proto
                }
            )
        }

    def switch(self, switch_to):
        self.table_name = switch_to
        self.initialize_queries()
        self.outdated_keys.clear()

    def on_received_response(self, session, response, event, callback=None):
        self.pool.release(session)

        if callback is not None:
            callback(response)

        try:
            response.result()
            self.stats.save_event(event)
        except ydb.Error as e:
            self.stats.save_event(event, e.status)

    def send_query(self, query, params, callback=None):
        session = self.pool.acquire()
        f = session.transaction().async_execute(
            self.queries[query], parameters=params, commit_tx=True, settings=self.ops)
        f.add_done_callback(
            lambda response: self.on_received_response(
                session, response, query, callback
            )
        )
        return f

    def on_list_response(self, base, f, switch=True):
        try:
            response = f.result()
        except ydb.Error:
            return

        tables = []
        for child in response.children:
            if child.is_directory():
                continue

            tables.append(
                os.path.join(
                    base, child.name
                )
            )

        candidates = list(sorted(tables))
        if switch:
            switch_to = candidates.pop()
            self.switch(switch_to)
        self.drop_queue.extend(candidates)

    def list_copies_dir(self):
        f = self.driver.scheme_client.async_list_directory(self.copies_dir)
        f.add_done_callback(
            lambda x: self.on_list_response(
                self.copies_dir, x, switch=False
            )
        )

    def list_working_dir(self):
        f = self.driver.scheme_client.async_list_directory(self.working_dir)
        f.add_done_callback(
            lambda x: self.on_list_response(
                self.working_dir, x, switch=True,
            )
        )

    def remove_outdated(self):
        try:
            keys_set = self.outdated_keys.popleft()
        except IndexError:
            return

        return
        return self.send_query(
            EventKind.REMOVE_OUTDATED,
            params={
                '$keys': keys_set
            }
        )

    def on_find_outdated(self, resp):
        try:
            rs = resp.result()
            self.outdated_keys.append(rs[0].rows)
        except ydb.Error:
            return

    def find_outdated(self):
        # ensure queue is not large enough
        if len(self.outdated_keys) > self.outdated_keys_max_size:
            return

        outdated_timestamp = timestamp() - self.outdated_period
        return self.send_query(
            EventKind.FIND_OUTDATED,
            callback=self.on_find_outdated,
            params={
                '$key': outdated_timestamp
            }
        )

    def write(self):
        current_timestamp = timestamp()
        blob = next(self.blobs_iter)
        return self.send_query(
            EventKind.WRITE, {
                '$key': current_timestamp,
                '$value': blob,
                '$timestamp': current_timestamp,
            }
        )

    def move_iterator(self, it, callback):
        try:
            next_f = next(it)
            next_f.add_done_callback(lambda x: callback(it, x))
        except StopIteration:
            return

    def on_read_table_chunk(self, it, f):
        try:
            f.result()
            self.stats.save_event(EventKind.READ_TABLE_CHUNK)
        except ydb.Error as e:
            self.stats.save_event(EventKind.READ_TABLE_CHUNK, e)
        self.move_iterator(it, self.on_read_table_chunk)

    def on_scan_query_chunk(self, it, f):
        try:
            f.result()
            self.stats.save_event(EventKind.SCAN_QUERY_CHUNK)
        except ydb.Error as e:
            self.stats.save_event(EventKind.SCAN_QUERY_CHUNK, e)

        self.move_iterator(it, self.on_scan_query_chunk)

    def start_scan_query(self):
        it = self.driver.table_client.async_scan_query('select count(*) as cnt from `%s`' % self.table_name)
        self.stats.save_event(EventKind.START_SCAN_QUERY)
        self.move_iterator(it, self.on_scan_query_chunk)

    def start_read_table(self):
        with self.pool.checkout() as session:
            it = session.async_read_table(self.table_name, columns=('key', ))
            self.stats.save_event(EventKind.START_READ_TABLE)
            self.move_iterator(it, self.on_read_table_chunk)

    def alter_table(self):
        session = self.pool.acquire()
        add_column = ydb.Column('column_%d' % random.randint(1, 100000), ydb.OptionalType(ydb.PrimitiveType.Utf8))
        f = session.async_alter_table(
            self.table_name, add_columns=(add_column, ), drop_columns=(), settings=self.ops)
        f.add_done_callback(
            lambda response: self.on_received_response(
                session, response, EventKind.ALTER_TABLE,
            )
        )

    def drop_table(self):
        duplicates = set()
        while len(self.drop_queue) > 0:
            candidate = self.drop_queue.popleft()
            if candidate in duplicates:
                continue

            duplicates.add(candidate)
            session = self.pool.acquire()
            f = session.async_drop_table(candidate, settings=self.ops)
            f.add_done_callback(
                lambda response: self.on_received_response(
                    session, response, EventKind.DROP_TABLE,
                )
            )

    def copy_table(self):
        session = self.pool.acquire()
        dst_table = self.table_name_with_timestamp(self.copies_dir)
        f = session.async_copy_table(self.table_name, dst_table, settings=self.ops)
        f.add_done_callback(
            lambda response: self.on_received_response(
                session, response, EventKind.COPY_TABLE
            )
        )


class Workload(object):
    def __init__(self, endpoint, database, duration):
        self.database = database
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.SessionPool(self.driver, size=200)
        self.round_size = 1000
        self.duration = duration
        self.delayed_events = queue.Queue()
        self.workload_stats = WorkloadStats(*EventKind.list())
        self.ydb_queues = [
            YdbQueue(idx, database, self.workload_stats, self.driver, self.pool)
            for idx in range(2)
        ]

    def random_points(self, size=1):
        return set([random.randint(0, self.round_size) for _ in range(size)])

    def loop(self):
        started_at = time.time()
        round_id_it = itertools.count(start=1)
        queue_it = itertools.cycle(self.ydb_queues)

        while time.time() - started_at < self.duration:

            for ydb_queue in self.ydb_queues:
                ydb_queue.list_working_dir()
                ydb_queue.list_copies_dir()

            round_id = next(round_id_it)
            if round_id % 10 == 0:
                for ydb_queue in self.ydb_queues:
                    ydb_queue.prepare_new_queue()

            self.workload_stats.print_stats()

            schedule = []
            for op in EventKind.rare():
                schedule.extend([(point, op) for point in self.random_points()])

            for op in EventKind.periodic_tasks():
                schedule.extend([(point, op) for point in self.random_points(size=50)])

            schedule = collections.deque(list(sorted(schedule)))

            print("Starting round_id %d" % round_id)
            print("Round schedule %s", schedule)
            for step_id in six.moves.range(self.round_size):

                if time.time() - started_at > self.duration:
                    break

                ydb_queue = next(queue_it)

                if step_id % 100 == 0:
                    print("step_id %d" % step_id)

                yield ydb_queue.write
                yield ydb_queue.find_outdated
                yield ydb_queue.remove_outdated

                while len(schedule) > 0:
                    scheduled_at, op = schedule[0]
                    if scheduled_at != step_id:
                        break

                    schedule.popleft()
                    yield getattr(ydb_queue, op)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.stop()
        self.driver.stop()


if __name__ == '__main__':
    text = """\033[92mQueue workload\x1b[0m"""
    parser = argparse.ArgumentParser(description=text, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--endpoint', default='localhost:2135', help="An endpoint to be used")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=10 ** 9, type=lambda x: int(x), help='A duration of workload in seconds.')
    args = parser.parse_args()
    with Workload(args.endpoint, args.database, args.duration) as workload:
        for handle in workload.loop():
            handle()
