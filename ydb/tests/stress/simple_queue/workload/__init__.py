# -*- coding: utf-8 -*-
import copy
import os
import random
import time
import string
import threading
import collections
import itertools
import queue
import ydb
from library.python.monlib.metric_registry import MetricRegistry
import socket


BLOB_MIN_SIZE = 128 * 1024


def random_string(size):
    return ''.join([random.choice(string.ascii_lowercase) for _ in range(size)])


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


def get_table_description(table_name, mode):
    if mode == "row":
        store_entry = "STORE = ROW,"
        ttl_entry = """TTL = Interval("PT240S") ON `timestamp` AS SECONDS,"""
    elif mode == "column":
        store_entry = "STORE = COLUMN,"
        ttl_entry = ""
    else:
        raise RuntimeError("Unkown mode: {}".format(mode))

    return f"""
        CREATE TABLE `{table_name}` (
            key Uint64 NOT NULL,
            `timestamp` Uint64 NOT NULL,
            value Utf8 FAMILY lz4_family NOT NULL,
            PRIMARY KEY (key),
            FAMILY lz4_family (
                COMPRESSION = "lz4"
            ),
            INDEX by_timestamp GLOBAL ON (`timestamp`)
        )
        WITH (
            {store_entry}
            {ttl_entry}
            AUTO_PARTITIONING_BY_SIZE = ENABLED,
            AUTO_PARTITIONING_BY_LOAD = ENABLED,
            AUTO_PARTITIONING_PARTITION_SIZE_MB = 128,
            READ_REPLICAS_SETTINGS = "PER_AZ:1",
            KEY_BLOOM_FILTER = ENABLED
        );
    """.format(table_name=table_name)


def timestamp():
    return int(time.time())


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
        for event_kind, stats in self.by_events_stats.items():
            something_appended = False
            total_response_count = sum([responses_count.get() for responses_count in stats.values()])
            if total_response_count == 0:
                continue

            for response_kind, responses_count in stats.items():
                value = responses_count.get()
                is_success = response_kind == status_code_to_label()
                if value > 0 or is_success:
                    something_appended = True
                    line = "EventKind: {event_kind}, {response_kind} responses count: {responses_count}".format(
                        event_kind=event_kind,
                        response_kind=response_kind,
                        responses_count=value,
                    )
                    if is_success:
                        line += " ({:.2f}%)".format(100.0 * value / total_response_count)
                    report.append(line)
            if something_appended:
                report.append("")
        report.append("=" * 120)
        print("\n".join(report))


class YdbQueue(object):
    def __init__(self, idx, database, stats, driver, pool, mode):
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
        self.driver.scheme_client.make_directory(self.working_dir)
        self.driver.scheme_client.make_directory(self.copies_dir)
        self.mode = mode
        print("Working dir %s" % self.working_dir)
        f = self.prepare_new_queue(self.table_name)
        f.result()
        # a queue with tables to drop
        self.drop_queue = collections.deque()
        # a set with keys that are ready to be removed
        self.outdated_keys = collections.deque()
        self.outdated_keys_max_size = 50

    def table_name_with_timestamp(self, working_dir=None):
        if working_dir is not None:
            return os.path.join(working_dir, "queue_" + str(timestamp()))
        return os.path.join(self.working_dir, "queue_" + str(timestamp()))

    def prepare_new_queue(self, table_name=None):
        session = self.pool.acquire()
        table_name = self.table_name_with_timestamp() if table_name is None else table_name
        f = session.async_execute_scheme(get_table_description(table_name, self.mode), settings=self.ops)
        f.add_done_callback(lambda x: self.on_received_response(session, x, 'create'))
        return f

    def switch(self, switch_to):
        self.table_name = switch_to
        self.outdated_keys.clear()

    def on_received_response(self, session, response, event, callback=None):
        self.pool.release(session)

        if callback is not None:
            callback(response)

        try:
            response.result()
            self.stats.save_event(event)
        except ydb.Error as e:
            debug = False
            if debug:
                print(event)
                print(e)
                print()

            self.stats.save_event(event, e.status)

    def send_query(self, query, parameters, event_kind, callback=None):
        session = self.pool.acquire()
        f = session.transaction().async_execute(
            query, parameters=parameters, commit_tx=True, settings=self.ops)
        f.add_done_callback(
            lambda response: self.on_received_response(
                session, response, event_kind, callback
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

        query = ydb.DataQuery(
            """
            --!syntax_v1
            DECLARE $keys as List<Struct<key: Uint64>>;
            DELETE FROM `{}` ON SELECT `key` FROM AS_TABLE($keys);
            """.format(self.table_name), {
                '$keys': ydb.ListType(ydb.StructType().add_member('key', ydb.PrimitiveType.Uint64)).proto
            }
        )
        parameters = {
            '$keys': keys_set
        }

        return self.send_query(query=query, event_kind=EventKind.REMOVE_OUTDATED, parameters=parameters)

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
        query = """
            --!syntax_v1
            SELECT `key` FROM `{table_name}`
            WHERE `key` <= {outdated_timestamp}
            ORDER BY `key`
            LIMIT 50;
            """.format(table_name=self.table_name, outdated_timestamp=outdated_timestamp)
        parameters = None
        return self.send_query(query=query, event_kind=EventKind.FIND_OUTDATED, parameters=parameters, callback=self.on_find_outdated)

    def write(self):
        current_timestamp = timestamp()
        blob = next(self.blobs_iter)
        query = ydb.DataQuery(
            """
            --!syntax_v1
            DECLARE $key as Uint64;
            DECLARE $value as Utf8;
            DECLARE $timestamp as Uint64;
            UPSERT INTO `{}` (`key`, `timestamp`, `value`) VALUES ($key, $timestamp, $value);
            """.format(self.table_name),
            {
                '$key': ydb.PrimitiveType.Uint64.proto,
                '$value': ydb.PrimitiveType.Utf8.proto,
                '$timestamp': ydb.PrimitiveType.Uint64.proto,
            }
        )
        parameters = {
            '$key': current_timestamp,
            '$value': blob,
            '$timestamp': current_timestamp,
        }

        return self.send_query(query=query, event_kind=EventKind.WRITE, parameters=parameters)

    def move_iterator(self, it, callback):
        next_f = next(it)
        next_f.add_done_callback(lambda x: callback(it, x))

    def on_read_table_chunk(self, it, f):
        try:
            f.result()
            self.stats.save_event(EventKind.READ_TABLE_CHUNK)
        except ydb.Error as e:
            self.stats.save_event(EventKind.READ_TABLE_CHUNK, e.status)
        except StopIteration:
            return
        self.move_iterator(it, self.on_read_table_chunk)

    def on_scan_query_chunk(self, it, f):
        try:
            f.result()
            self.stats.save_event(EventKind.SCAN_QUERY_CHUNK)
        except ydb.Error as e:
            self.stats.save_event(EventKind.SCAN_QUERY_CHUNK, e.status)
        except StopIteration:
            return

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
        query = "ALTER TABLE `{table_name}` ADD COLUMN column_{val} Utf8".format(
            table_name=self.table_name,
            val=random.randint(1, 100000),
        )

        f = session.async_execute_scheme(query, settings=self.ops)
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
    def __init__(self, endpoint, database, duration, mode):
        self.database = database
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.SessionPool(self.driver, size=200)
        self.round_size = 1000
        self.duration = duration
        self.delayed_events = queue.Queue()
        self.workload_stats = WorkloadStats(*EventKind.list())
        # TODO: run both modes in parallel?
        self.mode = mode
        self.ydb_queues = [
            YdbQueue(idx, database, self.workload_stats, self.driver, self.pool, self.mode)
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
                print("Table name: %s" % ydb_queue.table_name)

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
            print("Round schedule %s" % schedule)
            for step_id in range(self.round_size):

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
