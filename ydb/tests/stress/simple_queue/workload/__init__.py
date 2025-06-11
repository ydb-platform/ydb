# -*- coding: utf-8 -*-
from concurrent.futures import ThreadPoolExecutor
import copy
import os
import random
import time
import string
import threading
import socket
import collections
import itertools
import queue
import traceback
import ydb
from library.python.monlib.metric_registry import MetricRegistry

BLOB_MIN_SIZE = 128 * 1024


def random_string(size):
    return ''.join(random.choices(string.ascii_lowercase, k=size))


def generate_blobs(count=32):
    return [random_string(idx * BLOB_MIN_SIZE) for idx in range(1, count + 1)]


class EventKind(object):
    CREATE_TABLE = 'create_table'
    ALTER_TABLE = 'alter_table'
    DROP_TABLE = 'drop_table'

    READ_TABLE = 'read_table'

    WRITE = 'write'
    REMOVE_OUTDATED = 'remove_outdated'
    FIND_OUTDATED = 'find_outdated'

    BATCH_UPDATE = 'batch_update'
    BATCH_DELETE = 'batch_delete'

    @classmethod
    def periodic_tasks_column(cls):
        return (
            cls.READ_TABLE,
        )

    @classmethod
    def periodic_tasks_row(cls):
        return (
            cls.READ_TABLE,

            cls.BATCH_DELETE,
            cls.BATCH_UPDATE,
        )

    @classmethod
    def rare(cls):
        return (
            cls.ALTER_TABLE,
            cls.DROP_TABLE,
        )

    @classmethod
    def list(cls):
        return (
            cls.DROP_TABLE,
            cls.ALTER_TABLE,

            cls.READ_TABLE,

            cls.FIND_OUTDATED,
            cls.REMOVE_OUTDATED,
            cls.WRITE,

            cls.BATCH_DELETE,
            cls.BATCH_UPDATE,

            cls.CREATE_TABLE
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
        self.prepare_new_queue(self.table_name)
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
        table_name = self.table_name_with_timestamp() if table_name is None else table_name
        self.send_query(get_table_description(table_name, self.mode), parameters=None, event_kind=EventKind.CREATE_TABLE)

    def switch(self, switch_to):
        self.table_name = switch_to
        self.outdated_keys.clear()

    def update_stats(self, event):
        self.stats.save_event(event)

    def send_query(self, query, parameters, event_kind):
        try:
            result_list = self.pool.execute_with_retries(query, parameters=parameters, retry_settings=ydb.RetrySettings(max_retries=0), settings=self.ops)
            self.update_stats(event_kind)
            return result_list
        except ydb.Error as e:
            # print(f'{event_kind}: produced an error {e}')
            self.stats.save_event(event_kind, e.status)
            return None
        except Exception as e:
            # print(f'{event_kind}: produced an unxpected error {e}')
            self.stats.save_event(event_kind, e.status)

    def process_dir_content(self, base, response, switch=True):
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

        if switch and len(candidates) > 0:
            switch_to = candidates.pop()
            self.switch(switch_to)
        self.drop_queue.extend(candidates)

    def list_copies_dir(self):
        response = self.driver.scheme_client.list_directory(self.copies_dir)
        self.process_dir_content(
            self.copies_dir, response, switch=False
        )

    def list_working_dir(self):
        response = self.driver.scheme_client.list_directory(self.working_dir)
        self.process_dir_content(
            self.working_dir, response, switch=True,
        )

    def read_table(self):
        it = self.send_query("SELECT `key` FROM `{}`".format(self.table_name), None, EventKind.READ_TABLE)
        try:
            while it is not None:
                self.update_stats(EventKind.READ_TABLE)
                it = next(it)
        except ydb.Error as e:
            self.stats.save_event(EventKind.READ_TABLE, e.status)
        except StopIteration:
            return

    def remove_outdated(self):
        try:
            keys_set = self.outdated_keys.popleft()
            if len(keys_set) == 0:
                return
        except IndexError:
            return

        query = """
            --!syntax_v1
            DECLARE $keys as List<Struct<key: Uint64>>;
            DELETE FROM `{}` ON SELECT `key` FROM AS_TABLE($keys);
            """.format(self.table_name)

        parameters = {
            '$keys': (keys_set, ydb.ListType(ydb.StructType().add_member('key', ydb.PrimitiveType.Uint64)).proto)
        }

        self.send_query(query=query, event_kind=EventKind.REMOVE_OUTDATED, parameters=parameters)

    def update_outdated_keys(self, resp):
        if resp is not None:
            self.outdated_keys.append(resp.rows)

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
        response = self.send_query(query=query, event_kind=EventKind.FIND_OUTDATED, parameters=parameters)
        self.update_outdated_keys(response)

    def write(self):
        current_timestamp = timestamp()
        blob = next(self.blobs_iter)
        query = """
            --!syntax_v1
            DECLARE $key as Uint64;
            DECLARE $value as Utf8;
            DECLARE $timestamp as Uint64;
            UPSERT INTO `{}` (`key`, `timestamp`, `value`) VALUES ($key, $timestamp, $value);
            """.format(self.table_name)

        parameters = {
            '$key': (current_timestamp, ydb.PrimitiveType.Uint64.proto),
            '$value': (blob, ydb.PrimitiveType.Utf8.proto),
            '$timestamp': (current_timestamp, ydb.PrimitiveType.Uint64.proto),
        }

        self.send_query(query=query, event_kind=EventKind.WRITE, parameters=parameters)

    def alter_table(self):
        query = "ALTER TABLE `{table_name}` ADD COLUMN column_{val} Utf8".format(
            table_name=self.table_name,
            val=random.randint(1, 100000),
        )

        self.send_query(query, parameters=None, event_kind=EventKind.ALTER_TABLE)

    def drop_table(self):
        duplicates = set()
        while len(self.drop_queue) > 0:
            candidate = self.drop_queue.popleft()
            if candidate in duplicates:
                continue

            duplicates.add(candidate)
            session = self.pool.acquire()
            try:
                session.drop_table(candidate, settings=self.ops)
                self.update_stats(EventKind.DROP_TABLE)
            except ydb.Error as e:
                self.stats.save_event(EventKind.DROP_TABLE, e.status)
            finally:
                self.pool.release(session)

    def batch_update(self):
        blob = next(self.blobs_iter)
        parameters = {
            "$value": blob,
            "$timestamp": timestamp() - 10
        }
        self.send_query("BATCH UPDATE `{}` SET value = $value WHERE `timestamp` >= $timestamp;".format(self.table_name), parameters, EventKind.BATCH_UPDATE)

    def batch_delete(self):
        parameters = {
            "$timestamp": timestamp() - 20
        }
        self.send_query("BATCH DELETE FROM `{}` WHERE `timestamp` <= $timestamp;".format(self.table_name), parameters, EventKind.BATCH_DELETE)


class Workload:
    def __init__(self, endpoint, database, duration, mode):
        self.database = database
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.QuerySessionPool(self.driver, size=200)
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
        self.pool_semaphore = threading.BoundedSemaphore(value=100)
        self.worker_exception = []

    def random_points(self, size=1):
        return set([random.randint(0, self.round_size) for _ in range(size)])

    def wrapper(self, f):
        try:
            if len(self.worker_exception) == 0:
                f()
        except Exception as e:
            self.worker_exception.append(traceback.format_exc(e))
        finally:
            self.pool_semaphore.release()

    def loop(self):
        started_at = time.time()
        round_id_it = itertools.count(start=1)
        queue_it = itertools.cycle(self.ydb_queues)

        while time.time() - started_at < self.duration:

            for ydb_queue in self.ydb_queues:
                yield ydb_queue.list_working_dir
                yield ydb_queue.list_copies_dir
                print("Table name: %s" % ydb_queue.table_name)

            round_id = next(round_id_it)
            if round_id % 10 == 0:
                for ydb_queue in self.ydb_queues:
                    yield ydb_queue.prepare_new_queue

            self.workload_stats.print_stats()

            schedule = []
            for op in EventKind.rare():
                schedule.extend([(point, op) for point in self.random_points()])

            for op in EventKind.periodic_tasks_row() if self.mode == 'row' else EventKind.periodic_tasks_column():
                schedule.extend([(point, op) for point in self.random_points(size=50)])

            schedule = collections.deque(list(sorted(schedule)))

            print(f"Starting round_id {round_id}")
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

    def start(self):
        pool = ThreadPoolExecutor()
        for lambda_call in self.loop():
            if len(self.worker_exception) == 0:
                self.pool_semaphore.acquire()
                pool.submit(self.wrapper, lambda_call)
            else:
                assert False, f"Worker exceptions {self.worker_exception}"
        pool.shutdown(wait=True)
        self.workload_stats.print_stats()
