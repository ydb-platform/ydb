# -*- coding: utf-8 -*-
import json
import time
import random
from datetime import datetime
from tornado import gen
from tornado.ioloop import IOLoop
from ydb.tests.library.serializability.checker import SerializabilityError, SerializabilityChecker
import ydb

KEY_PREFIX_TYPE = ydb.TupleType().add_element(ydb.OptionalType(ydb.PrimitiveType.Uint64))


QUERY_POINT_READS = '''\
--!syntax_v1

DECLARE $data AS List<Struct<
    key: Uint64>>;

SELECT t.key AS key, t.value AS value
FROM AS_TABLE($data) AS d
INNER JOIN `{TABLE}` AS t ON t.key = d.key;
'''


QUERY_POINT_WRITES = '''\
--!syntax_v1

DECLARE $data AS List<Struct<
    key: Uint64,
    value: Uint64>>;

UPSERT INTO `{TABLE}`
SELECT key, value
FROM AS_TABLE($data);
'''


QUERY_POINT_READS_WRITES = '''\
--!syntax_v1

DECLARE $reads AS List<Struct<
    key: Uint64>>;
DECLARE $writes AS List<Struct<
    key: Uint64,
    value: Uint64>>;

SELECT t.key AS key, t.value AS value
FROM AS_TABLE($reads) AS r
INNER JOIN `{TABLE}` AS t ON t.key = r.key;

UPSERT INTO `{TABLE}`
SELECT key, value
FROM AS_TABLE($writes);
'''


QUERY_RANGE_READS = '''\
--!syntax_v1

DECLARE $minKey AS Uint64;
DECLARE $maxKey AS Uint64;

SELECT key, value
FROM `{TABLE}`
WHERE key >= $minKey AND key <= $maxKey;
'''


def generate_random_name(cnt=20):
    return ''.join(
        random.choice('abcdefghijklmnopqrstuvwxyz')
        for _ in range(cnt))


class DummyLogger(object):
    def _print(self, msg, *args):
        if args:
            msg = msg % args
        print(msg)

    debug = _print
    info = _print
    warning = _print
    error = _print
    critical = _print


class History(object):
    def __init__(self):
        self.items = []

    class Prepare(object):
        def __init__(self, keys):
            self.keys = keys

        def to_json(self):
            return ['prepare', self.keys]

        def apply_to(self, checker):
            checker.prepare_keys(self.keys)

    class Begin(object):
        def __init__(self, op, value, read_keys=(), write_keys=(), linearizable=None):
            if linearizable not in (None, 'per_key', 'global'):
                raise ValueError('Unsupported linearizable %r' % (linearizable,))
            self.op = op
            self.value = value
            self.read_keys = sorted(read_keys)
            self.write_keys = sorted(write_keys)
            self.linearizable = linearizable

        def to_json(self):
            result = ['begin', self.op, self.value, self.read_keys, self.write_keys]
            if self.linearizable is not None:
                result.append(self.linearizable)
            return result

        def apply_to(self, checker):
            if self.value is None:
                self.value = checker.get_next_node_value()
            node = checker.new_node()
            if node.value != self.value:
                raise RuntimeError('Allocated node %r (expected %r)' % (node.value, self.value))
            node.description = self.op
            node.expected_read_keys = tuple(self.read_keys)
            if self.linearizable is None or self.linearizable == 'per_key':
                checker.ensure_linearizable_reads(node, self.read_keys)
                checker.ensure_linearizable_writes(node, self.write_keys)
            elif self.linearizable == 'global':
                checker.ensure_linearizable_globally(node)
            else:
                assert False, 'Unexpected value for linearizable'
            for key in self.write_keys:
                checker.ensure_write_value(node, key)
            return node

    class Abort(object):
        def __init__(self, op, value):
            self.op = op
            self.value = value

        def to_json(self):
            return ['abort', self.op, self.value]

        def apply_to(self, checker):
            node = checker.nodes.get(self.value)
            if node is None:
                raise RuntimeError('Abort of unknown node %r' % (self.value,))
            checker.abort(node)
            return node

    class Commit(object):
        def __init__(self, op, value, observed=None, flags=None):
            if flags not in (None, 'partial_read'):
                raise ValueError('Unsupported flags %r' % (flags,))
            self.op = op
            self.value = value
            if isinstance(observed, (list, tuple)):
                self.observed = list(observed)
            elif isinstance(observed, dict):
                self.observed = sorted(observed.items())
            elif observed is None:
                self.observed = []
            else:
                raise TypeError('Unexpected value for observed: %r' % (observed,))
            self.flags = flags

        def to_json(self):
            result = ['commit', self.op, self.value, self.observed]
            if self.flags is not None:
                result.append(self.flags)
            return result

        def apply_to(self, checker):
            node = checker.nodes.get(self.value)
            if node is None:
                raise RuntimeError('Commit of unknown node %r' % (self.value,))
            seen = set()
            for key, value in self.observed:
                checker.ensure_read_value(node, key, value)
                seen.add(key)
            if self.flags != 'partial_read':
                for key in node.expected_read_keys:
                    if key not in seen:
                        checker.ensure_read_value(node, key, 0)
            checker.commit(node)
            return node

    def add(self, item):
        self.items.append(item)
        return item

    def to_json(self):
        return [item.to_json() for item in self.items]

    def apply_to(self, checker):
        for item in self.items:
            item.apply_to(checker)

    @classmethod
    def from_json(cls, items):
        self = cls()
        for item in items:
            assert isinstance(item, list) and item
            if item[0] == 'prepare':
                self.items.append(cls.Prepare(*item[1:]))
            elif item[0] == 'begin':
                self.items.append(cls.Begin(*item[1:]))
            elif item[0] == 'abort':
                self.items.append(cls.Abort(*item[1:]))
            elif item[0] == 'commit':
                self.items.append(cls.Commit(*item[1:]))
        return self

    def write_to_file(self, filename):
        with open(filename, 'w') as f:
            first = True
            f.write('[')
            for item in self.items:
                if not first:
                    f.write(',\n ')
                f.write(json.dumps(item.to_json()))
                first = False
            f.write(']\n')


class DatabaseCheckerOptions(object):
    def __init__(self):
        self.keys = 40
        self.shards = 4
        self.readers = 100
        self.writers = 100
        self.readwriters = 100
        self.readtablers = 100
        self.rangereaders = 100
        self.seconds = 2.0
        self.read_table_ranges = False
        self.ignore_read_table = False
        self.read_table_snapshot = None


class DatabaseChecker(object):
    def __init__(self, endpoint, database, path=None, logger=None):
        if not database.startswith('/'):
            database = '/' + database

        if not path:
            path = database
        elif not path.startswith('/'):
            path = database + '/' + path

        self.endpoint = endpoint
        self.database = database
        self.path = path
        self.logger = logger

        ydb.interceptor.monkey_patch_event_handler()

        self.driver = ydb.Driver(ydb.ConnectionParams(endpoint, database))
        self.driver.wait()

        self.sessions = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self.driver.stop()

    class SessionContext(object):
        def __init__(self, checker, session):
            self._checker = checker
            self._session = session

        def __enter__(self):
            return self._session

        def __exit__(self, exc_type=None, exc_val=None, exc_tb=None):
            assert self._session is not None
            if exc_type is not None or self._session.closing():
                # self._session.delete()
                self._session = None
            else:
                self._checker.sessions.append(self._session)
                self._session = None

    @gen.coroutine
    def async_session(self):
        if self.sessions:
            session = self.sessions.pop()
        else:
            session = yield self.driver.table_client.session().async_create()
        raise gen.Return(self.SessionContext(self, session))

    def sync_session(self):
        if self.sessions:
            session = self.sessions.pop()
        else:
            session = self.driver.table_client.session().create()
        return self.SessionContext(self, session)

    @gen.coroutine
    def async_perform_point_reads(self, history, table, options, checker, deadline):
        with (yield self.async_session()) as session:
            read_query = yield session.async_prepare(QUERY_POINT_READS.format(
                TABLE=table))

            while time.time() < deadline:
                keys = checker.select_read_from_write_keys(cnt=random.randint(1, options.shards))
                if not keys:
                    # There are not enough in-progress writes to this table yet, spin a little
                    yield gen.sleep(0.000001)
                    continue

                node = history.add(History.Begin('reads', None, read_keys=keys)).apply_to(checker)

                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    rss = yield tx.async_execute(
                        read_query,
                        parameters={
                            '$data': [
                                {'key': key} for key in keys
                            ],
                        },
                        commit_tx=simple_tx,
                    )
                    if not simple_tx:
                        yield tx.async_commit()
                except ydb.Aborted:
                    history.add(History.Abort('reads', node.value)).apply_to(checker)
                except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                    pass  # transaction outcome unknown
                else:
                    values = {}
                    for row in rss[0].rows:
                        values[row.key] = row.value

                    history.add(History.Commit('reads', node.value, values)).apply_to(checker)
                finally:
                    if tx.tx_id is not None:
                        try:
                            yield tx.async_rollback()
                        except ydb.Error:
                            pass

    @gen.coroutine
    def async_perform_point_writes(self, history, table, options, checker, deadline):
        with (yield self.async_session()) as session:
            write_query = yield session.async_prepare(QUERY_POINT_WRITES.format(
                TABLE=table))

            while time.time() < deadline:
                keys = checker.select_write_keys(cnt=random.randint(1, options.shards))

                node = history.add(History.Begin('writes', None, write_keys=keys)).apply_to(checker)

                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    yield tx.async_execute(
                        write_query,
                        parameters={
                            '$data': [
                                {'key': key, 'value': node.value} for key in keys
                            ],
                        },
                        commit_tx=simple_tx,
                    )
                    if not simple_tx:
                        yield tx.async_commit()
                except ydb.Aborted:
                    history.add(History.Abort('writes', node.value)).apply_to(checker)
                except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                    pass  # transaction outcome unknown
                else:
                    history.add(History.Commit('writes', node.value)).apply_to(checker)
                finally:
                    if tx.tx_id is not None:
                        try:
                            yield tx.async_rollback()
                        except ydb.Error:
                            pass

                checker.release_write_keys(keys)

    @gen.coroutine
    def async_perform_point_reads_writes(self, history, table, options, checker, deadline, keysets):
        with (yield self.async_session()) as session:
            read_query = yield session.async_prepare(QUERY_POINT_READS.format(
                TABLE=table))
            write_query = yield session.async_prepare(QUERY_POINT_WRITES.format(
                TABLE=table))
            read_write_query = yield session.async_prepare(QUERY_POINT_READS_WRITES.format(
                TABLE=table))

            while time.time() < deadline:
                read_keys = checker.select_read_keys(cnt=random.randint(1, options.shards))
                write_keys = checker.select_write_keys(cnt=random.randint(1, options.shards))

                keysets.add(tuple(sorted(write_keys)))

                node = history.add(History.Begin('reads+writes', None, read_keys=read_keys, write_keys=write_keys)).apply_to(checker)

                # Read/Write tx may fail with TLI
                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    if simple_tx:
                        rss = yield tx.async_execute(
                            read_write_query,
                            parameters={
                                '$reads': [
                                    {'key': key} for key in read_keys
                                ],
                                '$writes': [
                                    {'key': key, 'value': node.value} for key in write_keys
                                ],
                            },
                            commit_tx=True,
                        )
                    else:
                        rss = yield tx.async_execute(
                            read_query,
                            parameters={
                                '$data': [
                                    {'key': key} for key in read_keys
                                ],
                            },
                            commit_tx=False,
                        )
                        yield tx.async_execute(
                            write_query,
                            parameters={
                                '$data': [
                                    {'key': key, 'value': node.value} for key in write_keys
                                ],
                            },
                            commit_tx=True,
                        )
                except ydb.Aborted:
                    history.add(History.Abort('reads+writes', node.value)).apply_to(checker)
                except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                    pass  # transaction outcome unknown
                else:
                    values = {}
                    for row in rss[0].rows:
                        values[row.key] = row.value

                    history.add(History.Commit('reads+writes', node.value, values)).apply_to(checker)
                finally:
                    if tx.tx_id is not None:
                        try:
                            yield tx.async_rollback()
                        except ydb.Error:
                            pass

                keysets.discard(tuple(sorted(write_keys)))

                checker.release_write_keys(write_keys)

    @gen.coroutine
    def async_perform_verifying_reads(self, history, table, options, checker, deadline, keysets):
        with (yield self.async_session()) as session:
            read_query = yield session.async_prepare(QUERY_POINT_READS.format(
                TABLE=table))

            while time.time() < deadline:
                if not keysets:
                    # There are not enough in-progress writes to this table yet, spin a little
                    yield gen.sleep(0.000001)
                    continue

                keys = random.choice(list(keysets))

                node = history.add(History.Begin('reads_of_writes', None, read_keys=keys)).apply_to(checker)

                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    rss = yield tx.async_execute(
                        read_query,
                        parameters={
                            '$data': [
                                {'key': key} for key in keys
                            ],
                        },
                        commit_tx=simple_tx,
                    )
                    if not simple_tx:
                        yield tx.async_commit()
                except ydb.Aborted:
                    history.add(History.Abort('reads_of_writes', node.value)).apply_to(checker)
                except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                    pass  # transaction outcome unknown
                else:
                    values = {}
                    for row in rss[0].rows:
                        values[row.key] = row.value

                    history.add(History.Commit('reads_of_writes', node.value, values)).apply_to(checker)
                finally:
                    if tx.tx_id is not None:
                        try:
                            yield tx.async_rollback()
                        except ydb.Error:
                            pass

    @gen.coroutine
    def async_perform_range_reads(self, history, table, options, checker, deadline):
        with (yield self.async_session()) as session:
            range_query = yield session.async_prepare(QUERY_RANGE_READS.format(
                TABLE=table))

            while time.time() < deadline:
                min_key = random.randint(0, options.keys)
                max_key = random.randint(min_key, options.keys)
                read_keys = list(range(min_key, max_key + 1))

                node = history.add(History.Begin('read_range', None, read_keys=read_keys)).apply_to(checker)

                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    rss = yield tx.async_execute(
                        range_query,
                        parameters={
                            '$minKey': min_key,
                            '$maxKey': max_key,
                        },
                        commit_tx=simple_tx,
                    )
                    if not simple_tx:
                        yield tx.async_commit()
                except ydb.Aborted:
                    history.add(History.Abort('read_range', node.value)).apply_to(checker)
                except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                    pass  # transaction outcome unknown
                else:
                    values = {}
                    for row in rss[0].rows:
                        values[row.key] = row.value

                    history.add(History.Commit('read_range', node.value, values)).apply_to(checker)
                finally:
                    if tx.tx_id is not None:
                        try:
                            yield tx.async_rollback()
                        except ydb.Error:
                            pass

    @gen.coroutine
    def async_perform_read_tables(self, history, table, options, checker, deadline):
        with (yield self.async_session()) as session:
            while time.time() < deadline:
                if options.read_table_ranges:
                    min_key = random.randint(0, options.keys)
                    max_key = random.randint(min_key, options.keys)
                    read_keys = list(range(min_key, max_key + 1))
                    key_range = ydb.KeyRange(
                        ydb.KeyBound((min_key,), KEY_PREFIX_TYPE, inclusive=True),
                        ydb.KeyBound((max_key,), KEY_PREFIX_TYPE, inclusive=True))
                else:
                    read_keys = sorted(checker.keys)
                    key_range = None

                if not options.ignore_read_table:
                    node = history.add(History.Begin('read_table', None, read_keys=read_keys)).apply_to(checker)

                values = {}
                failed = False
                for chunk_future in session.async_read_table(table, key_range, use_snapshot=options.read_table_snapshot):
                    try:
                        chunk = yield chunk_future
                    except StopIteration:
                        break
                    except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                        failed = True
                        break
                    for row in chunk.rows:
                        values[row.key] = row.value

                if not options.ignore_read_table:
                    # We mark ReadTable committed even when it fails
                    history.add(History.Commit('read_table', node.value, values, flags='partial_read' if failed else None)).apply_to(checker)

    @gen.coroutine
    def async_perform_test(self, history, table, options, checker):
        futures = []

        deadline = time.time() + options.seconds

        for _ in range(options.readers):
            futures.append(self.async_perform_point_reads(history, table, options, checker, deadline=deadline))

        for _ in range(options.writers):
            futures.append(self.async_perform_point_writes(history, table, options, checker, deadline=deadline))

        readwrite_keysets = set()
        for _ in range(options.readwriters):
            futures.append(self.async_perform_point_reads_writes(history, table, options, checker, deadline=deadline, keysets=readwrite_keysets))
            futures.append(self.async_perform_verifying_reads(history, table, options, checker, deadline=deadline, keysets=readwrite_keysets))

        for _ in range(options.rangereaders):
            futures.append(self.async_perform_range_reads(history, table, options, checker, deadline=deadline))

        for _ in range(options.readtablers):
            futures.append(self.async_perform_read_tables(history, table, options, checker, deadline=deadline))

        waiter = gen.WaitIterator(*futures)
        while not waiter.done():
            yield waiter.next()

    def before_test(self, table, options):
        with self.sync_session() as session:
            splits = []
            for i in range(options.shards - 1):
                splits.append(ydb.KeyBound((options.keys * (i + 1) // options.shards,)))
            profile = (
                ydb.TableProfile()
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_explicit_partitions(
                        ydb.ExplicitPartitions(splits)
                    )
                )
            )
            description = (
                ydb.TableDescription()
                .with_column(ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                .with_column(ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.Uint64)))
                .with_primary_key('key')
                .with_profile(profile)
            )

            if self.logger is not None:
                self.logger.info('Creating %s', table)

            session.create_table(table, description)

    def before_verify(self, history, table, options, checker):
        with self.sync_session() as session:
            node = history.add(History.Begin('read_table_final', None, read_keys=checker.keys, linearizable='global')).apply_to(checker)

            if self.logger is not None:
                self.logger.info('Reading table %s', table)

            while True:
                values = {}
                try:
                    for chunk in session.read_table(table):
                        for row in chunk.rows:
                            values[row.key] = row.value
                except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                    if self.logger is not None:
                        self.logger.info('Temporary failure, retrying...')
                    continue
                else:
                    break

            history.add(History.Commit('read_table_final', node.value, values)).apply_to(checker)

    def after_test(self, table, options):
        with self.sync_session() as session:
            # Avoid leaving tables around
            if self.logger is not None:
                self.logger.info('Dropping %s', table)
            session.drop_table(table)

    def run(self, options=None):
        if options is None:
            options = DatabaseCheckerOptions()

        table = self.path + '/' + datetime.now().strftime('%Y%m%d_%H%M%S_') + generate_random_name()

        self.before_test(table, options)

        keep_data = False

        try:
            # Assume table is initially empty
            history = History()
            checker = SerializabilityChecker(logger=self.logger)
            history.add(History.Prepare(options.keys)).apply_to(checker)

            if self.logger is not None:
                self.logger.info('Starting load test...')

            IOLoop.current().run_sync(lambda: self.async_perform_test(history, table, options, checker))

            self.before_verify(history, table, options, checker)

            if self.logger is not None:
                self.logger.info(
                    'Generated %d nodes (%d committed, %d aborted, %d unknown)',
                    len(checker.nodes),
                    len(checker.committed),
                    len(checker.aborted),
                    len(checker.nodes) - len(checker.committed) - len(checker.aborted))

            checker.verify()

        except SerializabilityError as e:
            keep_data = True
            e.table = table
            e.history = history
            raise

        finally:
            if not keep_data:
                self.after_test(table, options)

        if self.logger is not None:
            self.logger.info('OK')
