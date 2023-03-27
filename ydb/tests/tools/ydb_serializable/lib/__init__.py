# -*- coding: utf-8 -*-
import json
import time
import random
from datetime import datetime
import asyncio
from ydb.tests.library.serializability.checker import SerializabilityError, SerializabilityChecker
import ydb.aio


KEY_PREFIX_TYPE = ydb.TupleType().add_element(ydb.OptionalType(ydb.PrimitiveType.Uint64))


def generate_query_point_reads(table, var='$data'):
    text = '''\
        DECLARE {var} AS List<Struct<
            key: Uint64>>;

        SELECT t.key AS key, t.value AS value
        FROM AS_TABLE({var}) AS d
        INNER JOIN `{TABLE}` AS t ON t.key = d.key;
    '''.format(TABLE=table, var=var)
    row_type = (
        ydb.StructType()
           .add_member('key', ydb.PrimitiveType.Uint64))
    return ydb.DataQuery(text, {
        var: ydb.ListType(row_type),
        })


def generate_query_point_writes(table, var='$data'):
    text = '''\
        DECLARE {var} AS List<Struct<
            key: Uint64,
            value: Uint64>>;

        UPSERT INTO `{TABLE}`
        SELECT key, value
        FROM AS_TABLE({var});
    '''.format(TABLE=table, var=var)
    row_type = (
        ydb.StructType()
           .add_member('key', ydb.PrimitiveType.Uint64)
           .add_member('value', ydb.PrimitiveType.Uint64))
    return ydb.DataQuery(text, {
        var: ydb.ListType(row_type),
        })


def generate_query_point_reads_writes(table, readsvar='$reads', writesvar='$writes'):
    text = '''\
        DECLARE {readsvar} AS List<Struct<
            key: Uint64>>;
        DECLARE {writesvar} AS List<Struct<
            key: Uint64,
            value: Uint64>>;

        SELECT t.key AS key, t.value AS value
        FROM AS_TABLE({readsvar}) AS d
        INNER JOIN `{TABLE}` AS t ON t.key = d.key;

        UPSERT INTO `{TABLE}`
        SELECT key, value
        FROM AS_TABLE({writesvar});
    '''.format(TABLE=table, readsvar=readsvar, writesvar=writesvar)
    reads_row_type = (
        ydb.StructType()
           .add_member('key', ydb.PrimitiveType.Uint64))
    writes_row_type = (
        ydb.StructType()
           .add_member('key', ydb.PrimitiveType.Uint64)
           .add_member('value', ydb.PrimitiveType.Uint64))
    return ydb.DataQuery(text, {
        readsvar: ydb.ListType(reads_row_type),
        writesvar: ydb.ListType(writes_row_type),
        })


def generate_query_range_reads(table, minvar='$minKey', maxvar='$maxKey'):
    text = '''\
        DECLARE {minvar} AS Uint64;
        DECLARE {maxvar} AS Uint64;

        SELECT key, value
        FROM `{TABLE}`
        WHERE key >= {minvar} AND key <= {maxvar};
    '''.format(TABLE=table, minvar=minvar, maxvar=maxvar)
    return ydb.DataQuery(text, {
        minvar: ydb.PrimitiveType.Uint64,
        maxvar: ydb.PrimitiveType.Uint64,
        })


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
        self.pool = None

    async def async_init(self):
        if self.pool is None:
            self.driver = ydb.aio.Driver(ydb.ConnectionParams(self.endpoint, self.database))
            await self.driver.wait(fail_fast=True)
            self.pool = ydb.aio.SessionPool(self.driver, 2 ** 32)

    async def __aenter__(self):
        await self.async_init()
        return self

    async def __aexit__(self, exc_type=None, exc_val=None, exc_tb=None):
        await self.pool.stop()
        await self.driver.stop()

    async def async_retry_operation(self, callable, deadline):
        while time.time() < deadline:
            try:
                async with self.pool.checkout() as session:
                    try:
                        result = await callable(session)
                    except (ydb.Aborted, ydb.Undetermined, ydb.NotFound, ydb.InternalError):
                        raise  # these are not retried
                    except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                        continue  # retry
            except ydb.BadSession:
                continue  # retry
            return result
        else:
            raise ydb.Aborted('deadline reached')

    async def async_perform_point_reads(self, history, table, options, checker, deadline):
        read_query = generate_query_point_reads(table)

        while time.time() < deadline:
            keys = checker.select_read_from_write_keys(cnt=random.randint(1, options.shards))
            if not keys:
                # There are not enough in-progress writes to this table yet, spin a little
                await asyncio.sleep(0.000001)
                continue

            node = history.add(History.Begin('reads', None, read_keys=keys)).apply_to(checker)

            async def perform(session):
                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    rss = await tx.execute(
                        read_query,
                        parameters={
                            '$data': [
                                {'key': key} for key in keys
                            ],
                        },
                        commit_tx=simple_tx,
                    )
                    if not simple_tx:
                        await tx.commit()
                    return rss
                finally:
                    if tx.tx_id is not None:
                        try:
                            await tx.rollback()
                        except ydb.Error:
                            pass

            try:
                rss = await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('reads', node.value)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                values = {}
                for row in rss[0].rows:
                    values[row.key] = row.value

                history.add(History.Commit('reads', node.value, values)).apply_to(checker)

    async def async_perform_point_writes(self, history, table, options, checker, deadline):
        write_query = generate_query_point_writes(table)

        while time.time() < deadline:
            keys = checker.select_write_keys(cnt=random.randint(1, options.shards))

            node = history.add(History.Begin('writes', None, write_keys=keys)).apply_to(checker)

            async def perform(session):
                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    await tx.execute(
                        write_query,
                        parameters={
                            '$data': [
                                {'key': key, 'value': node.value} for key in keys
                            ],
                        },
                        commit_tx=simple_tx,
                    )
                    if not simple_tx:
                        await tx.commit()
                finally:
                    if tx.tx_id is not None:
                        try:
                            await tx.rollback()
                        except ydb.Error:
                            pass

            try:
                await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('writes', node.value)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                history.add(History.Commit('writes', node.value)).apply_to(checker)

            checker.release_write_keys(keys)

    async def async_perform_point_reads_writes(self, history, table, options, checker, deadline, keysets):
        read_query = generate_query_point_reads(table)
        write_query = generate_query_point_writes(table)
        read_write_query = generate_query_point_reads_writes(table)

        while time.time() < deadline:
            read_keys = checker.select_read_keys(cnt=random.randint(1, options.shards))
            write_keys = checker.select_write_keys(cnt=random.randint(1, options.shards))

            keysets.add(tuple(sorted(write_keys)))

            node = history.add(History.Begin('reads+writes', None, read_keys=read_keys, write_keys=write_keys)).apply_to(checker)

            async def perform(session):
                # Read/Write tx may fail with TLI
                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    if simple_tx:
                        rss = await tx.execute(
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
                        rss = await tx.execute(
                            read_query,
                            parameters={
                                '$data': [
                                    {'key': key} for key in read_keys
                                ],
                            },
                            commit_tx=False,
                        )
                        await tx.execute(
                            write_query,
                            parameters={
                                '$data': [
                                    {'key': key, 'value': node.value} for key in write_keys
                                ],
                            },
                            commit_tx=True,
                        )
                    return rss
                finally:
                    if tx.tx_id is not None:
                        try:
                            await tx.rollback()
                        except ydb.Error:
                            pass

            try:
                rss = await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('reads+writes', node.value)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                values = {}
                for row in rss[0].rows:
                    values[row.key] = row.value

                history.add(History.Commit('reads+writes', node.value, values)).apply_to(checker)

            keysets.discard(tuple(sorted(write_keys)))

            checker.release_write_keys(write_keys)

    async def async_perform_verifying_reads(self, history, table, options, checker, deadline, keysets):
        read_query = generate_query_point_reads(table)

        while time.time() < deadline:
            if not keysets:
                # There are not enough in-progress writes to this table yet, spin a little
                await asyncio.sleep(0.000001)
                continue

            keys = random.choice(list(keysets))

            node = history.add(History.Begin('reads_of_writes', None, read_keys=keys)).apply_to(checker)

            async def perform(session):
                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    rss = await tx.execute(
                        read_query,
                        parameters={
                            '$data': [
                                {'key': key} for key in keys
                            ],
                        },
                        commit_tx=simple_tx,
                    )
                    if not simple_tx:
                        await tx.commit()
                    return rss
                finally:
                    if tx.tx_id is not None:
                        try:
                            await tx.rollback()
                        except ydb.Error:
                            pass

            try:
                rss = await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('reads_of_writes', node.value)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                values = {}
                for row in rss[0].rows:
                    values[row.key] = row.value

                history.add(History.Commit('reads_of_writes', node.value, values)).apply_to(checker)

    async def async_perform_range_reads(self, history, table, options, checker, deadline):
        range_query = generate_query_range_reads(table)

        while time.time() < deadline:
            min_key = random.randint(0, options.keys)
            max_key = random.randint(min_key, options.keys)
            read_keys = list(range(min_key, max_key + 1))

            node = history.add(History.Begin('read_range', None, read_keys=read_keys)).apply_to(checker)

            async def perform(session):
                tx = session.transaction(ydb.SerializableReadWrite())
                try:
                    simple_tx = bool(random.randint(0, 1))
                    rss = await tx.execute(
                        range_query,
                        parameters={
                            '$minKey': min_key,
                            '$maxKey': max_key,
                        },
                        commit_tx=simple_tx,
                    )
                    if not simple_tx:
                        await tx.commit()
                    return rss
                finally:
                    if tx.tx_id is not None:
                        try:
                            await tx.rollback()
                        except ydb.Error:
                            pass

            try:
                rss = await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('read_range', node.value)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                values = {}
                for row in rss[0].rows:
                    values[row.key] = row.value

                history.add(History.Commit('read_range', node.value, values)).apply_to(checker)

    async def async_perform_read_tables(self, history, table, options, checker, deadline):
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

            async def perform(session):
                values = {}
                failed = False
                chunks = await session.read_table(table, key_range, use_snapshot=options.read_table_snapshot)
                try:
                    async for chunk in chunks:
                        for row in chunk.rows:
                            values[row.key] = row.value
                except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                    failed = True
                return (values, failed)

            try:
                values, failed = await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('read_table', node.value)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                if not options.ignore_read_table:
                    # We mark ReadTable committed even when it fails
                    history.add(History.Commit('read_table', node.value, values, flags='partial_read' if failed else None)).apply_to(checker)

    async def async_perform_test(self, history, table, options, checker):
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

        random.shuffle(futures)

        await asyncio.gather(*futures)

    async def async_before_test(self, table, options):
        async def perform(session):
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

            await session.create_table(table, description)

        await self.pool.retry_operation(perform)

    async def async_before_verify(self, history, table, options, checker):
        async def perform(session):
            node = history.add(History.Begin('read_table_final', None, read_keys=checker.keys, linearizable='global')).apply_to(checker)

            if self.logger is not None:
                self.logger.info('Reading table %s', table)

            while True:
                values = {}
                try:
                    chunks = await session.read_table(table)
                    async for chunk in chunks:
                        for row in chunk.rows:
                            values[row.key] = row.value
                except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                    if self.logger is not None:
                        self.logger.info('Temporary failure, retrying...')
                    continue
                else:
                    break

            history.add(History.Commit('read_table_final', node.value, values)).apply_to(checker)

        await self.pool.retry_operation(perform)

    async def async_after_test(self, table, options):
        async def perform(session):
            # Avoid leaving tables around
            if self.logger is not None:
                self.logger.info('Dropping %s', table)
            await session.drop_table(table)

        await self.pool.retry_operation(perform)

    async def async_run(self, options=None):
        if options is None:
            options = DatabaseCheckerOptions()

        table = self.path + '/' + datetime.now().strftime('%Y%m%d_%H%M%S_') + generate_random_name()

        await self.async_before_test(table, options)

        keep_data = False

        try:
            # Assume table is initially empty
            history = History()
            checker = SerializabilityChecker(logger=self.logger)
            history.add(History.Prepare(options.keys)).apply_to(checker)

            if self.logger is not None:
                self.logger.info('Starting load test...')

            await self.async_perform_test(history, table, options, checker)

            await self.async_before_verify(history, table, options, checker)

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
                await self.async_after_test(table, options)

        if self.logger is not None:
            self.logger.info('OK')
