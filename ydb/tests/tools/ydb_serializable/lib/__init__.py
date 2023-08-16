# -*- coding: utf-8 -*-
import json
import time
import random
from contextlib import contextmanager
from datetime import datetime
import asyncio
from ydb.tests.library.serializability.checker import SerializabilityError, SerializabilityChecker
import ydb.aio


KEY_PREFIX_TYPE = ydb.TupleType().add_element(ydb.OptionalType(ydb.PrimitiveType.Uint64))


class Query(ydb.DataQuery):
    __slots__ = ydb.DataQuery.__slots__ + ('declares', 'statements')

    def __init__(self, declares, statements, types):
        super().__init__(''.join(declares + statements), types)
        self.declares = declares
        self.statements = statements

    def __add__(self, other):
        if not isinstance(other, Query):
            raise TypeError('cannot add %r and %r' % (self, other))
        return Query(
            self.declares + other.declares,
            self.statements + other.statements,
            dict(**self.parameters_types, **other.parameters_types))


def generate_query_point_reads(table, var='$data'):
    declares = [
        '''\
        DECLARE {var} AS List<Struct<
            key: Uint64>>;
        '''.format(var=var),
    ]
    statements = [
        '''\
        SELECT t.key AS key, t.value AS value
        FROM AS_TABLE({var}) AS d
        INNER JOIN `{table}` AS t ON t.key = d.key;
        '''.format(table=table, var=var),
    ]
    row_type = (
        ydb.StructType()
           .add_member('key', ydb.PrimitiveType.Uint64))
    return Query(declares, statements, {var: ydb.ListType(row_type)})


def generate_query_point_writes(table, var='$data'):
    declares = [
        '''\
        DECLARE {var} AS List<Struct<
            key: Uint64,
            value: Uint64>>;
        '''.format(var=var),
    ]
    statements = [
        '''\
        UPSERT INTO `{table}`
        SELECT key, value
        FROM AS_TABLE({var});
        '''.format(table=table, var=var),
    ]
    row_type = (
        ydb.StructType()
           .add_member('key', ydb.PrimitiveType.Uint64)
           .add_member('value', ydb.PrimitiveType.Uint64))
    return Query(declares, statements, {var: ydb.ListType(row_type)})


def generate_query_point_reads_writes(table, readsvar='$reads', writesvar='$writes'):
    return (
        generate_query_point_reads(table, readsvar) +
        generate_query_point_writes(table, writesvar)
    )


def generate_query_range_reads(table, minvar='$minKey', maxvar='$maxKey'):
    declares = [
        '''\
        DECLARE {minvar} AS Uint64;
        DECLARE {maxvar} AS Uint64;
        '''.format(minvar=minvar, maxvar=maxvar),
    ]
    statements = [
        '''\
        SELECT key, value
        FROM `{table}`
        WHERE key >= {minvar} AND key <= {maxvar};
        '''.format(table=table, minvar=minvar, maxvar=maxvar),
    ]
    return Query(
        declares,
        statements,
        {
            minvar: ydb.PrimitiveType.Uint64,
            maxvar: ydb.PrimitiveType.Uint64,
        })


def generate_query_range_read_with_limit(table, minvar='$minKey', maxvar='$maxKey', limitvar='$limit'):
    declares = [
        f'''\
        DECLARE {minvar} AS Uint64;
        DECLARE {maxvar} AS Uint64;
        DECLARE {limitvar} AS Uint64;
        ''',
    ]
    statements = [
        f'''\
        SELECT key, value
        FROM `{table}`
        WHERE key >= {minvar} AND key <= {maxvar}
        ORDER BY key
        LIMIT {limitvar};
        ''',
    ]
    return Query(
        declares,
        statements,
        {
            minvar: ydb.PrimitiveType.Uint64,
            maxvar: ydb.PrimitiveType.Uint64,
            limitvar: ydb.PrimitiveType.Uint64,
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
        self.oplog = []

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
                raise ValueError('Unexpected value for linearizable: %r' % (self.linearizable,))
            for key in self.write_keys:
                checker.ensure_write_value(node, key)
            return node

    class Abort(object):
        def __init__(self, op, value, observed=None):
            self.op = op
            self.value = value
            if observed is None:
                self.observed = None
            elif isinstance(observed, (list, tuple)):
                self.observed = list(observed)
            elif isinstance(observed, dict):
                self.observed = sorted(observed.items())
            else:
                raise TypeError('Unexpected value for observed: %r' % (observed,))

        def to_json(self):
            result = ['abort', self.op, self.value]
            if self.observed is not None:
                result.append(self.observed)
            return result

        def apply_to(self, checker):
            node = checker.nodes.get(self.value)
            if node is None:
                raise RuntimeError('Abort of unknown node %r' % (self.value,))
            if self.observed is not None:
                seen = set()
                for key, value in self.observed:
                    checker.ensure_read_value(node, key, value)
                    seen.add(key)
                for key in node.expected_read_keys:
                    if key not in seen:
                        checker.ensure_read_value(node, key, 0)
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

    class OpLogEntry(object):
        def __init__(self, start_time, op, description):
            self.start_time = start_time
            self.end_time = None
            self.op = op
            self.description = description
            self._results = []

        def result(self, result):
            self._results.append(result)

        def done(self):
            if self.end_time is None:
                self.end_time = time.time()

        def to_json(self):
            return [self.start_time, self.end_time, self.op, self.description] + self._results

    def log_op_begin(self, op, description):
        entry = self.OpLogEntry(time.time(), op, description)
        self.oplog.append(entry)
        return entry

    @contextmanager
    def log_op(self, op, description):
        entry = self.log_op_begin(op, description)
        try:
            try:
                yield entry
            finally:
                entry.done()
        except BaseException as e:
            entry.result(f'Exception: {e}')
            raise

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

    def write_log_to_file(self, filename):
        if not self.oplog:
            return
        with open(filename, 'w') as f:
            first = True
            f.write('[')
            for item in self.oplog:
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
        self.rwrs = 0
        self.readwriters = 100
        self.readtablers = 100
        self.rangereaders = 100
        self.seconds = 2.0
        self.read_table_ranges = False
        self.ignore_read_table = False
        self.read_table_snapshot = None
        self.oplog_results = False


class DatabaseChecker(object):
    def __init__(self, endpoint, database, path=None, logger=None, print_unique_errors=0):
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

        self._stopping = False
        self._unique_errors = {}
        self._print_unique_errors = int(print_unique_errors)

    async def async_init(self):
        if self.pool is None:
            self.driver = ydb.aio.Driver(ydb.ConnectionParams(self.endpoint, self.database))
            await self.driver.wait(fail_fast=True)
            self.pool = ydb.aio.SessionPool(self.driver, 2 ** 32)

    async def __aenter__(self):
        await self.async_init()
        return self

    async def __aexit__(self, exc_type=None, exc_val=None, exc_tb=None):
        self._stopping = True
        await self.pool.stop()
        await self.driver.stop()

    def is_stopping(self):
        return self._stopping

    def _report_error(self, e):
        if not self._print_unique_errors:
            return
        text = '%s: %s' % (type(e), str(e))
        if text not in self._unique_errors:
            self._unique_errors[text] = 1
            if self._print_unique_errors > 1:
                import traceback
                traceback.print_exc()
            else:
                self.logger.debug(text)
        else:
            self._unique_errors[text] += 1
            if (self._unique_errors[text] % 1000) == 0:
                self.logger.debug('%s x%d' % (text, self._unique_errors[text]))

    async def async_retry_operation(self, callable, deadline):
        while time.time() < deadline and not self.is_stopping():
            try:
                async with self.pool.checkout() as session:
                    try:
                        try:
                            result = await callable(session)
                        except Exception as e:
                            self._report_error(e)
                            raise
                    except (ydb.Aborted, ydb.Undetermined, ydb.NotFound, ydb.InternalError):
                        raise  # these are not retried
                    except (ydb.Unavailable, ydb.Overloaded, ydb.ConnectionError):
                        continue  # retry
            except ydb.BadSession:
                continue  # retry
            return result
        else:
            if self.is_stopping():
                raise ydb.Aborted('stopping')
            else:
                raise ydb.Aborted('deadline reached')

    async def async_perform_point_reads(self, history, table, options, checker, deadline):
        read_query = generate_query_point_reads(table)

        while time.time() < deadline and not self.is_stopping():
            keys = checker.select_read_from_write_keys(cnt=random.randint(1, options.shards))
            if not keys:
                # There are not enough in-progress writes to this table yet, spin a little
                await asyncio.sleep(0.000001)
                continue

            node = history.add(History.Begin('reads', None, read_keys=keys)).apply_to(checker)
            observed_values = None

            async def perform(session):
                nonlocal observed_values
                observed_values = None
                async with session.transaction(ydb.SerializableReadWrite()) as tx:
                    simple_tx = bool(random.randint(0, 1))
                    with history.log_op(node.value, 'read+commit' if simple_tx else 'read') as log:
                        rss = await tx.execute(
                            read_query,
                            parameters={
                                '$data': [
                                    {'key': key} for key in keys
                                ],
                            },
                            commit_tx=simple_tx,
                        )
                    observed_values = {}
                    for row in rss[0].rows:
                        observed_values[row.key] = row.value
                    if options.oplog_results:
                        log.result(dict(observed_values))
                    if not simple_tx:
                        with history.log_op(node.value, 'commit'):
                            await tx.commit()

            try:
                await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('reads', node.value, observed_values)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                history.add(History.Commit('reads', node.value, observed_values)).apply_to(checker)

    async def async_perform_point_writes(self, history, table, options, checker, deadline):
        write_query = generate_query_point_writes(table)

        while time.time() < deadline and not self.is_stopping():
            keys = checker.select_write_keys(cnt=random.randint(1, options.shards))

            node = history.add(History.Begin('writes', None, write_keys=keys)).apply_to(checker)

            async def perform(session):
                async with session.transaction(ydb.SerializableReadWrite()) as tx:
                    simple_tx = bool(random.randint(0, 1))
                    with history.log_op(node.value, 'write+commit' if simple_tx else 'write'):
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
                        with history.log_op(node.value, 'commit'):
                            await tx.commit()

            try:
                await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('writes', node.value)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                history.add(History.Commit('writes', node.value)).apply_to(checker)

            checker.release_write_keys(keys)

    async def async_perform_point_rwr(self, history, table, options, checker, deadline):
        read1_query = generate_query_point_reads(table, '$reads1')
        write_query = generate_query_point_writes(table, '$writes')
        read2_query = generate_query_point_reads(table, '$reads2')
        rwr_query = read1_query + write_query + read2_query
        wr_query = write_query + read2_query

        while time.time() < deadline and not self.is_stopping():
            read1_cnt = random.randint(0, options.shards * 2)
            read1_keys = checker.select_read_keys(cnt=read1_cnt) if read1_cnt else []
            write_keys = checker.select_write_keys(cnt=random.randint(1, options.shards * 2))
            read2_keys = checker.select_read_from_write_keys(cnt=random.randint(1, options.shards * 2))

            read_keys = sorted(set(read1_keys).union(set(read2_keys).difference(set(write_keys))))
            write_keys_set = set(write_keys)

            node = history.add(History.Begin('rwr', None, read_keys=read_keys, write_keys=write_keys)).apply_to(checker)
            observed_values = None

            async def perform(session):
                nonlocal observed_values
                observed_values = None
                async with session.transaction(ydb.SerializableReadWrite()) as tx:
                    if read1_keys:
                        read1_params = {
                            '$reads1': [
                                {'key': key} for key in read1_keys
                            ],
                        }
                    else:
                        read1_params = {}
                    write_params = {
                        '$writes': [
                            {'key': key, 'value': node.value} for key in write_keys
                        ],
                    }
                    read2_params = {
                        '$reads2': [
                            {'key': key} for key in read2_keys
                        ],
                    }
                    simple_tx = bool(random.randint(0, 1))
                    fuse_commit = bool(random.randint(0, 1))
                    if simple_tx:
                        query = rwr_query if read1_keys else wr_query
                        parameters = dict(**read1_params, **write_params, **read2_params)
                        with history.log_op(node.value, 'read+write+read+commit' if read1_keys else 'write+read+commit') as log:
                            rss = await tx.execute(query, parameters, commit_tx=True)
                        if read1_keys:
                            read1 = rss[0]
                            read2 = rss[1]
                            if options.oplog_results:
                                log.result({row.key: row.value for row in read1.rows})
                                log.result({row.key: row.value for row in read2.rows})
                        else:
                            read1 = None
                            read2 = rss[0]
                            if options.oplog_results:
                                log.result({row.key: row.value for row in read2.rows})
                    elif read1_keys:
                        with history.log_op(node.value, 'read') as log:
                            rss = await tx.execute(read1_query, read1_params, commit_tx=False)
                        read1 = rss[0]
                        if options.oplog_results:
                            log.result({row.key: row.value for row in read1.rows})
                    else:
                        read1 = None
                    if read1_keys:
                        observed_values = {}
                        for row in read1.rows:
                            observed_values[row.key] = row.value
                        node.expected_read_keys = tuple(read1_keys)
                    if not simple_tx:
                        with history.log_op(node.value, 'write'):
                            await tx.execute(write_query, write_params, commit_tx=False)
                        with history.log_op(node.value, 'read+commit' if fuse_commit else 'read') as log:
                            rss = await tx.execute(read2_query, read2_params, commit_tx=fuse_commit)
                        read2 = rss[0]
                        if options.oplog_results:
                            log.result({row.key: row.value for row in read2.rows})
                    for row in read2.rows:
                        if row.key in write_keys_set:
                            if row.value != node.value:
                                raise SerializabilityError('Tx %r writes to key %r but then reads %r' % (node.value, row.key, row.value))
                        else:
                            if observed_values is None:
                                observed_values = {}
                            observed_values[row.key] = row.value
                    node.expected_read_keys = tuple(read_keys)
                    if not simple_tx and not fuse_commit:
                        with history.log_op(node.value, 'commit'):
                            await tx.commit()

            try:
                await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('rwr', node.value, observed_values)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                history.add(History.Commit('rwr', node.value, observed_values)).apply_to(checker)

            checker.release_write_keys(write_keys)

    async def async_perform_point_reads_writes(self, history, table, options, checker, deadline, keysets):
        read_query = generate_query_point_reads(table)
        write_query = generate_query_point_writes(table)
        read_write_query = generate_query_point_reads_writes(table)

        while time.time() < deadline and not self.is_stopping():
            read_keys = checker.select_read_keys(cnt=random.randint(1, options.shards))
            write_keys = checker.select_write_keys(cnt=random.randint(1, options.shards))

            keysets.add(tuple(sorted(write_keys)))

            node = history.add(History.Begin('reads+writes', None, read_keys=read_keys, write_keys=write_keys)).apply_to(checker)

            async def perform(session):
                # Read/Write tx may fail with TLI
                async with session.transaction(ydb.SerializableReadWrite()) as tx:
                    simple_tx = bool(random.randint(0, 1))
                    fuse_commit = bool(random.randint(0, 1))
                    if simple_tx:
                        with history.log_op(node.value, 'read+write+commit' if fuse_commit else 'read+write') as log:
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
                                commit_tx=fuse_commit,
                            )
                        if options.oplog_results:
                            log.result({row.key: row.value for row in rss[0].rows})
                    else:
                        with history.log_op(node.value, 'read') as log:
                            rss = await tx.execute(
                                read_query,
                                parameters={
                                    '$data': [
                                        {'key': key} for key in read_keys
                                    ],
                                },
                                commit_tx=False,
                            )
                        if options.oplog_results:
                            log.result({row.key: row.value for row in rss[0].rows})
                        with history.log_op(node.value, 'write+commit' if fuse_commit else 'write') as log:
                            await tx.execute(
                                write_query,
                                parameters={
                                    '$data': [
                                        {'key': key, 'value': node.value} for key in write_keys
                                    ],
                                },
                                commit_tx=fuse_commit,
                            )
                    if not fuse_commit:
                        with history.log_op(node.value, 'commit'):
                            await tx.commit()
                    return rss

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

        while time.time() < deadline and not self.is_stopping():
            if not keysets:
                # There are not enough in-progress writes to this table yet, spin a little
                await asyncio.sleep(0.000001)
                continue

            keys = random.choice(list(keysets))

            node = history.add(History.Begin('reads_of_writes', None, read_keys=keys)).apply_to(checker)

            async def perform(session):
                async with session.transaction(ydb.SerializableReadWrite()) as tx:
                    simple_tx = bool(random.randint(0, 1))
                    with history.log_op(node.value, 'read+commit' if simple_tx else 'read') as log:
                        rss = await tx.execute(
                            read_query,
                            parameters={
                                '$data': [
                                    {'key': key} for key in keys
                                ],
                            },
                            commit_tx=simple_tx,
                        )
                    if options.oplog_results:
                        log.result({row.key: row.value for row in rss[0].rows})
                    if not simple_tx:
                        with history.log_op(node.value, 'commit'):
                            await tx.commit()
                    return rss

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

        while time.time() < deadline and not self.is_stopping():
            min_key = random.randint(0, options.keys)
            max_key = random.randint(min_key, options.keys)
            read_keys = list(range(min_key, max_key + 1))

            node = history.add(History.Begin('read_range', None, read_keys=read_keys)).apply_to(checker)

            async def perform(session):
                async with session.transaction(ydb.SerializableReadWrite()) as tx:
                    simple_tx = bool(random.randint(0, 1))
                    with history.log_op(node.value, 'range_read+commit' if simple_tx else 'range_read') as log:
                        rss = await tx.execute(
                            range_query,
                            parameters={
                                '$minKey': min_key,
                                '$maxKey': max_key,
                            },
                            commit_tx=simple_tx,
                        )
                    if options.oplog_results:
                        log.result({row.key: row.value for row in rss[0].rows})
                    if not simple_tx:
                        with history.log_op(node.value, 'commit'):
                            await tx.commit()
                    return rss

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

    async def async_perform_range_reads_with_limit(self, history, table, options, checker, deadline):
        range_query = generate_query_range_read_with_limit(table)

        while time.time() < deadline and not self.is_stopping():
            min_key = random.randint(0, options.keys)
            max_key = random.randint(min_key, options.keys)
            limit = random.randint(1, options.keys)
            read_keys = list(range(min_key, max_key + 1))

            node = history.add(History.Begin('read_range_limit', None, read_keys=read_keys)).apply_to(checker)

            async def perform(session):
                async with session.transaction(ydb.SerializableReadWrite()) as tx:
                    simple_tx = bool(random.randint(0, 1))
                    with history.log_op(node.value, f'range_read_limit_{limit}+commit' if simple_tx else f'range_read_limit_{limit}') as log:
                        rss = await tx.execute(
                            range_query,
                            parameters={
                                '$minKey': min_key,
                                '$maxKey': max_key,
                                '$limit': limit,
                            },
                            commit_tx=simple_tx,
                        )
                    if options.oplog_results:
                        log.result({row.key: row.value for row in rss[0].rows})
                    if not simple_tx:
                        with history.log_op(node.value, 'commit'):
                            await tx.commit()
                    return rss

            try:
                rss = await self.async_retry_operation(perform, deadline)
            except ydb.Aborted:
                history.add(History.Abort('read_range_limit', node.value)).apply_to(checker)
            except ydb.Undetermined:
                pass  # transaction outcome unknown
            else:
                values = {}
                num_observed = 0
                last_observed = None
                for row in rss[0].rows:
                    num_observed += 1
                    last_observed = row.key
                    values[row.key] = row.value

                if num_observed > limit:
                    raise SerializabilityError(f'Tx {node.value} got {num_observed} rows with limit {limit}')

                if num_observed == limit:
                    # Iteration stops when reaching limit so it's ok not to see beyond the last row
                    node.expected_read_keys = tuple(range(min_key, last_observed + 1))

                history.add(History.Commit('read_range_limit', node.value, values)).apply_to(checker)

    async def async_perform_read_tables(self, history, table, options, checker, deadline):
        while time.time() < deadline and not self.is_stopping():
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
                try:
                    with history.log_op(node.value, 'read_table') as log:
                        chunks = await session.read_table(table, key_range, use_snapshot=options.read_table_snapshot)
                        async for chunk in chunks:
                            for row in chunk.rows:
                                values[row.key] = row.value
                            if options.oplog_results:
                                log.result({row.key: row.value for row in chunk.rows})
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

        for _ in range(options.rwrs):
            futures.append(self.async_perform_point_rwr(history, table, options, checker, deadline=deadline))

        readwrite_keysets = set()
        for _ in range(options.readwriters):
            futures.append(self.async_perform_point_reads_writes(history, table, options, checker, deadline=deadline, keysets=readwrite_keysets))
            futures.append(self.async_perform_verifying_reads(history, table, options, checker, deadline=deadline, keysets=readwrite_keysets))

        for i in range(options.rangereaders):
            if i % 2 == 0:
                futures.append(self.async_perform_range_reads(history, table, options, checker, deadline=deadline))
            else:
                futures.append(self.async_perform_range_reads_with_limit(history, table, options, checker, deadline=deadline))

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
                    with history.log_op(node.value, 'read_table_final') as log:
                        chunks = await session.read_table(table)
                        async for chunk in chunks:
                            for row in chunk.rows:
                                values[row.key] = row.value
                            if options.oplog_results:
                                log.result({row.key: row.value for row in chunk.rows})
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
