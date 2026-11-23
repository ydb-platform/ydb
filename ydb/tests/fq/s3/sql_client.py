"""SQL helpers for S3 tests, using the same YDB driver as streaming tests."""

import copy
import json
import time
from dataclasses import dataclass, field

import ydb
from ydb import convert
from ydb.query.base import QueryExecMode

from ydb.tests.fq.streaming_common.common import YdbClient


def sql_string(value):
    return json.dumps(str(value), ensure_ascii=False)


def sql_identifier(value):
    return '`' + str(value).replace('`', '``') + '`'


def column_type(type_proto):
    kind = type_proto.WhichOneof('type')
    if kind == 'optional_type':
        return column_type(type_proto.optional_type.item).removesuffix(' NOT NULL')
    if kind == 'decimal_type':
        value = type_proto.decimal_type
        return f'Decimal({value.precision}, {value.scale}) NOT NULL'
    if kind == 'pg_type':
        return {16: 'pgbool', 20: 'pgint8', 23: 'pgint4', 25: 'pgtext', 700: 'pgfloat4', 701: 'pgfloat8'}[
            type_proto.pg_type.oid
        ]
    assert kind == 'type_id', f'Unsupported external-table column type: {type_proto}'
    from ydb.public.api.protos.ydb_value_pb2 import Type

    name = Type.PrimitiveTypeId.Name(type_proto.type_id)
    name = {
        'UTF8': 'Utf8',
        'JSON_DOCUMENT': 'JsonDocument',
        'DYNUMBER': 'DyNumber',
        'TZ_DATE': 'TzDate',
        'TZ_DATETIME': 'TzDatetime',
        'TZ_TIMESTAMP': 'TzTimestamp',
    }.get(name, name.title())
    return name + ' NOT NULL'


@dataclass
class SqlResult:
    result_sets: list = field(default_factory=list)
    stats: object = None
    exception: Exception | None = None
    name: str | None = None

    @property
    def error(self):
        if self.exception is None:
            return ''
        messages = []

        def collect(issues):
            for issue in issues:
                messages.append(issue.message)
                collect(issue.issues)

        collect(getattr(self.exception, 'issues', None) or [])
        return '\n'.join(messages) if messages else str(self.exception)

    @property
    def error_code(self):
        status = getattr(self.exception, 'status', None)
        return getattr(status, 'name', '')

    @property
    def ast(self):
        return self.stats.query_ast if self.stats is not None else ''

    @property
    def statistics(self):
        return json.loads(self.stats.query_plan) if self.stats is not None and self.stats.query_plan else {}

    def check(self, success=True):
        if success:
            assert self.exception is None, self.error
        else:
            assert self.exception is not None, 'SQL unexpectedly succeeded'
        return self

    def result_set(self, index=0, offset=0, limit=None):
        self.check()
        result = self.result_sets[index]
        if offset or limit is not None:
            return convert.ResultSet(
                result.columns,
                result.rows[offset : None if limit is None else offset + limit],
                result.truncated,
            )
        return result

    def metric(self, name):
        """Sum stage aggregates; ignore their per-node/per-task breakdowns."""
        values = []

        def visit(node):
            if isinstance(node, dict):
                stats = node.get('Stats', {})
                if name in stats:
                    value = stats[name]
                    values.append(int(value['Sum'] if isinstance(value, dict) else value))
                for key, value in node.items():
                    if key != 'Stats':
                        visit(value)
            elif isinstance(node, list):
                for value in node:
                    visit(value)

        visit(self.statistics)
        assert values, f'{name} is missing in query statistics: {self.statistics}'
        return sum(values)

    def __str__(self):
        return self.error or str(self.statistics)


class S3Client:
    def __init__(self, ydb_client: YdbClient, s3_url: str, directory: str):
        self.ydb_client = ydb_client
        self.s3_url = s3_url
        self.directory = directory
        settings = (
            ydb.QueryClientSettings()
            .with_native_timestamp_in_result_sets(False)
            .with_native_interval_in_result_sets(False)
            .with_native_json_in_result_sets(False)
            .with_native_date_in_result_sets(False)
            .with_native_datetime_in_result_sets(False)
        )
        self.pool = ydb.QuerySessionPool(ydb_client.driver, query_client_settings=settings)
        self.retry_settings = copy.copy(ydb_client.retry_settings)
        # Negative tests deliberately return INTERNAL_ERROR, which closes the
        # KQP session. Retrying would hide the original issue behind BAD_SESSION.
        self.retry_settings.retry_internal_error = False
        self.objects = []

    def path(self, name: str) -> str:
        """Resolve a schema object; TablePathPrefix does not qualify dynamic source names."""
        return name if name.startswith('/') else self.directory + '/' + name

    def execute(self, sql, *, explain=False, timeout=120, parameters=None):
        statement = f'PRAGMA TablePathPrefix = {sql_string(self.directory)};\n{sql}'
        result = SqlResult()

        def run(session):
            result.result_sets = []
            try:
                with session.execute(
                    statement,
                    parameters=parameters,
                    exec_mode=QueryExecMode.EXPLAIN if explain else QueryExecMode.EXECUTE,
                    stats_mode=ydb.QueryStatsMode.PROFILE,
                    settings=ydb.BaseRequestSettings().with_timeout(timeout),
                ) as stream:
                    result.result_sets = convert.aggregate_result_sets_by_index(stream)
            except ydb.issues.Error:
                # The server can retire a failed session before the attach
                # stream reports it. Never put that session back into service.
                session.delete()
                raise
            finally:
                result.stats = session.last_query_stats

        try:
            self.pool.retry_operation_sync(run, self.retry_settings)
        except ydb.issues.Error as error:
            result.exception = error
        return result

    def query(self, sql, **kwargs):
        return self.execute(sql, **kwargs).check().result_sets

    def start_topic_query(self, sql, topic_path, consumer, *, timeout=60):
        """Start SQL and wait for its topic reader before the caller writes data."""
        self.ydb_client.query(f'ALTER TOPIC {sql_identifier(topic_path)} ADD CONSUMER {sql_identifier(consumer)};')
        future = self.ydb_client.query_async(f'PRAGMA pq.Consumer = {sql_string(consumer)};\n{sql}', timeout=timeout)
        deadline = time.monotonic() + timeout
        while True:
            if future.done():
                future.result()
                raise AssertionError('Topic SQL query finished before input was written')
            description = self.ydb_client.driver.topic_client.describe_consumer(
                topic_path, consumer, include_stats=True
            )
            partitions = [partition for partition in description.partitions if partition.active]
            if partitions and all(
                partition.partition_consumer_stats is not None and partition.partition_consumer_stats.read_session_id
                for partition in partitions
            ):
                return future
            assert time.monotonic() < deadline, f'Topic reader did not start: {topic_path}, {consumer}'
            time.sleep(0.1)

    def create_external_data_source(self, name, bucket, *, expect_error=False):
        name = self.path(name)
        result = self.execute(f'''
            CREATE EXTERNAL DATA SOURCE {sql_identifier(name)} WITH (
                SOURCE_TYPE = 'ObjectStorage',
                LOCATION = {sql_string(self.s3_url + '/' + bucket + '/')},
                AUTH_METHOD = 'NONE'
            );
        ''')
        result.name = name
        result.check(not expect_error)
        if result.exception is None:
            self.objects.append(('EXTERNAL DATA SOURCE', name))
        return result

    def create_external_table(
        self,
        name,
        path,
        format,
        data_source,
        columns,
        *,
        format_setting=None,
        projection=None,
        partitioned_by=None,
        compression=None,
        expect_error=False,
    ):
        name = self.path(name)
        schema = ',\n'.join(f'{sql_identifier(col.name)} {column_type(col.type)}' for col in columns)
        options = {
            'DATA_SOURCE': self.path(data_source),
            'LOCATION': path,
            'FORMAT': format,
        }
        options.update(format_setting or {})
        if compression:
            options['COMPRESSION'] = compression
        if partitioned_by:
            options['PARTITIONED_BY'] = json.dumps(partitioned_by)
        if projection:
            options.update(projection)
        settings = ',\n'.join(f'{sql_identifier(key)} = {sql_string(value)}' for key, value in options.items())
        result = self.execute(f'CREATE EXTERNAL TABLE {sql_identifier(name)} ({schema}) WITH ({settings});')
        result.name = name
        result.check(not expect_error)
        if result.exception is None:
            self.objects.append(('EXTERNAL TABLE', name))
        return result

    def list_directory(self):
        return self.ydb_client.driver.scheme_client.list_directory(self.directory)

    def drop(self, kind, name):
        name = self.path(name)
        self.execute(f'DROP {kind} {sql_identifier(name)};').check()
        self.objects.remove((kind, name))

    def close(self):
        try:
            for kind, name in reversed(self.objects):
                self.execute(f'DROP {kind} IF EXISTS {sql_identifier(name)};').check()
            self.ydb_client.driver.scheme_client.remove_directory(self.directory)
        finally:
            self.pool.stop()
