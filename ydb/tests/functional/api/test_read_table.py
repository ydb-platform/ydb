# -*- coding: utf-8 -*-
import logging
import pytest
import time

from hamcrest import assert_that, equal_to, less_than_or_equal_to, is_, none
from concurrent import futures

from ydb.tests.library.harness.kikimr_cluster import kikimr_cluster_factory
from ydb.tests.library.common.types import TabletTypes
from ydb.tests.oss.ydb_sdk_import import ydb
# call this as soon as possible to patch grpc event_handler implementation with additional calls
ydb.interceptor.monkey_patch_event_handler()

logger = logging.getLogger(__name__)


class AbstractReadTableTest(object):
    @classmethod
    def setup_class(cls):
        cls.cluster = kikimr_cluster_factory()
        cls.cluster.start()
        cls.logger = logger.getChild(cls.__name__)
        cls.driver = ydb.Driver(
            ydb.DriverConfig(
                database='/Root',
                endpoint="%s:%s" % (
                    cls.cluster.nodes[1].host, cls.cluster.nodes[1].port
                )
            )
        )
        cls.driver.wait()

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, 'cluster'):
            cls.cluster.stop()

    def _prepare_stream(self, method_kind, session, table_name):
        self.logger.debug("Preparing stream, %s", method_kind)
        callee = getattr(session, method_kind)
        return callee(table_name)

    def _consume_stream_chunk(self, stream):
        self.logger.debug("consuming stream chunk")
        chunk_or_future = next(stream)
        if isinstance(chunk_or_future, futures.Future):
            self.logger.debug("received chunk future")
            timeout = 10
            try:
                chunk_or_future = chunk_or_future.result(timeout=timeout)
            except futures.TimeoutError:
                self.logger.debug("Failed to consume chunk by timeout %d", timeout)
                raise RuntimeError("Failed to consume chunk")
        self.logger.debug("received chunk data, %s", chunk_or_future)
        return chunk_or_future

    def _prepare_test(self, name, partitions=8, with_data=True):
        session = ydb.retry_operation_sync(lambda: self.driver.table_client.session().create())
        table_name = '/Root/{}'.format(name)
        self.logger.debug("Preparing data for test %s", name)
        session.create_table(
            table_name,
            ydb.TableDescription()
            .with_primary_keys('Key1', 'Key2')
            .with_columns(
                ydb.Column('Key1', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column('Key2', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column('Value', ydb.OptionalType(ydb.PrimitiveType.Utf8)),
            )
            .with_profile(
                ydb.TableProfile()
                .with_partitioning_policy(
                    ydb.PartitioningPolicy()
                    .with_uniform_partitions(
                        partitions
                    )
                )
            )
        )
        prepared = session.prepare(
            'DECLARE $data as List<Struct<Key1: Uint64, Key2: Uint64, Value: Utf8>>; '
            'UPSERT INTO {} '
            'SELECT Key1, Key2, Value FROM '
            'AS_TABLE($data);'.format(
                name
            )
        )

        data_by_shard_id = {}
        with session.transaction() as tx:
            max_value = 2 ** 64
            shard_key_bound = max_value // partitions
            data = []

            for shard_id in range(partitions):
                data_by_shard_id[shard_id] = []

            for idx in range(partitions * partitions):
                shard_id = idx % partitions
                table_row = {'Key1': shard_id * shard_key_bound + idx, 'Key2': idx + 1000, 'Value': str(idx ** 4)}
                if with_data:
                    data_by_shard_id[shard_id].append(table_row)
                    data.append(table_row)

            tx.execute(
                prepared,
                commit_tx=True,
                parameters={
                    '$data': data
                }
            )
        self.logger.debug("Test successfully prepared")
        return session, table_name, data, data_by_shard_id

    def _kill_tablets(self):
        response = self.cluster.client.tablet_state(TabletTypes.FLAT_DATASHARD)
        tablet_ids = [info.TabletId for info in response.TabletStateInfo]
        for tablet_id in tablet_ids:
            self.cluster.client.tablet_kill(tablet_id)
        self.logger.debug("Killed tablets, tablet ids: %s", tablet_ids)


class TestReadTableSuccessStories(AbstractReadTableTest):
    def test_read_table_only_specified_ranges(self):
        session, table_name, _, by_shard_id = self._prepare_test('test_read_table_only_specified_ranges')

        description = session.describe_table(
            table_name, ydb.DescribeTableSettings().with_include_shard_key_bounds(
                True
            )
        )

        sizes_by_range = []
        for key_range in description.shard_key_ranges:
            stream = session.read_table(table_name, key_range)

            data_chunks = []
            while True:
                try:
                    chunk = self._consume_stream_chunk(stream)
                    data_chunks.append(chunk)
                    sizes_by_range.append(len(chunk.rows))
                except StopIteration:
                    break

        assert_that(
            sorted(sizes_by_range),
            equal_to(
                sorted(
                    list(
                        len(value)
                        for value in by_shard_id.values()
                    )
                )
            )
        )

    def test_read_table_constructed_key_range(self):
        session, table_name, data, _ = self._prepare_test('test_read_table_constructed_key_range')
        key_type = ydb.TupleType().add_element(
            ydb.OptionalType(ydb.PrimitiveType.Uint64)).add_element(
            ydb.OptionalType(ydb.PrimitiveType.Uint64)
        )
        key_prefix_type = ydb.TupleType().add_element(
            ydb.OptionalType(
                ydb.PrimitiveType.Uint64
            )
        )

        for table_row in data:
            table_iterator = session.read_table(
                table_name,
                ydb.KeyRange(
                    ydb.KeyBound.inclusive((table_row['Key1'], table_row['Key2']), key_type),
                    ydb.KeyBound.exclusive((table_row['Key1'], table_row['Key2'] + 100), key_type)
                )
            )
            read_rows_count = 0
            for chunk in table_iterator:
                read_rows_count += len(chunk.rows)

            assert_that(
                read_rows_count,
                equal_to(
                    1
                )
            )

            table_iterator = session.read_table(
                table_name,
                ydb.KeyRange(
                    ydb.KeyBound.inclusive((table_row['Key1'], ), key_prefix_type),
                    ydb.KeyBound.exclusive((table_row['Key1'] + 1, ), key_prefix_type)
                )
            )

            read_rows_count = 0
            for chunk in table_iterator:
                read_rows_count += len(chunk.rows)

            assert_that(
                read_rows_count,
                equal_to(
                    1
                )
            )

    def test_read_table_reads_only_specified_columns(self):
        session, table_name, _, _ = self._prepare_test('test_read_table_reads_only_specified_columns')
        stream = session.read_table(
            table_name,
            row_limit=25,
            ordered=True,
            columns=(
                'Key1',
            )
        )

        total_rows = 0
        data_chunks = []

        last_processed_row = None
        for chunk in stream:
            total_rows += len(chunk.rows)
            data_chunks.append(chunk)

            for row in chunk.rows:
                # ensure that only required columns are in answer
                assert_that(
                    row.get('Key2', None),
                    is_(
                        none()
                    )
                )

                if last_processed_row is not None:
                    assert_that(
                        last_processed_row.Key1,
                        less_than_or_equal_to(
                            row.Key1
                        )
                    )

                last_processed_row = row

        assert_that(
            total_rows,
            equal_to(
                25
            )
        )

    def test_read_table_without_data_has_snapshot(self):
        session, table_name, _, _ = self._prepare_test('test_read_table_without_data_has_snapshot', with_data=False)
        stream = session.read_table(
            table_name,
            ordered=True,
        )

        chunks = 0
        for chunk in stream:
            assert chunk.columns
            assert not chunk.rows
            assert chunk.snapshot is not None
            chunks += 1

        assert chunks == 1


class TestReadTableTruncatedResults(AbstractReadTableTest):
    @pytest.mark.parametrize('method_kind', ['async_read_table', 'read_table'])
    def test_truncated_results(self, method_kind):
        session, table_name, prepared_data, _ = self._prepare_test('test_truncated_results', partitions=32)

        sall_it = self.driver.table_client.scan_query('select * from `%s` order by Key1' % table_name)
        prev_key = None
        while True:
            try:
                next_response = next(sall_it)
            except StopIteration:
                break

            self._kill_tablets()

            for row in next_response.result_set.rows:
                self.logger.debug("Processing row %s", row)

                if prev_key is not None:
                    assert prev_key <= row.Key1

                prev_key = row.Key1

        self._kill_tablets()
        has_success = False
        for _ in range(6):
            stream = self._prepare_stream(method_kind, session, table_name)
            data_chunks, total_rows, iteration = [], 0, 0
            failed = False
            while True:
                iteration += 1

                try:
                    data = self._consume_stream_chunk(stream)
                    data_chunks.append(data)
                    total_rows += len(data.rows)
                except StopIteration:
                    break
                except ydb.Unavailable:
                    failed = True
                    break

            if failed:
                time.sleep(1)
                continue

            has_success = True
            assert_that(
                data_chunks[-1].truncated,
                equal_to(
                    True,  # investigate that value later
                )
            )

            assert_that(
                total_rows,
                equal_to(
                    len(prepared_data)
                )
            )

        assert has_success, "Unable to iterate over table after multiple iterations"


class TestReadTableWithTabletKills(AbstractReadTableTest):
    @pytest.mark.parametrize('method_kind', ['async_read_table', 'read_table'])
    def test_read_table_async_simple(self, method_kind):
        session, table_name, _, _ = self._prepare_test('test_read_table_async_simple')

        for _ in range(6):
            stream = self._prepare_stream(method_kind, session, table_name)
            data_chunks = []
            total_rows = 0

            iteration = 0
            while True:
                iteration += 1

                if iteration % 3 == 1:
                    self._kill_tablets()

                try:
                    data = self._consume_stream_chunk(stream)
                    data_chunks.append(data)
                    total_rows += len(data.rows)
                    logger.debug(iteration)
                except ydb.Unavailable as e:
                    self.logger.debug("received error, %s", e)
                    continue

                except StopIteration:
                    break
