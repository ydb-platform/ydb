from conftest import BaseTestSet
from ydb.tests.olap.scenario.helpers import (
    ScenarioTestHelper,
    TestContext,
    CreateTable,
    CreateTableStore,
    AlterTableLikeObject,
    AlterTable,
    AlterTableStore,
    DropTable,
    DropTableStore,
    AlterColumnFamily,
    AlterCompressionLevel,
    AddColumnFamily,
    AlterColumn,
    AlterFamily
)

from typing import List, Dict, Any
from ydb import PrimitiveType
from ydb.tests.olap.lib.utils import get_external_param, external_param_is_true
from datetime import datetime, timedelta
from string import ascii_lowercase
import random, threading, copy, logging, time

class TestAlterCompression(BaseTestSet):
    schema1 = (
        ScenarioTestHelper.Schema()
        .with_column(name='Key', type=PrimitiveType.Uint64, not_null=True)
        .with_column(name='Field', type=PrimitiveType.Utf8, not_null=True)
        .with_column(name='Doub', type=PrimitiveType.Double, not_null=True)
        .with_key_columns('Key')
    )

    class TestThread(threading.Thread):
        def run(self) -> None:
            self.exc = None
            try:
                self.ret = self._target(*self._args, **self._kwargs)
            except BaseException as e:
                self.exc = e

        def join(self, timeout=None):
            super().join(timeout)
            if self.exc:
                raise self.exc
            return self.ret

    def _loop_upsert(self, ctx: TestContext, table_path: str, start_index: int, count_rows: int, duration: timedelta):
        sth = ScenarioTestHelper(ctx)
        rows_written = 0
        deadline = datetime.now() + duration
        while datetime.now() < deadline:
            data: List[Dict[str, Any]] = []
            for i in range(rows_written, rows_written + count_rows):
                data.append({'Key' : i + start_index, 'Field' : f"Field_{i + start_index}", 'Doub' : (i + start_index) + 0.1 * ((i + start_index) % 10)})
            sth.bulk_upsert_data(table_path, self.schema1, data)
            rows_written += count_rows
            # assert sth.get_table_rows_count(table_path) == rows_written

    def _loop_alter_table(self, ctx: TestContext, action: AlterTableLikeObject, duration: timedelta):
        data_types = [PrimitiveType.Double, PrimitiveType.Int32, PrimitiveType.Uint64, PrimitiveType.Datetime, PrimitiveType.Utf8, PrimitiveType.String]
        deadline = datetime.now() + duration
        sth = ScenarioTestHelper(ctx)
        while datetime.now() < deadline:
            column_name = f'tmp_column_{threading.get_ident()}_' + ''.join(random.choice(ascii_lowercase) for _ in range(8))
            sth.execute_scheme_query(copy.deepcopy(action).add_column(sth.Column(column_name, random.choice(data_types))), retries=10)
            sth.execute_scheme_query(copy.deepcopy(action).drop_column(column_name), retries=10)
    
    def _upsert_and_alter(self, ctx: TestContext, is_standalone_tables: bool, table_store: str, tables: list[str], count_rows: int, duration: timedelta):
        sth = ScenarioTestHelper(ctx)
        threads = []
        if not is_standalone_tables:
            threads.append(self.TestThread(target=self._loop_alter_table, args=[ctx, AlterTableStore(table_store), duration]))
        
        for table in tables:
            start_index = sth.get_table_rows_count(table)
            if is_standalone_tables:
                threads.append(self.TestThread(target=self._loop_alter_table, args=[ctx, AlterTable(table), duration]))
            threads.append(self.TestThread(target=self._loop_upsert, args=[ctx, table, start_index, count_rows, duration]))

        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    def _get_volumes_column(self, ctx: TestContext, table_name: str, column_name: str) -> tuple[int, int]:
        sth = ScenarioTestHelper(ctx)
        pred_raw_bytes, pred_bytes = 0, 0
        raw_bytes, bytes = sth.get_volumes_columns(table_name, column_name)
        while pred_raw_bytes != raw_bytes and pred_bytes != bytes:
            pred_raw_bytes = raw_bytes
            pred_bytes = bytes
            time.sleep(5)
            raw_bytes, bytes = sth.get_volumes_columns(table_name, column_name)
        return raw_bytes, bytes
    
    def _volumes_columns(self, ctx: TestContext, tables: list[str], column_names: list[str]) -> dict[str, dict[str, tuple[int, int]]]:
        volumes: dict[str, dict[str, tuple[int, int]]] = dict()
        for table in tables: 
            volumes.setdefault(table, dict())
            for column_name in column_names:
                raw_bytes, bytes = self._get_volumes_column(ctx=ctx, table_name=table, column_name=column_name)
                volumes[table][column_name] = raw_bytes, bytes
                logging.info(f"Table: `{table}` Column: `{column_name}` raw_bytes = {raw_bytes}, bytes = {bytes}")
        return volumes
    

    def _read_data(self, ctx: TestContext, tables: list[str], column_names: list[str]) -> bool:
        sth = ScenarioTestHelper(ctx)
        columns = ', '.join(column_names)
        for table in tables:
            count_rows: int = sth.get_table_rows_count(table)
            scan_result = sth.execute_scan_query(f'SELECT {columns} FROM `{sth.get_full_path(table)}` ORDER BY Key')
            for i in range(count_rows):
                if not (scan_result.result_set.rows[i]['Key'] == i and scan_result.result_set.rows[i]['Field'] == f"Field_{i}" and scan_result.result_set.rows[i]['Doub'] == i + 0.1 * (i % 10)):
                    return False
        return True
    
    def _scenario(self, ctx: TestContext, tables: list[str], column_names: list[str], alter_action: AlterTable):
        sth = ScenarioTestHelper(ctx)
        self._upsert_and_alter(ctx=ctx, is_standalone_tables=self.is_standalone_tables, table_store=self.table_store, tables=tables, count_rows=self.count_rows_for_bulk_upsert, duration=self.duration_alter_and_insert)
        self._read_data(ctx=ctx, tables=tables, column_names=column_names)
        prev_volumes: dict[str, dict[str, tuple[int, int]]] = self._volumes_columns(ctx=ctx, tables=tables, column_names=column_names)
        sth.execute_scheme_query(alter_action)
        current_volumes: dict[str, dict[str, tuple[int, int]]] = self._volumes_columns(ctx=ctx, tables=tables, column_names=column_names)
        assert(self._read_data(ctx, tables=tables, column_names=column_names))

    # working with the table store is not supported yet, so is_standalone_tables = True
    def scenario_alter_compression(self, ctx: TestContext):
        random.seed(2)
        n_tables = int(get_external_param('n_tables', '3'))
        # is_standalone_tables = external_param_is_true('test-standalone-tables')
        self.is_standalone_tables = True
        self.duration_alter_and_insert = timedelta(seconds=int(get_external_param('duration_alter_and_insert', '5')))
        self.count_rows_for_bulk_upsert = int(get_external_param('count_rows_for_bulk_upsert', '1000'))
        self.table_store = 'TableStore'

        sth = ScenarioTestHelper(ctx)

        if not self.is_standalone_tables:
            sth.execute_scheme_query(CreateTableStore(self.table_store).with_schema(self.schema1))

        tables: list[str] = []
        for i in range(n_tables):
            table_name = f'Table{i}'
            tables.append(table_name if self.is_standalone_tables else f'{self.table_store}/{table_name}')
            sth.execute_scheme_query(CreateTable(tables[-1]).with_schema(self.schema1))
        column_names: list[str] = [column.name for column in self.schema1.columns]
        column_families: list[ScenarioTestHelper.ColumnFamily] = [sth.ColumnFamily(name='family1', compression=ScenarioTestHelper.Compression.LZ4, compression_level=None), sth.ColumnFamily(name='family2', compression=ScenarioTestHelper.Compression.ZSTD, compression_level=1)]

        add_family_action = AlterTable(table_name)
        for family in column_families:
            add_family_action.action(AddColumnFamily(family))
        sth.execute_scheme_query(add_family_action)
        
        assert(self._read_data(ctx=ctx, tables=tables, column_names=column_names))

        for column_name in column_names:
            prev_compression_level = column_families[-1].compression_level
            
            for family in column_families:
                self._scenario(ctx=ctx, tables=tables, column_names=column_names, alter_action=AlterTable(table_name).action(AlterColumn(column_name, AlterFamily(family.name))))

            self._scenario(ctx=ctx, tables=tables, column_names=column_names, alter_action=AlterTable(table_name).action(AlterColumnFamily(column_families[-1].name, AlterCompressionLevel(9))))

            self._scenario(ctx=ctx, tables=tables, column_names=column_names, alter_action=AlterTable(table_name).action(AlterColumnFamily(column_families[-1].name, AlterCompressionLevel(0))))

            self._scenario(ctx=ctx, tables=tables, column_names=column_names, alter_action=AlterTable(table_name).action(AlterColumnFamily(column_families[-1].name, AlterCompressionLevel(prev_compression_level))))

        for table in tables:
            sth.execute_scheme_query(DropTable(table))
        if not self.is_standalone_tables:
            sth.execute_scheme_query(DropTableStore(self.table_store))
