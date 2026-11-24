# -*- coding: utf-8 -*-

from ydb.tests.functional.ydb_cli.ydb_cli_helpers import BaseCliTestWithDatabase
from ydb.tests.oss.ydb_sdk_import import ydb

import os


class TestToolsTableOperations(BaseCliTestWithDatabase):

    @classmethod
    def setup_class(cls):
        super().setup_class()
        cls.session = cls.driver.table_client.session().create()

    def create_table_with_indexes_and_data(self, path, rows):
        self.session.create_table(
            path,
            ydb.TableDescription()
            .with_columns(
                ydb.Column('key', ydb.OptionalType(ydb.PrimitiveType.Uint32)),
                ydb.Column('id', ydb.OptionalType(ydb.PrimitiveType.Uint64)),
                ydb.Column('value', ydb.OptionalType(ydb.PrimitiveType.String))
            )
            .with_primary_keys('key')
            .with_indexes(
                ydb.TableIndex('by_id').with_index_columns('id'),
                ydb.TableIndex('by_value').with_index_columns('value')
            )
        )

        self.upsert_rows(path, rows)

    def upsert_rows(self, path, rows):
        query = self.session.prepare(
            f"""
            DECLARE $rows AS List<Struct<key: Uint32, id: Uint64, value: String>>;
            UPSERT INTO `{path}` SELECT * FROM AS_TABLE($rows);
            """
        )
        self.session.transaction().execute(
            query,
            {'$rows': [
                {'key': key, 'id': row_id, 'value': value.encode('utf-8')}
                for key, row_id, value in rows
            ]},
            commit_tx=True,
        )

    def verify_table(self, path, expected_rows, with_indexes=True):
        description = self.session.describe_table(path)
        actual_indexes = sorted((index.name, list(index.index_columns)) for index in description.indexes)
        expected_indexes = [('by_id', ['id']), ('by_value', ['value'])] if with_indexes else []
        assert actual_indexes == expected_indexes, f"Indexes at {path}: {actual_indexes}"

        # Read through each index as well as the table: metadata alone does not prove
        # that a copied or renamed index still points at the right data.
        views = [''] + [f' VIEW `{name}`' for name, _ in expected_indexes]
        for view in views:
            result = self.session.transaction().execute(
                f"SELECT `key`, `id`, `value` FROM `{path}`{view} ORDER BY `key`;",
                commit_tx=True,
            )
            actual_rows = [
                (row.key, row.id, row.value.decode('utf-8') if isinstance(row.value, bytes) else row.value)
                for row in result[0].rows
            ]
            assert actual_rows == expected_rows, f"Data at {path}{view}: {actual_rows}"

    def verify_table_names(self, directory, expected_names):
        actual_names = {entry.name for entry in self.driver.scheme_client.list_directory(directory).children}
        assert actual_names == expected_names

    def test_copy_multiple_tables_mixed_indexes(self, tmp_path):
        """Copy a working dataset with indexes and an archive without indexes."""
        directory = os.path.join(self.root_dir, tmp_path.name)
        self.driver.scheme_client.make_directory(directory)
        customers = os.path.join(directory, 'customers')
        events = os.path.join(directory, 'events')
        customers_copy = os.path.join(directory, 'customers_copy')
        events_copy = os.path.join(directory, 'events_copy')
        customer_rows = [(1, 101, 'Alice'), (2, 102, 'Bob')]
        event_rows = [(7, 701, 'created'), (9, 901, 'updated')]
        self.create_table_with_indexes_and_data(customers, customer_rows)
        self.create_table_with_indexes_and_data(events, event_rows)

        self.execute_ydb_cli_command([
            'tools', 'copy',
            '--item', f'source={customers},destination={tmp_path.name}/customers_copy',
            '--item', f'source={tmp_path.name}/events,destination={events_copy},omit-indexes=true',
        ])

        self.verify_table(customers_copy, customer_rows)
        self.verify_table(events_copy, event_rows, with_indexes=False)
        self.verify_table(events, event_rows)

        # The working table can keep changing without changing the copied snapshot.
        new_customer = [(3, 103, 'Carol')]
        self.upsert_rows(customers, new_customer)
        self.verify_table(customers, customer_rows + new_customer)
        self.verify_table(customers_copy, customer_rows)

    def test_rename_rollout_and_rollback(self, tmp_path):
        """Publish a staged table, retain a backup, then roll back the release."""
        directory = os.path.join(self.root_dir, tmp_path.name)
        self.driver.scheme_client.make_directory(directory)
        production = os.path.join(directory, 'production')
        staging = os.path.join(directory, 'staging')
        backup = os.path.join(directory, 'backup')
        original_rows = [(1, 101, 'old-one'), (2, 102, 'old-two')]
        staged_rows = [(1, 201, 'new-one'), (3, 203, 'new-three')]
        self.create_table_with_indexes_and_data(production, original_rows)
        self.create_table_with_indexes_and_data(staging, staged_rows)

        # An ordinary rename must not silently overwrite the live table.
        rejected = self.execute_ydb_cli_command([
            'tools', 'rename', '--item', f'src={staging},dst={production}',
        ], check_exit_code=False)
        assert rejected.exit_code != 0, rejected.stdout
        assert 'SCHEME_ERROR' in rejected.stderr, rejected.stderr
        self.verify_table(production, original_rows)
        self.verify_table(staging, staged_rows)
        self.verify_table_names(directory, {'production', 'staging'})

        self.execute_ydb_cli_command([
            'tools', 'rename',
            '--item', f'src={production},dst={tmp_path.name}/backup',
            '--item', f'src={tmp_path.name}/staging,dst={production}',
        ])
        self.verify_table(production, staged_rows)
        self.verify_table(backup, original_rows)
        self.verify_table_names(directory, {'production', 'backup'})

        self.execute_ydb_cli_command([
            'tools', 'rename',
            '--item', f'source={backup},destination={production},replace=true',
        ])
        self.verify_table(production, original_rows)
        self.verify_table_names(directory, {'production'})
