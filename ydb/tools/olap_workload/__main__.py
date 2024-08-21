# -*- coding: utf-8 -*-
import argparse
import ydb
import time
import os
import random
import string

ydb.interceptor.monkey_patch_event_handler()


def timestamp():
    return int(1000 * time.time())


def table_name_with_timestamp():
    return os.path.join("column_table_" + str(timestamp()))

def random_string(length):
   letters = string.ascii_lowercase
   return bytes(''.join(random.choice(letters) for i in range(length)), encoding='utf8')

def random_type():
    return random.choice([ydb.PrimitiveType.Int64, ydb.PrimitiveType.String])

def random_value(type):
    if isinstance(type, ydb.OptionalType):
        return random_value(type.item)
    if type == ydb.PrimitiveType.Int64:
        return random.randint(0, 1 << 31)
    if type == ydb.PrimitiveType.String:
        return random_string(random.randint(1, 32))


class Workload(object):
    def __init__(self, endpoint, database, duration, batch_size):
        self.database = database
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.SessionPool(self.driver, size=200)
        self.duration = duration
        self.batch_size = batch_size

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.stop()
        self.driver.stop()

    def run_query_ignore_errors(self, callee):
        try:
            self.pool.retry_operation_sync(callee)
        except Exception as e:
            print(type(e), e)

    def create_table(self, table_name):
        print(f"Create table {table_name}")

        def callee(session):
            session.execute_scheme(
                f"""
                CREATE TABLE {table_name} (
                id Int64 NOT NULL,
                i64Val Int64,
                PRIMARY KEY(id)
                )
                PARTITION BY HASH(id)
                WITH (
                    STORE = COLUMN
                )
            """
            )

        self.run_query_ignore_errors(callee)

    def drop_table(self, table_name):
        print(f"Drop table {table_name}")

        def callee(session):
            session.drop_table(self.database + "/" + table_name)

        self.run_query_ignore_errors(callee)

    def add_column(self, table_name, col_name, col_type):
        print(f"Add column {table_name}.{col_name} {str(col_type)}")

        def callee(session):
            session.execute_scheme(f"ALTER TABLE {table_name} ADD COLUMN {col_name} {str(col_type)}")

        self.run_query_ignore_errors(callee)

    def drop_column(self, table_name, col_name):
        print(f"Drop column {table_name}.{col_name}")

        def callee(session):
            session.execute_scheme(f"ALTER TABLE {table_name} DROP COLUMN {col_name}")

        self.run_query_ignore_errors(callee)

    def generate_batch(self, schema):
        data = []

        for i in range(self.batch_size):
            data.append({c.name: random_value(c.type) for c in schema})

        return data

    def add_batch(self, table_name, schema):
        print(f"Add batch {table_name}")

        column_types = ydb.BulkUpsertColumns()

        for c in schema:
            column_types.add_column(c.name, c.type)

        batch = self.generate_batch(schema)

        self.driver.table_client.bulk_upsert(self.database + "/" + table_name, batch, column_types)

    def list_tables(self):
        db = self.driver.scheme_client.list_directory(self.database)
        return [t.name for t in db.children if t.type == ydb.SchemeEntryType.COLUMN_TABLE]

    def list_columns(self, table_name):
        path = self.database + "/" + table_name

        def callee(session):
            return session.describe_table(path).columns

        return self.pool.retry_operation_sync(callee)

    def rows_count(self, table_name):
        return self.driver.table_client.scan_query(f"SELECT count(*) FROM {table_name}").next().result_set.rows[0][0]

    def select_n(self, table_name, limit):
        print(f"Select {limit} from {table_name}")
        self.driver.table_client.scan_query(f"SELECT * FROM {table_name} limit {limit}").next()

    def drop_all_tables(self):
        for t in self.list_tables():
            if t.startswith("column_table_"):
                self.drop_table(t)

    def drop_all_columns(self, table_name):
        for c in self.list_columns(table_name):
            if c.name != "id":
                self.drop_column(table_name, c.name)

    def queries_while_alter(self):
        table_name = "queries_while_alter"

        schema = self.list_columns(table_name)

        self.select_n(table_name, 1000)
        self.add_batch(table_name, schema)
        self.select_n(table_name, 100)
        self.add_batch(table_name, schema)
        self.select_n(table_name, 300)

        if len(schema) > 50:
            self.drop_all_columns(table_name)

        if self.rows_count(table_name) > 100000:
            self.drop_table(table_name)

        col = "col_" + str(timestamp())
        self.add_column(table_name, col, random_type())

    def run(self):
        started_at = time.time()

        while time.time() - started_at < self.duration:
            try:
                self.create_table("queries_while_alter")

                self.drop_all_tables()

                self.queries_while_alter()

                table_name = table_name_with_timestamp()
                self.create_table(table_name)
            except Exception as e:
                print(type(e), e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="olap stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='localhost:2135', help="An endpoint to be used")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds.')
    parser.add_argument('--batch_size', default=1000, help='Batch size for bulk insert')
    args = parser.parse_args()
    with Workload(args.endpoint, args.database, args.duration, args.batch_size) as workload:
        workload.run()
