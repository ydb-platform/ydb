# -*- coding: utf-8 -*-
import argparse
import ydb
import time
import os
import random
import string
from threading import Thread

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
        return random.randint(0, 60)
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
                a Int64,
                b Int64,
                c Int64,
                d Int64,
                e Int64,
                f Int64,
                g Int64,
                h Int64,
                i Int64,
                j Int64,
                k Int64,
                l Int64,
                m Int64,
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

    def list_columns(self, table_name):
        path = self.database + "/" + table_name
        def callee(session):
            return session.describe_table(path).columns
        return self.pool.retry_operation_sync(callee)

    def select_n(self, table_name, limit):
        print(f"Select {limit} from {table_name}")
        self.driver.table_client.scan_query(f"SELECT * FROM {table_name} limit {limit}").next()

    def run_many_upserts(self, thread_id, iters):
        table_name = "multi_upsert"
        schema = self.list_columns(table_name)
        for i in range(iters):
            self.add_batch(table_name, schema)
            self.select_n(table_name, 3)
            print(f'thread #{thread_id}: {i} of {iters} passed')

def run_workload(endpoint, database, duration, batch_size):
    with Workload(endpoint, database, duration, batch_size) as workload:
        time.sleep(1)
        workload.create_table('multi_upsert')
        time.sleep(1)
        start = timestamp()
        s = set()
        for i in range(2):
            s.add(Thread(target=workload.run_many_upserts, args=(i, 100)))
        
        for x in s:
            x.start()
        for x in s:
            x.join()
        
        end = timestamp()
        print(f'Elapsed time: {(end - start) / 1000:.2f} sec')
        
        workload.drop_table('multi_upsert')
    
    # with Workload(endpoint, database, duration, batch_size) as workload:
    #    workload.run()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="olap stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='localhost:2135', help="An endpoint to be used")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds.')
    parser.add_argument('--batch_size', default=1000, help='Batch size for bulk insert')
    args = parser.parse_args()
    run_workload(args.endpoint, args.database, args.duration, args.batch_size)
