# -*- coding: utf-8 -*-
import argparse
import ydb
import time
import os

ydb.interceptor.monkey_patch_event_handler()

def timestamp():
    return int(1000 * time.time())

def table_name_with_timestamp():
    return os.path.join("column_table_" + str(timestamp()))


class Workload(object):
    def __init__(self, endpoint, database, duration):
        self.database = database
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.SessionPool(self.driver, size=200)
        self.duration = duration

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.stop()
        self.driver.stop()

    def create_table(self, table_name):
        with self.pool.checkout() as s:
            try:
                s.execute_scheme(
                """
                    CREATE TABLE %s (
                    id Int64 NOT NULL,
                    i64Val Int64,
                    PRIMARY KEY(id)
                    )
                    PARTITION BY HASH(id)
                    WITH (
                        STORE = COLUMN
                    )
                """ % table_name
                )

                print("Table %s created" % table_name)
            except ydb.SchemeError as e:
                print(e)

    def drop_table(self, table_name):
        with self.pool.checkout() as s:
            try:
                s.drop_table(self.database + "/" + table_name)

                print("Table %s dropped" % table_name)
            except ydb.SchemeError as e:
                print(e)

    def run(self):
        started_at = time.time()

        while time.time() - started_at < self.duration:
            table_name = table_name_with_timestamp()
            self.create_table(table_name)

            time.sleep(5)

            self.drop_table(table_name)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="olap stability workload", formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--endpoint', default='localhost:2135', help="An endpoint to be used")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=10 ** 9, type=lambda x: int(x), help='A duration of workload in seconds.')
    args = parser.parse_args()
    with Workload(args.endpoint, args.database, args.duration) as workload:
        workload.run()
