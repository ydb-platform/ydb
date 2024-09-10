# -*- coding: utf-8 -*-
import argparse
import ydb
import logging
import time
import os
import random
import string


ydb.interceptor.monkey_patch_event_handler()


logger = logging.getLogger("StatisticsWorkload")


def table_name_with_prefix(table_prefix):
    table_suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    return os.path.join(table_prefix + "_" + table_suffix)


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
    def __init__(self, endpoint, database, duration, batch_size, batch_count):
        self.database = database
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.SessionPool(self.driver, size=200)
        self.duration = duration
        self.batch_size = batch_size
        self.batch_count = batch_count

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.stop()
        self.driver.stop()

    def run_query_ignore_errors(self, callee):
        try:
            self.pool.retry_operation_sync(callee)
        except Exception as e:
            logger.error(f'{type(e)}, {e}')

    def generate_batch(self, schema):
        data = []
        for i in range(self.batch_size):
            data.append({c.name: random_value(c.type) for c in schema})
        return data

    def get_tables(self):
        db = self.driver.scheme_client.list_directory(self.database)
        return [t.name for t in db.children]

    def create_table(self, table_name):
        logger.info(f"create table '{table_name}'")

        def callee(session):
            session.execute_scheme(f"""
                CREATE TABLE `{table_name}` (
                id    Int64 NOT NULL,
                value Int64,
                PRIMARY KEY(id)
                )
                PARTITION BY HASH(id)
                WITH (
                    STORE = COLUMN
                )
            """)
        self.run_query_ignore_errors(callee)

    def enable_statistics(self, table_name):
        def callee(session):
            session.execute_scheme(f"""
            ALTER OBJECT `{table_name}` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_key, TYPE=COUNT_MIN_SKETCH, FEATURES=`{'column_names': ['id']}`);
            """)
            session.execute_scheme(f"""
            ALTER OBJECT `{table_name}` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=cms_value, TYPE=COUNT_MIN_SKETCH, FEATURES=`{'column_names': ['value']}`);
            """)
        self.run_query_ignore_errors(callee)

    def drop_table(self, table_name):
        logger.info(f'drop table {table_name}')

        def callee(session):
            session.drop_table(table_name)
        self.run_query_ignore_errors(callee)

    def drop_all_tables_with_prefix(self, prefix):
        for t in self.get_tables():
            if t.startswith(prefix):
                self.drop_table(self.database + "/" + t)

    def list_columns(self, table_name):
        def callee(session):
            return session.describe_table(self.database + "/" + table_name).columns
        return self.pool.retry_operation_sync(callee)

    def add_batch(self, table_name, schema):
        column_types = ydb.BulkUpsertColumns()
        for c in schema:
            column_types.add_column(c.name, c.type)
        batch = self.generate_batch(schema)
        logger.info(f"batch size: {len(batch)}")
        self.driver.table_client.bulk_upsert(self.database + "/" + table_name, batch, column_types)

    def add_data(self, table_name):
        schema = self.list_columns(table_name)
        for i in range(self.batch_count):
            logger.info(f"add batch #{i}")
            self.add_batch(table_name, schema)

    def delete_from_table(self, table_name):
        logger.info(f"delete from table '{table_name}'")

        def callee(session):
            session.transaction().execute(f"DELETE FROM `{table_name}`", commit_tx=True)
        self.run_query_ignore_errors(callee)

    def rows_count(self, table_name):
        return self.driver.table_client.scan_query(f"SELECT count(*) FROM `{table_name}`").next().result_set.rows[0][0]

    def analyze(self, table_name):
        table_path = self.database + "/" + table_name
        logger.info(f"analyze '{table_name}'")

        def callee(session):
            session.execute_scheme(f"ANALYZE `{table_path}`")
        self.run_query_ignore_errors(callee)

    def execute(self):
        table_prefix = "test_table"
        table_name = table_name_with_prefix(table_prefix)
        table_statistics = ".metadata/_statistics"

        try:
            logger.info("start new round")

            self.pool.acquire()

            self.delete_from_table(table_statistics)
            if self.rows_count(table_statistics) > 0:
                logger.error(f"table '{table_statistics}' is not empty")
                return

            self.drop_all_tables_with_prefix(table_prefix)
            self.create_table(table_name)

            self.add_data(table_name)
            count = self.rows_count(table_name)
            logger.info(f"number of rows in table '{table_name}' {count}")
            if count == 0:
                logger.error(f"table {table_name} is empty")
                return

            logger.info("waiting to receive information about the table from scheme shard")
            time.sleep(300)

            self.analyze(table_name)

            count = self.rows_count(table_statistics)
            logger.info(f"number of rows in table '{table_statistics}' {count}")
            if count == 0:
                logger.error(f"table '{table_statistics}' is empty")
        except Exception as e:
            logger.error(f"{type(e)}, {e}")

    def run(self):
        started_at = time.time()

        while time.time() - started_at < self.duration:
            self.execute()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="statistics stability workload", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='localhost:2135', help="An endpoint to be used")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--batch_size', default=1000, help='Batch size for bulk insert')
    parser.add_argument('--batch_count', default=3, help='The number of butches to be inserted')
    parser.add_argument('--log_file', default='', help='Append log into specified file')

    args = parser.parse_args()

    if args.log_file:
        logging.basicConfig(
            filename=args.log_file,
            filemode='a',
            format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s',
            datefmt='%H:%M:%S',
            level=logging.INFO
        )

    with Workload(args.endpoint, args.database, args.duration, args.batch_size, args.batch_count) as workload:
        workload.run()
