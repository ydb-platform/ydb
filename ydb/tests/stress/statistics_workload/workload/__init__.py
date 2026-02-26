# -*- coding: utf-8 -*-
import ydb
from ydb.tests.stress.common.instrumented_pools import InstrumentedSessionPool, InstrumentedQuerySessionPool
import json
import logging
import time
import os
import random
import string
from ydb.tests.library.clients.kikimr_client import kikimr_client_factory
from ydb.tests.library.common.protobuf_ss import SchemeDescribeRequest

ydb.interceptor.monkey_patch_event_handler()


logger = logging.getLogger("StatisticsWorkload")


def table_name_with_prefix(table_prefix):
    table_suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))
    return os.path.join(table_prefix + "_" + table_suffix)


def random_string(length):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(length))


def random_type():
    return random.choice([ydb.PrimitiveType.Int64, ydb.PrimitiveType.String])


def random_value(type):
    if isinstance(type, ydb.OptionalType):
        return random_value(type.item)
    if type == ydb.PrimitiveType.Int64:
        return random.randint(0, 1 << 31)
    if type == ydb.PrimitiveType.String:
        return bytes(random_string(random.randint(1, 32)), encoding='utf8')


class Workload(object):
    def __init__(self, host, port, database, duration, batch_size, batch_count, prefix):
        self.database = database
        self.driver = ydb.Driver(ydb.DriverConfig(f"{host}:{port}", database))
        self.kikimr_client = kikimr_client_factory(host, port)
        self.pool = InstrumentedSessionPool(self.driver, size=200)
        self.duration = duration
        self.batch_size = batch_size
        self.batch_count = batch_count
        self.table_prefix = prefix

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

    def create_table(self, table_name):
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
        def callee(session):
            session.drop_table(table_name)
        self.run_query_ignore_errors(callee)

    def list_columns(self, table_path):
        def callee(session):
            return session.describe_table(table_path).columns
        return self.pool.retry_operation_sync(callee)

    def add_data(self, table_path, trace_id):
        logger.info(f"[{trace_id}] insert {self.batch_count} batches of {self.batch_size} bytes each")
        schema = self.list_columns(table_path)
        column_types = ydb.BulkUpsertColumns()

        for c in schema:
            column_types.add_column(c.name, c.type)

        for i in range(self.batch_count):
            logger.info(f"[{trace_id}] add batch #{i}")
            batch = self.generate_batch(schema)
            self.driver.table_client.bulk_upsert(table_path, batch, column_types)

    def rows_count(self, table_name):
        return self.driver.table_client.scan_query(f"SELECT count(*) FROM `{table_name}`").next().result_set.rows[0][0]

    def statistics_count(self, table_statistics, path_id):
        query = f"SELECT count(*) FROM `{table_statistics}` WHERE local_path_id = {path_id}"
        return self.driver.table_client.scan_query(query).next().result_set.rows[0][0]

    def analyze(self, table_path):
        def callee(session):
            session.execute_scheme(f"ANALYZE `{table_path}`")
        self.run_query_ignore_errors(callee)

    def get_planner_row_count_estimate(self, table_name):
        with InstrumentedQuerySessionPool(self.driver) as session_pool:
            res = session_pool.explain_with_retries(f"SELECT count(*) FROM {table_name}")
            logger.debug(f"SELECT count explain: {res}")
            explain = json.loads(res)

        def get_estimate(plan_node):
            if plan_node.get("Name") == "TableFullScan" and plan_node.get("Table") == table_name:
                rc = plan_node.get("E-Rows")
                return int(rc) if rc is not None else None
            for p in plan_node.get("Plans", []) + plan_node.get("Operators", []):
                rc = get_estimate(p)
                if rc is not None:
                    return rc

        rc = get_estimate(explain["Plan"])
        logger.info(f"planner row count estimate: {rc}")
        return rc

    def execute(self):
        table_name = table_name_with_prefix(self.table_prefix)
        table_path = self.database + "/" + table_name
        table_statistics = ".metadata/_statistics"
        trace_id = random_string(5)

        try:
            logger.info(f"[{trace_id}] start new round")

            self.pool.acquire()

            logger.info(f"[{trace_id}] create table '{table_name}'")
            self.create_table(table_name)

            scheme = self.kikimr_client.send(
                SchemeDescribeRequest(table_path).protobuf,
                method='SchemeDescribe'
            )
            path_id = scheme.PathDescription.Self.PathId
            logger.info(f"[{trace_id}] table '{table_name}' path id: {path_id}")

            self.add_data(table_path, trace_id)
            count = self.rows_count(table_name)
            logger.info(f"[{trace_id}] number of rows in table '{table_name}' {count}")
            if count != self.batch_count * self.batch_size:
                raise Exception(f"[{trace_id}] the number of rows in the '{table_name}' does not match the expected")

            logger.info(f"[{trace_id}] waiting to receive information about the table '{table_name}' from scheme shard")
            time.sleep(300)

            logger.info(f"[{trace_id}] analyze '{table_name}'")
            self.analyze(table_path)

            count = self.statistics_count(table_statistics, path_id)
            logger.info(f"[{trace_id}] number of rows in statistics table '{table_statistics}' {count}")
            if count == 0:
                raise Exception(f"[{trace_id}] statistics table '{table_statistics}' is empty")

            planner_rc = self.get_planner_row_count_estimate(table_name)
            if planner_rc != self.batch_count * self.batch_size:
                raise Exception(
                    f"[{trace_id}] row count planner estimate for '{table_name}': {planner_rc}"
                    f" does not match the expected value: {self.batch_count * self.batch_size}")
        except Exception as e:
            logger.error(f"[{trace_id}] {type(e)}, {e}")

        logger.info(f"[{trace_id}] drop table '{table_name}'")
        self.drop_table(table_path)

    def run(self):
        started_at = time.time()

        while time.time() - started_at < self.duration:
            self.execute()
