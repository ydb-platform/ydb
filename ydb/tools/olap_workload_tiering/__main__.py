# -*- coding: utf-8 -*-
import argparse
import ydb
import time
import datetime
import os
import random
import string
import dataclasses
import json
import typing as tp
import traceback

ydb.interceptor.monkey_patch_event_handler()


@dataclasses.dataclass
class ObjectStorageParams:
    endpoint: str
    bucket: str
    access_key: str
    secret_key: str
    scheme: str = 'HTTP'
    verify_ssl: bool = False

    def to_proto_str(self) -> str:
        return (
            f'Scheme: {self.scheme}\n'
            f'VerifySSL: {str(self.verify_ssl).lower()}\n'
            f'Endpoint: "{self.endpoint}"\n'
            f'Bucket: "{self.bucket}"\n'
            f'AccessKey: "{self.access_key}"\n'
            f'SecretKey: "{self.secret_key}"\n'
        )


@dataclasses.dataclass
class TieringRule:
    tier_name: str
    duration_for_evict: str

    def to_dict(self):
        return {
            'tierName': self.tier_name,
            'durationForEvict': self.duration_for_evict,
        }


@dataclasses.dataclass
class TieringPolicy:
    rules: list[TieringRule]

    def __init__(self, rules: tp.List[TieringRule]):
        self.rules = rules

    def to_json(self) -> str:
        return json.dumps({'rules': list(map(lambda x: x.to_dict(), self.rules))})


def timestamp():
    return int(1000000 * datetime.datetime.now().timestamp())


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


def generate_value(type):
    if isinstance(type, ydb.OptionalType):
        return generate_value(type.item)
    if type == ydb.PrimitiveType.Timestamp:
        return timestamp()
    return random_value(type)


class Workload(object):
    def __init__(self, endpoint, database, duration, bucket_configs: tp.List[ObjectStorageParams]):
        assert len(bucket_configs) == 2
        self.database = database
        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.pool = ydb.SessionPool(self.driver, size=200)
        self.duration = duration
        self.buckets = bucket_configs

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

    def create_table_store(self, path):
        print(f"Create table store {path}")

        def callee(session):
            session.execute_scheme(f"""
                CREATE TABLESTORE IF NOT EXISTS `{path}` (
                timestamp Timestamp NOT NULL,
                data String,
                PRIMARY KEY(timestamp)
                )
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 64
                )
            """)

        self.pool.retry_operation_sync(callee)

    def create_table(self, path):
        print(f"Create table {path}")

        def callee(session):
            session.execute_scheme(f"""
                CREATE TABLE IF NOT EXISTS `{path}` (
                timestamp Timestamp NOT NULL,
                data String,
                PRIMARY KEY(timestamp)
                )
                WITH (
                    STORE = COLUMN,
                    AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 64
                )
            """)

        self.pool.retry_operation_sync(callee)

    def create_tier(self, name, data_source: ObjectStorageParams):
        print(f"Create tier {name}")

        def callee(session):
            session.execute_scheme(f"""
                CREATE OBJECT IF NOT EXISTS `{name}` (TYPE TIER)
                    WITH (tierConfig = `Name: "{name}" ObjectStorage: {{{data_source.to_proto_str()}}}`);
                ALTER OBJECT `{name}` (TYPE TIER)
                    SET (tierConfig = `Name: "{name}" ObjectStorage: {{{data_source.to_proto_str()}}}`);
            """)

        self.pool.retry_operation_sync(callee)

    def create_tiering_rule(self, name, default_column: str, policy: TieringPolicy):
        print(f"Create tier {name}")

        def callee(session):
            session.execute_scheme(f"""
                CREATE OBJECT IF NOT EXISTS `{name}` (TYPE TIERING_RULE)
                    WITH (defaultColumn = {default_column}, description = `{policy.to_json()}`);
                ALTER OBJECT `{name}` (TYPE TIERING_RULE)
                    SET (defaultColumn = {default_column}, description = `{policy.to_json()}`);
            """)

        self.pool.retry_operation_sync(callee)

    def set_tiering_rule(self, table: str, tiering_rule: tp.Optional[str]):
        print(f"Alter table {table} set tiering rule {tiering_rule}")

        def callee(session):
            if tiering_rule is not None:
                session.execute_scheme(f"""
                    ALTER TABLE `{table}` SET TIERING "{tiering_rule}"
                """)
            else:
                session.execute_scheme(f"""
                    ALTER TABLE `{table}` RESET (TIERING)
                """)

        self.pool.retry_operation_sync(callee)

    def list_columns(self, table):
        path = self.database + "/" + table

        def callee(session):
            return session.describe_table(path).columns

        return self.pool.retry_operation_sync(callee)

    def generate_batch(self, schema, batch_size):
        data = []

        for i in range(batch_size):
            data.append({c.name: generate_value(c.type) for c in schema})

        return data

    def add_batch(self, table, schema, batch_size):
        print(f"Add batch {table}")

        column_types = ydb.BulkUpsertColumns()

        for c in schema:
            column_types.add_column(c.name, c.type)

        batch = self.generate_batch(schema, batch_size)

        self.driver.table_client.bulk_upsert(self.database + "/" + table, batch, column_types)
    
    def setup_tables(self):
        self.create_table_store('olap_tiering_store')

        tables = []
        for i in range(4):
            tables.append(f'olap_tiering_store/table{i}')
            self.create_table(tables[-1])

        tiers: list[str] = []
        for i, bucket in enumerate(self.buckets):
            tiers.append(f'OlapTiering:tier{i}')
            self.create_tier(tiers[-1], bucket)

        tiering_policy_configs: list[TieringPolicy] = []
        tiering_policy_configs.append(TieringPolicy([TieringRule(tiers[0], '1s')]))
        tiering_policy_configs.append(TieringPolicy([TieringRule(tiers[1], '1s')]))
        tiering_policy_configs.append(TieringPolicy([TieringRule(tiers[0], '100000d')]))
        tiering_policy_configs.append(TieringPolicy([TieringRule(tiers[1], '100000d')]))
        tiering_policy_configs.append(TieringPolicy([TieringRule(tiers[0], '1s'), TieringRule(tiers[1], '100000d')]))
        tiering_policy_configs.append(TieringPolicy([TieringRule(tiers[1], '1s'), TieringRule(tiers[0], '100000d')]))

        tiering_rules = []
        for i, rule in enumerate(tiering_policy_configs):
            tiering_rules.append(f'OlapTiering:tiering_rule{i}')
            self.create_tiering_rule(tiering_rules[-1], 'timestamp', rule)

        self.tables = tables
        self.tiers = tiers
        self.tiering_rules = tiering_rules
        self.schema = self.list_columns(tables[0])
    
    def run(self):
        started_at = time.time()
        
        self.setup_tables()
        
        while time.time() - started_at < self.duration:
            try:
                for table in self.tables:
                    self.add_batch(table, self.schema, 1000)
                    self.set_tiering_rule(table, random.choice(self.tiering_rules + [None]))
            except Exception as e:
                print(traceback.format_exc())


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="olap stability workload: tiering", formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('--endpoint', default='localhost:2135', help="An endpoint to be used")
    parser.add_argument('--database', default=None, required=True, help='A database to connect')
    parser.add_argument('--duration', default=120, type=lambda x: int(x), help='A duration of workload in seconds')
    parser.add_argument('--s3-endpoint', default='http://storage.yandexcloud.net', help='S3 endpoint')
    parser.add_argument('--s3-access-key', type=str, required=True, help='Access key for S3')
    parser.add_argument('--s3-secret-key', type=str, required=True, help='Secret key for S3')
    parser.add_argument('--s3-buckets', type=str, nargs=2, help='S3 buckets for tiers')
    args = parser.parse_args()
    bucket_configs = [
        ObjectStorageParams(
            scheme='HTTP',
            verify_ssl=False,
            endpoint=args.s3_endpoint,
            bucket=bucket,
            access_key=args.s3_access_key,
            secret_key=args.s3_secret_key
        ) for bucket in args.s3_buckets
    ]
    with Workload(args.endpoint, args.database, args.duration, bucket_configs) as workload:
        workload.run()
