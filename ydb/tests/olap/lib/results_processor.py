from __future__ import annotations
import json
import ydb
import os
import logging
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import external_param_is_true, get_external_param
from time import time_ns


class ResultsProcessor:
    class Endpoint:
        def __init__(self, ep: str, db: str, table: str, key: str, iam_file: str) -> None:
            self._driver = YdbCluster._create_ydb_driver(ep, db, oauth=key, iam_file=iam_file)
            self._db = db
            self._table = table

        def send_data(self, data):
            try:
                ydb.retry_operation_sync(
                    lambda: self._driver.table_client.bulk_upsert(
                        os.path.join(self._db, self._table), [data], ResultsProcessor._columns_types
                    )
                )
            except BaseException as e:
                logging.error(f'Exception while send results: {e}')

    _endpoints : list[ResultsProcessor.Endpoint] = None
    _run_id : int = None

    send_results = external_param_is_true('send-results')
    _columns_types = (
        ydb.BulkUpsertColumns()
        .add_column('Db', ydb.PrimitiveType.Utf8)
        .add_column('Kind', ydb.PrimitiveType.Utf8)
        .add_column('Suite', ydb.PrimitiveType.Utf8)
        .add_column('Test', ydb.PrimitiveType.Utf8)
        .add_column('Timestamp', ydb.PrimitiveType.Timestamp)
        .add_column('RunId', ydb.PrimitiveType.Uint64)
        .add_column('Success', ydb.PrimitiveType.Uint8)
        .add_column('MinDuration', ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column('MaxDuration', ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column('MeanDuration', ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column('MedianDuration', ydb.OptionalType(ydb.PrimitiveType.Uint64))
        .add_column('Attempt', ydb.OptionalType(ydb.PrimitiveType.Int32))
        .add_column('Stats', ydb.OptionalType(ydb.PrimitiveType.JsonDocument))
        .add_column('Info', ydb.OptionalType(ydb.PrimitiveType.JsonDocument))
    )

    @classmethod
    def get_endpoints(cls):
        if cls._endpoints is None:
            endpoints = get_external_param('results-endpoint', 'grpc://ydb-ru-prestable.yandex.net:2135').split(',')
            dbs = get_external_param('results-db', '/ru-prestable/kikimr/preprod/olap-click-perf').split(',')
            tables = get_external_param('results-table', 'tests_results').split(',')
            count = max(len(endpoints), len(dbs), len(tables))
            common_key = os.getenv('RESULT_YDB_OAUTH', None)
            cls._endpoints = []
            for i in range(count):
                ep = endpoints[i] if i < len(endpoints) else endpoints[-1]
                db = dbs[i] if i < len(dbs) else dbs[-1]
                table = tables[i] if i < len(tables) else tables[-1]
                iam_file = os.getenv(f'RESULT_IAM_FILE_{i}', None)
                key = None
                if iam_file is None:
                    key = os.getenv(f'RESULT_YDB_OAUTH_{i}', common_key)
                cls._endpoints.append(ResultsProcessor.Endpoint(ep, db, table, key, iam_file))
        return cls._endpoints

    @classmethod
    def get_run_id(cls) -> int:
        if cls._run_id is None:
            cls._run_id = time_ns()
        return cls._run_id

    @staticmethod
    def get_cluster_id():
        run_id = get_external_param('run-id', YdbCluster.tables_path)
        return os.path.join(YdbCluster.ydb_endpoint, YdbCluster.ydb_database, run_id)

    @classmethod
    def upload_results(
        cls,
        kind: str,
        suite: str,
        test: str,
        timestamp: float,
        is_successful: bool,
        min_duration: float | None = None,
        max_duration: float | None = None,
        mean_duration: float | None = None,
        median_duration: float | None = None,
        duration: float | None = None,
        attempt: int | None = None,
        statistics: dict | None = None,
    ):
        if not cls.send_results:
            return

        def _get_duration(dur: float | None):
            if dur is not None:
                return int(1000000 * dur)
            if duration is not None:
                return int(1000000 * duration)
            return None

        info = {'cluster': YdbCluster.get_cluster_info()}

        report_url = os.getenv('ALLURE_RESOURCE_URL', None)
        if report_url is None:
            sandbox_task_id = get_external_param('SANDBOX_TASK_ID', None)
            if sandbox_task_id is not None:
                report_url = f'https://sandbox.yandex-team.ru/task/{sandbox_task_id}/allure_report'
        if report_url is not None:
            info['report_url'] = report_url

        data = {
            'Db': cls.get_cluster_id(),
            'Kind': kind,
            'Suite': suite,
            'Test': test,
            'Timestamp': int(1000000 * timestamp),
            'RunId': cls.get_run_id(),
            'Success': 1 if is_successful else 0,
            'MinDuration': _get_duration(min_duration),
            'MaxDuration': _get_duration(max_duration),
            'MeanDuration': _get_duration(mean_duration),
            'MedianDuration': _get_duration(median_duration),
            'Attempt': attempt,
            'Stats': json.dumps(statistics) if statistics is not None else None,
            'Info': json.dumps(info),
        }
        for endpoint in cls.get_endpoints():
            endpoint.send_data(data)
