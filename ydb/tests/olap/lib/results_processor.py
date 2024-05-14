import json
import ydb
import os
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.utils import external_param_is_true, get_external_param
from time import time_ns


class ResultsProcessor:
    _results_driver : ydb.Driver = None
    _run_id : int = None

    send_results = external_param_is_true('send-results')
    results_endpoint = get_external_param('results-endpoint', 'grpc://ydb-ru-prestable.yandex.net:2135')
    results_database = get_external_param('results-db', '/ru-prestable/kikimr/preprod/olap-click-perf')
    results_table = get_external_param('results-table', 'tests_results')
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
    def get_results_driver(cls):
        if cls._results_driver is None:
            cls._results_driver = YdbCluster._create_ydb_driver(
                cls.results_endpoint, cls.results_database, os.getenv('RESULT_YDB_OAUTH', None)
            )
        return cls._results_driver

    @classmethod
    def get_run_id(cls) -> int:
        if cls._run_id is None:
            cls._run_id = time_ns()
        return cls._run_id

    @staticmethod
    def get_cluster_id():
        return os.path.join(YdbCluster.ydb_endpoint, YdbCluster.ydb_database, YdbCluster.tables_path)

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
        sandbox_task_id = get_external_param('SANDBOX_TASK_ID', None)
        if sandbox_task_id is not None:
            info['report_url'] = f'https://sandbox.yandex-team.ru/task/{sandbox_task_id}/allure_report'
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
        cls.get_results_driver().table_client.bulk_upsert(
            os.path.join(cls.results_database, cls.results_table), [data], cls._columns_types
        )
