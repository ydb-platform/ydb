from __future__ import annotations
from enum import StrEnum
from os import getenv
from pathlib import Path
from time import time
from .conftest import LoadSuiteBase
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.tpcc_deviation import (
    DeviationCheckResult,
    check_tpcc_deviation,
    key_measurement_specs,
)
from ydb.tests.olap.lib.allure_utils import time_interval_str
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper, TxMode
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import ScenarioTestHelper
from ydb.tests.olap.lib.ydb_cluster import YdbCluster
from ydb.tests.olap.lib.compaction import force_datashard_compact_legacy
import ydb.tests.olap.lib.remote_execution as remote_execution
import logging


class CompactionMode(StrEnum):
    NONE = 'none'
    LEGACY = 'legacy'
    SDK = 'sdk'

    @staticmethod
    def get() -> CompactionMode:
        param = get_external_param('tpcc-compaction-mode', CompactionMode.NONE.value)
        try:
            return CompactionMode(param)
        except ValueError:
            raise ValueError(f'invalid tpcc-compaction-mode {param}')


class TpccSuiteBase(LoadSuiteBase):
    warehouses: int = 4500
    threads: int = 4
    time_s: float = 60 * float(getenv('TPCC_TIME_MINUTES', 30))
    # Legacy compaction timeout must not depend solely on low warehouse counts
    # (e.g. functional tests with 5 warehouses), otherwise setup becomes flaky.
    legacy_compaction_min_timeout_s: int = 60
    tx_mode: TxMode = TxMode.SerializableRW
    compaction_mode: CompactionMode = CompactionMode.get()
    _remote_cli_path: str = ''

    @classmethod
    def get_tpcc_path(cls) -> str:
        env_path = getenv('TPCC_TABLE_PATH')
        if env_path:
            return env_path
        return get_external_param(f'table-path-{cls.suite()}', f'tpcc/w{cls.warehouses}')

    @classmethod
    def do_setup_class(cls):
        if cls.verify_data and getenv('NO_VERIFY_DATA', '0') != '1' and getenv('NO_VERIFY_DATA_TPCC', '0') != '1':
            # cls.check_tables_size(folder=cls.get_tpcc_path(), tables={})
            pass
        cls._remote_cli_path = YdbCliHelper.deploy_remote_cli()

        # cleanup previous executions
        if not remote_execution.is_localhost(YdbCluster.get_client_host()):
            remote_execution.execute_command(YdbCluster.get_client_host(), 'sudo pkill -9 -x ydb', raise_on_error=False)

        wh_count = 0
        try:
            wh_count = ScenarioTestHelper(None).get_table_rows_count(f'{cls.get_tpcc_path()}/warehouse')
        except Exception as e:
            logging.info(f'catch exception while check warehouse count: {e}. Data will be reimport.')
            pass
        if wh_count < cls.warehouses:
            logging.info(f'warehouse count {wh_count} less then need {cls.warehouses}. Data will be reimport.')
            YdbCliHelper.clear_tpcc(cls.get_tpcc_path())
            YdbCliHelper.init_tpcc(cls.get_tpcc_path(), cls.warehouses)
            YdbCliHelper.import_data_tpcc(cls._remote_cli_path, cls.get_tpcc_path(), cls.warehouses, cls.compaction_mode == CompactionMode.SDK)
            if cls.compaction_mode == CompactionMode.LEGACY:
                tables = [
                    'oorder',
                    'district',
                    'item',
                    'warehouse',
                    'customer',
                    'order_line',
                    'new_order',
                    'stock',
                    'history',
                    'customer/idx_customer_name/indexImplTable',
                    'oorder/idx_order/indexImplTable'
                ]
                compaction_timeout = max(cls.legacy_compaction_min_timeout_s, cls.warehouses)
                force_datashard_compact_legacy(
                    [f'{cls.get_tpcc_path()}/{t}' for t in tables],
                    timeout=compaction_timeout,
                )

    @classmethod
    def get_key_measurements(cls) -> tuple[list[LoadSuiteBase.KeyMeasurement], str]:
        return [
            LoadSuiteBase.KeyMeasurement('tpcc_time_seconds', 'TPC-C Time', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Time to run (seconds)'),
            LoadSuiteBase.KeyMeasurement('tpcc_warehouses', 'TPC-C Warehouses', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Warehouses count'),
            LoadSuiteBase.KeyMeasurement('tpcc_max_sessions', 'TPC-C max-sessions', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Resolved max sessions (MaxInflight), after auto-detect if unset'),
            LoadSuiteBase.KeyMeasurement('tpcc_threads', 'TPC-C threads', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Resolved executor thread count, after auto-detect if unset'),
            LoadSuiteBase.KeyMeasurement('tpcc_warmup_seconds', 'TPC-C warmup', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Resolved warmup duration in seconds, after auto/min-floor adjustments'),
            LoadSuiteBase.KeyMeasurement('tpcc_efficiency', 'TPC-C Efficiency', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Efficiency of TPC-C'),
            LoadSuiteBase.KeyMeasurement('tpcc_tpmc', 'TPC-C TPMC', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Transactions per minute C of TPC-C'),
            *cls._tpcc_latency_key_measurements(),
            *cls._tpcc_deviation_key_measurements(),
        ], ''

    @classmethod
    def _tpcc_deviation_key_measurements(cls) -> list[LoadSuiteBase.KeyMeasurement]:
        """Degradation against the baseline, present only when the check has run."""
        return [
            LoadSuiteBase.KeyMeasurement(
                spec.name,
                spec.caption,
                [
                    LoadSuiteBase.KeyMeasurement.Interval(color, min, max)
                    for color, min, max in spec.intervals
                ],
                spec.description,
            )
            for spec in key_measurement_specs()
        ]

    @classmethod
    def _tpcc_latency_key_measurements(cls) -> list[LoadSuiteBase.KeyMeasurement]:
        measurements = []
        for tx in ('NewOrder', 'Delivery', 'Payment', 'StockLevel', 'OrderStatus'):
            measurements.extend([
                LoadSuiteBase.KeyMeasurement(
                    f'tpcc_{tx}_perc_90',
                    f'TPC-C {tx} p90 (full)',
                    [LoadSuiteBase.KeyMeasurement.Interval('#ccffcc')],
                    f'90 percentile Full (+inflight queue) of {tx} transactions in ms',
                ),
                LoadSuiteBase.KeyMeasurement(
                    f'tpcc_{tx}_ms_perc_90',
                    f'TPC-C {tx} p90 (ms)',
                    [LoadSuiteBase.KeyMeasurement.Interval('#ccffcc')],
                    f'90 percentile Ms (no queue wait) of {tx} transactions in ms',
                ),
                LoadSuiteBase.KeyMeasurement(
                    f'tpcc_{tx}_pure_perc_90',
                    f'TPC-C {tx} p90 (pure)',
                    [LoadSuiteBase.KeyMeasurement.Interval('#ccffcc')],
                    f'90 percentile Pure (in-tx queries) of {tx} transactions in ms',
                ),
            ])
        return measurements

    @staticmethod
    def _signal_tpcc_load_started() -> None:
        """Notify flamegraph collector that import/setup is done and load begins."""
        marker = getenv('FLAMEGRAPH_LOAD_MARKER')
        if not marker:
            return
        path = Path(marker)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f'{time()}\n', encoding='utf-8')
        logging.info('FLAMEGRAPH_LOAD_MARKER written: %s', marker)

    def test(self):
        assert len(self.get_users()) == 1, 'multiuser TPC-C not supported'
        self.save_nodes_state()
        # After setup_class import/compaction — right before TPC-C load (warmup+measure).
        self._signal_tpcc_load_started()
        result = YdbCliHelper.run_tpcc(
            remote_cli_path=self._remote_cli_path,
            users=self.get_users(),
            path=self.get_tpcc_path(),
            bench_time=self.time_s,
            warehouses=self.warehouses,
            threads=self.threads,
            tx_mode=self.tx_mode
        )[self.get_users()[0]]
        end_time = time()
        verify_errors = type(self).check_nodes_verifies_with_timing(result.start_time, end_time)
        node_errors = type(self).check_nodes_diagnostics_with_timing(result, result.start_time, end_time)
        stats = result.get_stats('test')
        measure_start_time = stats.get('tpcc_json', {}).get('summary', {}).get('measure_start_ts', result.start_time)
        summary = stats.get('tpcc_json', {}).get('summary', {})
        allure_table_strings = {
            'time_warmup': time_interval_str(result.start_time, measure_start_time),
            'time_measure': time_interval_str(measure_start_time, end_time),
            'compaction_mode': str(self.compaction_mode),
            'deploy_method': getenv('CI_DEPLOY_METHOD') or get_external_param('deploy-method', ''),
            'max_sessions': summary.get('max_sessions', ''),
            'threads': summary.get('threads', ''),
            'warmup_seconds': summary.get('warmup_seconds', ''),
        }
        deviation = DeviationCheckResult()
        if result.success and 'tpcc_json' in stats:
            run_type = f'ydb_cli_{str(self.tx_mode).replace("-rw", "")}_{getenv("TPCC_RUN_TYPE", "default")}'
            # Read the baseline before the upload, so that the current run is not part of it.
            deviation = check_tpcc_deviation(stats['tpcc_json'], run_type, result.start_time)
            # Results are stored regardless of the deviation check outcome.
            ResultsProcessor.upload_tpcc_results(stats['tpcc_json'], run_type, result.start_time)
        if deviation.summary:
            allure_table_strings['deviation_check'] = deviation.summary
        for signal, value in deviation.measurements.items():
            result.add_stat('test', signal, value)
        for error in deviation.errors:
            result.add_error(error)
        self.process_query_result(result, 'test', True, allure_table_strings=allure_table_strings, node_errors=node_errors, verify_errors=verify_errors)


class TestTpccW5000T0Serializable(TpccSuiteBase):
    warehouses: int = 5000
    threads: int = 0
    tx_mode = TxMode.SerializableRW


class TestTpccW12000T0Serializable(TpccSuiteBase):
    warehouses: int = 12000
    threads: int = 0
    tx_mode = TxMode.SerializableRW


class TestTpccW16000T0Serializable(TpccSuiteBase):
    warehouses: int = 16000
    threads: int = 0
    tx_mode = TxMode.SerializableRW


class TestTpccW20000T0Serializable(TpccSuiteBase):
    warehouses: int = 20000
    threads: int = 0
    tx_mode = TxMode.SerializableRW


class TestTpccUniversalSerializable(TpccSuiteBase):
    warehouses: int = int(getenv('TPCC_WAREHOUSES', 10))
    threads: int = 0
    tx_mode = TxMode.SerializableRW


class TestTpccW5000T0Snapshot(TpccSuiteBase):
    warehouses: int = 5000
    threads: int = 0
    tx_mode = TxMode.SnapshotRW


class TestTpccW12000T0Snapshot(TpccSuiteBase):
    warehouses: int = 12000
    threads: int = 0
    tx_mode = TxMode.SnapshotRW


class TestTpccW16000T0Snapshot(TpccSuiteBase):
    warehouses: int = 16000
    threads: int = 0
    tx_mode = TxMode.SnapshotRW


class TestTpccW20000T0Snapshot(TpccSuiteBase):
    warehouses: int = 20000
    threads: int = 0
    tx_mode = TxMode.SnapshotRW


class TestTpccUniversalT0Snapshot(TpccSuiteBase):
    warehouses: int = int(getenv('TPCC_WAREHOUSES', 10))
    threads: int = 0
    tx_mode = TxMode.SnapshotRW
