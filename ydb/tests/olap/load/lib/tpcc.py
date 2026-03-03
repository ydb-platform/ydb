from __future__ import annotations
from os import getenv
from .conftest import LoadSuiteBase
from ydb.tests.olap.lib.results_processor import ResultsProcessor
from ydb.tests.olap.lib.utils import get_external_param
from ydb.tests.olap.lib.ydb_cli import YdbCliHelper, TxMode
from ydb.tests.olap.scenario.helpers.scenario_tests_helper import ScenarioTestHelper
import logging


class TpccSuiteBase(LoadSuiteBase):
    warehouses: int = 4500
    threads: int = 4
    time_s: float = 60 * float(getenv('TPCC_TIME_MINUTES', 30))
    tx_mode: TxMode = TxMode.SerializableRW
    _remote_cli_path: str = ''

    @classmethod
    def get_tpcc_path(cls) -> str:
        return get_external_param(f'table-path-{cls.suite()}', f'tpcc/w{cls.warehouses}')

    @classmethod
    def do_setup_class(cls):
        if cls.verify_data and getenv('NO_VERIFY_DATA', '0') != '1' and getenv('NO_VERIFY_DATA_TPCC', '0') != '1':
            # cls.check_tables_size(folder=cls.get_tpcc_path(), tables={})
            pass
        cls._remote_cli_path = YdbCliHelper.deploy_remote_cli()
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
            YdbCliHelper.import_data_tpcc(cls._remote_cli_path, cls.get_tpcc_path(), cls.warehouses)

    @classmethod
    def get_key_measurements(cls) -> tuple[list[LoadSuiteBase.KeyMeasurement], str]:
        return [
            LoadSuiteBase.KeyMeasurement('tpcc_time_seconds', 'TPC-C Time', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Time to run (seconds)'),
            LoadSuiteBase.KeyMeasurement('tpcc_warehouses', 'TPC-C Warehouses', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Warehouses count'),
            LoadSuiteBase.KeyMeasurement('tpcc_efficiency', 'TPC-C Efficiency', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Efficiency of TPC-C'),
            LoadSuiteBase.KeyMeasurement('tpcc_tpmc', 'TPC-C TPMC', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], 'Transactions per minute C of TPC-C'),
            LoadSuiteBase.KeyMeasurement('tpcc_NewOrder_perc_90', 'TPC-C NewOrder p90', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], '90 percentile of NewOrder transactions in ms'),
            LoadSuiteBase.KeyMeasurement('tpcc_Delivery_perc_90', 'TPC-C Delivery p90', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], '90 percentile of Delivery transactions in ms'),
            LoadSuiteBase.KeyMeasurement('tpcc_Payment_perc_90', 'TPC-C Payment p90', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], '90 percentile of Payment transactions in ms'),
            LoadSuiteBase.KeyMeasurement('tpcc_StockLevel_perc_90', 'TPC-C StockLevel p90', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], '90 percentile of StockLevel transactions in ms'),
            LoadSuiteBase.KeyMeasurement('tpcc_OrderStatus_perc_90', 'TPC-C OrderStatus p90', [
                LoadSuiteBase.KeyMeasurement.Interval('#ccffcc'),
            ], '90 percentile of OrderStatus transactions in ms'),
        ], ''

    def test(self):
        assert len(self.get_users()) == 1, 'multiuser TPC-C not supported'
        result = YdbCliHelper.run_tpcc(
            remote_cli_path=self._remote_cli_path,
            users=self.get_users(),
            path=self.get_tpcc_path(),
            bench_time=self.time_s,
            warehouses=self.warehouses,
            threads=self.threads,
            tx_mode=self.tx_mode
        )[self.get_users()[0]]
        self.process_query_result(result, 'test', True)
        stats = result.get_stats('test')
        if result.success and 'tpcc_json' in stats:
            run_type = f'ydb_cli_{str(self.tx_mode).replace("-rw", "")}_{getenv("TPCC_RUN_TYPE", "default")}'
            ResultsProcessor.upload_tpcc_results(stats['tpcc_json'], run_type)


class TestTpccW3000T0Serializable(TpccSuiteBase):
    warehouses: int = 3000
    threads: int = 0
    tx_mode = TxMode.SerializableRW


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


class TestTpccW18000T0Serializable(TpccSuiteBase):
    warehouses: int = 18000
    threads: int = 0
    tx_mode = TxMode.SerializableRW


class TestTpccW3000T0Snapshot(TpccSuiteBase):
    warehouses: int = 3000
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


class TestTpccW25000T0Snapshot(TpccSuiteBase):
    warehouses: int = 25000
    threads: int = 0
    tx_mode = TxMode.SnapshotRW
