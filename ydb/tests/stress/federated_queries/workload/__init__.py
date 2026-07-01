# -*- coding: utf-8 -*-
import logging
import multiprocessing
import os
import random
import time

import library.python.port_manager
import requests

import ydb
from ydb.tests.stress.common.instrumented_pools import InstrumentedQuerySessionPool

from ydb.library.yql.tools.solomon_emulator.client.client import (
    cleanup_solomon,
    get_solomon_metrics,
)
from ydb.library.yql.tools.solomon_emulator.lib.config import Config
from ydb.library.yql.tools.solomon_emulator.lib.webapp import run_web_app

logger = logging.getLogger(__name__)


def _run_solomon_emulator(http_port, grpc_port):
    config = Config(auth=None, shards=[])
    run_web_app(config, http_port=http_port, grpc_port=grpc_port)


class Workload:
    SOLOMON_PROJECT = "stress_project"
    SOLOMON_CLUSTER = "stress_cluster"
    SOLOMON_SERVICE = "stress_service"
    SOURCE_NAME = "solomon_source"
    EMULATOR_STARTUP_TIMEOUT_SEC = 30
    INSERT_BATCH_SIZE = 10
    LABEL_VALUES = [
        "label_alpha",
        "label_beta",
        "label_gamma",
        "label_delta",
        "label_epsilon",
        "label_zeta",
        "label_eta",
        "label_theta",
    ]
    SENSOR_RANGE = (1, 1000)

    def __init__(self, endpoint, database, duration, prefix):
        self.database = database
        self.endpoint = endpoint
        self.duration = duration
        self.prefix = prefix

        self.solomon_process = None
        self.solomon_http_port = None
        self.solomon_grpc_port = None
        self.solomon_http_endpoint = None
        self.solomon_grpc_endpoint = None

        self.successful_inserts = 0
        self.attempted_inserts = 0

        self.driver = ydb.Driver(ydb.DriverConfig(endpoint, database))
        self.driver.wait(timeout=60)
        self.pool = InstrumentedQuerySessionPool(self.driver)
        logger.info("FederatedQueriesWorkload::init")

    def start_solomon_emulator(self):
        logger.info("Workload::start_solomon_emulator")
        pm = library.python.port_manager.PortManager()
        self.solomon_http_port = pm.get_port()
        self.solomon_grpc_port = pm.get_port()
        self.solomon_http_endpoint = f"localhost:{self.solomon_http_port}"
        self.solomon_grpc_endpoint = f"localhost:{self.solomon_grpc_port}"

        logger.info(
            f"starting solomon emulator: http={self.solomon_http_endpoint}, "
            f"grpc={self.solomon_grpc_endpoint}"
        )
        self.solomon_process = multiprocessing.Process(
            target=_run_solomon_emulator,
            args=(self.solomon_http_port, self.solomon_grpc_port),
            daemon=True,
        )
        self.solomon_process.start()

        os.environ["SOLOMON_HTTP_PORT"] = str(self.solomon_http_port)
        os.environ["SOLOMON_GRPC_PORT"] = str(self.solomon_grpc_port)
        os.environ["SOLOMON_HTTP_ENDPOINT"] = self.solomon_http_endpoint
        os.environ["SOLOMON_GRPC_ENDPOINT"] = self.solomon_grpc_endpoint
        os.environ["SOLOMON_HTTP_URL"] = f"http://{self.solomon_http_endpoint}"

        self._wait_emulator_ready()
        cleanup_solomon(self.SOLOMON_PROJECT, self.SOLOMON_CLUSTER, self.SOLOMON_SERVICE)

    def _wait_emulator_ready(self):
        deadline = time.time() + self.EMULATOR_STARTUP_TIMEOUT_SEC
        last_error = None
        while time.time() < deadline:
            if not self.solomon_process.is_alive():
                raise Exception(
                    f"solomon emulator process exited unexpectedly "
                    f"with code {self.solomon_process.exitcode}"
                )
            try:
                resp = requests.get(f"http://{self.solomon_http_endpoint}/ping", timeout=1)
                if resp.ok:
                    logger.info("solomon emulator is ready")
                    return
                last_error = f"http {resp.status_code}"
            except requests.RequestException as e:
                last_error = str(e)
            time.sleep(0.2)
        raise Exception(f"solomon emulator did not become ready in time: {last_error}")

    def stop_solomon_emulator(self):
        if self.solomon_process is None:
            return
        logger.info("Workload::stop_solomon_emulator")
        try:
            self.solomon_process.terminate()
            self.solomon_process.join(timeout=10)
            if self.solomon_process.is_alive():
                self.solomon_process.kill()
                self.solomon_process.join(timeout=5)
        finally:
            self.solomon_process = None

    def create_external_data_source(self):
        logger.info("Workload::create_external_data_source")
        self.pool.execute_with_retries(
            f"""
                CREATE EXTERNAL DATA SOURCE `{self.prefix}/{self.SOURCE_NAME}` WITH (
                    SOURCE_TYPE = "Solomon",
                    LOCATION = "{self.solomon_http_endpoint}",
                    AUTH_METHOD = "NONE",
                    USE_TLS = "false"
                );
            """
        )

    def drop_external_data_source(self):
        logger.info("Workload::drop_external_data_source")
        try:
            self.pool.execute_with_retries(
                f"DROP EXTERNAL DATA SOURCE `{self.prefix}/{self.SOURCE_NAME}`;"
            )
        except Exception as e:
            logger.warning(f"failed to drop external data source: {e}")

    def _build_insert_query(self):
        sink = (
            f"`{self.prefix}/{self.SOURCE_NAME}`."
            f"`{self.SOLOMON_PROJECT}/{self.SOLOMON_CLUSTER}/{self.SOLOMON_SERVICE}`"
        )
        rows = []
        for _ in range(self.INSERT_BATCH_SIZE):
            label = random.choice(self.LABEL_VALUES)
            sensor = random.randint(*self.SENSOR_RANGE)
            ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            rows.append(
                f'SELECT Unwrap(CAST("{ts}" AS Timestamp)) AS Ts, '
                f'"{label}" AS Label, {sensor} AS Sensor'
            )
        return f"INSERT INTO {sink}\n" + "\nUNION ALL\n".join(rows) + ";"

    def write_to_solomon(self):
        logger.info("Workload::write_to_solomon")
        finished_at = time.time() + self.duration
        while time.time() < finished_at:
            query = self._build_insert_query()
            self.attempted_inserts += 1
            try:
                self.pool.execute_with_retries(query)
                self.successful_inserts += 1
            except Exception as e:
                logger.warning(f"insert failed: {e}")
        logger.info(
            f"Workload::write_to_solomon end: "
            f"attempts={self.attempted_inserts} successes={self.successful_inserts}"
        )

    def verify_metrics(self):
        logger.info("Workload::verify_metrics")
        if self.successful_inserts == 0:
            raise Exception("no successful inserts during the workload")

        metrics = get_solomon_metrics(
            self.SOLOMON_PROJECT, self.SOLOMON_CLUSTER, self.SOLOMON_SERVICE
        )
        total_rows = len(metrics)
        labels_seen = set()
        for m in metrics:
            for k, v in m.get("labels", []):
                if k == "Label":
                    labels_seen.add(v)

        expected_rows = self.successful_inserts * self.INSERT_BATCH_SIZE
        logger.info(
            f"got {total_rows} metrics, "
            f"expected ~{expected_rows}, "
            f"unique labels seen={len(labels_seen)}"
        )
        if total_rows < expected_rows * 0.5:
            raise Exception(
                f"too few metrics received: got {total_rows}, "
                f"expected at least {int(expected_rows * 0.5)}"
            )
        if len(labels_seen) < 2:
            raise Exception(
                f"insufficient label diversity: got labels {labels_seen}"
            )

    def loop(self):
        self.start_solomon_emulator()
        try:
            self.create_external_data_source()
            try:
                self.write_to_solomon()
                self.verify_metrics()
            finally:
                self.drop_external_data_source()
        finally:
            self.stop_solomon_emulator()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pool.stop()
        self.driver.stop()
