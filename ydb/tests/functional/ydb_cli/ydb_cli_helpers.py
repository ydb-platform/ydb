# -*- coding: utf-8 -*-

import logging
import os
import yatest

from dataclasses import dataclass
from typing import Optional
from ydb.tests.library.harness.kikimr_config import KikimrConfigGenerator
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.canonical import set_canondata_root
from ydb.tests.oss.ydb_sdk_import import ydb

logger = logging.getLogger(__name__)


def ydb_bin(env_name: str = "YDB_CLI_BINARY") -> str:
    if os.getenv(env_name):
        return yatest.common.binary_path(os.getenv(env_name))
    raise RuntimeError(f"{env_name} environment variable is not specified")


def set_ydb_cli_test_canondata_root():
    set_canondata_root("ydb/tests/functional/ydb_cli/canondata")


class BaseCliTestWithDatabase:
    """Base class that starts a real YDB cluster for CLI tests."""

    startup_timeout = 4

    @classmethod
    def _start_cluster(cls, configurator: Optional[KikimrConfigGenerator] = None, cluster_name: str = "cluster"):
        if configurator is None:
            configurator = KikimrConfigGenerator()
        cluster = KiKiMR(configurator, cluster_name=cluster_name)
        cluster.start()
        cls.root_dir = "/Root"
        return cluster

    @classmethod
    def _start_driver(cls, database: str, credentials=None, cluster: Optional[KiKiMR] = None):
        if cluster is None:
            cluster = cls.cluster
        driver_config_kwargs = dict(
            database=database,
            endpoint=f"{cluster.nodes[1].host}:{cluster.nodes[1].port}",
            credentials=credentials,
        )
        # When the cluster runs in protected mode (mTLS), point the driver to
        # the per-cluster TLS data so it authenticates as clusteradmins@cert.
        if getattr(cluster.config, "protected_mode", False):
            with open(cluster.config.grpc_tls_ca_path, "rb") as f:
                root_certificates = f.read()
            with open(cluster.config.grpc_tls_cert_path, "rb") as f:
                certificate_chain = f.read()
            with open(cluster.config.grpc_tls_key_path, "rb") as f:
                private_key = f.read()
            driver_config_kwargs["root_certificates"] = root_certificates
            driver_config_kwargs["certificate_chain"] = certificate_chain
            driver_config_kwargs["private_key"] = private_key
            driver_config_kwargs["endpoint"] = (
                f"grpcs://{cluster.nodes[1].host}:{cluster.nodes[1].grpc_ssl_port}"
            )
        driver = ydb.Driver(ydb.DriverConfig(**driver_config_kwargs))
        driver.wait(timeout=cls.startup_timeout)
        return driver

    @classmethod
    def get_cluster_configurator(cls):
        """Override in subclasses to use a custom cluster config (e.g. extra feature flags)."""
        return None

    @classmethod
    def setup_class(cls):
        set_ydb_cli_test_canondata_root()
        cls.cluster = cls._start_cluster(configurator=cls.get_cluster_configurator())
        cls.driver = cls._start_driver(cls.root_dir)

    @classmethod
    def teardown_class(cls):
        if hasattr(cls, "driver") and cls.driver is not None:
            cls.driver.stop()
        if hasattr(cls, "cluster") and cls.cluster is not None:
            cls.cluster.stop()

    @classmethod
    def grpc_port(cls):
        return cls.cluster.nodes[1].grpc_port

    @classmethod
    def grpc_endpoint(cls):
        return f"grpc://localhost:{cls.grpc_port()}"

    @dataclass
    class ExecutionResult:
        stdout: str
        stderr: str
        exit_code: int = 0

    @classmethod
    def execute_ydb_cli_command(cls, args, database: Optional[str] = None, endpoint: Optional[str] = None, stdin=None, stdout=None, env=None, check_exit_code: bool = True) -> ExecutionResult:
        if database is None:
            database = cls.root_dir
        if endpoint is None:
            endpoint = cls.grpc_endpoint()

        execution = yatest.common.execute(
            [
                ydb_bin(),
                "--endpoint", endpoint,
                "--database", database
            ] +
            args, stdin=stdin, stdout=stdout, env=env, check_exit_code=check_exit_code
        )

        result = cls.ExecutionResult(
            stdout=execution.std_out.decode("utf-8") if execution.std_out else "",
            stderr=execution.std_err.decode("utf-8") if execution.std_err else "",
            exit_code=execution.exit_code,
        )
        logger.debug("stdout:\n%s", result.stdout)
        logger.debug("stderr:\n%s", result.stderr)
        logger.debug("exit_code: %d", result.exit_code)
        return result
