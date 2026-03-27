# -*- coding: utf-8 -*-
import json
import os
import textwrap
import threading
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer

import requests
import yatest.common

from library.python.port_manager import PortManager

from ydb.tests.library.common.wait_for import wait_for
from ydb.tests.library.harness.daemon import Daemon
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb


def mvp_meta_bin():
    return yatest.common.binary_path("ydb/mvp/meta/bin/mvp_meta")


class MetaService:
    def __init__(self, config_path, http_port):
        self.http_port = http_port
        self._daemon = Daemon(
            command=[
                mvp_meta_bin(),
                "--config",
                config_path,
                "--http-port",
                str(http_port),
                "--stderr",
            ],
            cwd=yatest.common.output_path(),
            timeout=30,
            stdout_file=os.path.join(yatest.common.output_path(), "mvp_meta.stdout"),
            stderr_file=os.path.join(yatest.common.output_path(), "mvp_meta.stderr"),
            stderr_on_error_lines=100,
        )

    @property
    def endpoint(self):
        return f"http://localhost:{self.http_port}"

    def _is_ready(self):
        try:
            return requests.get(f"{self.endpoint}/ping", timeout=1).status_code == 200
        except requests.RequestException:
            return False

    def start(self):
        self._daemon.start()
        ready = wait_for(
            self._is_ready,
            timeout_seconds=30,
            step_seconds=0.5,
        )
        if not ready:
            if self._daemon.is_alive():
                self._daemon.stop()
        assert ready, "mvp_meta did not become ready on /ping"
        return self

    def stop(self):
        self._daemon.stop()


class GrafanaMock:
    def __init__(self, handler_cls):
        port_manager = PortManager()
        self.port = port_manager.get_port()
        self._server = ThreadingHTTPServer(("localhost", self.port), handler_cls)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    @property
    def endpoint(self):
        return f"http://localhost:{self.port}"

    def start(self):
        self._thread.start()
        return self

    def stop(self):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def start_cluster_with_meta_table():
    cluster = KiKiMR()
    cluster.start()

    driver = ydb.Driver(
        ydb.DriverConfig(
            database="/Root",
            endpoint=f"{cluster.nodes[1].host}:{cluster.nodes[1].port}",
        )
    )
    driver.wait()

    session = ydb.retry_operation_sync(lambda: driver.table_client.session().create())
    session.execute_scheme(textwrap.dedent("""
        CREATE TABLE `/Root/ydb/MasterClusterExt.db` (
            name Utf8,
            k8s_namespace Utf8,
            datasource Utf8,
            PRIMARY KEY (name)
        );
    """))

    session.transaction().execute(textwrap.dedent("""
        UPSERT INTO `/Root/ydb/MasterClusterExt.db` (name, k8s_namespace, datasource)
        VALUES ("ydb-global", "ydb-workspace", "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63");
    """), commit_tx=True)

    return cluster, driver


def test_meta_support_links_reads_cluster_info_from_real_ydb():
    cluster = None
    driver = None
    meta = None
    try:
        cluster, driver = start_cluster_with_meta_table()

        port_manager = PortManager()
        http_port = port_manager.get_port()

        config_path = os.path.join(yatest.common.output_path(), "meta_support_links.yaml")
        with open(config_path, "w") as config:
            config.write(textwrap.dedent(f"""
                generic:
                  access_service_type: "nebius_v1"
                  logging:
                    stderr: true
                  server:
                    http_port: {http_port}

                meta:
                  meta_api_endpoint: "grpc://{cluster.nodes[1].host}:{cluster.nodes[1].port}"
                  meta_database: "/Root"

                  grafana:
                    endpoint: "https://grafana.example.test"

                  support_links:
                    cluster:
                      - source: "grafana/dashboard"
                        title: "Overview"
                        url: "/d/ydb/overview"
            """))

        meta = MetaService(config_path=config_path, http_port=http_port).start()
        response = requests.get(
            f"{meta.endpoint}/meta/support_links",
            params={"cluster": "ydb-global"},
            timeout=10,
        )
        assert response.status_code == 200, response.text

        payload = response.json()
        assert payload["links"] == [{
            "title": "Overview",
            "url": "https://grafana.example.test/d/ydb/overview?var-workspace=ydb-workspace&var-ds=3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63&var-cluster=ydb-global",
        }]
        assert "errors" not in payload
    finally:
        if meta is not None:
            meta.stop()
        if driver is not None:
            driver.stop()
        if cluster is not None:
            cluster.stop()


def test_meta_support_links_searches_dashboards_via_grafana_mock():
    class MetadataHandler(BaseHTTPRequestHandler):
        def do_GET(self):
            assert self.path == "/computeMetadata/v1/instance/service-accounts/default/token"
            assert self.headers.get("Metadata-Flavor") == "Google"

            body = json.dumps({
                "access_token": "test-token",
                "expires_in": 3600,
            }).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            return

    class GrafanaSearchHandler(BaseHTTPRequestHandler):
        expected_auth = "Bearer test-token"

        def do_GET(self):
            assert self.path.startswith("/api/search?"), self.path
            assert "tag=ydb-common" in self.path, self.path
            assert self.headers.get("Authorization") == self.expected_auth

            body = json.dumps([
                {
                    "title": "CPU",
                    "url": "/d/ydb_cpu/cpu",
                }
            ]).encode("utf-8")

            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format, *args):
            return

    cluster = None
    driver = None
    meta = None
    grafana = None
    metadata = None
    try:
        cluster, driver = start_cluster_with_meta_table()
        metadata = GrafanaMock(MetadataHandler).start()
        grafana = GrafanaMock(GrafanaSearchHandler).start()

        port_manager = PortManager()
        http_port = port_manager.get_port()

        config_path = os.path.join(yatest.common.output_path(), "meta_support_links_search.yaml")
        with open(config_path, "w") as config:
            config.write(textwrap.dedent(f"""
                generic:
                  access_service_type: "nebius_v1"
                  logging:
                    stderr: true
                  server:
                    http_port: {http_port}
                  auth:
                    tokens:
                      metadata_token_info:
                        - name: "service-account-jwt"
                          endpoint: "{metadata.endpoint}/computeMetadata/v1/instance/service-accounts/default/token"

                meta:
                  meta_api_endpoint: "grpc://{cluster.nodes[1].host}:{cluster.nodes[1].port}"
                  meta_database: "/Root"
                  meta_database_token_name: "service-account-jwt"

                  grafana:
                    endpoint: "{grafana.endpoint}"

                  support_links:
                    cluster:
                      - source: "grafana/dashboard/search"
                        tag: "ydb-common"
            """))

        meta = MetaService(config_path=config_path, http_port=http_port).start()
        expected_links = [{
            "title": "CPU",
            "url": f"{grafana.endpoint}/d/ydb_cpu/cpu?var-workspace=ydb-workspace&var-ds=3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63&var-cluster=ydb-global",
        }]
        response_holder = {}
        payload_holder = {}

        def support_links_ready():
            try:
                response = requests.get(
                    f"{meta.endpoint}/meta/support_links",
                    params={"cluster": "ydb-global"},
                    timeout=10,
                )
            except requests.RequestException:
                return False

            response_holder["response"] = response
            if response.status_code != 200:
                return False

            payload = response.json()
            payload_holder["payload"] = payload
            return payload.get("links") == expected_links and "errors" not in payload

        ready = wait_for(
            support_links_ready,
            timeout_seconds=10,
            step_seconds=0.5,
        )
        assert ready, response_holder.get("response").text if response_holder.get("response") is not None else "No response from meta"
        assert payload_holder["payload"]["links"] == expected_links
        assert "errors" not in payload_holder["payload"]
    finally:
        if meta is not None:
            meta.stop()
        if grafana is not None:
            grafana.stop()
        if metadata is not None:
            metadata.stop()
        if driver is not None:
            driver.stop()
        if cluster is not None:
            cluster.stop()
