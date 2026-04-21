import os
import textwrap
from contextlib import contextmanager

import yatest.common

from library.python.port_manager import PortManager

from ydb.tests.functional.mvp.common.http_env import BaseHttpEnv
from ydb.tests.functional.mvp.common.mvp_service import MvpHttpService
from ydb.tests.library.harness.kikimr_runner import KiKiMR
from ydb.tests.oss.ydb_sdk_import import ydb

CLUSTER_NAME = "ydb-global"
WORKSPACE_NAME = "ydb-workspace"
DATASOURCE_ID = "3f8a1e2c-6b7d-4c91-9a52-1d7f0e8b4a63"
ROOT_DATABASE = "/Root"
META_TABLE_PATH = "/Root/ydb/MasterClusterExt.db"
GRAFANA_DASHBOARD_PATH = "/d/ydb/overview"
ABSOLUTE_GRAFANA_DASHBOARD_URL = "https://external.example.test/d/ydb/overview"
DATABASE_NAME = "/Root/db1"
MISSING_CLUSTER_NAME = "missing-cluster"
MISSING_CLUSTER_ERROR = f"Cluster '{MISSING_CLUSTER_NAME}' is not found in MasterClusterExt.db"
MISSING_CLUSTER_PARAMETER_ERROR = (
    "Invalid identity parameters. Supported entities: cluster requires 'cluster'; "
    "database requires 'cluster' and 'database'."
)


def mvp_meta_bin():
    return yatest.common.binary_path(os.getenv("MVP_META_BINARY"))


class MetaService:
    def __init__(self, config_path, http_port):
        self._service = MvpHttpService(
            binary_path=mvp_meta_bin(),
            config_path=config_path,
            http_port=http_port,
            service_name="mvp_meta",
        )

    @property
    def endpoint(self):
        return self._service.endpoint

    def start(self):
        self._service.start()
        return self

    def stop(self):
        self._service.stop()


def start_cluster_with_meta_table(
    cluster_name=CLUSTER_NAME,
    k8s_namespace=WORKSPACE_NAME,
    datasource=DATASOURCE_ID,
):
    cluster = KiKiMR()
    cluster.start()

    driver = ydb.Driver(
        ydb.DriverConfig(
            database=ROOT_DATABASE,
            endpoint=f"{cluster.nodes[1].host}:{cluster.nodes[1].port}",
        )
    )
    driver.wait()

    session = ydb.retry_operation_sync(lambda: driver.table_client.session().create())
    session.execute_scheme(textwrap.dedent("""
        CREATE TABLE `{table_path}` (
            name Utf8,
            k8s_namespace Utf8,
            datasource Utf8,
            PRIMARY KEY (name)
        );
    """).format(table_path=META_TABLE_PATH))

    session.transaction().execute(textwrap.dedent(f"""
        UPSERT INTO `{META_TABLE_PATH}` (name, k8s_namespace, datasource)
        VALUES ("{cluster_name}", "{k8s_namespace}", "{datasource}");
    """), commit_tx=True)

    return cluster, driver


class MetaSupportLinksEnv(BaseHttpEnv):
    def __init__(self, meta_service):
        super().__init__(meta_service)
        self.meta_service = meta_service

    def get_support_links(self, cluster_name=None, database=None):
        params = {}
        if cluster_name is not None:
            params["cluster"] = cluster_name
        if database is not None:
            params["database"] = database
        return self.get(
            "/meta/support_links",
            params=params,
            timeout=10,
        )

    def get_ok_support_links_payload(self, cluster_name=None, database=None):
        response = self.get_support_links(cluster_name=cluster_name, database=database)
        assert response.status_code == 200, response.text
        return response.json()

    def get_bad_request_support_links_payload(self, cluster_name=None, database=None):
        response = self.get_support_links(cluster_name=cluster_name, database=database)
        assert response.status_code == 400, response.text
        return response.json()


def write_meta_support_links_config(
    config_path,
    http_port,
    grpc_endpoint,
    url=GRAFANA_DASHBOARD_PATH,
    include_grafana_endpoint=True,
):
    config_lines = [
        "generic:",
        '  access_service_type: "nebius_v1"',
        "  logging:",
        "    stderr: true",
        "  server:",
        f"    http_port: {http_port}",
        "",
        "meta:",
        f'  meta_api_endpoint: "grpc://{grpc_endpoint}"',
        f'  meta_database: "{ROOT_DATABASE}"',
        "",
    ]
    if include_grafana_endpoint:
        config_lines.extend([
            "  grafana:",
            '    endpoint: "https://grafana.example.test"',
            "",
        ])
    config_lines.extend([
        "  support_links:",
        "    cluster:",
        '      - source: "grafana/dashboard"',
        '        title: "Overview"',
        f'        url: "{url}"',
        "    database:",
        '      - source: "grafana/dashboard"',
        '        title: "Overview"',
        f'        url: "{url}"',
        "",
    ])
    with open(config_path, "w") as config:
        config.write("\n".join(config_lines))


def start_meta_support_links_service(cluster, http_port, url=GRAFANA_DASHBOARD_PATH, include_grafana_endpoint=True):
    config_path = os.path.join(yatest.common.output_path(), "meta_support_links.yaml")
    write_meta_support_links_config(
        config_path=config_path,
        http_port=http_port,
        grpc_endpoint=f"{cluster.nodes[1].host}:{cluster.nodes[1].port}",
        url=url,
        include_grafana_endpoint=include_grafana_endpoint,
    )
    return MetaService(config_path=config_path, http_port=http_port).start()


@contextmanager
def started_meta_support_links_env(
    cluster_name=CLUSTER_NAME,
    k8s_namespace=WORKSPACE_NAME,
    datasource=DATASOURCE_ID,
    url=GRAFANA_DASHBOARD_PATH,
    include_grafana_endpoint=True,
):
    cluster, driver = start_cluster_with_meta_table(
        cluster_name=cluster_name,
        k8s_namespace=k8s_namespace,
        datasource=datasource,
    )
    try:
        with PortManager() as port_manager:
            http_port = port_manager.get_port()
            meta = start_meta_support_links_service(
                cluster,
                http_port,
                url=url,
                include_grafana_endpoint=include_grafana_endpoint,
            )
            try:
                yield MetaSupportLinksEnv(meta)
            finally:
                meta.stop()
    finally:
        driver.stop()
        cluster.stop()
