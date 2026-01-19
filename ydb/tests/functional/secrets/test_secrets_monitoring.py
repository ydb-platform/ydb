# -*- coding: utf-8 -*-
from .conftest import create_secrets, DATABASE, ROOT
import logging
import requests

logger = logging.getLogger(__name__)

CLUSTER_CONFIG = dict(
    additional_log_configs={
        # 'TX_PROXY': LogLevels.DEBUG,
    },
)


def get_tenant_schemeshard_id(ydb_cluster, root_path, database_path):
    mon_url = f"http://{cluster_http_endpoint(ydb_cluster)}"
    response = requests.get(f"{mon_url}/viewer/json/describe?database={root_path}&path={database_path}", timeout=10)
    response.raise_for_status()
    data = response.json()

    return int(data["PathDescription"]["Self"]["SchemeshardId"])


def cluster_http_endpoint(cluster):
    return f"{cluster.nodes[1].host}:{cluster.nodes[1].mon_port}"


def test_secrets_monitoring_hides_sensitive_column(db_fixture, ydb_cluster):
    secret_name = "secret_name"
    secret_value = "test_secret_value"
    create_secrets(db_fixture, [secret_name], [secret_value])

    tablet_id = get_tenant_schemeshard_id(ydb_cluster, ROOT, DATABASE)
    db_url = f"http://{cluster_http_endpoint(ydb_cluster)}/tablets/db?TabletID={tablet_id}&TableID=127"

    response = requests.get(db_url, timeout=10)
    response.raise_for_status()
    html = response.text

    assert "<i>&lt;HIDDEN VALUE&gt;</i>" in html, "HTML should contain hidden value marker"

    assert secret_value not in html, "HTML should NOT contain secret value"
