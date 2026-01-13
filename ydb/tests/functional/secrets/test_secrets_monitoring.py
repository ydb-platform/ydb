# -*- coding: utf-8 -*-
from .conftest import run_with_assert, DATABASE
import logging
import requests

logger = logging.getLogger(__name__)

CLUSTER_CONFIG = dict(
    additional_log_configs={
        # 'TX_PROXY': LogLevels.DEBUG,
    },
    extra_feature_flags=[
        "enable_schema_secrets",
    ],
)


def get_tenant_schemeshard_id(cluster, database_path):
    """Get tenant SchemeShard tablet ID using /viewer/json/describe endpoint
    Similar to tablets_movement.py list_shards method
    Uses PathOwnerId from the response, which is the SchemeshardId for the tenant
    """
    mon_url = f"http://{cluster_http_endpoint(cluster)}"
    # Query describe endpoint to get PathOwnerId, which is the SchemeshardId
    response = requests.get(
        f"{mon_url}/viewer/json/describe?database=/Root&path={database_path}",
        timeout=10
    )
    response.raise_for_status()
    data = response.json()
    
    # PathOwnerId is the SchemeshardId for the tenant database
    if "PathDescription" in data:
        path_desc = data["PathDescription"]
        if "PathOwnerId" in path_desc:
            return str(path_desc["PathOwnerId"])
    
    # Fallback: check if PathOwnerId is directly in the response
    if "PathOwnerId" in data:
        return str(data["PathOwnerId"])
    
    return None


def cluster_http_endpoint(cluster):
    """Get HTTP endpoint for cluster"""
    return f"{cluster.nodes[1].host}:{cluster.nodes[1].mon_port}"


def test_secrets_monitoring_hides_sensitive_column(db_fixture, ydb_cluster):
    """Test that sensitive column Description is hidden in SchemeShard monitoring"""
    # Create a secret
    secret_path = f"{DATABASE}/test_secret"
    secret_value = "my_secret_value_12345"
    query = f"CREATE SECRET `{secret_path}` WITH ( value='{secret_value}' );"
    run_with_assert(db_fixture, query)

    # Get tenant SchemeShard tablet ID using /viewer/json/describe endpoint
    # This endpoint returns PathOwnerId which is the SchemeshardId for the tenant database
    tablet_id = get_tenant_schemeshard_id(ydb_cluster, DATABASE)
    assert tablet_id is not None, f"Could not find tenant SCHEMESHARD tablet ID for path {DATABASE}"

    # Access monitoring page for Secrets table (TableID=127) in SchemeShard
    db_url = f"http://{cluster_http_endpoint(ydb_cluster)}/tablets/db?TabletID={tablet_id}&TableID=127"
    
    # Make HTTP request to monitoring page
    response = requests.get(db_url, timeout=10)
    response.raise_for_status()
    html = response.text

    # Check that sensitive column value is hidden
    assert "<i>&lt;HIDDEN VALUE&gt;</i>" in html, \
        f"HTML should contain hidden value marker. HTML snippet: {html[:1000]}"

    # Check that secret value is NOT visible
    assert secret_value not in html, \
        f"HTML should NOT contain secret value. HTML snippet: {html[:500]}"

