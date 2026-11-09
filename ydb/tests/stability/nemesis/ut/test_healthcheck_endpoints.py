from __future__ import annotations

import pytest

from ydb.tests.library.stability.healthcheck.healthcheck_reporter import (
    HealthCheckEndpoint,
    HealthCheckReporter,
    _healthcheck_endpoint,
)


def test_string_host_keeps_the_production_default_port():
    assert _healthcheck_endpoint("node.example") == HealthCheckEndpoint(
        logical_host="node.example",
        connect_host="node.example",
        grpc_port=2135,
    )


def test_endpoint_keeps_logical_identity_separate_from_connection_address():
    endpoint = HealthCheckEndpoint(
        logical_host="node-7",
        connect_host="localhost",
        grpc_port=32017,
    )
    assert _healthcheck_endpoint(endpoint) is endpoint


def test_reporter_rejects_duplicate_logical_hosts_before_starting():
    with pytest.raises(ValueError, match="logical_host values must be unique"):
        HealthCheckReporter(
            [
                HealthCheckEndpoint("node-1", "localhost", 32001),
                HealthCheckEndpoint("node-1", "localhost", 32002),
            ]
        )
