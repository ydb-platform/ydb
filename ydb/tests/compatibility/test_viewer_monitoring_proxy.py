# -*- coding: utf-8 -*-
import json
import pytest
import urllib.request

from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture


class TestViewerMonitoringProxyCompatibility(MixedClusterFixture):
    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def _request_node_viewer_nodes(self, source_node_id, target_node_id):
        source_node = self.cluster.nodes[source_node_id]
        url = f"http://{source_node.host}:{source_node.mon_port}/node/{target_node_id}/viewer/json/nodes?timeout=10000"
        with urllib.request.urlopen(url, timeout=10) as response:
            status = response.getcode()
            body = response.read().decode("utf-8", errors="replace")
        return status, body, json.loads(body)

    def _assert_node_viewer_nodes_works(self, source_node_id, target_node_id):
        status, body, payload = self._request_node_viewer_nodes(source_node_id, target_node_id)
        assert status == 200, (
            f"Unexpected HTTP status for /node/{target_node_id}/viewer/json/nodes "
            f"from node {source_node_id}: {status}"
        )
        assert isinstance(payload, dict), (
            f"Expected JSON object for /node/{target_node_id}/viewer/json/nodes "
            f"from node {source_node_id}, got: {body}"
        )
        assert "Nodes" in payload and isinstance(payload["Nodes"], list), (
            f"Expected 'Nodes' list in /node/{target_node_id}/viewer/json/nodes "
            f"from node {source_node_id}, got: {body}"
        )
        assert payload["Nodes"], (
            f"Expected non-empty 'Nodes' list in /node/{target_node_id}/viewer/json/nodes "
            f"from node {source_node_id}, got: {body}"
        )

    def test_cross_node_viewer_proxy(self):
        node_ids = sorted(self.cluster.nodes.keys())
        assert len(node_ids) >= 2, "Expected at least 2 nodes in compatibility cluster"

        representative_node_by_binary = {}
        for node_id in node_ids:
            representative_node_by_binary.setdefault(self.config.get_binary_path(node_id), node_id)

        representative_nodes = list(representative_node_by_binary.values())
        if len(representative_nodes) >= 2:
            checks = []
            for source_node_id in representative_nodes:
                for target_node_id in representative_nodes:
                    if source_node_id != target_node_id:
                        checks.append((source_node_id, target_node_id))
        else:
            checks = [(node_ids[0], node_ids[1])]

        for source_node_id, target_node_id in checks:
            retry_assertions(
                lambda source_node_id=source_node_id, target_node_id=target_node_id: self._assert_node_viewer_nodes_works(source_node_id, target_node_id),
                timeout_seconds=30,
                step_seconds=1,
            )
