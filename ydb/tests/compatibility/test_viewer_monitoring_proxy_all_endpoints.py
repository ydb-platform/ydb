# -*- coding: utf-8 -*-
import json
import socket
import time
import urllib.error
import urllib.parse
import urllib.request

import pytest

from ydb.tests.library.common.wait_for import retry_assertions
from ydb.tests.library.compatibility.fixtures import MixedClusterFixture


class TestViewerMonitoringProxyAllEndpointsAvailability(MixedClusterFixture):
    REQUEST_TIMEOUT_SECONDS = 10
    REQUEST_TIMEOUT_QUERY_MS = 10000
    COMMON_QUERY_PARAMS = {
        "enums": "true",
        "ui64": "true",
        "direct": "true",
    }
    ENDPOINT_QUERY_PARAMS = {
        "/viewer/nodes": {
            "tablets": "true",
            "sysinfo": "true",
            "nodeinfo": "true",
            "pdisk": "true",
            "vdisk": "true",
            "storage": "true",
            "nodelist": "true",
        },
        "/viewer/storage": {
            "groups": "true",
            "pdisks": "true",
            "vdisks": "true",
        },
        "/viewer/storage_stats": {
            "include": "all",
        },
        "/viewer/cluster": {
            "tablets": "true",
        },
        "/viewer/compute": {
            "all": "true",
        },
    }

    @pytest.fixture(autouse=True, scope="function")
    def setup(self):
        yield from self.setup_cluster()

    def _get_representative_pairs(self):
        node_ids = sorted(self.cluster.nodes.keys())
        assert len(node_ids) >= 2, "Expected at least 2 nodes in compatibility cluster"

        representative_node_by_binary = {}
        for node_id in node_ids:
            representative_node_by_binary.setdefault(self.config.get_binary_path(node_id), node_id)

        representative_nodes = list(representative_node_by_binary.values())
        if len(representative_nodes) >= 2:
            return [
                (source_node_id, target_node_id)
                for source_node_id in representative_nodes
                for target_node_id in representative_nodes
                if source_node_id != target_node_id
            ]
        return [(node_ids[0], node_ids[1])]

    @classmethod
    def _with_query_params(cls, url, timeout_ms, endpoint):
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        query["timeout"] = [str(timeout_ms)]
        for key, value in cls.COMMON_QUERY_PARAMS.items():
            query[key] = [value]
        for key, value in cls.ENDPOINT_QUERY_PARAMS.get(endpoint, {}).items():
            query[key] = [value]
        return urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(query, doseq=True), parsed.fragment)
        )

    def _make_proxy_url(self, source_node_id, target_node_id, path):
        source_node = self.cluster.nodes[source_node_id]
        base_url = f"http://{source_node.host}:{source_node.mon_port}/node/{target_node_id}{path}"
        return self._with_query_params(base_url, self.REQUEST_TIMEOUT_QUERY_MS, path)

    def _request_status(self, url):
        started = time.monotonic()
        try:
            with urllib.request.urlopen(url, timeout=self.REQUEST_TIMEOUT_SECONDS) as response:
                status = response.getcode()
        except urllib.error.HTTPError as error:
            status = error.code
        except (urllib.error.URLError, socket.timeout, TimeoutError) as error:
            raise AssertionError(f"Request failed for {url}: {error}")
        elapsed = time.monotonic() - started
        return status, elapsed

    def _request_capabilities(self, source_node_id, target_node_id):
        url = self._make_proxy_url(source_node_id, target_node_id, "/viewer/json/capabilities")
        with urllib.request.urlopen(url, timeout=self.REQUEST_TIMEOUT_SECONDS) as response:
            status = response.getcode()
            body = response.read().decode("utf-8", errors="replace")
        payload = json.loads(body)
        return status, body, payload

    def _get_all_endpoints(self, source_node_id, target_node_id):
        status, body, payload = self._request_capabilities(source_node_id, target_node_id)
        assert status == 200, (
            f"Unexpected HTTP status for /viewer/json/capabilities via node {source_node_id} "
            f"to node {target_node_id}: {status}"
        )
        assert isinstance(payload, dict), (
            f"Expected JSON object for /viewer/json/capabilities via node {source_node_id} "
            f"to node {target_node_id}, got: {body}"
        )
        capabilities = payload.get("Capabilities")
        assert isinstance(capabilities, dict) and capabilities, (
            f"Expected non-empty 'Capabilities' object for /viewer/json/capabilities via node {source_node_id} "
            f"to node {target_node_id}, got: {body}"
        )

        # /api/viewer.yaml is not a capability endpoint, but it is part of viewer API surface.
        endpoints = sorted(set(capabilities.keys()) | {"/api/viewer.yaml"})
        return endpoints

    def _assert_endpoint_responds_fast(self, source_node_id, target_node_id, endpoint):
        url = self._make_proxy_url(source_node_id, target_node_id, endpoint)
        status, elapsed = self._request_status(url)
        assert 100 <= status <= 599, (
            f"Unexpected HTTP status for {endpoint} via node {source_node_id} "
            f"to node {target_node_id}: {status}"
        )
        assert elapsed <= self.REQUEST_TIMEOUT_SECONDS, (
            f"Slow response for {endpoint} via node {source_node_id} to node {target_node_id}: "
            f"{elapsed:.3f}s > {self.REQUEST_TIMEOUT_SECONDS}s"
        )

    def test_monitoring_proxy_all_viewer_endpoints_respond(self):
        for source_node_id, target_node_id in self._get_representative_pairs():
            endpoints = retry_assertions(
                lambda source_node_id=source_node_id, target_node_id=target_node_id:
                    self._get_all_endpoints(source_node_id, target_node_id),
                timeout_seconds=30,
                step_seconds=1,
            )

            for endpoint in endpoints:
                retry_assertions(
                    lambda source_node_id=source_node_id, target_node_id=target_node_id, endpoint=endpoint:
                        self._assert_endpoint_responds_fast(source_node_id, target_node_id, endpoint),
                    timeout_seconds=30,
                    step_seconds=1,
                )
