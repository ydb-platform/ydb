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
    RESPONSE_GRACE_SECONDS = 1
    CLIENT_TIMEOUT_SECONDS = REQUEST_TIMEOUT_SECONDS + RESPONSE_GRACE_SECONDS
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

    def _make_proxy_url(self, source_node_id, target_node_id, path, query_params=None):
        source_node = self.cluster.nodes[source_node_id]
        base_url = f"http://{source_node.host}:{source_node.mon_port}/node/{target_node_id}{path}"
        url = self._with_query_params(base_url, self.REQUEST_TIMEOUT_QUERY_MS, path)
        if not query_params:
            return url
        parsed = urllib.parse.urlsplit(url)
        query = urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        for key, value in query_params.items():
            query[key] = [str(value)]
        return urllib.parse.urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, urllib.parse.urlencode(query, doseq=True), parsed.fragment)
        )

    def _perform_request(self, method, url, body=None, headers=None):
        request_body = None
        request_headers = dict(headers or {})
        if body is not None:
            request_body = json.dumps(body).encode("utf-8")
            request_headers.setdefault("Content-Type", "application/json")

        request = urllib.request.Request(url, data=request_body, headers=request_headers, method=method)
        started = time.monotonic()
        try:
            with urllib.request.urlopen(request, timeout=self.CLIENT_TIMEOUT_SECONDS) as response:
                status = response.getcode()
        except urllib.error.HTTPError as error:
            status = error.code
        except (urllib.error.URLError, socket.timeout, TimeoutError) as error:
            raise AssertionError(f"Request failed for {method} {url}: {error}")
        elapsed = time.monotonic() - started
        return status, elapsed

    def _assert_request_responds_fast(
        self,
        source_node_id,
        target_node_id,
        path,
        method="GET",
        query_params=None,
        body=None,
        headers=None,
    ):
        endpoint = path
        url = self._make_proxy_url(
            source_node_id,
            target_node_id,
            endpoint,
            query_params=query_params,
        )
        status, elapsed = self._perform_request(
            method,
            url,
            body=body,
            headers=headers,
        )
        assert 100 <= status <= 599, (
            f"Unexpected HTTP status for {endpoint} via node {source_node_id} "
            f"to node {target_node_id}: {status}"
        )
        assert elapsed <= self.CLIENT_TIMEOUT_SECONDS, (
            f"Slow response for {endpoint} via node {source_node_id} to node {target_node_id}: "
            f"{elapsed:.3f}s > {self.CLIENT_TIMEOUT_SECONDS}s "
            f"(request timeout {self.REQUEST_TIMEOUT_SECONDS}s)"
        )

    def test_monitoring_proxy_nodes_endpoint_responds(self):
        for source_node_id, target_node_id in self._get_representative_pairs():
            retry_assertions(
                lambda source_node_id=source_node_id, target_node_id=target_node_id:
                    self._assert_request_responds_fast(
                        source_node_id,
                        target_node_id,
                        "/viewer/nodes",
                        query_params={
                            "database": self.database_path,
                        },
                    ),
                timeout_seconds=30,
                step_seconds=1,
            )

    def test_monitoring_proxy_query_endpoint_responds(self):
        for source_node_id, target_node_id in self._get_representative_pairs():
            retry_assertions(
                lambda source_node_id=source_node_id, target_node_id=target_node_id:
                    self._assert_request_responds_fast(
                        source_node_id,
                        target_node_id,
                        "/viewer/query",
                        method="POST",
                        query_params={
                            "schema": "multi",
                            "base64": "false",
                        },
                        body={
                            "database": self.database_path,
                            "query": "SELECT 1;",
                            "action": "execute-query",
                            "schema": "multi",
                            "base64": False,
                        },
                        headers={
                            "Accept": "application/json",
                        },
                    ),
                timeout_seconds=30,
                step_seconds=1,
            )

    def test_monitoring_proxy_storage_groups_endpoint_responds(self):
        for source_node_id, target_node_id in self._get_representative_pairs():
            retry_assertions(
                lambda source_node_id=source_node_id, target_node_id=target_node_id:
                    self._assert_request_responds_fast(
                        source_node_id,
                        target_node_id,
                        "/storage/groups",
                        query_params={
                            "fields_required": "all",
                        },
                    ),
                timeout_seconds=30,
                step_seconds=1,
            )

    def test_monitoring_proxy_cluster_endpoint_responds(self):
        for source_node_id, target_node_id in self._get_representative_pairs():
            retry_assertions(
                lambda source_node_id=source_node_id, target_node_id=target_node_id:
                    self._assert_request_responds_fast(
                        source_node_id,
                        target_node_id,
                        "/viewer/cluster",
                        query_params={
                            "database": self.database_path,
                        },
                    ),
                timeout_seconds=30,
                step_seconds=1,
            )

    def test_monitoring_proxy_tenantinfo_endpoint_responds(self):
        for source_node_id, target_node_id in self._get_representative_pairs():
            retry_assertions(
                lambda source_node_id=source_node_id, target_node_id=target_node_id:
                    self._assert_request_responds_fast(
                        source_node_id,
                        target_node_id,
                        "/viewer/tenantinfo",
                        query_params={
                            "database": self.database_path,
                            "path": self.database_path,
                            "tablets": "false",
                            "storage": "true",
                        },
                    ),
                timeout_seconds=30,
                step_seconds=1,
            )
