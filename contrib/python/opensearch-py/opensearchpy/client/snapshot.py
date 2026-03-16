# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
#  Licensed to Elasticsearch B.V. under one or more contributor
#  license agreements. See the NOTICE file distributed with
#  this work for additional information regarding copyright
#  ownership. Elasticsearch B.V. licenses this file to you under
#  the Apache License, Version 2.0 (the "License"); you may
#  not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# 	http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.


# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


from typing import Any

from .utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class SnapshotClient(NamespacedClient):
    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "wait_for_completion",
    )
    def create(
        self,
        *,
        repository: Any,
        snapshot: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a snapshot within an existing repository.


        :arg repository: The name of the repository where the snapshot
            will be stored.
        :arg snapshot: The name of the snapshot. Must be unique in the
            repository.
        :arg body: The snapshot definition.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg wait_for_completion: When `true`, the request returns a
            response when the snapshot is complete. When `false`, the request
            returns a response when the snapshot initializes. Default is false.
        """
        for param in (repository, snapshot):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_snapshot", repository, snapshot),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
    )
    def delete(
        self,
        *,
        repository: Any,
        snapshot: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a snapshot.


        :arg repository: The name of the snapshot repository to delete.
        :arg snapshot: A comma-separated list of snapshot names to
            delete from the repository.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Explicit operation timeout for
            connection to cluster-manager node
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (repository, snapshot):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_snapshot", repository, snapshot),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "ignore_unavailable",
        "master_timeout",
        "pretty",
        "source",
        "verbose",
    )
    def get(
        self,
        *,
        repository: Any,
        snapshot: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about a snapshot.


        :arg repository: A comma-separated list of snapshot repository
            names used to limit the request. Wildcard (*) expressions are supported.
        :arg snapshot: A comma-separated list of snapshot names to
            retrieve. Also accepts wildcard expressions. (`*`). To get information
            about all snapshots in a registered repository, use a wildcard (`*`) or
            `_all`. To get information about any snapshots that are currently
            running, use `_current`.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: When `false`, the request returns an
            error for any snapshots that are unavailable. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg verbose: When `true`, returns additional information about
            each snapshot, such as the version of OpenSearch which took the
            snapshot, the start and end times of the snapshot, and the number of
            shards contained in the snapshot. When `false`, returns only snapshot
            names and contained indexes. This is useful when the snapshots belong to
            a cloud-based repository, where each blob read is a cost or performance
            concern.
        """
        for param in (repository, snapshot):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "GET",
            _make_path("_snapshot", repository, snapshot),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def delete_repository(
        self,
        *,
        repository: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a snapshot repository.


        :arg repository: The name of the snapshot repository to
            unregister. Wildcard (`*`) patterns are supported.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Explicit operation timeout for
            connection to cluster-manager node
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: The amount of time to wait for a response.
        """
        if repository in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'repository'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_snapshot", repository),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "local",
        "master_timeout",
        "pretty",
        "source",
    )
    def get_repository(
        self,
        *,
        repository: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about a snapshot repository.


        :arg repository: A comma-separated list of repository names.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg local: Whether to get information from the local node.
            Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Explicit operation timeout for
            connection to cluster-manager node
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", _make_path("_snapshot", repository), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
        "verify",
    )
    def create_repository(
        self,
        *,
        repository: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a snapshot repository.


        :arg repository: The name for the newly registered repository.
        :arg body: The repository definition.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Explicit operation timeout for
            connection to cluster-manager node
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: The amount of time to wait for a response.
        :arg verify: When `true`, verifies the creation of the snapshot
            repository.
        """
        for param in (repository, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_snapshot", repository),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "wait_for_completion",
    )
    def restore(
        self,
        *,
        repository: Any,
        snapshot: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Restores a snapshot.


        :arg repository: The name of the repository containing the
            snapshot
        :arg snapshot: The name of the snapshot to restore.
        :arg body: Determines which settings and indexes to restore when
            restoring a snapshot
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Explicit operation timeout for
            connection to cluster-manager node
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg wait_for_completion: Whether to return a response after the
            restore operation has completed. When `false`, the request returns a
            response when the restore operation initializes. When `true`, the
            request returns a response when the restore operation completes. Default
            is false.
        """
        for param in (repository, snapshot):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "POST",
            _make_path("_snapshot", repository, snapshot, "_restore"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "ignore_unavailable",
        "master_timeout",
        "pretty",
        "source",
    )
    def status(
        self,
        *,
        repository: Any = None,
        snapshot: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about the status of a snapshot.


        :arg repository: The name of the repository containing the
            snapshot.
        :arg snapshot: A comma-separated list of snapshot names.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: Whether to ignore any unavailable
            snapshots, When `false`, a `SnapshotMissingException` is thrown. Default
            is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Explicit operation timeout for
            connection to cluster-manager node
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_snapshot", repository, snapshot, "_status"),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def verify_repository(
        self,
        *,
        repository: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Verifies a repository.


        :arg repository: The name of the repository containing the
            snapshot.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Explicit operation timeout for
            connection to cluster-manager node
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: The amount of time to wait for a response.
        """
        if repository in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'repository'.")

        return self.transport.perform_request(
            "POST",
            _make_path("_snapshot", repository, "_verify"),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def cleanup_repository(
        self,
        *,
        repository: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Removes any stale data from a snapshot repository.


        :arg repository: Snapshot repository to clean up.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: The amount of time to wait for a response.
        """
        if repository in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'repository'.")

        return self.transport.perform_request(
            "POST",
            _make_path("_snapshot", repository, "_cleanup"),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
    )
    def clone(
        self,
        *,
        repository: Any,
        snapshot: Any,
        target_snapshot: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a clone of all or part of a snapshot in the same repository as the
        original snapshot.


        :arg repository: The name of repository which will contain the
            snapshots clone.
        :arg snapshot: The name of the original snapshot.
        :arg target_snapshot: The name of the cloned snapshot.
        :arg body: The snapshot clone definition.
        :arg cluster_manager_timeout: The amount of time to wait for a
            response from the cluster manager node. For more information about
            supported time units, see [Common
            parameters](https://opensearch.org/docs/latest/api-reference/common-
            parameters/#time-units).
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Explicit operation timeout for
            connection to cluster-manager node
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        for param in (repository, snapshot, target_snapshot, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_snapshot", repository, snapshot, "_clone", target_snapshot),
            params=params,
            headers=headers,
            body=body,
        )
