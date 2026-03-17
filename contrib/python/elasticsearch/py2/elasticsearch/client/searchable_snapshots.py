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

from .utils import SKIP_IN_PATH, NamespacedClient, _make_path, query_params


class SearchableSnapshotsClient(NamespacedClient):
    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "ignore_unavailable",
        response_mimetypes=["application/json"],
    )
    def clear_cache(self, index=None, params=None, headers=None):
        """
        Clear the cache of searchable snapshots.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/searchable-snapshots-apis.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg index: A comma-separated list of index names
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indices that are open, closed or both.  Valid choices: open,
            closed, none, all  Default: open
        :arg ignore_unavailable: Whether specified concrete indices
            should be ignored when unavailable (missing or closed)
        """
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_searchable_snapshots", "cache", "clear"),
            params=params,
            headers=headers,
        )

    @query_params(
        "master_timeout",
        "storage",
        "wait_for_completion",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def mount(self, repository, snapshot, body, params=None, headers=None):
        """
        Mount a snapshot as a searchable index.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/searchable-snapshots-api-mount-snapshot.html>`_

        :arg repository: The name of the repository containing the
            snapshot of the index to mount
        :arg snapshot: The name of the snapshot of the index to mount
        :arg body: The restore configuration for mounting the snapshot
            as searchable
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg storage: Selects the kind of local storage used to
            accelerate searches. Experimental, and defaults to `full_copy`
        :arg wait_for_completion: Should this request wait until the
            operation has completed before returning
        """
        for param in (repository, snapshot, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "POST",
            _make_path("_snapshot", repository, snapshot, "_mount"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def repository_stats(self, repository, params=None, headers=None):
        """
        DEPRECATED: This API is replaced by the Repositories Metering API.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/searchable-snapshots-apis.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg repository: The repository for which to get the stats for
        """
        if repository in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'repository'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_snapshot", repository, "_stats"),
            params=params,
            headers=headers,
        )

    @query_params(
        "level",
        response_mimetypes=["application/json"],
    )
    def stats(self, index=None, params=None, headers=None):
        """
        Retrieve shard-level statistics about searchable snapshots.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/searchable-snapshots-apis.html>`_

        :arg index: A comma-separated list of index names
        :arg level: Return stats aggregated at cluster, index or shard
            level  Valid choices: cluster, indices, shards  Default: indices
        """
        return self.transport.perform_request(
            "GET",
            _make_path(index, "_searchable_snapshots", "stats"),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def cache_stats(self, node_id=None, params=None, headers=None):
        """
        Retrieve node-level cache statistics about searchable snapshots.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/searchable-snapshots-apis.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information; use `_local` to return information from
            the node you're connecting to, leave empty to get information from all
            nodes
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_searchable_snapshots", node_id, "cache", "stats"),
            params=params,
            headers=headers,
        )
