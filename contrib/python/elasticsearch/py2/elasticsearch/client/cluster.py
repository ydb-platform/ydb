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


class ClusterClient(NamespacedClient):
    @query_params(
        "expand_wildcards",
        "level",
        "local",
        "master_timeout",
        "timeout",
        "wait_for_active_shards",
        "wait_for_events",
        "wait_for_no_initializing_shards",
        "wait_for_no_relocating_shards",
        "wait_for_nodes",
        "wait_for_status",
        response_mimetypes=["application/json"],
    )
    def health(self, index=None, params=None, headers=None):
        """
        Returns basic information about the health of the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-health.html>`_

        :arg index: Limit the information returned to a specific index
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indices that are open, closed or both.  Valid choices: open,
            closed, hidden, none, all  Default: all
        :arg level: Specify the level of detail for returned information
            Valid choices: cluster, indices, shards  Default: cluster
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg timeout: Explicit operation timeout
        :arg wait_for_active_shards: Wait until the specified number of
            shards is active
        :arg wait_for_events: Wait until all currently queued events
            with the given priority are processed  Valid choices: immediate, urgent,
            high, normal, low, languid
        :arg wait_for_no_initializing_shards: Whether to wait until
            there are no initializing shards in the cluster
        :arg wait_for_no_relocating_shards: Whether to wait until there
            are no relocating shards in the cluster
        :arg wait_for_nodes: Wait until the specified number of nodes is
            available
        :arg wait_for_status: Wait until cluster is in a specific state
            Valid choices: green, yellow, red
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cluster", "health", index),
            params=params,
            headers=headers,
        )

    @query_params(
        "local",
        "master_timeout",
        response_mimetypes=["application/json"],
    )
    def pending_tasks(self, params=None, headers=None):
        """
        Returns a list of any cluster-level changes (e.g. create index, update mapping,
        allocate or fail shard) which have not yet been executed.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-pending.html>`_

        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Specify timeout for connection to master
        """
        return self.transport.perform_request(
            "GET", "/_cluster/pending_tasks", params=params, headers=headers
        )

    @query_params(
        "allow_no_indices",
        "expand_wildcards",
        "flat_settings",
        "ignore_unavailable",
        "local",
        "master_timeout",
        "wait_for_metadata_version",
        "wait_for_timeout",
        response_mimetypes=["application/json"],
    )
    def state(self, metric=None, index=None, params=None, headers=None):
        """
        Returns a comprehensive information about the state of the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-state.html>`_

        :arg metric: Limit the information returned to the specified
            metrics  Valid choices: _all, blocks, metadata, nodes, routing_table,
            routing_nodes, master_node, version
        :arg index: A comma-separated list of index names; use `_all` or
            empty string to perform the operation on all indices
        :arg allow_no_indices: Whether to ignore if a wildcard indices
            expression resolves into no concrete indices. (This includes `_all`
            string or when no indices have been specified)
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indices that are open, closed or both.  Valid choices: open,
            closed, hidden, none, all  Default: open
        :arg flat_settings: Return settings in flat format (default:
            false)
        :arg ignore_unavailable: Whether specified concrete indices
            should be ignored when unavailable (missing or closed)
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Specify timeout for connection to master
        :arg wait_for_metadata_version: Wait for the metadata version to
            be equal or greater than the specified metadata version
        :arg wait_for_timeout: The maximum time to wait for
            wait_for_metadata_version before timing out
        """
        if index and metric in SKIP_IN_PATH:
            metric = "_all"

        return self.transport.perform_request(
            "GET",
            _make_path("_cluster", "state", metric, index),
            params=params,
            headers=headers,
        )

    @query_params(
        "flat_settings",
        "timeout",
        response_mimetypes=["application/json"],
    )
    def stats(self, node_id=None, params=None, headers=None):
        """
        Returns high-level overview of cluster statistics.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-stats.html>`_

        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information; use `_local` to return information from
            the node you're connecting to, leave empty to get information from all
            nodes
        :arg flat_settings: Return settings in flat format (default:
            false)
        :arg timeout: Explicit operation timeout
        """
        return self.transport.perform_request(
            "GET",
            "/_cluster/stats"
            if node_id in SKIP_IN_PATH
            else _make_path("_cluster", "stats", "nodes", node_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "dry_run",
        "explain",
        "master_timeout",
        "metric",
        "retry_failed",
        "timeout",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def reroute(self, body=None, params=None, headers=None):
        """
        Allows to manually change the allocation of individual shards in the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-reroute.html>`_

        :arg body: The definition of `commands` to perform (`move`,
            `cancel`, `allocate`)
        :arg dry_run: Simulate the operation only and return the
            resulting state
        :arg explain: Return an explanation of why the commands can or
            cannot be executed
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg metric: Limit the information returned to the specified
            metrics. Defaults to all but metadata  Valid choices: _all, blocks,
            metadata, nodes, routing_table, master_node, version
        :arg retry_failed: Retries allocation of shards that are blocked
            due to too many subsequent allocation failures
        :arg timeout: Explicit operation timeout
        """
        return self.transport.perform_request(
            "POST", "/_cluster/reroute", params=params, headers=headers, body=body
        )

    @query_params(
        "flat_settings",
        "include_defaults",
        "master_timeout",
        "timeout",
        response_mimetypes=["application/json"],
    )
    def get_settings(self, params=None, headers=None):
        """
        Returns cluster settings.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-get-settings.html>`_

        :arg flat_settings: Return settings in flat format (default:
            false)
        :arg include_defaults: Whether to return all default clusters
            setting.
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg timeout: Explicit operation timeout
        """
        return self.transport.perform_request(
            "GET", "/_cluster/settings", params=params, headers=headers
        )

    @query_params(
        "flat_settings",
        "master_timeout",
        "timeout",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def put_settings(self, body, params=None, headers=None):
        """
        Updates the cluster settings.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-update-settings.html>`_

        :arg body: The settings to be updated. Can be either `transient`
            or `persistent` (survives cluster restart).
        :arg flat_settings: Return settings in flat format (default:
            false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg timeout: Explicit operation timeout
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "PUT", "/_cluster/settings", params=params, headers=headers, body=body
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def remote_info(self, params=None, headers=None):
        """
        Returns the information about configured remote clusters.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-remote-info.html>`_
        """
        return self.transport.perform_request(
            "GET", "/_remote/info", params=params, headers=headers
        )

    @query_params(
        "include_disk_info",
        "include_yes_decisions",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def allocation_explain(self, body=None, params=None, headers=None):
        """
        Provides explanations for shard allocations in the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cluster-allocation-explain.html>`_

        :arg body: The index, shard, and primary flag to explain. Empty
            means 'explain a randomly-chosen unassigned shard'
        :arg include_disk_info: Return information about disk usage and
            shard sizes (default: false)
        :arg include_yes_decisions: Return 'YES' decisions in
            explanation (default: false)
        """
        return self.transport.perform_request(
            "POST",
            "/_cluster/allocation/explain",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "master_timeout",
        "timeout",
        response_mimetypes=["application/json"],
    )
    def delete_component_template(self, name, params=None, headers=None):
        """
        Deletes a component template

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/indices-component-template.html>`_

        :arg name: The name of the template
        :arg master_timeout: Specify timeout for connection to master
        :arg timeout: Explicit operation timeout
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_component_template", name),
            params=params,
            headers=headers,
        )

    @query_params(
        "local",
        "master_timeout",
        response_mimetypes=["application/json"],
    )
    def get_component_template(self, name=None, params=None, headers=None):
        """
        Returns one or more component templates

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/indices-component-template.html>`_

        :arg name: The comma separated names of the component templates
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_component_template", name),
            params=params,
            headers=headers,
        )

    @query_params(
        "create",
        "master_timeout",
        "timeout",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def put_component_template(self, name, body, params=None, headers=None):
        """
        Creates or updates a component template

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/indices-component-template.html>`_

        :arg name: The name of the template
        :arg body: The template definition
        :arg create: Whether the index template should only be added if
            new or can also replace an existing one
        :arg master_timeout: Specify timeout for connection to master
        :arg timeout: Explicit operation timeout
        """
        for param in (name, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_component_template", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "local",
        "master_timeout",
        response_mimetypes=["application/json"],
    )
    def exists_component_template(self, name, params=None, headers=None):
        """
        Returns information about whether a particular component template exist

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/indices-component-template.html>`_

        :arg name: The name of the template
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "HEAD",
            _make_path("_component_template", name),
            params=params,
            headers=headers,
        )

    @query_params(
        "wait_for_removal",
        response_mimetypes=["application/json"],
    )
    def delete_voting_config_exclusions(self, params=None, headers=None):
        """
        Clears cluster voting config exclusions.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/voting-config-exclusions.html>`_

        :arg wait_for_removal: Specifies whether to wait for all
            excluded nodes to be removed from the cluster before clearing the voting
            configuration exclusions list.  Default: True
        """
        return self.transport.perform_request(
            "DELETE",
            "/_cluster/voting_config_exclusions",
            params=params,
            headers=headers,
        )

    @query_params(
        "node_ids",
        "node_names",
        "timeout",
        response_mimetypes=["application/json"],
    )
    def post_voting_config_exclusions(self, params=None, headers=None):
        """
        Updates the cluster voting config exclusions by node ids or node names.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/voting-config-exclusions.html>`_

        :arg node_ids: A comma-separated list of the persistent ids of
            the nodes to exclude from the voting configuration. If specified, you
            may not also specify ?node_names.
        :arg node_names: A comma-separated list of the names of the
            nodes to exclude from the voting configuration. If specified, you may
            not also specify ?node_ids.
        :arg timeout: Explicit operation timeout  Default: 30s
        """
        return self.transport.perform_request(
            "POST", "/_cluster/voting_config_exclusions", params=params, headers=headers
        )
