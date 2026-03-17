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

from .utils import NamespacedClient, _make_path, query_params


class CatClient(NamespacedClient):
    @query_params(
        "expand_wildcards",
        "format",
        "h",
        "help",
        "local",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def aliases(self, name=None, params=None, headers=None):
        """
        Shows information about currently configured aliases to indices including
        filter and routing infos.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-alias.html>`_

        :arg name: A comma-separated list of alias names to return
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indices that are open, closed or both.  Valid choices: open,
            closed, hidden, none, all  Default: all
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "aliases", name), params=params, headers=headers
        )

    @query_params(
        "bytes",
        "format",
        "h",
        "help",
        "local",
        "master_timeout",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def allocation(self, node_id=None, params=None, headers=None):
        """
        Provides a snapshot of how many shards are allocated to each data node and how
        much disk space they are using.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-allocation.html>`_

        :arg node_id: A comma-separated list of node IDs or names to
            limit the returned information
        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "allocation", node_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "format",
        "h",
        "help",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def count(self, index=None, params=None, headers=None):
        """
        Provides quick access to the document count of the entire cluster, or
        individual indices.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-count.html>`_

        :arg index: A comma-separated list of index names to limit the
            returned information
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "count", index), params=params, headers=headers
        )

    @query_params(
        "format",
        "h",
        "help",
        "s",
        "time",
        "ts",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def health(self, params=None, headers=None):
        """
        Returns a concise representation of the cluster health.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-health.html>`_

        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg ts: Set to false to disable timestamping  Default: True
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", "/_cat/health", params=params, headers=headers
        )

    @query_params(
        "help",
        "s",
        response_mimetypes=["text/plain"],
    )
    def help(self, params=None, headers=None):
        """
        Returns help for the Cat APIs.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat.html>`_

        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        """
        return self.transport.perform_request(
            "GET", "/_cat", params=params, headers=headers
        )

    @query_params(
        "bytes",
        "expand_wildcards",
        "format",
        "h",
        "health",
        "help",
        "include_unloaded_segments",
        "local",
        "master_timeout",
        "pri",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def indices(self, index=None, params=None, headers=None):
        """
        Returns information about indices: number of primaries and replicas, document
        counts, disk size, ...

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-indices.html>`_

        :arg index: A comma-separated list of index names to limit the
            returned information
        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indices that are open, closed or both.  Valid choices: open,
            closed, hidden, none, all  Default: all
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg health: A health status ("green", "yellow", or "red" to
            filter only indices matching the specified health status  Valid choices:
            green, yellow, red
        :arg help: Return help information
        :arg include_unloaded_segments: If set to true segment stats
            will include stats for segments that are not currently loaded into
            memory
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg pri: Set to true to return stats only for primary shards
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "indices", index), params=params, headers=headers
        )

    @query_params(
        "format",
        "h",
        "help",
        "local",
        "master_timeout",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def master(self, params=None, headers=None):
        """
        Returns information about the master node.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-master.html>`_

        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", "/_cat/master", params=params, headers=headers
        )

    @query_params(
        "bytes",
        "format",
        "full_id",
        "h",
        "help",
        "include_unloaded_segments",
        "local",
        "master_timeout",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def nodes(self, params=None, headers=None):
        """
        Returns basic statistics about performance of cluster nodes.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-nodes.html>`_

        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg full_id: Return the full node ID instead of the shortened
            version (default: false)
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg include_unloaded_segments: If set to true segment stats
            will include stats for segments that are not currently loaded into
            memory
        :arg local: Calculate the selected nodes using the local cluster
            state rather than the state from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", "/_cat/nodes", params=params, headers=headers
        )

    @query_params(
        "active_only",
        "bytes",
        "detailed",
        "format",
        "h",
        "help",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def recovery(self, index=None, params=None, headers=None):
        """
        Returns information about index shard recoveries, both on-going completed.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-recovery.html>`_

        :arg index: Comma-separated list or wildcard expression of index
            names to limit the returned information
        :arg active_only: If `true`, the response only includes ongoing
            shard recoveries
        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg detailed: If `true`, the response includes detailed
            information about shard recoveries
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "recovery", index), params=params, headers=headers
        )

    @query_params(
        "bytes",
        "format",
        "h",
        "help",
        "local",
        "master_timeout",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def shards(self, index=None, params=None, headers=None):
        """
        Provides a detailed view of shard allocation on nodes.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-shards.html>`_

        :arg index: A comma-separated list of index names to limit the
            returned information
        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "shards", index), params=params, headers=headers
        )

    @query_params(
        "bytes",
        "format",
        "h",
        "help",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def segments(self, index=None, params=None, headers=None):
        """
        Provides low-level information about the segments in the shards of an index.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-segments.html>`_

        :arg index: A comma-separated list of index names to limit the
            returned information
        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "segments", index), params=params, headers=headers
        )

    @query_params(
        "format",
        "h",
        "help",
        "local",
        "master_timeout",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def pending_tasks(self, params=None, headers=None):
        """
        Returns a concise representation of the cluster pending tasks.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-pending-tasks.html>`_

        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", "/_cat/pending_tasks", params=params, headers=headers
        )

    @query_params(
        "format",
        "h",
        "help",
        "local",
        "master_timeout",
        "s",
        "size",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def thread_pool(self, thread_pool_patterns=None, params=None, headers=None):
        """
        Returns cluster-wide thread pool statistics per node. By default the active,
        queue and rejected statistics are returned for all thread pools.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-thread-pool.html>`_

        :arg thread_pool_patterns: A comma-separated list of regular-
            expressions to filter the thread pools in the output
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg size: The multiplier in which to display values  Valid
            choices: , k, m, g, t, p
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "thread_pool", thread_pool_patterns),
            params=params,
            headers=headers,
        )

    @query_params(
        "bytes",
        "format",
        "h",
        "help",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def fielddata(self, fields=None, params=None, headers=None):
        """
        Shows how much heap memory is currently being used by fielddata on every data
        node in the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-fielddata.html>`_

        :arg fields: A comma-separated list of fields to return the
            fielddata size
        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "fielddata", fields),
            params=params,
            headers=headers,
        )

    @query_params(
        "format",
        "h",
        "help",
        "include_bootstrap",
        "local",
        "master_timeout",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def plugins(self, params=None, headers=None):
        """
        Returns information about installed plugins across nodes node.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-plugins.html>`_

        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg include_bootstrap: Include bootstrap plugins in the
            response
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", "/_cat/plugins", params=params, headers=headers
        )

    @query_params(
        "format",
        "h",
        "help",
        "local",
        "master_timeout",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def nodeattrs(self, params=None, headers=None):
        """
        Returns information about custom node attributes.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-nodeattrs.html>`_

        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", "/_cat/nodeattrs", params=params, headers=headers
        )

    @query_params(
        "format",
        "h",
        "help",
        "local",
        "master_timeout",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def repositories(self, params=None, headers=None):
        """
        Returns information about snapshot repositories registered in the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-repositories.html>`_

        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg local: Return local information, do not retrieve the state
            from master node
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", "/_cat/repositories", params=params, headers=headers
        )

    @query_params(
        "format",
        "h",
        "help",
        "ignore_unavailable",
        "master_timeout",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def snapshots(self, repository=None, params=None, headers=None):
        """
        Returns all snapshots in a specific repository.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-snapshots.html>`_

        :arg repository: Name of repository from which to fetch the
            snapshot information
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg ignore_unavailable: Set to true to ignore unavailable
            snapshots
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "snapshots", repository),
            params=params,
            headers=headers,
        )

    @query_params(
        "actions",
        "detailed",
        "format",
        "h",
        "help",
        "nodes",
        "parent_task_id",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def tasks(self, params=None, headers=None):
        """
        Returns information about the tasks currently executing on one or more nodes in
        the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/tasks.html>`_

        :arg actions: A comma-separated list of actions that should be
            returned. Leave empty to return all.
        :arg detailed: Return detailed task information (default: false)
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg nodes: A comma-separated list of node IDs or names to limit
            the returned information; use `_local` to return information from the
            node you're connecting to, leave empty to get information from all nodes
        :arg parent_task_id: Return tasks with specified parent task id
            (node_id:task_number). Set to -1 to return all.
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", "/_cat/tasks", params=params, headers=headers
        )

    @query_params(
        "format",
        "h",
        "help",
        "local",
        "master_timeout",
        "s",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def templates(self, name=None, params=None, headers=None):
        """
        Returns information about existing templates.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-templates.html>`_

        :arg name: A pattern that returned template names must match
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        :arg master_timeout: Explicit operation timeout for connection
            to master node
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET", _make_path("_cat", "templates", name), params=params, headers=headers
        )

    @query_params(
        "allow_no_match",
        "bytes",
        "format",
        "h",
        "help",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def ml_data_frame_analytics(self, id=None, params=None, headers=None):
        """
        Gets configuration and usage information about data frame analytics jobs.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-dfanalytics.html>`_

        :arg id: The ID of the data frame analytics to fetch
        :arg allow_no_match: Whether to ignore if a wildcard expression
            matches no configs. (This includes `_all` string or when no configs have
            been specified)
        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "ml", "data_frame", "analytics", id),
            params=params,
            headers=headers,
        )

    @query_params(
        "allow_no_datafeeds",
        "allow_no_match",
        "format",
        "h",
        "help",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def ml_datafeeds(self, datafeed_id=None, params=None, headers=None):
        """
        Gets configuration and usage information about datafeeds.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-datafeeds.html>`_

        :arg datafeed_id: The ID of the datafeeds stats to fetch
        :arg allow_no_datafeeds: Whether to ignore if a wildcard
            expression matches no datafeeds. (This includes `_all` string or when no
            datafeeds have been specified)
        :arg allow_no_match: Whether to ignore if a wildcard expression
            matches no datafeeds. (This includes `_all` string or when no datafeeds
            have been specified)
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "ml", "datafeeds", datafeed_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "allow_no_jobs",
        "allow_no_match",
        "bytes",
        "format",
        "h",
        "help",
        "s",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def ml_jobs(self, job_id=None, params=None, headers=None):
        """
        Gets configuration and usage information about anomaly detection jobs.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-anomaly-detectors.html>`_

        :arg job_id: The ID of the jobs stats to fetch
        :arg allow_no_jobs: Whether to ignore if a wildcard expression
            matches no jobs. (This includes `_all` string or when no jobs have been
            specified)
        :arg allow_no_match: Whether to ignore if a wildcard expression
            matches no jobs. (This includes `_all` string or when no jobs have been
            specified)
        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "ml", "anomaly_detectors", job_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "allow_no_match",
        "bytes",
        "format",
        "from_",
        "h",
        "help",
        "s",
        "size",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def ml_trained_models(self, model_id=None, params=None, headers=None):
        """
        Gets configuration and usage information about inference trained models.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-trained-model.html>`_

        :arg model_id: The ID of the trained models stats to fetch
        :arg allow_no_match: Whether to ignore if a wildcard expression
            matches no trained models. (This includes `_all` string or when no
            trained models have been specified)  Default: True
        :arg bytes: The unit in which to display byte values  Valid
            choices: b, k, kb, m, mb, g, gb, t, tb, p, pb
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg from_: skips a number of trained models
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg size: specifies a max number of trained models to get
            Default: 100
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        if "from_" in params:
            params["from"] = params.pop("from_")

        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "ml", "trained_models", model_id),
            params=params,
            headers=headers,
        )

    @query_params(
        "allow_no_match",
        "format",
        "from_",
        "h",
        "help",
        "s",
        "size",
        "time",
        "v",
        response_mimetypes=["text/plain", "application/json"],
    )
    def transforms(self, transform_id=None, params=None, headers=None):
        """
        Gets configuration and usage information about transforms.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/cat-transforms.html>`_

        :arg transform_id: The id of the transform for which to get
            stats. '_all' or '*' implies all transforms
        :arg allow_no_match: Whether to ignore if a wildcard expression
            matches no transforms. (This includes `_all` string or when no
            transforms have been specified)
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        :arg from_: skips a number of transform configs, defaults to 0
        :arg h: Comma-separated list of column names to display
        :arg help: Return help information
        :arg s: Comma-separated list of column names or column aliases
            to sort by
        :arg size: specifies a max number of transforms to get, defaults
            to 100
        :arg time: The unit in which to display time values  Valid
            choices: d, h, m, s, ms, micros, nanos
        :arg v: Verbose mode. Display column headers
        """
        if "from_" in params:
            params["from"] = params.pop("from_")

        return self.transport.perform_request(
            "GET",
            _make_path("_cat", "transforms", transform_id),
            params=params,
            headers=headers,
        )
