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


class WatcherClient(NamespacedClient):
    @query_params(
        response_mimetypes=["application/json"],
    )
    async def ack_watch(self, watch_id, action_id=None, params=None, headers=None):
        """
        Acknowledges a watch, manually throttling the execution of the watch's actions.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-ack-watch.html>`_

        :arg watch_id: Watch ID
        :arg action_id: A comma-separated list of the action ids to be
            acked
        """
        if watch_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'watch_id'.")

        return await self.transport.perform_request(
            "PUT",
            _make_path("_watcher", "watch", watch_id, "_ack", action_id),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def activate_watch(self, watch_id, params=None, headers=None):
        """
        Activates a currently inactive watch.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-activate-watch.html>`_

        :arg watch_id: Watch ID
        """
        if watch_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'watch_id'.")

        return await self.transport.perform_request(
            "PUT",
            _make_path("_watcher", "watch", watch_id, "_activate"),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def deactivate_watch(self, watch_id, params=None, headers=None):
        """
        Deactivates a currently active watch.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-deactivate-watch.html>`_

        :arg watch_id: Watch ID
        """
        if watch_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'watch_id'.")

        return await self.transport.perform_request(
            "PUT",
            _make_path("_watcher", "watch", watch_id, "_deactivate"),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def delete_watch(self, id, params=None, headers=None):
        """
        Removes a watch from Watcher.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-delete-watch.html>`_

        :arg id: Watch ID
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_watcher", "watch", id),
            params=params,
            headers=headers,
        )

    @query_params(
        "debug",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def execute_watch(self, body=None, id=None, params=None, headers=None):
        """
        Forces the execution of a stored watch.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-execute-watch.html>`_

        :arg body: Execution control
        :arg id: Watch ID
        :arg debug: indicates whether the watch should execute in debug
            mode
        """
        return await self.transport.perform_request(
            "PUT",
            _make_path("_watcher", "watch", id, "_execute"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def get_watch(self, id, params=None, headers=None):
        """
        Retrieves a watch by its ID.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-get-watch.html>`_

        :arg id: Watch ID
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "GET", _make_path("_watcher", "watch", id), params=params, headers=headers
        )

    @query_params(
        "active",
        "if_primary_term",
        "if_seq_no",
        "version",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def put_watch(self, id, body=None, params=None, headers=None):
        """
        Creates a new watch, or updates an existing one.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-put-watch.html>`_

        :arg id: Watch ID
        :arg body: The watch
        :arg active: Specify whether the watch is in/active by default
        :arg if_primary_term: only update the watch if the last
            operation that has changed the watch has the specified primary term
        :arg if_seq_no: only update the watch if the last operation that
            has changed the watch has the specified sequence number
        :arg version: Explicit version number for concurrency control
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "PUT",
            _make_path("_watcher", "watch", id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def start(self, params=None, headers=None):
        """
        Starts Watcher if it is not already running.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-start.html>`_
        """
        return await self.transport.perform_request(
            "POST", "/_watcher/_start", params=params, headers=headers
        )

    @query_params(
        "emit_stacktraces",
        response_mimetypes=["application/json"],
    )
    async def stats(self, metric=None, params=None, headers=None):
        """
        Retrieves the current Watcher metrics.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-stats.html>`_

        :arg metric: Controls what additional stat metrics should be
            include in the response  Valid choices: _all, queued_watches,
            current_watches, pending_watches
        :arg emit_stacktraces: Emits stack traces of currently running
            watches
        """
        return await self.transport.perform_request(
            "GET",
            _make_path("_watcher", "stats", metric),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def stop(self, params=None, headers=None):
        """
        Stops Watcher if it is running.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-stop.html>`_
        """
        return await self.transport.perform_request(
            "POST", "/_watcher/_stop", params=params, headers=headers
        )

    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def query_watches(self, body=None, params=None, headers=None):
        """
        Retrieves stored watches.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/watcher-api-query-watches.html>`_

        :arg body: From, size, query, sort and search_after
        """
        return await self.transport.perform_request(
            "POST",
            "/_watcher/_query/watches",
            params=params,
            headers=headers,
            body=body,
        )
