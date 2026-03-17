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


class RollupClient(NamespacedClient):
    @query_params(
        response_mimetypes=["application/json"],
    )
    async def delete_job(self, id, params=None, headers=None):
        """
        Deletes an existing rollup job.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/rollup-delete-job.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg id: The ID of the job to delete
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "DELETE", _make_path("_rollup", "job", id), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def get_jobs(self, id=None, params=None, headers=None):
        """
        Retrieves the configuration, stats, and status of rollup jobs.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/rollup-get-job.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg id: The ID of the job(s) to fetch. Accepts glob patterns,
            or left blank for all jobs
        """
        return await self.transport.perform_request(
            "GET", _make_path("_rollup", "job", id), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def get_rollup_caps(self, id=None, params=None, headers=None):
        """
        Returns the capabilities of any rollup jobs that have been configured for a
        specific index or index pattern.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/rollup-get-rollup-caps.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg id: The ID of the index to check rollup capabilities on, or
            left blank for all jobs
        """
        return await self.transport.perform_request(
            "GET", _make_path("_rollup", "data", id), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def get_rollup_index_caps(self, index, params=None, headers=None):
        """
        Returns the rollup capabilities of all jobs inside of a rollup index (e.g. the
        index where rollup data is stored).

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/rollup-get-rollup-index-caps.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg index: The rollup index or index pattern to obtain rollup
            capabilities from.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return await self.transport.perform_request(
            "GET", _make_path(index, "_rollup", "data"), params=params, headers=headers
        )

    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def put_job(self, id, body, params=None, headers=None):
        """
        Creates a rollup job.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/rollup-put-job.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg id: The ID of the job to create
        :arg body: The job configuration
        """
        for param in (id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return await self.transport.perform_request(
            "PUT",
            _make_path("_rollup", "job", id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "rest_total_hits_as_int",
        "typed_keys",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def rollup_search(
        self, index, body, doc_type=None, params=None, headers=None
    ):
        """
        Enables searching rolled-up data using the standard query DSL.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/rollup-search.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg index: The indices or index-pattern(s) (containing rollup
            or regular data) that should be searched
        :arg body: The search request body
        :arg doc_type: The doc type inside the index
        :arg rest_total_hits_as_int: Indicates whether hits.total should
            be rendered as an integer or an object in the rest search response
        :arg typed_keys: Specify whether aggregation and suggester names
            should be prefixed by their respective types in the response
        """
        for param in (index, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return await self.transport.perform_request(
            "POST",
            _make_path(index, doc_type, "_rollup_search"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def start_job(self, id, params=None, headers=None):
        """
        Starts an existing, stopped rollup job.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/rollup-start-job.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg id: The ID of the job to start
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "POST",
            _make_path("_rollup", "job", id, "_start"),
            params=params,
            headers=headers,
        )

    @query_params(
        "timeout",
        "wait_for_completion",
        response_mimetypes=["application/json"],
    )
    async def stop_job(self, id, params=None, headers=None):
        """
        Stops an existing, started rollup job.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/rollup-stop-job.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg id: The ID of the job to stop
        :arg timeout: Block for (at maximum) the specified duration
            while waiting for the job to stop.  Defaults to 30s.
        :arg wait_for_completion: True if the API should block until the
            job has fully stopped, false if should be executed async. Defaults to
            false.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return await self.transport.perform_request(
            "POST",
            _make_path("_rollup", "job", id, "_stop"),
            params=params,
            headers=headers,
        )

    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def rollup(self, index, rollup_index, body, params=None, headers=None):
        """
        Rollup an index

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/xpack-rollup.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version

        :arg index: The index to roll up
        :arg rollup_index: The name of the rollup index to create
        :arg body: The rollup configuration
        """
        for param in (index, rollup_index, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return await self.transport.perform_request(
            "POST",
            _make_path(index, "_rollup", rollup_index),
            params=params,
            headers=headers,
            body=body,
        )
