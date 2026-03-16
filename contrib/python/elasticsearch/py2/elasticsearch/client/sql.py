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


class SqlClient(NamespacedClient):
    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def clear_cursor(self, body, params=None, headers=None):
        """
        Clears the SQL cursor

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/clear-sql-cursor-api.html>`_

        :arg body: Specify the cursor value in the `cursor` element to
            clean the cursor.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "POST", "/_sql/close", params=params, headers=headers, body=body
        )

    @query_params(
        "format",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def query(self, body, params=None, headers=None):
        """
        Executes a SQL request

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/sql-search-api.html>`_

        :arg body: Use the `query` element to start a query. Use the
            `cursor` element to continue a query.
        :arg format: a short version of the Accept header, e.g. json,
            yaml
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "POST", "/_sql", params=params, headers=headers, body=body
        )

    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def translate(self, body, params=None, headers=None):
        """
        Translates SQL into Elasticsearch queries

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/sql-translate-api.html>`_

        :arg body: Specify the query in the `query` element.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "POST", "/_sql/translate", params=params, headers=headers, body=body
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def delete_async(self, id, params=None, headers=None):
        """
        Deletes an async SQL search or a stored synchronous SQL search. If the search
        is still running, the API cancels it.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/delete-async-sql-search-api.html>`_

        :arg id: The async search ID
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_sql", "async", "delete", id),
            params=params,
            headers=headers,
        )

    @query_params(
        "delimiter",
        "format",
        "keep_alive",
        "wait_for_completion_timeout",
        response_mimetypes=["application/json"],
    )
    def get_async(self, id, params=None, headers=None):
        """
        Returns the current status and available results for an async SQL search or
        stored synchronous SQL search

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-async-sql-search-api.html>`_

        :arg id: The async search ID
        :arg delimiter: Separator for CSV results  Default: ,
        :arg format: Short version of the Accept header, e.g. json, yaml
        :arg keep_alive: Retention period for the search and its results
            Default: 5d
        :arg wait_for_completion_timeout: Duration to wait for complete
            results
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "GET", _make_path("_sql", "async", id), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def get_async_status(self, id, params=None, headers=None):
        """
        Returns the current status of an async SQL search or a stored synchronous SQL
        search

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-async-sql-search-status-api.html>`_

        :arg id: The async search ID
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_sql", "async", "status", id),
            params=params,
            headers=headers,
        )
