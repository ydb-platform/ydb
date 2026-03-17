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


class EnrichClient(NamespacedClient):
    @query_params(
        response_mimetypes=["application/json"],
    )
    def delete_policy(self, name, params=None, headers=None):
        """
        Deletes an existing enrich policy and its enrich index.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/delete-enrich-policy-api.html>`_

        :arg name: The name of the enrich policy
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_enrich", "policy", name),
            params=params,
            headers=headers,
        )

    @query_params(
        "wait_for_completion",
        response_mimetypes=["application/json"],
    )
    def execute_policy(self, name, params=None, headers=None):
        """
        Creates the enrich index for an existing enrich policy.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/execute-enrich-policy-api.html>`_

        :arg name: The name of the enrich policy
        :arg wait_for_completion: Should the request should block until
            the execution is complete.  Default: True
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_enrich", "policy", name, "_execute"),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def get_policy(self, name=None, params=None, headers=None):
        """
        Gets information about an enrich policy.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-enrich-policy-api.html>`_

        :arg name: A comma-separated list of enrich policy names
        """
        return self.transport.perform_request(
            "GET", _make_path("_enrich", "policy", name), params=params, headers=headers
        )

    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def put_policy(self, name, body, params=None, headers=None):
        """
        Creates a new enrich policy.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/put-enrich-policy-api.html>`_

        :arg name: The name of the enrich policy
        :arg body: The enrich policy to register
        """
        for param in (name, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_enrich", "policy", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def stats(self, params=None, headers=None):
        """
        Gets enrich coordinator statistics and information about enrich policies that
        are currently executing.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/enrich-stats-api.html>`_
        """
        return self.transport.perform_request(
            "GET", "/_enrich/_stats", params=params, headers=headers
        )
