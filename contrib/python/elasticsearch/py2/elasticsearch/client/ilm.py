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


class IlmClient(NamespacedClient):
    @query_params(
        response_mimetypes=["application/json"],
    )
    def delete_lifecycle(self, policy, params=None, headers=None):
        """
        Deletes the specified lifecycle policy definition. A currently used policy
        cannot be deleted.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-delete-lifecycle.html>`_

        :arg policy: The name of the index lifecycle policy
        """
        if policy in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'policy'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_ilm", "policy", policy),
            params=params,
            headers=headers,
        )

    @query_params(
        "only_errors",
        "only_managed",
        response_mimetypes=["application/json"],
    )
    def explain_lifecycle(self, index, params=None, headers=None):
        """
        Retrieves information about the index's current lifecycle state, such as the
        currently executing phase, action, and step.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-explain-lifecycle.html>`_

        :arg index: The name of the index to explain
        :arg only_errors: filters the indices included in the response
            to ones in an ILM error state, implies only_managed
        :arg only_managed: filters the indices included in the response
            to ones managed by ILM
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "GET", _make_path(index, "_ilm", "explain"), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def get_lifecycle(self, policy=None, params=None, headers=None):
        """
        Returns the specified policy definition. Includes the policy version and last
        modified date.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-get-lifecycle.html>`_

        :arg policy: The name of the index lifecycle policy
        """
        return self.transport.perform_request(
            "GET", _make_path("_ilm", "policy", policy), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def get_status(self, params=None, headers=None):
        """
        Retrieves the current index lifecycle management (ILM) status.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-get-status.html>`_
        """
        return self.transport.perform_request(
            "GET", "/_ilm/status", params=params, headers=headers
        )

    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def move_to_step(self, index, body=None, params=None, headers=None):
        """
        Manually moves an index into the specified step and executes that step.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-move-to-step.html>`_

        :arg index: The name of the index whose lifecycle step is to
            change
        :arg body: The new lifecycle step to move to
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "POST",
            _make_path("_ilm", "move", index),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def put_lifecycle(self, policy, body=None, params=None, headers=None):
        """
        Creates a lifecycle policy

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-put-lifecycle.html>`_

        :arg policy: The name of the index lifecycle policy
        :arg body: The lifecycle policy definition to register
        """
        if policy in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'policy'.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_ilm", "policy", policy),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def remove_policy(self, index, params=None, headers=None):
        """
        Removes the assigned lifecycle policy and stops managing the specified index

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-remove-policy.html>`_

        :arg index: The name of the index to remove policy on
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "POST", _make_path(index, "_ilm", "remove"), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def retry(self, index, params=None, headers=None):
        """
        Retries executing the policy for an index that is in the ERROR step.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-retry-policy.html>`_

        :arg index: The name of the indices (comma-separated) whose
            failed lifecycle step is to be retry
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "POST", _make_path(index, "_ilm", "retry"), params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def start(self, params=None, headers=None):
        """
        Start the index lifecycle management (ILM) plugin.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-start.html>`_
        """
        return self.transport.perform_request(
            "POST", "/_ilm/start", params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def stop(self, params=None, headers=None):
        """
        Halts all lifecycle management operations and stops the index lifecycle
        management (ILM) plugin

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-stop.html>`_
        """
        return self.transport.perform_request(
            "POST", "/_ilm/stop", params=params, headers=headers
        )

    @query_params(
        "dry_run",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def migrate_to_data_tiers(self, body=None, params=None, headers=None):
        """
        Migrates the indices and ILM policies away from custom node attribute
        allocation routing to data tiers routing

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/ilm-migrate-to-data-tiers.html>`_

        :arg body: Optionally specify a legacy index template name to
            delete and optionally specify a node attribute name used for index shard
            routing (defaults to "data")
        :arg dry_run: If set to true it will simulate the migration,
            providing a way to retrieve the ILM policies and indices that need to be
            migrated. The default is false
        """
        return self.transport.perform_request(
            "POST",
            "/_ilm/migrate_to_data_tiers",
            params=params,
            headers=headers,
            body=body,
        )
