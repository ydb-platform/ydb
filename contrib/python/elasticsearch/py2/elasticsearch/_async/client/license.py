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

from .utils import NamespacedClient, query_params


class LicenseClient(NamespacedClient):
    @query_params(
        response_mimetypes=["application/json"],
    )
    async def delete(self, params=None, headers=None):
        """
        Deletes licensing information for the cluster

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/delete-license.html>`_
        """
        return await self.transport.perform_request(
            "DELETE", "/_license", params=params, headers=headers
        )

    @query_params(
        "accept_enterprise",
        "local",
        response_mimetypes=["application/json"],
    )
    async def get(self, params=None, headers=None):
        """
        Retrieves licensing information for the cluster

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-license.html>`_

        :arg accept_enterprise: If the active license is an enterprise
            license, return type as 'enterprise' (default: false)
        :arg local: Return local information, do not retrieve the state
            from master node (default: false)
        """
        return await self.transport.perform_request(
            "GET", "/_license", params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def get_basic_status(self, params=None, headers=None):
        """
        Retrieves information about the status of the basic license.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-basic-status.html>`_
        """
        return await self.transport.perform_request(
            "GET", "/_license/basic_status", params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def get_trial_status(self, params=None, headers=None):
        """
        Retrieves information about the status of the trial license.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-trial-status.html>`_
        """
        return await self.transport.perform_request(
            "GET", "/_license/trial_status", params=params, headers=headers
        )

    @query_params(
        "acknowledge",
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def post(self, body=None, params=None, headers=None):
        """
        Updates the license for the cluster.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/update-license.html>`_

        :arg body: licenses to be installed
        :arg acknowledge: whether the user has acknowledged acknowledge
            messages (default: false)
        """
        return await self.transport.perform_request(
            "PUT", "/_license", params=params, headers=headers, body=body
        )

    @query_params(
        "acknowledge",
        response_mimetypes=["application/json"],
    )
    async def post_start_basic(self, params=None, headers=None):
        """
        Starts an indefinite basic license.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/start-basic.html>`_

        :arg acknowledge: whether the user has acknowledged acknowledge
            messages (default: false)
        """
        return await self.transport.perform_request(
            "POST", "/_license/start_basic", params=params, headers=headers
        )

    @query_params(
        "acknowledge",
        "type",
        response_mimetypes=["application/json"],
    )
    async def post_start_trial(self, params=None, headers=None):
        """
        starts a limited time trial license.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/start-trial.html>`_

        :arg acknowledge: whether the user has acknowledged acknowledge
            messages (default: false)
        :arg type: The type of trial license to generate (default:
            "trial")
        """
        return await self.transport.perform_request(
            "POST", "/_license/start_trial", params=params, headers=headers
        )
