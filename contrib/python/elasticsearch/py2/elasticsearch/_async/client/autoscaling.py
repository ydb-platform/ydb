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


class AutoscalingClient(NamespacedClient):
    @query_params(
        response_mimetypes=["application/json"],
    )
    async def delete_autoscaling_policy(self, name, params=None, headers=None):
        """
        Deletes an autoscaling policy. Designed for indirect use by ECE/ESS and ECK.
        Direct use is not supported.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/autoscaling-delete-autoscaling-policy.html>`_

        :arg name: the name of the autoscaling policy
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return await self.transport.perform_request(
            "DELETE",
            _make_path("_autoscaling", "policy", name),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def get_autoscaling_policy(self, name, params=None, headers=None):
        """
        Retrieves an autoscaling policy. Designed for indirect use by ECE/ESS and ECK.
        Direct use is not supported.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/autoscaling-get-autoscaling-policy.html>`_

        :arg name: the name of the autoscaling policy
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'name'.")

        return await self.transport.perform_request(
            "GET",
            _make_path("_autoscaling", "policy", name),
            params=params,
            headers=headers,
        )

    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    async def put_autoscaling_policy(self, name, body, params=None, headers=None):
        """
        Creates a new autoscaling policy. Designed for indirect use by ECE/ESS and ECK.
        Direct use is not supported.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/autoscaling-put-autoscaling-policy.html>`_

        :arg name: the name of the autoscaling policy
        :arg body: the specification of the autoscaling policy
        """
        for param in (name, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return await self.transport.perform_request(
            "PUT",
            _make_path("_autoscaling", "policy", name),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def get_autoscaling_capacity(self, params=None, headers=None):
        """
        Gets the current autoscaling capacity based on the configured autoscaling
        policy. Designed for indirect use by ECE/ESS and ECK. Direct use is not
        supported.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/autoscaling-get-autoscaling-capacity.html>`_
        """
        return await self.transport.perform_request(
            "GET", "/_autoscaling/capacity", params=params, headers=headers
        )
