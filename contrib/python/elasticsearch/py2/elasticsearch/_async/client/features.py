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


class FeaturesClient(NamespacedClient):
    @query_params(
        "master_timeout",
        response_mimetypes=["application/json"],
    )
    async def get_features(self, params=None, headers=None):
        """
        Gets a list of features which can be included in snapshots using the
        feature_states field when creating a snapshot

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/get-features-api.html>`_

        :arg master_timeout: Explicit operation timeout for connection
            to master node
        """
        return await self.transport.perform_request(
            "GET", "/_features", params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    async def reset_features(self, params=None, headers=None):
        """
        Resets the internal state of features, usually by deleting system indices

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/modules-snapshots.html>`_

        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version
        """
        return await self.transport.perform_request(
            "POST", "/_features/_reset", params=params, headers=headers
        )
