# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#
# Modifications Copyright OpenSearch Contributors. See
# GitHub history for details.
#
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


from typing import Any

from .utils import NamespacedClient, query_params


class FeaturesClient(NamespacedClient):
    @query_params("master_timeout", "cluster_manager_timeout")
    async def get_features(self, params: Any = None, headers: Any = None) -> Any:
        """
        Gets a list of features which can be included in snapshots using the
        feature_states field when creating a snapshot


        :arg master_timeout (Deprecated: use cluster_manager_timeout): Explicit operation timeout for connection
            to master node
        :arg cluster_manager_timeout: Explicit operation timeout for connection
            to cluster_manager node
        """
        return await self.transport.perform_request(
            "GET", "/_features", params=params, headers=headers
        )

    @query_params()
    async def reset_features(self, params: Any = None, headers: Any = None) -> Any:
        """
        Resets the internal state of features, usually by deleting system indices


        .. warning::

            This API is **experimental** so may include breaking changes
            or be removed in a future version
        """
        return await self.transport.perform_request(
            "POST", "/_features/_reset", params=params, headers=headers
        )
