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


class MigrationClient(NamespacedClient):
    @query_params(
        response_mimetypes=["application/json"],
    )
    def deprecations(self, index=None, params=None, headers=None):
        """
        Retrieves information about different cluster, node, and index level settings
        that use deprecated features that will be removed or changed in the next major
        version.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/migration-api-deprecation.html>`_

        :arg index: Index pattern
        """
        return self.transport.perform_request(
            "GET",
            _make_path(index, "_migration", "deprecations"),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def get_feature_upgrade_status(self, params=None, headers=None):
        """
        Find out whether system features need to be upgraded or not

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/migration-api-feature-upgrade.html>`_
        """
        return self.transport.perform_request(
            "GET", "/_migration/system_features", params=params, headers=headers
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def post_feature_upgrade(self, params=None, headers=None):
        """
        Begin upgrades for system features

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/migration-api-feature-upgrade.html>`_
        """
        return self.transport.perform_request(
            "POST", "/_migration/system_features", params=params, headers=headers
        )
