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

from .utils import SKIP_IN_PATH, NamespacedClient, _bulk_body, _make_path, query_params


class MonitoringClient(NamespacedClient):
    @query_params(
        "interval",
        "system_api_version",
        "system_id",
        request_mimetypes=["application/x-ndjson"],
        response_mimetypes=["application/json"],
    )
    async def bulk(self, body, doc_type=None, params=None, headers=None):
        """
        Used by the monitoring features to send monitoring data.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/monitor-elasticsearch-cluster.html>`_

        :arg body: The operation definition and data (action-data
            pairs), separated by newlines
        :arg doc_type: Default document type for items which don't
            provide one
        :arg interval: Collection interval (e.g., '10s' or '10000ms') of
            the payload
        :arg system_api_version: API Version of the monitored system
        :arg system_id: Identifier of the monitored system
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        body = _bulk_body(self.transport.serializer, body)
        return await self.transport.perform_request(
            "POST",
            _make_path("_monitoring", doc_type, "bulk"),
            params=params,
            headers=headers,
            body=body,
        )
