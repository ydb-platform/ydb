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


class LogstashClient(NamespacedClient):
    @query_params(
        response_mimetypes=["application/json"],
    )
    def delete_pipeline(self, id, params=None, headers=None):
        """
        Deletes Logstash Pipelines used by Central Management

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/logstash-api-delete-pipeline.html>`_

        :arg id: The ID of the Pipeline
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "DELETE",
            _make_path("_logstash", "pipeline", id),
            params=params,
            headers=headers,
        )

    @query_params(
        response_mimetypes=["application/json"],
    )
    def get_pipeline(self, id, params=None, headers=None):
        """
        Retrieves Logstash Pipelines used by Central Management

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/logstash-api-get-pipeline.html>`_

        :arg id: A comma-separated list of Pipeline IDs
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "GET",
            _make_path("_logstash", "pipeline", id),
            params=params,
            headers=headers,
        )

    @query_params(
        request_mimetypes=["application/json"],
        response_mimetypes=["application/json"],
    )
    def put_pipeline(self, id, body, params=None, headers=None):
        """
        Adds and updates Logstash Pipelines used for Central Management

        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.17/logstash-api-put-pipeline.html>`_

        :arg id: The ID of the Pipeline
        :arg body: The Pipeline to add or update
        """
        for param in (id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_logstash", "pipeline", id),
            params=params,
            headers=headers,
            body=body,
        )
