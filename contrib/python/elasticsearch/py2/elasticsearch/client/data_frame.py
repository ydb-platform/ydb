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


class Data_FrameClient(NamespacedClient):
    @query_params()
    def delete_data_frame_transform(self, transform_id, params=None, headers=None):
        """
        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.x/delete-data-frame-transform.html>`_

        :arg transform_id: The id of the transform to delete
        """
        if transform_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'transform_id'."
            )
        return self.transport.perform_request(
            "DELETE",
            _make_path("_data_frame", "transforms", transform_id),
            params=params,
            headers=headers,
        )

    @query_params("from_", "size")
    def get_data_frame_transform(self, transform_id=None, params=None, headers=None):
        """
        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.x/get-data-frame-transform.html>`_

        :arg transform_id: The id or comma delimited list of id expressions of
            the transforms to get, '_all' or '*' implies get all transforms
        :arg from_: skips a number of transform configs, defaults to 0
        :arg size: specifies a max number of transforms to get, defaults to 100
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_data_frame", "transforms", transform_id),
            params=params,
            headers=headers,
        )

    @query_params()
    def get_data_frame_transform_stats(
        self, transform_id=None, params=None, headers=None
    ):
        """
        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.x/get-data-frame-transform-stats.html>`_

        :arg transform_id: The id of the transform for which to get stats.
            '_all' or '*' implies all transforms
        """
        return self.transport.perform_request(
            "GET",
            _make_path("_data_frame", "transforms", transform_id, "_stats"),
            params=params,
            headers=headers,
        )

    @query_params()
    def preview_data_frame_transform(self, body, params=None, headers=None):
        """
        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.x/preview-data-frame-transform.html>`_

        :arg body: The definition for the data_frame transform to preview
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")
        return self.transport.perform_request(
            "POST",
            "/_data_frame/transforms/_preview",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params()
    def put_data_frame_transform(self, transform_id, body, params=None, headers=None):
        """
        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.x/put-data-frame-transform.html>`_

        :arg transform_id: The id of the new transform.
        :arg body: The data frame transform definition
        """
        for param in (transform_id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")
        return self.transport.perform_request(
            "PUT",
            _make_path("_data_frame", "transforms", transform_id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("timeout")
    def start_data_frame_transform(self, transform_id, params=None, headers=None):
        """
        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.x/start-data-frame-transform.html>`_

        :arg transform_id: The id of the transform to start
        :arg timeout: Controls the time to wait for the transform to start
        """
        if transform_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'transform_id'."
            )
        return self.transport.perform_request(
            "POST",
            _make_path("_data_frame", "transforms", transform_id, "_start"),
            params=params,
            headers=headers,
        )

    @query_params("timeout", "wait_for_completion")
    def stop_data_frame_transform(self, transform_id, params=None, headers=None):
        """
        `<https://www.elastic.co/guide/en/elasticsearch/reference/7.x/stop-data-frame-transform.html>`_

        :arg transform_id: The id of the transform to stop
        :arg timeout: Controls the time to wait until the transform has stopped.
            Default to 30 seconds
        :arg wait_for_completion: Whether to wait for the transform to fully
            stop before returning or not. Default to false
        """
        if transform_id in SKIP_IN_PATH:
            raise ValueError(
                "Empty value passed for a required argument 'transform_id'."
            )
        return self.transport.perform_request(
            "POST",
            _make_path("_data_frame", "transforms", transform_id, "_stop"),
            params=params,
            headers=headers,
        )
