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

import typing as t

from elastic_transport import ObjectApiResponse

from ._base import NamespacedClient
from .utils import SKIP_IN_PATH, _quote, _rewrite_parameters


class MigrationClient(NamespacedClient):

    @_rewrite_parameters()
    async def deprecations(
        self,
        *,
        index: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get deprecation information.
          Get information about different cluster, node, and index level settings that use deprecated features that will be removed or changed in the next major version.</p>
          <p>TIP: This APIs is designed for indirect use by the Upgrade Assistant.
          You are strongly recommended to use the Upgrade Assistant.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-deprecations>`_

        :param index: Comma-separate list of data streams or indices to check. Wildcard
            (*) expressions are supported.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_migration/deprecations'
        else:
            __path_parts = {}
            __path = "/_migration/deprecations"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="migration.deprecations",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_feature_upgrade_status(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get feature migration information.
          Version upgrades sometimes require changes to how features store configuration information and data in system indices.
          Check which features need to be migrated and the status of any migrations that are in progress.</p>
          <p>TIP: This API is designed for indirect use by the Upgrade Assistant.
          You are strongly recommended to use the Upgrade Assistant.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-get-feature-upgrade-status>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_migration/system_features"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="migration.get_feature_upgrade_status",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def post_feature_upgrade(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Start the feature migration.
          Version upgrades sometimes require changes to how features store configuration information and data in system indices.
          This API starts the automatic migration process.</p>
          <p>Some functionality might be temporarily unavailable during the migration process.</p>
          <p>TIP: The API is designed for indirect use by the Upgrade Assistant. We strongly recommend you use the Upgrade Assistant.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-migration-get-feature-upgrade-status>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_migration/system_features"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="migration.post_feature_upgrade",
            path_parts=__path_parts,
        )
