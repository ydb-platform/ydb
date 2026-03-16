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


class SynonymsClient(NamespacedClient):

    @_rewrite_parameters()
    def delete_synonym(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a synonym set.</p>
          <p>You can only delete a synonyms set that is not in use by any index analyzer.</p>
          <p>Synonyms sets can be used in synonym graph token filters and synonym token filters.
          These synonym filters can be used as part of search analyzers.</p>
          <p>Analyzers need to be loaded when an index is restored (such as when a node starts, or the index becomes open).
          Even if the analyzer is not used on any field mapping, it still needs to be loaded on the index recovery phase.</p>
          <p>If any analyzers cannot be loaded, the index becomes unavailable and the cluster status becomes red or yellow as index shards are not available.
          To prevent that, synonyms sets that are used in analyzers can't be deleted.
          A delete request in this case will return a 400 response code.</p>
          <p>To remove a synonyms set, you must first remove all indices that contain analyzers using it.
          You can migrate an index by creating a new index that does not contain the token filter with the synonyms set, and use the reindex API in order to copy over the index data.
          Once finished, you can delete the index.
          When the synonyms set is not used in analyzers, you will be able to delete it.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-delete-synonym>`_

        :param id: The synonyms set identifier to delete.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_synonyms/{__path_parts["id"]}'
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
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="synonyms.delete_synonym",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete_synonym_rule(
        self,
        *,
        set_id: str,
        rule_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a synonym rule.
          Delete a synonym rule from a synonym set.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-delete-synonym-rule>`_

        :param set_id: The ID of the synonym set to update.
        :param rule_id: The ID of the synonym rule to delete.
        :param refresh: If `true`, the request will refresh the analyzers with the deleted
            synonym rule and wait for the new synonyms to be available before returning.
            If `false`, analyzers will not be reloaded with the deleted synonym rule
        """
        if set_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'set_id'")
        if rule_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'rule_id'")
        __path_parts: t.Dict[str, str] = {
            "set_id": _quote(set_id),
            "rule_id": _quote(rule_id),
        }
        __path = f'/_synonyms/{__path_parts["set_id"]}/{__path_parts["rule_id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="synonyms.delete_synonym_rule",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={"from": "from_"},
    )
    def get_synonym(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        size: t.Optional[int] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a synonym set.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym>`_

        :param id: The synonyms set identifier to retrieve.
        :param from_: The starting offset for query rules to retrieve.
        :param size: The max number of query rules to retrieve.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_synonyms/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if from_ is not None:
            __query["from"] = from_
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if size is not None:
            __query["size"] = size
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="synonyms.get_synonym",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_synonym_rule(
        self,
        *,
        set_id: str,
        rule_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a synonym rule.
          Get a synonym rule from a synonym set.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym-rule>`_

        :param set_id: The ID of the synonym set to retrieve the synonym rule from.
        :param rule_id: The ID of the synonym rule to retrieve.
        """
        if set_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'set_id'")
        if rule_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'rule_id'")
        __path_parts: t.Dict[str, str] = {
            "set_id": _quote(set_id),
            "rule_id": _quote(rule_id),
        }
        __path = f'/_synonyms/{__path_parts["set_id"]}/{__path_parts["rule_id"]}'
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
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="synonyms.get_synonym_rule",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={"from": "from_"},
    )
    def get_synonyms_sets(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        size: t.Optional[int] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get all synonym sets.
          Get a summary of all defined synonym sets.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-get-synonym>`_

        :param from_: The starting offset for synonyms sets to retrieve.
        :param size: The maximum number of synonyms sets to retrieve.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_synonyms"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if from_ is not None:
            __query["from"] = from_
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if size is not None:
            __query["size"] = size
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="synonyms.get_synonyms_sets",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("synonyms_set",),
    )
    def put_synonym(
        self,
        *,
        id: str,
        synonyms_set: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update a synonym set.
          Synonyms sets are limited to a maximum of 10,000 synonym rules per set.
          If you need to manage more synonym rules, you can create multiple synonym sets.</p>
          <p>When an existing synonyms set is updated, the search analyzers that use the synonyms set are reloaded automatically for all indices.
          This is equivalent to invoking the reload search analyzers API for all indices that use the synonyms set.</p>
          <p>For practical examples of how to create or update a synonyms set, refer to the External documentation.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-put-synonym>`_

        :param id: The ID of the synonyms set to be created or updated.
        :param synonyms_set: The synonym rules definitions for the synonyms set.
        :param refresh: If `true`, the request will refresh the analyzers with the new
            synonyms set and wait for the new synonyms to be available before returning.
            If `false`, analyzers will not be reloaded with the new synonym set
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        if synonyms_set is None and body is None:
            raise ValueError("Empty value passed for parameter 'synonyms_set'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_synonyms/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if synonyms_set is not None:
                __body["synonyms_set"] = synonyms_set
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="synonyms.put_synonym",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("synonyms",),
    )
    def put_synonym_rule(
        self,
        *,
        set_id: str,
        rule_id: str,
        synonyms: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update a synonym rule.
          Create or update a synonym rule in a synonym set.</p>
          <p>If any of the synonym rules included is invalid, the API returns an error.</p>
          <p>When you update a synonym rule, all analyzers using the synonyms set will be reloaded automatically to reflect the new rule.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-synonyms-put-synonym-rule>`_

        :param set_id: The ID of the synonym set.
        :param rule_id: The ID of the synonym rule to be updated or created.
        :param synonyms: The synonym rule information definition, which must be in Solr
            format.
        :param refresh: If `true`, the request will refresh the analyzers with the new
            synonym rule and wait for the new synonyms to be available before returning.
            If `false`, analyzers will not be reloaded with the new synonym rule
        """
        if set_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'set_id'")
        if rule_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'rule_id'")
        if synonyms is None and body is None:
            raise ValueError("Empty value passed for parameter 'synonyms'")
        __path_parts: t.Dict[str, str] = {
            "set_id": _quote(set_id),
            "rule_id": _quote(rule_id),
        }
        __path = f'/_synonyms/{__path_parts["set_id"]}/{__path_parts["rule_id"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if not __body:
            if synonyms is not None:
                __body["synonyms"] = synonyms
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="synonyms.put_synonym_rule",
            path_parts=__path_parts,
        )
