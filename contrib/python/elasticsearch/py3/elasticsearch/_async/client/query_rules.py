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


class QueryRulesClient(NamespacedClient):

    @_rewrite_parameters()
    async def delete_rule(
        self,
        *,
        ruleset_id: str,
        rule_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a query rule.
          Delete a query rule within a query ruleset.
          This is a destructive action that is only recoverable by re-adding the same rule with the create or update query rule API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-delete-rule>`_

        :param ruleset_id: The unique identifier of the query ruleset containing the
            rule to delete
        :param rule_id: The unique identifier of the query rule within the specified
            ruleset to delete
        """
        if ruleset_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'ruleset_id'")
        if rule_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'rule_id'")
        __path_parts: t.Dict[str, str] = {
            "ruleset_id": _quote(ruleset_id),
            "rule_id": _quote(rule_id),
        }
        __path = f'/_query_rules/{__path_parts["ruleset_id"]}/_rule/{__path_parts["rule_id"]}'
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
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="query_rules.delete_rule",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def delete_ruleset(
        self,
        *,
        ruleset_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a query ruleset.
          Remove a query ruleset and its associated data.
          This is a destructive action that is not recoverable.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-delete-ruleset>`_

        :param ruleset_id: The unique identifier of the query ruleset to delete
        """
        if ruleset_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'ruleset_id'")
        __path_parts: t.Dict[str, str] = {"ruleset_id": _quote(ruleset_id)}
        __path = f'/_query_rules/{__path_parts["ruleset_id"]}'
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
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="query_rules.delete_ruleset",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_rule(
        self,
        *,
        ruleset_id: str,
        rule_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a query rule.
          Get details about a query rule within a query ruleset.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-get-rule>`_

        :param ruleset_id: The unique identifier of the query ruleset containing the
            rule to retrieve
        :param rule_id: The unique identifier of the query rule within the specified
            ruleset to retrieve
        """
        if ruleset_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'ruleset_id'")
        if rule_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'rule_id'")
        __path_parts: t.Dict[str, str] = {
            "ruleset_id": _quote(ruleset_id),
            "rule_id": _quote(rule_id),
        }
        __path = f'/_query_rules/{__path_parts["ruleset_id"]}/_rule/{__path_parts["rule_id"]}'
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
            endpoint_id="query_rules.get_rule",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    async def get_ruleset(
        self,
        *,
        ruleset_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a query ruleset.
          Get details about a query ruleset.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-get-ruleset>`_

        :param ruleset_id: The unique identifier of the query ruleset
        """
        if ruleset_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'ruleset_id'")
        __path_parts: t.Dict[str, str] = {"ruleset_id": _quote(ruleset_id)}
        __path = f'/_query_rules/{__path_parts["ruleset_id"]}'
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
            endpoint_id="query_rules.get_ruleset",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={"from": "from_"},
    )
    async def list_rulesets(
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

          <p>Get all query rulesets.
          Get summarized information about the query rulesets.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-list-rulesets>`_

        :param from_: The offset from the first result to fetch.
        :param size: The maximum number of results to retrieve.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_query_rules"
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
        return await self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="query_rules.list_rulesets",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("actions", "criteria", "type", "priority"),
    )
    async def put_rule(
        self,
        *,
        ruleset_id: str,
        rule_id: str,
        actions: t.Optional[t.Mapping[str, t.Any]] = None,
        criteria: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        type: t.Optional[t.Union[str, t.Literal["exclude", "pinned"]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        priority: t.Optional[int] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update a query rule.
          Create or update a query rule within a query ruleset.</p>
          <p>IMPORTANT: Due to limitations within pinned queries, you can only pin documents using ids or docs, but cannot use both in single rule.
          It is advised to use one or the other in query rulesets, to avoid errors.
          Additionally, pinned queries have a maximum limit of 100 pinned hits.
          If multiple matching rules pin more than 100 documents, only the first 100 documents are pinned in the order they are specified in the ruleset.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-put-rule>`_

        :param ruleset_id: The unique identifier of the query ruleset containing the
            rule to be created or updated.
        :param rule_id: The unique identifier of the query rule within the specified
            ruleset to be created or updated.
        :param actions: The actions to take when the rule is matched. The format of this
            action depends on the rule type.
        :param criteria: The criteria that must be met for the rule to be applied. If
            multiple criteria are specified for a rule, all criteria must be met for
            the rule to be applied.
        :param type: The type of rule.
        :param priority:
        """
        if ruleset_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'ruleset_id'")
        if rule_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'rule_id'")
        if actions is None and body is None:
            raise ValueError("Empty value passed for parameter 'actions'")
        if criteria is None and body is None:
            raise ValueError("Empty value passed for parameter 'criteria'")
        if type is None and body is None:
            raise ValueError("Empty value passed for parameter 'type'")
        __path_parts: t.Dict[str, str] = {
            "ruleset_id": _quote(ruleset_id),
            "rule_id": _quote(rule_id),
        }
        __path = f'/_query_rules/{__path_parts["ruleset_id"]}/_rule/{__path_parts["rule_id"]}'
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
        if not __body:
            if actions is not None:
                __body["actions"] = actions
            if criteria is not None:
                __body["criteria"] = criteria
            if type is not None:
                __body["type"] = type
            if priority is not None:
                __body["priority"] = priority
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="query_rules.put_rule",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("rules",),
    )
    async def put_ruleset(
        self,
        *,
        ruleset_id: str,
        rules: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update a query ruleset.
          There is a limit of 100 rules per ruleset.
          This limit can be increased by using the <code>xpack.applications.rules.max_rules_per_ruleset</code> cluster setting.</p>
          <p>IMPORTANT: Due to limitations within pinned queries, you can only select documents using <code>ids</code> or <code>docs</code>, but cannot use both in single rule.
          It is advised to use one or the other in query rulesets, to avoid errors.
          Additionally, pinned queries have a maximum limit of 100 pinned hits.
          If multiple matching rules pin more than 100 documents, only the first 100 documents are pinned in the order they are specified in the ruleset.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-put-ruleset>`_

        :param ruleset_id: The unique identifier of the query ruleset to be created or
            updated.
        :param rules:
        """
        if ruleset_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'ruleset_id'")
        if rules is None and body is None:
            raise ValueError("Empty value passed for parameter 'rules'")
        __path_parts: t.Dict[str, str] = {"ruleset_id": _quote(ruleset_id)}
        __path = f'/_query_rules/{__path_parts["ruleset_id"]}'
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
        if not __body:
            if rules is not None:
                __body["rules"] = rules
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="query_rules.put_ruleset",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("match_criteria",),
    )
    async def test(
        self,
        *,
        ruleset_id: str,
        match_criteria: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Test a query ruleset.
          Evaluate match criteria against a query ruleset to identify the rules that would match that criteria.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-query-rules-test>`_

        :param ruleset_id: The unique identifier of the query ruleset to be created or
            updated
        :param match_criteria: The match criteria to apply to rules in the given query
            ruleset. Match criteria should match the keys defined in the `criteria.metadata`
            field of the rule.
        """
        if ruleset_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'ruleset_id'")
        if match_criteria is None and body is None:
            raise ValueError("Empty value passed for parameter 'match_criteria'")
        __path_parts: t.Dict[str, str] = {"ruleset_id": _quote(ruleset_id)}
        __path = f'/_query_rules/{__path_parts["ruleset_id"]}/_test'
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
        if not __body:
            if match_criteria is not None:
                __body["match_criteria"] = match_criteria
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return await self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="query_rules.test",
            path_parts=__path_parts,
        )
