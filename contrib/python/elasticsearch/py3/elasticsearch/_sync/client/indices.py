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

from elastic_transport import HeadApiResponse, ObjectApiResponse

from ._base import NamespacedClient
from .utils import (
    SKIP_IN_PATH,
    Stability,
    _quote,
    _rewrite_parameters,
    _stability_warning,
)


class IndicesClient(NamespacedClient):

    @_rewrite_parameters()
    def add_block(
        self,
        *,
        index: str,
        block: t.Union[str, t.Literal["metadata", "read", "read_only", "write"]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Add an index block.</p>
          <p>Add an index block to an index.
          Index blocks limit the operations allowed on an index by blocking specific operation types.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-add-block>`_

        :param index: A comma-separated list or wildcard expression of index names used
            to limit the request. By default, you must explicitly name the indices you
            are adding blocks to. To allow the adding of blocks to indices with `_all`,
            `*`, or other wildcard expressions, change the `action.destructive_requires_name`
            setting to `false`. You can update this setting in the `elasticsearch.yml`
            file or by using the cluster update settings API.
        :param block: The block type to add to the index.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param expand_wildcards: The type of index that wildcard patterns can match.
            If the request can target data streams, this argument determines whether
            wildcard expressions match hidden data streams. It supports comma-separated
            values, such as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param master_timeout: The period to wait for the master node. If the master
            node is not available before the timeout expires, the request fails and returns
            an error. It can also be set to `-1` to indicate that the request should
            never timeout.
        :param timeout: The period to wait for a response from all relevant nodes in
            the cluster after updating the cluster metadata. If no response is received
            before the timeout expires, the cluster metadata update still applies but
            the response will indicate that it was not completely acknowledged. It can
            also be set to `-1` to indicate that the request should never timeout.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if block in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'block'")
        __path_parts: t.Dict[str, str] = {
            "index": _quote(index),
            "block": _quote(block),
        }
        __path = f'/{__path_parts["index"]}/_block/{__path_parts["block"]}'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.add_block",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "analyzer",
            "attributes",
            "char_filter",
            "explain",
            "field",
            "filter",
            "normalizer",
            "text",
            "tokenizer",
        ),
    )
    def analyze(
        self,
        *,
        index: t.Optional[str] = None,
        analyzer: t.Optional[str] = None,
        attributes: t.Optional[t.Sequence[str]] = None,
        char_filter: t.Optional[t.Sequence[t.Union[str, t.Mapping[str, t.Any]]]] = None,
        error_trace: t.Optional[bool] = None,
        explain: t.Optional[bool] = None,
        field: t.Optional[str] = None,
        filter: t.Optional[t.Sequence[t.Union[str, t.Mapping[str, t.Any]]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        normalizer: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        text: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        tokenizer: t.Optional[t.Union[str, t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get tokens from text analysis.
          The analyze API performs analysis on a text string and returns the resulting tokens.</p>
          <p>Generating excessive amount of tokens may cause a node to run out of memory.
          The <code>index.analyze.max_token_count</code> setting enables you to limit the number of tokens that can be produced.
          If more than this limit of tokens gets generated, an error occurs.
          The <code>_analyze</code> endpoint without a specified index will always use <code>10000</code> as its limit.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-analyze>`_

        :param index: Index used to derive the analyzer. If specified, the `analyzer`
            or field parameter overrides this value. If no index is specified or the
            index does not have a default analyzer, the analyze API uses the standard
            analyzer.
        :param analyzer: The name of the analyzer that should be applied to the provided
            `text`. This could be a built-in analyzer, or an analyzer that’s been configured
            in the index.
        :param attributes: Array of token attributes used to filter the output of the
            `explain` parameter.
        :param char_filter: Array of character filters used to preprocess characters
            before the tokenizer.
        :param explain: If `true`, the response includes token attributes and additional
            details.
        :param field: Field used to derive the analyzer. To use this parameter, you must
            specify an index. If specified, the `analyzer` parameter overrides this value.
        :param filter: Array of token filters used to apply after the tokenizer.
        :param normalizer: Normalizer to use to convert text into a single token.
        :param text: Text to analyze. If an array of strings is provided, it is analyzed
            as a multi-value field.
        :param tokenizer: Tokenizer to use to convert text into tokens.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_analyze'
        else:
            __path_parts = {}
            __path = "/_analyze"
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
            if analyzer is not None:
                __body["analyzer"] = analyzer
            if attributes is not None:
                __body["attributes"] = attributes
            if char_filter is not None:
                __body["char_filter"] = char_filter
            if explain is not None:
                __body["explain"] = explain
            if field is not None:
                __body["field"] = field
            if filter is not None:
                __body["filter"] = filter
            if normalizer is not None:
                __body["normalizer"] = normalizer
            if text is not None:
                __body["text"] = text
            if tokenizer is not None:
                __body["tokenizer"] = tokenizer
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.analyze",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    def cancel_migrate_reindex(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Cancel a migration reindex operation.</p>
          <p>Cancel a migration reindex attempt for a data stream or index.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-cancel-migrate-reindex>`_

        :param index: The index or data stream name
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/_migration/reindex/{__path_parts["index"]}/_cancel'
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
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.cancel_migrate_reindex",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def clear_cache(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        fielddata: t.Optional[bool] = None,
        fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[bool] = None,
        request: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Clear the cache.
          Clear the cache of one or more indices.
          For data streams, the API clears the caches of the stream's backing indices.</p>
          <p>By default, the clear cache API clears all caches.
          To clear only specific caches, use the <code>fielddata</code>, <code>query</code>, or <code>request</code> parameters.
          To clear the cache only of specific fields, use the <code>fields</code> parameter.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-clear-cache>`_

        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (`*`). To target all data streams
            and indices, omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param fielddata: If `true`, clears the fields cache. Use the `fields` parameter
            to clear the cache of specific fields only.
        :param fields: Comma-separated list of field names used to limit the `fielddata`
            parameter.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param query: If `true`, clears the query cache.
        :param request: If `true`, clears the request cache.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_cache/clear'
        else:
            __path_parts = {}
            __path = "/_cache/clear"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if fielddata is not None:
            __query["fielddata"] = fielddata
        if fields is not None:
            __query["fields"] = fields
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        if query is not None:
            __query["query"] = query
        if request is not None:
            __query["request"] = request
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.clear_cache",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("aliases", "settings"),
    )
    def clone(
        self,
        *,
        index: str,
        target: str,
        aliases: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Clone an index.
          Clone an existing index into a new index.
          Each original primary shard is cloned into a new primary shard in the new index.</p>
          <p>IMPORTANT: Elasticsearch does not apply index templates to the resulting index.
          The API also does not copy index metadata from the original index.
          Index metadata includes aliases, index lifecycle management phase definitions, and cross-cluster replication (CCR) follower information.
          For example, if you clone a CCR follower index, the resulting clone will not be a follower index.</p>
          <p>The clone API copies most index settings from the source index to the resulting index, with the exception of <code>index.number_of_replicas</code> and <code>index.auto_expand_replicas</code>.
          To set the number of replicas in the resulting index, configure these settings in the clone request.</p>
          <p>Cloning works as follows:</p>
          <ul>
          <li>First, it creates a new target index with the same definition as the source index.</li>
          <li>Then it hard-links segments from the source index into the target index. If the file system does not support hard-linking, all segments are copied into the new index, which is a much more time consuming process.</li>
          <li>Finally, it recovers the target index as though it were a closed index which had just been re-opened.</li>
          </ul>
          <p>IMPORTANT: Indices can only be cloned if they meet the following requirements:</p>
          <ul>
          <li>The index must be marked as read-only and have a cluster health status of green.</li>
          <li>The target index must not exist.</li>
          <li>The source index must have the same number of primary shards as the target index.</li>
          <li>The node handling the clone process must have sufficient free disk space to accommodate a second copy of the existing index.</li>
          </ul>
          <p>The current write index on a data stream cannot be cloned.
          In order to clone the current write index, the data stream must first be rolled over so that a new write index is created and then the previous write index can be cloned.</p>
          <p>NOTE: Mappings cannot be specified in the <code>_clone</code> request. The mappings of the source index will be used for the target index.</p>
          <p><strong>Monitor the cloning process</strong></p>
          <p>The cloning process can be monitored with the cat recovery API or the cluster health API can be used to wait until all primary shards have been allocated by setting the <code>wait_for_status</code> parameter to <code>yellow</code>.</p>
          <p>The <code>_clone</code> API returns as soon as the target index has been added to the cluster state, before any shards have been allocated.
          At this point, all shards are in the state unassigned.
          If, for any reason, the target index can't be allocated, its primary shard will remain unassigned until it can be allocated on that node.</p>
          <p>Once the primary shard is allocated, it moves to state initializing, and the clone process begins.
          When the clone operation completes, the shard will become active.
          At that point, Elasticsearch will try to allocate any replicas and may decide to relocate the primary shard to another node.</p>
          <p><strong>Wait for active shards</strong></p>
          <p>Because the clone operation creates a new index to clone the shards to, the wait for active shards setting on index creation applies to the clone index action as well.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-clone>`_

        :param index: Name of the source index to clone.
        :param target: Name of the target index to create.
        :param aliases: Aliases for the resulting index.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param settings: Configuration options for the target index.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`).
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if target in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'target'")
        __path_parts: t.Dict[str, str] = {
            "index": _quote(index),
            "target": _quote(target),
        }
        __path = f'/{__path_parts["index"]}/_clone/{__path_parts["target"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        if not __body:
            if aliases is not None:
                __body["aliases"] = aliases
            if settings is not None:
                __body["settings"] = settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.clone",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def close(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Close an index.
          A closed index is blocked for read or write operations and does not allow all operations that opened indices allow.
          It is not possible to index documents or to search for documents in a closed index.
          Closed indices do not have to maintain internal data structures for indexing or searching documents, which results in a smaller overhead on the cluster.</p>
          <p>When opening or closing an index, the master node is responsible for restarting the index shards to reflect the new state of the index.
          The shards will then go through the normal recovery process.
          The data of opened and closed indices is automatically replicated by the cluster to ensure that enough shard copies are safely kept around at all times.</p>
          <p>You can open and close multiple indices.
          An error is thrown if the request explicitly refers to a missing index.
          This behaviour can be turned off using the <code>ignore_unavailable=true</code> parameter.</p>
          <p>By default, you must explicitly name the indices you are opening or closing.
          To open or close indices with <code>_all</code>, <code>*</code>, or other wildcard expressions, change the<code> action.destructive_requires_name</code> setting to <code>false</code>. This setting can also be changed with the cluster update settings API.</p>
          <p>Closed indices consume a significant amount of disk-space which can cause problems in managed environments.
          Closing indices can be turned off with the cluster settings API by setting <code>cluster.indices.close.enable</code> to <code>false</code>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-close>`_

        :param index: Comma-separated list or wildcard expression of index names used
            to limit the request.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`).
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_close'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.close",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("aliases", "mappings", "settings"),
    )
    def create(
        self,
        *,
        index: str,
        aliases: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        mappings: t.Optional[t.Mapping[str, t.Any]] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an index.
          You can use the create index API to add a new index to an Elasticsearch cluster.
          When creating an index, you can specify the following:</p>
          <ul>
          <li>Settings for the index.</li>
          <li>Mappings for fields in the index.</li>
          <li>Index aliases</li>
          </ul>
          <p><strong>Wait for active shards</strong></p>
          <p>By default, index creation will only return a response to the client when the primary copies of each shard have been started, or the request times out.
          The index creation response will indicate what happened.
          For example, <code>acknowledged</code> indicates whether the index was successfully created in the cluster, <code>while shards_acknowledged</code> indicates whether the requisite number of shard copies were started for each shard in the index before timing out.
          Note that it is still possible for either <code>acknowledged</code> or <code>shards_acknowledged</code> to be <code>false</code>, but for the index creation to be successful.
          These values simply indicate whether the operation completed before the timeout.
          If <code>acknowledged</code> is false, the request timed out before the cluster state was updated with the newly created index, but it probably will be created sometime soon.
          If <code>shards_acknowledged</code> is false, then the request timed out before the requisite number of shards were started (by default just the primaries), even if the cluster state was successfully updated to reflect the newly created index (that is to say, <code>acknowledged</code> is <code>true</code>).</p>
          <p>You can change the default of only waiting for the primary shards to start through the index setting <code>index.write.wait_for_active_shards</code>.
          Note that changing this setting will also affect the <code>wait_for_active_shards</code> value on all subsequent write operations.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create>`_

        :param index: Name of the index you wish to create. Index names must meet the
            following criteria: * Lowercase only * Cannot include `\\`, `/`, `*`, `?`,
            `"`, `<`, `>`, `|`, ` ` (space character), `,`, or `#` * Indices prior to
            7.0 could contain a colon (`:`), but that has been deprecated and will not
            be supported in later versions * Cannot start with `-`, `_`, or `+` * Cannot
            be `.` or `..` * Cannot be longer than 255 bytes (note thtat it is bytes,
            so multi-byte characters will reach the limit faster) * Names starting with
            `.` are deprecated, except for hidden indices and internal indices managed
            by plugins
        :param aliases: Aliases for the index.
        :param mappings: Mapping for fields in the index. If specified, this mapping
            can include: - Field names - Field data types - Mapping parameters
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param settings: Configuration options for the index.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`).
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        if not __body:
            if aliases is not None:
                __body["aliases"] = aliases
            if mappings is not None:
                __body["mappings"] = mappings
            if settings is not None:
                __body["settings"] = settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.create",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def create_data_stream(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a data stream.</p>
          <p>You must have a matching index template with data stream enabled.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create-data-stream>`_

        :param name: Name of the data stream, which must meet the following criteria:
            Lowercase only; Cannot include `\\`, `/`, `*`, `?`, `"`, `<`, `>`, `|`, `,`,
            `#`, `:`, or a space character; Cannot start with `-`, `_`, `+`, or `.ds-`;
            Cannot be `.` or `..`; Cannot be longer than 255 bytes. Multi-byte characters
            count towards this limit faster.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.create_data_stream",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="create_from",
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    def create_from(
        self,
        *,
        source: str,
        dest: str,
        create_from: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create an index from a source index.</p>
          <p>Copy the mappings and settings from the source index to a destination index while allowing request settings and mappings to override the source values.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-create-from>`_

        :param source: The source index or data stream name
        :param dest: The destination index or data stream name
        :param create_from:
        """
        if source in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'source'")
        if dest in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'dest'")
        if create_from is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'create_from' and 'body', one of them should be set."
            )
        elif create_from is not None and body is not None:
            raise ValueError("Cannot set both 'create_from' and 'body'")
        __path_parts: t.Dict[str, str] = {
            "source": _quote(source),
            "dest": _quote(dest),
        }
        __path = f'/_create_from/{__path_parts["source"]}/{__path_parts["dest"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __body = create_from if create_from is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.create_from",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def data_streams_stats(
        self,
        *,
        name: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get data stream stats.</p>
          <p>Get statistics for one or more data streams.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-data-streams-stats-1>`_

        :param name: Comma-separated list of data streams used to limit the request.
            Wildcard expressions (`*`) are supported. To target all data streams in a
            cluster, omit this parameter or use `*`.
        :param expand_wildcards: Type of data stream that wildcard patterns can match.
            Supports comma-separated values, such as `open,hidden`.
        """
        __path_parts: t.Dict[str, str]
        if name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_data_stream/{__path_parts["name"]}/_stats'
        else:
            __path_parts = {}
            __path = "/_data_stream/_stats"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
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
            endpoint_id="indices.data_streams_stats",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete indices.
          Deleting an index deletes its documents, shards, and metadata.
          It does not delete related Kibana components, such as data views, visualizations, or dashboards.</p>
          <p>You cannot delete the current write index of a data stream.
          To delete the index, you must roll over the data stream so a new write index is created.
          You can then use the delete index API to delete the previous write index.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete>`_

        :param index: Comma-separated list of indices to delete. You cannot specify index
            aliases. By default, this parameter does not support wildcards (`*`) or `_all`.
            To use wildcards or `_all`, set the `action.destructive_requires_name` cluster
            setting to `false`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.delete",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete_alias(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete an alias.
          Removes a data stream or index from an alias.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-alias>`_

        :param index: Comma-separated list of data streams or indices used to limit the
            request. Supports wildcards (`*`).
        :param name: Comma-separated list of aliases to remove. Supports wildcards (`*`).
            To remove all aliases, use `*` or `_all`.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "name": _quote(name)}
        __path = f'/{__path_parts["index"]}/_alias/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.delete_alias",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete_data_lifecycle(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete data stream lifecycles.
          Removes the data stream lifecycle from a data stream, rendering it not managed by the data stream lifecycle.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-data-lifecycle>`_

        :param name: A comma-separated list of data streams of which the data stream
            lifecycle will be deleted; use `*` to get all data streams
        :param expand_wildcards: Whether wildcard expressions should get expanded to
            open or closed indices (default: open)
        :param master_timeout: Specify timeout for connection to master
        :param timeout: Explicit timestamp for the document
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}/_lifecycle'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.delete_data_lifecycle",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete_data_stream(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete data streams.
          Deletes one or more data streams and their backing indices.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-data-stream>`_

        :param name: Comma-separated list of data streams to delete. Wildcard (`*`) expressions
            are supported.
        :param expand_wildcards: Type of data stream that wildcard patterns can match.
            Supports comma-separated values,such as `open,hidden`.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.delete_data_stream",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete_data_stream_options(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete data stream options.
          Removes the data stream options from a data stream.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-data-stream-options>`_

        :param name: A comma-separated list of data streams of which the data stream
            options will be deleted; use `*` to get all data streams
        :param expand_wildcards: Whether wildcard expressions should get expanded to
            open or closed indices (default: open)
        :param master_timeout: Specify timeout for connection to master
        :param timeout: Explicit timestamp for the document
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}/_options'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.delete_data_stream_options",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete_index_template(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete an index template.
          The provided <!-- raw HTML omitted --> may contain multiple template names separated by a comma. If multiple template
          names are specified then there is no wildcard support and the provided names should match completely with
          existing templates.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-index-template>`_

        :param name: Comma-separated list of index template names used to limit the request.
            Wildcard (*) expressions are supported.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_index_template/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.delete_index_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete_template(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a legacy index template.
          IMPORTANT: This documentation is about legacy index templates, which are deprecated and will be replaced by the composable templates introduced in Elasticsearch 7.8.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-delete-template>`_

        :param name: The name of the legacy index template to delete. Wildcard (`*`)
            expressions are supported.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_template/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.delete_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    def disk_usage(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flush: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        run_expensive_tasks: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Analyze the index disk usage.
          Analyze the disk usage of each field of an index or data stream.
          This API might not support indices created in previous Elasticsearch versions.
          The result of a small index can be inaccurate as some parts of an index might not be analyzed by the API.</p>
          <p>NOTE: The total size of fields of the analyzed shards of the index in the response is usually smaller than the index <code>store_size</code> value because some small metadata files are ignored and some parts of data files might not be scanned by the API.
          Since stored fields are stored together in a compressed format, the sizes of stored fields are also estimates and can be inaccurate.
          The stored size of the <code>_id</code> field is likely underestimated while the <code>_source</code> field is overestimated.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-disk-usage>`_

        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. It’s recommended to execute this API with a single
            index (or the latest backing index of a data stream) as the API consumes
            resources significantly.
        :param allow_no_indices: If false, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param flush: If `true`, the API performs a flush before analysis. If `false`,
            the response may not include uncommitted data.
        :param ignore_unavailable: If `true`, missing or closed indices are not included
            in the response.
        :param run_expensive_tasks: Analyzing field disk usage is resource-intensive.
            To use the API, this parameter must be set to `true`.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_disk_usage'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flush is not None:
            __query["flush"] = flush
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        if run_expensive_tasks is not None:
            __query["run_expensive_tasks"] = run_expensive_tasks
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.disk_usage",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="config",
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    def downsample(
        self,
        *,
        index: str,
        target_index: str,
        config: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Downsample an index.
          Aggregate a time series (TSDS) index and store pre-computed statistical summaries (<code>min</code>, <code>max</code>, <code>sum</code>, <code>value_count</code> and <code>avg</code>) for each metric field grouped by a configured time interval.
          For example, a TSDS index that contains metrics sampled every 10 seconds can be downsampled to an hourly index.
          All documents within an hour interval are summarized and stored as a single document in the downsample index.</p>
          <p>NOTE: Only indices in a time series data stream are supported.
          Neither field nor document level security can be defined on the source index.
          The source index must be read only (<code>index.blocks.write: true</code>).</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-downsample>`_

        :param index: Name of the time series index to downsample.
        :param target_index: Name of the index to create.
        :param config:
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if target_index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'target_index'")
        if config is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'config' and 'body', one of them should be set."
            )
        elif config is not None and body is not None:
            raise ValueError("Cannot set both 'config' and 'body'")
        __path_parts: t.Dict[str, str] = {
            "index": _quote(index),
            "target_index": _quote(target_index),
        }
        __path = f'/{__path_parts["index"]}/_downsample/{__path_parts["target_index"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __body = config if config is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.downsample",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def exists(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flat_settings: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        include_defaults: t.Optional[bool] = None,
        local: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> HeadApiResponse:
        """
        .. raw:: html

          <p>Check indices.
          Check if one or more indices, index aliases, or data streams exist.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-exists>`_

        :param index: Comma-separated list of data streams, indices, and aliases. Supports
            wildcards (`*`).
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param flat_settings: If `true`, returns settings in flat format.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param include_defaults: If `true`, return all default settings in the response.
        :param local: If `true`, the request retrieves information from the local node
            only.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flat_settings is not None:
            __query["flat_settings"] = flat_settings
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if local is not None:
            __query["local"] = local
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "HEAD",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.exists",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def exists_alias(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> HeadApiResponse:
        """
        .. raw:: html

          <p>Check aliases.</p>
          <p>Check if one or more data stream or index aliases exist.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-exists-alias>`_

        :param name: Comma-separated list of aliases to check. Supports wildcards (`*`).
        :param index: Comma-separated list of data streams or indices used to limit the
            request. Supports wildcards (`*`). To target all data streams and indices,
            omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, requests that include a missing data stream
            or index in the target indices or data streams return an error.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH and name not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index), "name": _quote(name)}
            __path = f'/{__path_parts["index"]}/_alias/{__path_parts["name"]}'
        elif name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_alias/{__path_parts["name"]}'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "HEAD",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.exists_alias",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def exists_index_template(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flat_settings: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        local: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> HeadApiResponse:
        """
        .. raw:: html

          <p>Check index templates.</p>
          <p>Check whether index templates exist.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-exists-index-template>`_

        :param name: Comma-separated list of index template names used to limit the request.
            Wildcard (*) expressions are supported.
        :param flat_settings: If true, returns settings in flat format.
        :param local: If true, the request retrieves information from the local node
            only. Defaults to false, which means information is retrieved from the master
            node.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_index_template/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flat_settings is not None:
            __query["flat_settings"] = flat_settings
        if human is not None:
            __query["human"] = human
        if local is not None:
            __query["local"] = local
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "HEAD",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.exists_index_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def exists_template(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flat_settings: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        local: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> HeadApiResponse:
        """
        .. raw:: html

          <p>Check existence of index templates.
          Get information about whether index templates exist.
          Index templates define settings, mappings, and aliases that can be applied automatically to new indices.</p>
          <p>IMPORTANT: This documentation is about legacy index templates, which are deprecated and will be replaced by the composable templates introduced in Elasticsearch 7.8.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-exists-template>`_

        :param name: A comma-separated list of index template names used to limit the
            request. Wildcard (`*`) expressions are supported.
        :param flat_settings: Indicates whether to use a flat format for the response.
        :param local: Indicates whether to get information from the local node only.
        :param master_timeout: The period to wait for the master node. If the master
            node is not available before the timeout expires, the request fails and returns
            an error. To indicate that the request should never timeout, set it to `-1`.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_template/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flat_settings is not None:
            __query["flat_settings"] = flat_settings
        if human is not None:
            __query["human"] = human
        if local is not None:
            __query["local"] = local
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "HEAD",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.exists_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def explain_data_lifecycle(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        include_defaults: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get the status for a data stream lifecycle.
          Get information about an index or data stream's current data stream lifecycle status, such as time since index creation, time since rollover, the lifecycle configuration managing the index, or any errors encountered during lifecycle execution.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-explain-data-lifecycle>`_

        :param index: The name of the index to explain
        :param include_defaults: indicates if the API should return the default values
            the system uses for the index's lifecycle
        :param master_timeout: Specify timeout for connection to master
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_lifecycle/explain'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.explain_data_lifecycle",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    def field_usage_stats(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get field usage stats.
          Get field usage information for each shard and field of an index.
          Field usage statistics are automatically captured when queries are running on a cluster.
          A shard-level search request that accesses a given field, even if multiple times during that request, is counted as a single use.</p>
          <p>The response body reports the per-shard usage count of the data structures that back the fields in the index.
          A given request will increment each count by a maximum value of 1, even if the request accesses the same field multiple times.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-field-usage-stats>`_

        :param index: Comma-separated list or wildcard expression of index names used
            to limit the request.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param fields: Comma-separated list or wildcard expressions of fields to include
            in the statistics.
        :param ignore_unavailable: If `true`, missing or closed indices are not included
            in the response.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_field_usage_stats'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if fields is not None:
            __query["fields"] = fields
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.field_usage_stats",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def flush(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        force: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        wait_if_ongoing: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Flush data streams or indices.
          Flushing a data stream or index is the process of making sure that any data that is currently only stored in the transaction log is also permanently stored in the Lucene index.
          When restarting, Elasticsearch replays any unflushed operations from the transaction log into the Lucene index to bring it back into the state that it was in before the restart.
          Elasticsearch automatically triggers flushes as needed, using heuristics that trade off the size of the unflushed transaction log against the cost of performing each flush.</p>
          <p>After each operation has been flushed it is permanently stored in the Lucene index.
          This may mean that there is no need to maintain an additional copy of it in the transaction log.
          The transaction log is made up of multiple files, called generations, and Elasticsearch will delete any generation files when they are no longer needed, freeing up disk space.</p>
          <p>It is also possible to trigger a flush on one or more indices using the flush API, although it is rare for users to need to call this API directly.
          If you call the flush API after indexing some documents then a successful response indicates that Elasticsearch has flushed all the documents that were indexed before the flush API was called.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-flush>`_

        :param index: Comma-separated list of data streams, indices, and aliases to flush.
            Supports wildcards (`*`). To flush all data streams and indices, omit this
            parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param force: If `true`, the request forces a flush even if there are no changes
            to commit to the index.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param wait_if_ongoing: If `true`, the flush operation blocks until execution
            when another flush operation is running. If `false`, Elasticsearch returns
            an error if you request a flush when another flush operation is running.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_flush'
        else:
            __path_parts = {}
            __path = "/_flush"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if force is not None:
            __query["force"] = force
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        if wait_if_ongoing is not None:
            __query["wait_if_ongoing"] = wait_if_ongoing
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.flush",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def forcemerge(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flush: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        max_num_segments: t.Optional[int] = None,
        only_expunge_deletes: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        wait_for_completion: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Force a merge.
          Perform the force merge operation on the shards of one or more indices.
          For data streams, the API forces a merge on the shards of the stream's backing indices.</p>
          <p>Merging reduces the number of segments in each shard by merging some of them together and also frees up the space used by deleted documents.
          Merging normally happens automatically, but sometimes it is useful to trigger a merge manually.</p>
          <p>WARNING: We recommend force merging only a read-only index (meaning the index is no longer receiving writes).
          When documents are updated or deleted, the old version is not immediately removed but instead soft-deleted and marked with a &quot;tombstone&quot;.
          These soft-deleted documents are automatically cleaned up during regular segment merges.
          But force merge can cause very large (greater than 5 GB) segments to be produced, which are not eligible for regular merges.
          So the number of soft-deleted documents can then grow rapidly, resulting in higher disk usage and worse search performance.
          If you regularly force merge an index receiving writes, this can also make snapshots more expensive, since the new documents can't be backed up incrementally.</p>
          <p><strong>Blocks during a force merge</strong></p>
          <p>Calls to this API block until the merge is complete (unless request contains <code>wait_for_completion=false</code>).
          If the client connection is lost before completion then the force merge process will continue in the background.
          Any new requests to force merge the same indices will also block until the ongoing force merge is complete.</p>
          <p><strong>Running force merge asynchronously</strong></p>
          <p>If the request contains <code>wait_for_completion=false</code>, Elasticsearch performs some preflight checks, launches the request, and returns a task you can use to get the status of the task.
          However, you can not cancel this task as the force merge task is not cancelable.
          Elasticsearch creates a record of this task as a document at <code>_tasks/&lt;task_id&gt;</code>.
          When you are done with a task, you should delete the task document so Elasticsearch can reclaim the space.</p>
          <p><strong>Force merging multiple indices</strong></p>
          <p>You can force merge multiple indices with a single request by targeting:</p>
          <ul>
          <li>One or more data streams that contain multiple backing indices</li>
          <li>Multiple indices</li>
          <li>One or more aliases</li>
          <li>All data streams and indices in a cluster</li>
          </ul>
          <p>Each targeted shard is force-merged separately using the force_merge threadpool.
          By default each node only has a single <code>force_merge</code> thread which means that the shards on that node are force-merged one at a time.
          If you expand the <code>force_merge</code> threadpool on a node then it will force merge its shards in parallel</p>
          <p>Force merge makes the storage for the shard being merged temporarily increase, as it may require free space up to triple its size in case <code>max_num_segments parameter</code> is set to <code>1</code>, to rewrite all segments into a new one.</p>
          <p><strong>Data streams and time-based indices</strong></p>
          <p>Force-merging is useful for managing a data stream's older backing indices and other time-based indices, particularly after a rollover.
          In these cases, each index only receives indexing traffic for a certain period of time.
          Once an index receive no more writes, its shards can be force-merged to a single segment.
          This can be a good idea because single-segment shards can sometimes use simpler and more efficient data structures to perform searches.
          For example:</p>
          <pre><code>POST /.ds-my-data-stream-2099.03.07-000001/_forcemerge?max_num_segments=1
          </code></pre>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-forcemerge>`_

        :param index: A comma-separated list of index names; use `_all` or empty string
            to perform the operation on all indices
        :param allow_no_indices: Whether to ignore if a wildcard indices expression resolves
            into no concrete indices. (This includes `_all` string or when no indices
            have been specified)
        :param expand_wildcards: Whether to expand wildcard expression to concrete indices
            that are open, closed or both.
        :param flush: Specify whether the index should be flushed after performing the
            operation (default: true)
        :param ignore_unavailable: Whether specified concrete indices should be ignored
            when unavailable (missing or closed)
        :param max_num_segments: The number of segments the index should be merged into
            (default: dynamic)
        :param only_expunge_deletes: Specify whether the operation should only expunge
            deleted documents
        :param wait_for_completion: Should the request wait until the force merge is
            completed.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_forcemerge'
        else:
            __path_parts = {}
            __path = "/_forcemerge"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flush is not None:
            __query["flush"] = flush
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if max_num_segments is not None:
            __query["max_num_segments"] = max_num_segments
        if only_expunge_deletes is not None:
            __query["only_expunge_deletes"] = only_expunge_deletes
        if pretty is not None:
            __query["pretty"] = pretty
        if wait_for_completion is not None:
            __query["wait_for_completion"] = wait_for_completion
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.forcemerge",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        features: t.Optional[
            t.Union[
                t.Sequence[t.Union[str, t.Literal["aliases", "mappings", "settings"]]],
                t.Union[str, t.Literal["aliases", "mappings", "settings"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flat_settings: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        include_defaults: t.Optional[bool] = None,
        local: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get index information.
          Get information about one or more indices. For data streams, the API returns information about the
          stream’s backing indices.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get>`_

        :param index: Comma-separated list of data streams, indices, and index aliases
            used to limit the request. Wildcard expressions (*) are supported.
        :param allow_no_indices: If false, the request returns an error if any wildcard
            expression, index alias, or _all value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting foo*,bar* returns an error if an index starts
            with foo but no index starts with bar.
        :param expand_wildcards: Type of index that wildcard expressions can match. If
            the request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as open,hidden.
        :param features: Return only information on specified index features
        :param flat_settings: If true, returns settings in flat format.
        :param ignore_unavailable: If false, requests that target a missing index return
            an error.
        :param include_defaults: If true, return all default settings in the response.
        :param local: If true, the request retrieves information from the local node
            only. Defaults to false, which means information is retrieved from the master
            node.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if features is not None:
            __query["features"] = features
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flat_settings is not None:
            __query["flat_settings"] = flat_settings
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if local is not None:
            __query["local"] = local
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_alias(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get aliases.
          Retrieves information for one or more data stream or index aliases.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-alias>`_

        :param index: Comma-separated list of data streams or indices used to limit the
            request. Supports wildcards (`*`). To target all data streams and indices,
            omit this parameter or use `*` or `_all`.
        :param name: Comma-separated list of aliases to retrieve. Supports wildcards
            (`*`). To retrieve all aliases, omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH and name not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index), "name": _quote(name)}
            __path = f'/{__path_parts["index"]}/_alias/{__path_parts["name"]}'
        elif index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_alias'
        elif name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_alias/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_alias"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_alias",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_data_lifecycle(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        include_defaults: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get data stream lifecycles.</p>
          <p>Get the data stream lifecycle configuration of one or more data streams.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-data-lifecycle>`_

        :param name: Comma-separated list of data streams to limit the request. Supports
            wildcards (`*`). To target all data streams, omit this parameter or use `*`
            or `_all`.
        :param expand_wildcards: Type of data stream that wildcard patterns can match.
            Supports comma-separated values, such as `open,hidden`.
        :param include_defaults: If `true`, return all default settings in the response.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}/_lifecycle'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_data_lifecycle",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_data_lifecycle_stats(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get data stream lifecycle stats.
          Get statistics about the data streams that are managed by a data stream lifecycle.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-data-lifecycle-stats>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_lifecycle/stats"
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
            endpoint_id="indices.get_data_lifecycle_stats",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_data_stream(
        self,
        *,
        name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        include_defaults: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        verbose: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get data streams.</p>
          <p>Get information about one or more data streams.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-data-stream>`_

        :param name: Comma-separated list of data stream names used to limit the request.
            Wildcard (`*`) expressions are supported. If omitted, all data streams are
            returned.
        :param expand_wildcards: Type of data stream that wildcard patterns can match.
            Supports comma-separated values, such as `open,hidden`.
        :param include_defaults: If true, returns all relevant default configurations
            for the index template.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param verbose: Whether the maximum timestamp for each data stream should be
            calculated and returned.
        """
        __path_parts: t.Dict[str, str]
        if name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_data_stream/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_data_stream"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if verbose is not None:
            __query["verbose"] = verbose
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_data_stream",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_data_stream_options(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get data stream options.</p>
          <p>Get the data stream options configuration of one or more data streams.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-data-stream-options>`_

        :param name: Comma-separated list of data streams to limit the request. Supports
            wildcards (`*`). To target all data streams, omit this parameter or use `*`
            or `_all`.
        :param expand_wildcards: Type of data stream that wildcard patterns can match.
            Supports comma-separated values, such as `open,hidden`.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}/_options'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_data_stream_options",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_data_stream_settings(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get data stream settings.</p>
          <p>Get setting information for one or more data streams.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-data-stream-settings>`_

        :param name: A comma-separated list of data streams or data stream patterns.
            Supports wildcards (`*`).
        :param master_timeout: The period to wait for a connection to the master node.
            If no response is received before the timeout expires, the request fails
            and returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}/_settings'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_data_stream_settings",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_field_mapping(
        self,
        *,
        fields: t.Union[str, t.Sequence[str]],
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        include_defaults: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get mapping definitions.
          Retrieves mapping definitions for one or more fields.
          For data streams, the API retrieves field mappings for the stream’s backing indices.</p>
          <p>This API is useful if you don't need a complete mapping or if an index mapping contains a large number of fields.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-mapping>`_

        :param fields: Comma-separated list or wildcard expression of fields used to
            limit returned information. Supports wildcards (`*`).
        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (`*`). To target all data streams
            and indices, omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param include_defaults: If `true`, return all default settings in the response.
        """
        if fields in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'fields'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH and fields not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index), "fields": _quote(fields)}
            __path = f'/{__path_parts["index"]}/_mapping/field/{__path_parts["fields"]}'
        elif fields not in SKIP_IN_PATH:
            __path_parts = {"fields": _quote(fields)}
            __path = f'/_mapping/field/{__path_parts["fields"]}'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_field_mapping",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_index_template(
        self,
        *,
        name: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flat_settings: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        include_defaults: t.Optional[bool] = None,
        local: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get index templates.
          Get information about one or more index templates.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-index-template>`_

        :param name: Comma-separated list of index template names used to limit the request.
            Wildcard (*) expressions are supported.
        :param flat_settings: If true, returns settings in flat format.
        :param include_defaults: If true, returns all relevant default configurations
            for the index template.
        :param local: If true, the request retrieves information from the local node
            only. Defaults to false, which means information is retrieved from the master
            node.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        __path_parts: t.Dict[str, str]
        if name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_index_template/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_index_template"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flat_settings is not None:
            __query["flat_settings"] = flat_settings
        if human is not None:
            __query["human"] = human
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if local is not None:
            __query["local"] = local
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_index_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_mapping(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        local: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get mapping definitions.
          For data streams, the API retrieves mappings for the stream’s backing indices.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-mapping>`_

        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (`*`). To target all data streams
            and indices, omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param local: If `true`, the request retrieves information from the local node
            only.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_mapping'
        else:
            __path_parts = {}
            __path = "/_mapping"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if local is not None:
            __query["local"] = local
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_mapping",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    @_stability_warning(Stability.EXPERIMENTAL)
    def get_migrate_reindex_status(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get the migration reindexing status.</p>
          <p>Get the status of a migration reindex attempt for a data stream or index.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-migration>`_

        :param index: The index or data stream name.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/_migration/reindex/{__path_parts["index"]}/_status'
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
            endpoint_id="indices.get_migrate_reindex_status",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_settings(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flat_settings: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        include_defaults: t.Optional[bool] = None,
        local: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get index settings.
          Get setting information for one or more indices.
          For data streams, it returns setting information for the stream's backing indices.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-settings>`_

        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (`*`). To target all data streams
            and indices, omit this parameter or use `*` or `_all`.
        :param name: Comma-separated list or wildcard expression of settings to retrieve.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with foo but no index starts with `bar`.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param flat_settings: If `true`, returns settings in flat format.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param include_defaults: If `true`, return all default settings in the response.
        :param local: If `true`, the request retrieves information from the local node
            only. If `false`, information is retrieved from the master node.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH and name not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index), "name": _quote(name)}
            __path = f'/{__path_parts["index"]}/_settings/{__path_parts["name"]}'
        elif index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_settings'
        elif name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_settings/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_settings"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flat_settings is not None:
            __query["flat_settings"] = flat_settings
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if local is not None:
            __query["local"] = local
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_settings",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_template(
        self,
        *,
        name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flat_settings: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        local: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get legacy index templates.
          Get information about one or more index templates.</p>
          <p>IMPORTANT: This documentation is about legacy index templates, which are deprecated and will be replaced by the composable templates introduced in Elasticsearch 7.8.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-get-template>`_

        :param name: Comma-separated list of index template names used to limit the request.
            Wildcard (`*`) expressions are supported. To return all index templates,
            omit this parameter or use a value of `_all` or `*`.
        :param flat_settings: If `true`, returns settings in flat format.
        :param local: If `true`, the request retrieves information from the local node
            only.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        __path_parts: t.Dict[str, str]
        if name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_template/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_template"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flat_settings is not None:
            __query["flat_settings"] = flat_settings
        if human is not None:
            __query["human"] = human
        if local is not None:
            __query["local"] = local
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.get_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="reindex",
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    def migrate_reindex(
        self,
        *,
        reindex: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Reindex legacy backing indices.</p>
          <p>Reindex all legacy backing indices for a data stream.
          This operation occurs in a persistent task.
          The persistent task ID is returned immediately and the reindexing work is completed in that task.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-migrate-reindex>`_

        :param reindex:
        """
        if reindex is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'reindex' and 'body', one of them should be set."
            )
        elif reindex is not None and body is not None:
            raise ValueError("Cannot set both 'reindex' and 'body'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_migration/reindex"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        __body = reindex if reindex is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.migrate_reindex",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def migrate_to_data_stream(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Convert an index alias to a data stream.
          Converts an index alias to a data stream.
          You must have a matching index template that is data stream enabled.
          The alias must meet the following criteria:
          The alias must have a write index;
          All indices for the alias must have a <code>@timestamp</code> field mapping of a <code>date</code> or <code>date_nanos</code> field type;
          The alias must not have any filters;
          The alias must not use custom routing.
          If successful, the request removes the alias and creates a data stream with the same name.
          The indices for the alias become hidden backing indices for the stream.
          The write index for the alias becomes the write index for the stream.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-migrate-to-data-stream>`_

        :param name: Name of the index alias to convert to a data stream.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/_migrate/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.migrate_to_data_stream",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("actions",),
    )
    def modify_data_stream(
        self,
        *,
        actions: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update data streams.
          Performs one or more data stream modification actions in a single atomic operation.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-modify-data-stream>`_

        :param actions: Actions to perform.
        """
        if actions is None and body is None:
            raise ValueError("Empty value passed for parameter 'actions'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_data_stream/_modify"
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
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.modify_data_stream",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def open(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Open a closed index.
          For data streams, the API opens any closed backing indices.</p>
          <p>A closed index is blocked for read/write operations and does not allow all operations that opened indices allow.
          It is not possible to index documents or to search for documents in a closed index.
          This allows closed indices to not have to maintain internal data structures for indexing or searching documents, resulting in a smaller overhead on the cluster.</p>
          <p>When opening or closing an index, the master is responsible for restarting the index shards to reflect the new state of the index.
          The shards will then go through the normal recovery process.
          The data of opened or closed indices is automatically replicated by the cluster to ensure that enough shard copies are safely kept around at all times.</p>
          <p>You can open and close multiple indices.
          An error is thrown if the request explicitly refers to a missing index.
          This behavior can be turned off by using the <code>ignore_unavailable=true</code> parameter.</p>
          <p>By default, you must explicitly name the indices you are opening or closing.
          To open or close indices with <code>_all</code>, <code>*</code>, or other wildcard expressions, change the <code>action.destructive_requires_name</code> setting to <code>false</code>.
          This setting can also be changed with the cluster update settings API.</p>
          <p>Closed indices consume a significant amount of disk-space which can cause problems in managed environments.
          Closing indices can be turned off with the cluster settings API by setting <code>cluster.indices.close.enable</code> to <code>false</code>.</p>
          <p>Because opening or closing an index allocates its shards, the <code>wait_for_active_shards</code> setting on index creation applies to the <code>_open</code> and <code>_close</code> index actions as well.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-open>`_

        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (`*`). By default, you must explicitly
            name the indices you using to limit the request. To limit a request using
            `_all`, `*`, or other wildcard expressions, change the `action.destructive_requires_name`
            setting to false. You can update this setting in the `elasticsearch.yml`
            file or using the cluster update settings API.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`).
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_open'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.open",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def promote_data_stream(
        self,
        *,
        name: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Promote a data stream.
          Promote a data stream from a replicated data stream managed by cross-cluster replication (CCR) to a regular data stream.</p>
          <p>With CCR auto following, a data stream from a remote cluster can be replicated to the local cluster.
          These data streams can't be rolled over in the local cluster.
          These replicated data streams roll over only if the upstream data stream rolls over.
          In the event that the remote cluster is no longer available, the data stream in the local cluster can be promoted to a regular data stream, which allows these data streams to be rolled over in the local cluster.</p>
          <p>NOTE: When promoting a data stream, ensure the local cluster has a data stream enabled index template that matches the data stream.
          If this is missing, the data stream will not be able to roll over until a matching index template is created.
          This will affect the lifecycle management of the data stream and interfere with the data stream size and retention.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-promote-data-stream>`_

        :param name: The name of the data stream
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/_promote/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.promote_data_stream",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "filter",
            "index_routing",
            "is_write_index",
            "routing",
            "search_routing",
        ),
    )
    def put_alias(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        name: str,
        error_trace: t.Optional[bool] = None,
        filter: t.Optional[t.Mapping[str, t.Any]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        index_routing: t.Optional[str] = None,
        is_write_index: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        search_routing: t.Optional[str] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update an alias.
          Adds a data stream or index to an alias.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-alias>`_

        :param index: Comma-separated list of data streams or indices to add. Supports
            wildcards (`*`). Wildcard patterns that match both data streams and indices
            return an error.
        :param name: Alias to update. If the alias doesn’t exist, the request creates
            it. Index alias names support date math.
        :param filter: Query used to limit documents the alias can access.
        :param index_routing: Value used to route indexing operations to a specific shard.
            If specified, this overwrites the `routing` value for indexing operations.
            Data stream aliases don’t support this parameter.
        :param is_write_index: If `true`, sets the write index or data stream for the
            alias. If an alias points to multiple indices or data streams and `is_write_index`
            isn’t set, the alias rejects write requests. If an index alias points to
            one index and `is_write_index` isn’t set, the index automatically acts as
            the write index. Data stream aliases don’t automatically set a write data
            stream, even if the alias points to one data stream.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param routing: Value used to route indexing and search operations to a specific
            shard. Data stream aliases don’t support this parameter.
        :param search_routing: Value used to route search operations to a specific shard.
            If specified, this overwrites the `routing` value for search operations.
            Data stream aliases don’t support this parameter.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "name": _quote(name)}
        __path = f'/{__path_parts["index"]}/_alias/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if filter is not None:
                __body["filter"] = filter
            if index_routing is not None:
                __body["index_routing"] = index_routing
            if is_write_index is not None:
                __body["is_write_index"] = is_write_index
            if routing is not None:
                __body["routing"] = routing
            if search_routing is not None:
                __body["search_routing"] = search_routing
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.put_alias",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("data_retention", "downsampling", "enabled"),
    )
    def put_data_lifecycle(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        data_retention: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        downsampling: t.Optional[t.Mapping[str, t.Any]] = None,
        enabled: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update data stream lifecycles.
          Update the data stream lifecycle of the specified data streams.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-data-lifecycle>`_

        :param name: Comma-separated list of data streams used to limit the request.
            Supports wildcards (`*`). To target all data streams use `*` or `_all`.
        :param data_retention: If defined, every document added to this data stream will
            be stored at least for this time frame. Any time after this duration the
            document could be deleted. When empty, every document in this data stream
            will be stored indefinitely.
        :param downsampling: The downsampling configuration to execute for the managed
            backing index after rollover.
        :param enabled: If defined, it turns data stream lifecycle on/off (`true`/`false`)
            for this data stream. A data stream lifecycle that's disabled (enabled: `false`)
            will have no effect on the data stream.
        :param expand_wildcards: Type of data stream that wildcard patterns can match.
            Supports comma-separated values, such as `open,hidden`.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}/_lifecycle'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if data_retention is not None:
                __body["data_retention"] = data_retention
            if downsampling is not None:
                __body["downsampling"] = downsampling
            if enabled is not None:
                __body["enabled"] = enabled
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.put_data_lifecycle",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("failure_store",),
    )
    def put_data_stream_options(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        failure_store: t.Optional[t.Mapping[str, t.Any]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update data stream options.
          Update the data stream options of the specified data streams.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-data-stream-options>`_

        :param name: Comma-separated list of data streams used to limit the request.
            Supports wildcards (`*`). To target all data streams use `*` or `_all`.
        :param expand_wildcards: Type of data stream that wildcard patterns can match.
            Supports comma-separated values, such as `open,hidden`.
        :param failure_store: If defined, it will update the failure store configuration
            of every data stream resolved by the name expression.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}/_options'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if failure_store is not None:
                __body["failure_store"] = failure_store
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.put_data_stream_options",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="settings",
    )
    def put_data_stream_settings(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        settings: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        dry_run: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update data stream settings.</p>
          <p>This API can be used to override settings on specific data streams. These overrides will take precedence over what
          is specified in the template that the data stream matches. To prevent your data stream from getting into an invalid state,
          only certain settings are allowed. If possible, the setting change is applied to all
          backing indices. Otherwise, it will be applied when the data stream is next rolled over.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-data-stream-settings>`_

        :param name: A comma-separated list of data streams or data stream patterns.
        :param settings:
        :param dry_run: If `true`, the request does not actually change the settings
            on any data streams or indices. Instead, it simulates changing the settings
            and reports back to the user what would have happened had these settings
            actually been applied.
        :param master_timeout: The period to wait for a connection to the master node.
            If no response is received before the timeout expires, the request fails
            and returns an error.
        :param timeout: The period to wait for a response. If no response is received
            before the timeout expires, the request fails and returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        if settings is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'settings' and 'body', one of them should be set."
            )
        elif settings is not None and body is not None:
            raise ValueError("Cannot set both 'settings' and 'body'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_data_stream/{__path_parts["name"]}/_settings'
        __query: t.Dict[str, t.Any] = {}
        if dry_run is not None:
            __query["dry_run"] = dry_run
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __body = settings if settings is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.put_data_stream_settings",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "allow_auto_create",
            "composed_of",
            "data_stream",
            "deprecated",
            "ignore_missing_component_templates",
            "index_patterns",
            "meta",
            "priority",
            "template",
            "version",
        ),
        parameter_aliases={"_meta": "meta"},
    )
    def put_index_template(
        self,
        *,
        name: str,
        allow_auto_create: t.Optional[bool] = None,
        cause: t.Optional[str] = None,
        composed_of: t.Optional[t.Sequence[str]] = None,
        create: t.Optional[bool] = None,
        data_stream: t.Optional[t.Mapping[str, t.Any]] = None,
        deprecated: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_missing_component_templates: t.Optional[t.Sequence[str]] = None,
        index_patterns: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        meta: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        priority: t.Optional[int] = None,
        template: t.Optional[t.Mapping[str, t.Any]] = None,
        version: t.Optional[int] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update an index template.
          Index templates define settings, mappings, and aliases that can be applied automatically to new indices.</p>
          <p>Elasticsearch applies templates to new indices based on an wildcard pattern that matches the index name.
          Index templates are applied during data stream or index creation.
          For data streams, these settings and mappings are applied when the stream's backing indices are created.
          Settings and mappings specified in a create index API request override any settings or mappings specified in an index template.
          Changes to index templates do not affect existing indices, including the existing backing indices of a data stream.</p>
          <p>You can use C-style <code>/* *\\/</code> block comments in index templates.
          You can include comments anywhere in the request body, except before the opening curly bracket.</p>
          <p><strong>Multiple matching templates</strong></p>
          <p>If multiple index templates match the name of a new index or data stream, the template with the highest priority is used.</p>
          <p>Multiple templates with overlapping index patterns at the same priority are not allowed and an error will be thrown when attempting to create a template matching an existing index template at identical priorities.</p>
          <p><strong>Composing aliases, mappings, and settings</strong></p>
          <p>When multiple component templates are specified in the <code>composed_of</code> field for an index template, they are merged in the order specified, meaning that later component templates override earlier component templates.
          Any mappings, settings, or aliases from the parent index template are merged in next.
          Finally, any configuration on the index request itself is merged.
          Mapping definitions are merged recursively, which means that later mapping components can introduce new field mappings and update the mapping configuration.
          If a field mapping is already contained in an earlier component, its definition will be completely overwritten by the later one.
          This recursive merging strategy applies not only to field mappings, but also root options like <code>dynamic_templates</code> and <code>meta</code>.
          If an earlier component contains a <code>dynamic_templates</code> block, then by default new <code>dynamic_templates</code> entries are appended onto the end.
          If an entry already exists with the same key, then it is overwritten by the new definition.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-index-template>`_

        :param name: Index or template name
        :param allow_auto_create: This setting overrides the value of the `action.auto_create_index`
            cluster setting. If set to `true` in a template, then indices can be automatically
            created using that template even if auto-creation of indices is disabled
            via `actions.auto_create_index`. If set to `false`, then indices or data
            streams matching the template must always be explicitly created, and may
            never be automatically created.
        :param cause: User defined reason for creating/updating the index template
        :param composed_of: An ordered list of component template names. Component templates
            are merged in the order specified, meaning that the last component template
            specified has the highest precedence.
        :param create: If `true`, this request cannot replace or update existing index
            templates.
        :param data_stream: If this object is included, the template is used to create
            data streams and their backing indices. Supports an empty object. Data streams
            require a matching index template with a `data_stream` object.
        :param deprecated: Marks this index template as deprecated. When creating or
            updating a non-deprecated index template that uses deprecated components,
            Elasticsearch will emit a deprecation warning.
        :param ignore_missing_component_templates: The configuration option ignore_missing_component_templates
            can be used when an index template references a component template that might
            not exist
        :param index_patterns: Name of the index template to create.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param meta: Optional user metadata about the index template. It may have any
            contents. It is not automatically generated or used by Elasticsearch. This
            user-defined object is stored in the cluster state, so keeping it short is
            preferable To unset the metadata, replace the template without specifying
            it.
        :param priority: Priority to determine index template precedence when a new data
            stream or index is created. The index template with the highest priority
            is chosen. If no priority is specified the template is treated as though
            it is of priority 0 (lowest priority). This number is not automatically generated
            by Elasticsearch.
        :param template: Template to be applied. It may optionally include an `aliases`,
            `mappings`, or `settings` configuration.
        :param version: Version number used to manage index templates externally. This
            number is not automatically generated by Elasticsearch. External systems
            can use these version numbers to simplify template management. To unset a
            version, replace the template without specifying one.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_index_template/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if cause is not None:
            __query["cause"] = cause
        if create is not None:
            __query["create"] = create
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if allow_auto_create is not None:
                __body["allow_auto_create"] = allow_auto_create
            if composed_of is not None:
                __body["composed_of"] = composed_of
            if data_stream is not None:
                __body["data_stream"] = data_stream
            if deprecated is not None:
                __body["deprecated"] = deprecated
            if ignore_missing_component_templates is not None:
                __body["ignore_missing_component_templates"] = (
                    ignore_missing_component_templates
                )
            if index_patterns is not None:
                __body["index_patterns"] = index_patterns
            if meta is not None:
                __body["_meta"] = meta
            if priority is not None:
                __body["priority"] = priority
            if template is not None:
                __body["template"] = template
            if version is not None:
                __body["version"] = version
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.put_index_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "date_detection",
            "dynamic",
            "dynamic_date_formats",
            "dynamic_templates",
            "field_names",
            "meta",
            "numeric_detection",
            "properties",
            "routing",
            "runtime",
            "source",
        ),
        parameter_aliases={
            "_field_names": "field_names",
            "_meta": "meta",
            "_routing": "routing",
            "_source": "source",
        },
    )
    def put_mapping(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        date_detection: t.Optional[bool] = None,
        dynamic: t.Optional[
            t.Union[str, t.Literal["false", "runtime", "strict", "true"]]
        ] = None,
        dynamic_date_formats: t.Optional[t.Sequence[str]] = None,
        dynamic_templates: t.Optional[
            t.Sequence[t.Mapping[str, t.Mapping[str, t.Any]]]
        ] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        field_names: t.Optional[t.Mapping[str, t.Any]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        meta: t.Optional[t.Mapping[str, t.Any]] = None,
        numeric_detection: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        properties: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        routing: t.Optional[t.Mapping[str, t.Any]] = None,
        runtime: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        source: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        write_index_only: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update field mappings.
          Add new fields to an existing data stream or index.
          You can use the update mapping API to:</p>
          <ul>
          <li>Add a new field to an existing index</li>
          <li>Update mappings for multiple indices in a single request</li>
          <li>Add new properties to an object field</li>
          <li>Enable multi-fields for an existing field</li>
          <li>Update supported mapping parameters</li>
          <li>Change a field's mapping using reindexing</li>
          <li>Rename a field using a field alias</li>
          </ul>
          <p>Learn how to use the update mapping API with practical examples in the <a href="https://www.elastic.co/docs/manage-data/data-store/mapping/update-mappings-examples">Update mapping API examples</a> guide.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-mapping>`_

        :param index: A comma-separated list of index names the mapping should be added
            to (supports wildcards); use `_all` or omit to add the mapping on all indices.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param date_detection: Controls whether dynamic date detection is enabled.
        :param dynamic: Controls whether new fields are added dynamically.
        :param dynamic_date_formats: If date detection is enabled then new string fields
            are checked against 'dynamic_date_formats' and if the value matches then
            a new date field is added instead of string.
        :param dynamic_templates: Specify dynamic templates for the mapping.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param field_names: Control whether field names are enabled for the index.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param meta: A mapping type can have custom meta data associated with it. These
            are not used at all by Elasticsearch, but can be used to store application-specific
            metadata.
        :param numeric_detection: Automatically map strings into numeric data types for
            all fields.
        :param properties: Mapping for a field. For new fields, this mapping can include:
            - Field name - Field data type - Mapping parameters
        :param routing: Enable making a routing value required on indexed documents.
        :param runtime: Mapping of runtime fields for the index.
        :param source: Control whether the _source field is enabled on the index.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        :param write_index_only: If `true`, the mappings are applied only to the current
            write index for the target.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_mapping'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if write_index_only is not None:
            __query["write_index_only"] = write_index_only
        if not __body:
            if date_detection is not None:
                __body["date_detection"] = date_detection
            if dynamic is not None:
                __body["dynamic"] = dynamic
            if dynamic_date_formats is not None:
                __body["dynamic_date_formats"] = dynamic_date_formats
            if dynamic_templates is not None:
                __body["dynamic_templates"] = dynamic_templates
            if field_names is not None:
                __body["_field_names"] = field_names
            if meta is not None:
                __body["_meta"] = meta
            if numeric_detection is not None:
                __body["numeric_detection"] = numeric_detection
            if properties is not None:
                __body["properties"] = properties
            if routing is not None:
                __body["_routing"] = routing
            if runtime is not None:
                __body["runtime"] = runtime
            if source is not None:
                __body["_source"] = source
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.put_mapping",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="settings",
    )
    def put_settings(
        self,
        *,
        settings: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        flat_settings: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        preserve_existing: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        reopen: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update index settings.
          Changes dynamic index settings in real time.
          For data streams, index setting changes are applied to all backing indices by default.</p>
          <p>To revert a setting to the default value, use a null value.
          The list of per-index settings that can be updated dynamically on live indices can be found in index settings documentation.
          To preserve existing settings from being updated, set the <code>preserve_existing</code> parameter to <code>true</code>.</p>
          <p>For performance optimization during bulk indexing, you can disable the refresh interval.
          Refer to <a href="https://www.elastic.co/docs/deploy-manage/production-guidance/optimize-performance/indexing-speed#disable-refresh-interval">disable refresh interval</a> for an example.
          There are multiple valid ways to represent index settings in the request body. You can specify only the setting, for example:</p>
          <pre><code>{
            &quot;number_of_replicas&quot;: 1
          }
          </code></pre>
          <p>Or you can use an <code>index</code> setting object:</p>
          <pre><code>{
            &quot;index&quot;: {
              &quot;number_of_replicas&quot;: 1
            }
          }
          </code></pre>
          <p>Or you can use dot annotation:</p>
          <pre><code>{
            &quot;index.number_of_replicas&quot;: 1
          }
          </code></pre>
          <p>Or you can embed any of the aforementioned options in a <code>settings</code> object. For example:</p>
          <pre><code>{
            &quot;settings&quot;: {
              &quot;index&quot;: {
                &quot;number_of_replicas&quot;: 1
              }
            }
          }
          </code></pre>
          <p>NOTE: You can only define new analyzers on closed indices.
          To add an analyzer, you must close the index, define the analyzer, and reopen the index.
          You cannot close the write index of a data stream.
          To update the analyzer for a data stream's write index and future backing indices, update the analyzer in the index template used by the stream.
          Then roll over the data stream to apply the new analyzer to the stream's write index and future backing indices.
          This affects searches and any new data added to the stream after the rollover.
          However, it does not affect the data stream's backing indices or their existing data.
          To change the analyzer for existing backing indices, you must create a new data stream and reindex your data into it.
          Refer to <a href="https://www.elastic.co/docs/manage-data/data-store/text-analysis/specify-an-analyzer#update-analyzers-on-existing-indices">updating analyzers on existing indices</a> for step-by-step examples.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-settings>`_

        :param settings:
        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (`*`). To target all data streams
            and indices, omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param flat_settings: If `true`, returns settings in flat format.
        :param ignore_unavailable: If `true`, returns settings in flat format.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param preserve_existing: If `true`, existing index settings remain unchanged.
        :param reopen: Whether to close and reopen the index to apply non-dynamic settings.
            If set to `true` the indices to which the settings are being applied will
            be closed temporarily and then reopened in order to apply the changes.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        if settings is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'settings' and 'body', one of them should be set."
            )
        elif settings is not None and body is not None:
            raise ValueError("Cannot set both 'settings' and 'body'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_settings'
        else:
            __path_parts = {}
            __path = "/_settings"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if flat_settings is not None:
            __query["flat_settings"] = flat_settings
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if preserve_existing is not None:
            __query["preserve_existing"] = preserve_existing
        if pretty is not None:
            __query["pretty"] = pretty
        if reopen is not None:
            __query["reopen"] = reopen
        if timeout is not None:
            __query["timeout"] = timeout
        __body = settings if settings is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.put_settings",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "aliases",
            "index_patterns",
            "mappings",
            "order",
            "settings",
            "version",
        ),
    )
    def put_template(
        self,
        *,
        name: str,
        aliases: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        cause: t.Optional[str] = None,
        create: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        index_patterns: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        mappings: t.Optional[t.Mapping[str, t.Any]] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        order: t.Optional[int] = None,
        pretty: t.Optional[bool] = None,
        settings: t.Optional[t.Mapping[str, t.Any]] = None,
        version: t.Optional[int] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update a legacy index template.
          Index templates define settings, mappings, and aliases that can be applied automatically to new indices.
          Elasticsearch applies templates to new indices based on an index pattern that matches the index name.</p>
          <p>IMPORTANT: This documentation is about legacy index templates, which are deprecated and will be replaced by the composable templates introduced in Elasticsearch 7.8.</p>
          <p>Composable templates always take precedence over legacy templates.
          If no composable template matches a new index, matching legacy templates are applied according to their order.</p>
          <p>Index templates are only applied during index creation.
          Changes to index templates do not affect existing indices.
          Settings and mappings specified in create index API requests override any settings or mappings specified in an index template.</p>
          <p>You can use C-style <code>/* *\\/</code> block comments in index templates.
          You can include comments anywhere in the request body, except before the opening curly bracket.</p>
          <p><strong>Indices matching multiple templates</strong></p>
          <p>Multiple index templates can potentially match an index, in this case, both the settings and mappings are merged into the final configuration of the index.
          The order of the merging can be controlled using the order parameter, with lower order being applied first, and higher orders overriding them.
          NOTE: Multiple matching templates with the same order value will result in a non-deterministic merging order.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-put-template>`_

        :param name: The name of the template
        :param aliases: Aliases for the index.
        :param cause: User defined reason for creating/updating the index template
        :param create: If true, this request cannot replace or update existing index
            templates.
        :param index_patterns: Array of wildcard expressions used to match the names
            of indices during creation.
        :param mappings: Mapping for fields in the index.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param order: Order in which Elasticsearch applies this template if index matches
            multiple templates. Templates with lower 'order' values are merged first.
            Templates with higher 'order' values are merged later, overriding templates
            with lower values.
        :param settings: Configuration options for the index.
        :param version: Version number used to manage index templates externally. This
            number is not automatically generated by Elasticsearch. To unset a version,
            replace the template without specifying one.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_template/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if cause is not None:
            __query["cause"] = cause
        if create is not None:
            __query["create"] = create
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if aliases is not None:
                __body["aliases"] = aliases
            if index_patterns is not None:
                __body["index_patterns"] = index_patterns
            if mappings is not None:
                __body["mappings"] = mappings
            if order is not None:
                __body["order"] = order
            if settings is not None:
                __body["settings"] = settings
            if version is not None:
                __body["version"] = version
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.put_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def recovery(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        active_only: t.Optional[bool] = None,
        allow_no_indices: t.Optional[bool] = None,
        detailed: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get index recovery information.
          Get information about ongoing and completed shard recoveries for one or more indices.
          For data streams, the API returns information for the stream's backing indices.</p>
          <p>All recoveries, whether ongoing or complete, are kept in the cluster state and may be reported on at any time.</p>
          <p>Shard recovery is the process of initializing a shard copy, such as restoring a primary shard from a snapshot or creating a replica shard from a primary shard.
          When a shard recovery completes, the recovered shard is available for search and indexing.</p>
          <p>Recovery automatically occurs during the following processes:</p>
          <ul>
          <li>When creating an index for the first time.</li>
          <li>When a node rejoins the cluster and starts up any missing primary shard copies using the data that it holds in its data path.</li>
          <li>Creation of new replica shard copies from the primary.</li>
          <li>Relocation of a shard copy to a different node in the same cluster.</li>
          <li>A snapshot restore operation.</li>
          <li>A clone, shrink, or split operation.</li>
          </ul>
          <p>You can determine the cause of a shard recovery using the recovery or cat recovery APIs.</p>
          <p>The index recovery API reports information about completed recoveries only for shard copies that currently exist in the cluster.
          It only reports the last recovery for each shard copy and does not report historical information about earlier recoveries, nor does it report information about the recoveries of shard copies that no longer exist.
          This means that if a shard copy completes a recovery and then Elasticsearch relocates it onto a different node then the information about the original recovery will not be shown in the recovery API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-recovery>`_

        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (`*`). To target all data streams
            and indices, omit this parameter or use `*` or `_all`.
        :param active_only: If `true`, the response only includes ongoing shard recoveries.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param detailed: If `true`, the response includes detailed information about
            shard recoveries.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_recovery'
        else:
            __path_parts = {}
            __path = "/_recovery"
        __query: t.Dict[str, t.Any] = {}
        if active_only is not None:
            __query["active_only"] = active_only
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if detailed is not None:
            __query["detailed"] = detailed
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.recovery",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def refresh(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Refresh an index.
          A refresh makes recent operations performed on one or more indices available for search.
          For data streams, the API runs the refresh operation on the stream’s backing indices.</p>
          <p>By default, Elasticsearch periodically refreshes indices every second, but only on indices that have received one search request or more in the last 30 seconds.
          You can change this default interval with the <code>index.refresh_interval</code> setting.</p>
          <p>Refresh requests are synchronous and do not return a response until the refresh operation completes.</p>
          <p>Refreshes are resource-intensive.
          To ensure good cluster performance, it's recommended to wait for Elasticsearch's periodic refresh rather than performing an explicit refresh when possible.</p>
          <p>If your application workflow indexes documents and then runs a search to retrieve the indexed document, it's recommended to use the index API's <code>refresh=wait_for</code> query parameter option.
          This option ensures the indexing operation waits for a periodic refresh before running the search.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-refresh>`_

        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (`*`). To target all data streams
            and indices, omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_refresh'
        else:
            __path_parts = {}
            __path = "/_refresh"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.refresh",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def reload_search_analyzers(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        resource: t.Optional[str] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Reload search analyzers.
          Reload an index's search analyzers and their resources.
          For data streams, the API reloads search analyzers and resources for the stream's backing indices.</p>
          <p>IMPORTANT: After reloading the search analyzers you should clear the request cache to make sure it doesn't contain responses derived from the previous versions of the analyzer.</p>
          <p>You can use the reload search analyzers API to pick up changes to synonym files used in the <code>synonym_graph</code> or <code>synonym</code> token filter of a search analyzer.
          To be eligible, the token filter must have an <code>updateable</code> flag of <code>true</code> and only be used in search analyzers.</p>
          <p>NOTE: This API does not perform a reload for each shard of an index.
          Instead, it performs a reload for each node containing index shards.
          As a result, the total shard count returned by the API can differ from the number of index shards.
          Because reloading affects every node with an index shard, it is important to update the synonym file on every data node in the cluster--including nodes that don't contain a shard replica--before using this API.
          This ensures the synonym file is updated everywhere in the cluster in case shards are relocated in the future.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-reload-search-analyzers>`_

        :param index: A comma-separated list of index names to reload analyzers for
        :param allow_no_indices: Whether to ignore if a wildcard indices expression resolves
            into no concrete indices. (This includes `_all` string or when no indices
            have been specified)
        :param expand_wildcards: Whether to expand wildcard expression to concrete indices
            that are open, closed or both.
        :param ignore_unavailable: Whether specified concrete indices should be ignored
            when unavailable (missing or closed)
        :param resource: Changed resource to reload analyzers from if applicable
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_reload_search_analyzers'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        if resource is not None:
            __query["resource"] = resource
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.reload_search_analyzers",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def remove_block(
        self,
        *,
        index: str,
        block: t.Union[str, t.Literal["metadata", "read", "read_only", "write"]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Remove an index block.</p>
          <p>Remove an index block from an index.
          Index blocks limit the operations allowed on an index by blocking specific operation types.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-remove-block>`_

        :param index: A comma-separated list or wildcard expression of index names used
            to limit the request. By default, you must explicitly name the indices you
            are removing blocks from. To allow the removal of blocks from indices with
            `_all`, `*`, or other wildcard expressions, change the `action.destructive_requires_name`
            setting to `false`. You can update this setting in the `elasticsearch.yml`
            file or by using the cluster update settings API.
        :param block: The block type to remove from the index.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param expand_wildcards: The type of index that wildcard patterns can match.
            If the request can target data streams, this argument determines whether
            wildcard expressions match hidden data streams. It supports comma-separated
            values, such as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param master_timeout: The period to wait for the master node. If the master
            node is not available before the timeout expires, the request fails and returns
            an error. It can also be set to `-1` to indicate that the request should
            never timeout.
        :param timeout: The period to wait for a response from all relevant nodes in
            the cluster after updating the cluster metadata. If no response is received
            before the timeout expires, the cluster metadata update still applies but
            the response will indicate that it was not completely acknowledged. It can
            also be set to `-1` to indicate that the request should never timeout.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if block in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'block'")
        __path_parts: t.Dict[str, str] = {
            "index": _quote(index),
            "block": _quote(block),
        }
        __path = f'/{__path_parts["index"]}/_block/{__path_parts["block"]}'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.remove_block",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def resolve_cluster(
        self,
        *,
        name: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_throttled: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Resolve the cluster.</p>
          <p>Resolve the specified index expressions to return information about each cluster, including the local &quot;querying&quot; cluster, if included.
          If no index expression is provided, the API will return information about all the remote clusters that are configured on the querying cluster.</p>
          <p>This endpoint is useful before doing a cross-cluster search in order to determine which remote clusters should be included in a search.</p>
          <p>You use the same index expression with this endpoint as you would for cross-cluster search.
          Index and cluster exclusions are also supported with this endpoint.</p>
          <p>For each cluster in the index expression, information is returned about:</p>
          <ul>
          <li>Whether the querying (&quot;local&quot;) cluster is currently connected to each remote cluster specified in the index expression. Note that this endpoint actively attempts to contact the remote clusters, unlike the <code>remote/info</code> endpoint.</li>
          <li>Whether each remote cluster is configured with <code>skip_unavailable</code> as <code>true</code> or <code>false</code>.</li>
          <li>Whether there are any indices, aliases, or data streams on that cluster that match the index expression.</li>
          <li>Whether the search is likely to have errors returned when you do the cross-cluster search (including any authorization errors if you do not have permission to query the index).</li>
          <li>Cluster version information, including the Elasticsearch server version.</li>
          </ul>
          <p>For example, <code>GET /_resolve/cluster/my-index-*,cluster*:my-index-*</code> returns information about the local cluster and all remotely configured clusters that start with the alias <code>cluster*</code>.
          Each cluster returns information about whether it has any indices, aliases or data streams that match <code>my-index-*</code>.</p>
          <h2>Note on backwards compatibility</h2>
          <p>The ability to query without an index expression was added in version 8.18, so when
          querying remote clusters older than that, the local cluster will send the index
          expression <code>dummy*</code> to those remote clusters. Thus, if an errors occur, you may see a reference
          to that index expression even though you didn't request it. If it causes a problem, you can
          instead include an index expression like <code>*:*</code> to bypass the issue.</p>
          <h2>Advantages of using this endpoint before a cross-cluster search</h2>
          <p>You may want to exclude a cluster or index from a search when:</p>
          <ul>
          <li>A remote cluster is not currently connected and is configured with <code>skip_unavailable=false</code>. Running a cross-cluster search under those conditions will cause the entire search to fail.</li>
          <li>A cluster has no matching indices, aliases or data streams for the index expression (or your user does not have permissions to search them). For example, suppose your index expression is <code>logs*,remote1:logs*</code> and the remote1 cluster has no indices, aliases or data streams that match <code>logs*</code>. In that case, that cluster will return no results from that cluster if you include it in a cross-cluster search.</li>
          <li>The index expression (combined with any query parameters you specify) will likely cause an exception to be thrown when you do the search. In these cases, the &quot;error&quot; field in the <code>_resolve/cluster</code> response will be present. (This is also where security/permission errors will be shown.)</li>
          <li>A remote cluster is an older version that does not support the feature you want to use in your search.</li>
          </ul>
          <h2>Test availability of remote clusters</h2>
          <p>The <code>remote/info</code> endpoint is commonly used to test whether the &quot;local&quot; cluster (the cluster being queried) is connected to its remote clusters, but it does not necessarily reflect whether the remote cluster is available or not.
          The remote cluster may be available, while the local cluster is not currently connected to it.</p>
          <p>You can use the <code>_resolve/cluster</code> API to attempt to reconnect to remote clusters.
          For example with <code>GET _resolve/cluster</code> or <code>GET _resolve/cluster/*:*</code>.
          The <code>connected</code> field in the response will indicate whether it was successful.
          If a connection was (re-)established, this will also cause the <code>remote/info</code> endpoint to now indicate a connected status.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-resolve-cluster>`_

        :param name: A comma-separated list of names or index patterns for the indices,
            aliases, and data streams to resolve. Resources on remote clusters can be
            specified using the `<cluster>`:`<name>` syntax. Index and cluster exclusions
            (e.g., `-cluster1:*`) are also supported. If no index expression is specified,
            information about all remote clusters configured on the local cluster is
            returned without doing any index matching
        :param allow_no_indices: If false, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`. NOTE: This option is only supported
            when specifying an index expression. You will get an error if you specify
            index options to the `_resolve/cluster` API endpoint that takes no index
            expression.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`. NOTE: This option is only supported when specifying an
            index expression. You will get an error if you specify index options to the
            `_resolve/cluster` API endpoint that takes no index expression.
        :param ignore_throttled: If true, concrete, expanded, or aliased indices are
            ignored when frozen. NOTE: This option is only supported when specifying
            an index expression. You will get an error if you specify index options to
            the `_resolve/cluster` API endpoint that takes no index expression.
        :param ignore_unavailable: If false, the request returns an error if it targets
            a missing or closed index. NOTE: This option is only supported when specifying
            an index expression. You will get an error if you specify index options to
            the `_resolve/cluster` API endpoint that takes no index expression.
        :param timeout: The maximum time to wait for remote clusters to respond. If a
            remote cluster does not respond within this timeout period, the API response
            will show the cluster as not connected and include an error message that
            the request timed out. The default timeout is unset and the query can take
            as long as the networking layer is configured to wait for remote clusters
            that are not responding (typically 30 seconds).
        """
        __path_parts: t.Dict[str, str]
        if name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_resolve/cluster/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_resolve/cluster"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_throttled is not None:
            __query["ignore_throttled"] = ignore_throttled
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.resolve_cluster",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def resolve_index(
        self,
        *,
        name: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Resolve indices.
          Resolve the names and/or index patterns for indices, aliases, and data streams.
          Multiple patterns and remote clusters are supported.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-resolve-index>`_

        :param name: Comma-separated name(s) or index pattern(s) of the indices, aliases,
            and data streams to resolve. Resources on remote clusters can be specified
            using the `<cluster>`:`<name>` syntax.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_resolve/index/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.resolve_index",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("aliases", "conditions", "mappings", "settings"),
    )
    def rollover(
        self,
        *,
        alias: str,
        new_index: t.Optional[str] = None,
        aliases: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        conditions: t.Optional[t.Mapping[str, t.Any]] = None,
        dry_run: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        lazy: t.Optional[bool] = None,
        mappings: t.Optional[t.Mapping[str, t.Any]] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Roll over to a new index.
          TIP: It is recommended to use the index lifecycle rollover action to automate rollovers.</p>
          <p>The rollover API creates a new index for a data stream or index alias.
          The API behavior depends on the rollover target.</p>
          <p><strong>Roll over a data stream</strong></p>
          <p>If you roll over a data stream, the API creates a new write index for the stream.
          The stream's previous write index becomes a regular backing index.
          A rollover also increments the data stream's generation.</p>
          <p><strong>Roll over an index alias with a write index</strong></p>
          <p>TIP: Prior to Elasticsearch 7.9, you'd typically use an index alias with a write index to manage time series data.
          Data streams replace this functionality, require less maintenance, and automatically integrate with data tiers.</p>
          <p>If an index alias points to multiple indices, one of the indices must be a write index.
          The rollover API creates a new write index for the alias with <code>is_write_index</code> set to <code>true</code>.
          The API also <code>sets is_write_index</code> to <code>false</code> for the previous write index.</p>
          <p><strong>Roll over an index alias with one index</strong></p>
          <p>If you roll over an index alias that points to only one index, the API creates a new index for the alias and removes the original index from the alias.</p>
          <p>NOTE: A rollover creates a new index and is subject to the <code>wait_for_active_shards</code> setting.</p>
          <p><strong>Increment index names for an alias</strong></p>
          <p>When you roll over an index alias, you can specify a name for the new index.
          If you don't specify a name and the current index ends with <code>-</code> and a number, such as <code>my-index-000001</code> or <code>my-index-3</code>, the new index name increments that number.
          For example, if you roll over an alias with a current index of <code>my-index-000001</code>, the rollover creates a new index named <code>my-index-000002</code>.
          This number is always six characters and zero-padded, regardless of the previous index's name.</p>
          <p>If you use an index alias for time series data, you can use date math in the index name to track the rollover date.
          For example, you can create an alias that points to an index named <code>&lt;my-index-{now/d}-000001&gt;</code>.
          If you create the index on May 6, 2099, the index's name is <code>my-index-2099.05.06-000001</code>.
          If you roll over the alias on May 7, 2099, the new index's name is <code>my-index-2099.05.07-000002</code>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-rollover>`_

        :param alias: Name of the data stream or index alias to roll over.
        :param new_index: Name of the index to create. Supports date math. Data streams
            do not support this parameter.
        :param aliases: Aliases for the target index. Data streams do not support this
            parameter.
        :param conditions: Conditions for the rollover. If specified, Elasticsearch only
            performs the rollover if the current index satisfies these conditions. If
            this parameter is not specified, Elasticsearch performs the rollover unconditionally.
            If conditions are specified, at least one of them must be a `max_*` condition.
            The index will rollover if any `max_*` condition is satisfied and all `min_*`
            conditions are satisfied.
        :param dry_run: If `true`, checks whether the current index satisfies the specified
            conditions but does not perform a rollover.
        :param lazy: If set to true, the rollover action will only mark a data stream
            to signal that it needs to be rolled over at the next write. Only allowed
            on data streams.
        :param mappings: Mapping for fields in the index. If specified, this mapping
            can include field names, field data types, and mapping paramaters.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param settings: Configuration options for the index. Data streams do not support
            this parameter.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to all or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`).
        """
        if alias in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'alias'")
        __path_parts: t.Dict[str, str]
        if alias not in SKIP_IN_PATH and new_index not in SKIP_IN_PATH:
            __path_parts = {"alias": _quote(alias), "new_index": _quote(new_index)}
            __path = f'/{__path_parts["alias"]}/_rollover/{__path_parts["new_index"]}'
        elif alias not in SKIP_IN_PATH:
            __path_parts = {"alias": _quote(alias)}
            __path = f'/{__path_parts["alias"]}/_rollover'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if dry_run is not None:
            __query["dry_run"] = dry_run
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if lazy is not None:
            __query["lazy"] = lazy
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        if not __body:
            if aliases is not None:
                __body["aliases"] = aliases
            if conditions is not None:
                __body["conditions"] = conditions
            if mappings is not None:
                __body["mappings"] = mappings
            if settings is not None:
                __body["settings"] = settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.rollover",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def segments(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get index segments.
          Get low-level information about the Lucene segments in index shards.
          For data streams, the API returns information about the stream's backing indices.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-segments>`_

        :param index: Comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (`*`). To target all data streams
            and indices, omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_segments'
        else:
            __path_parts = {}
            __path = "/_segments"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.segments",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def shard_stores(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        status: t.Optional[
            t.Union[
                t.Sequence[t.Union[str, t.Literal["all", "green", "red", "yellow"]]],
                t.Union[str, t.Literal["all", "green", "red", "yellow"]],
            ]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get index shard stores.
          Get store information about replica shards in one or more indices.
          For data streams, the API retrieves store information for the stream's backing indices.</p>
          <p>The index shard stores API returns the following information:</p>
          <ul>
          <li>The node on which each replica shard exists.</li>
          <li>The allocation ID for each replica shard.</li>
          <li>A unique ID for each replica shard.</li>
          <li>Any errors encountered while opening the shard index or from an earlier failure.</li>
          </ul>
          <p>By default, the API returns store information only for primary shards that are unassigned or have one or more unassigned replica shards.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shard-stores>`_

        :param index: List of data streams, indices, and aliases used to limit the request.
        :param allow_no_indices: If false, the request returns an error if any wildcard
            expression, index alias, or _all value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams.
        :param ignore_unavailable: If true, missing or closed indices are not included
            in the response.
        :param status: List of shard health statuses used to limit the request.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_shard_stores'
        else:
            __path_parts = {}
            __path = "/_shard_stores"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if pretty is not None:
            __query["pretty"] = pretty
        if status is not None:
            __query["status"] = status
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.shard_stores",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("aliases", "settings"),
    )
    def shrink(
        self,
        *,
        index: str,
        target: str,
        aliases: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Shrink an index.
          Shrink an index into a new index with fewer primary shards.</p>
          <p>Before you can shrink an index:</p>
          <ul>
          <li>The index must be read-only.</li>
          <li>A copy of every shard in the index must reside on the same node.</li>
          <li>The index must have a green health status.</li>
          </ul>
          <p>To make shard allocation easier, we recommend you also remove the index's replica shards.
          You can later re-add replica shards as part of the shrink operation.</p>
          <p>The requested number of primary shards in the target index must be a factor of the number of shards in the source index.
          For example an index with 8 primary shards can be shrunk into 4, 2 or 1 primary shards or an index with 15 primary shards can be shrunk into 5, 3 or 1.
          If the number of shards in the index is a prime number it can only be shrunk into a single primary shard
          Before shrinking, a (primary or replica) copy of every shard in the index must be present on the same node.</p>
          <p>The current write index on a data stream cannot be shrunk. In order to shrink the current write index, the data stream must first be rolled over so that a new write index is created and then the previous write index can be shrunk.</p>
          <p>A shrink operation:</p>
          <ul>
          <li>Creates a new target index with the same definition as the source index, but with a smaller number of primary shards.</li>
          <li>Hard-links segments from the source index into the target index. If the file system does not support hard-linking, then all segments are copied into the new index, which is a much more time consuming process. Also if using multiple data paths, shards on different data paths require a full copy of segment files if they are not on the same disk since hardlinks do not work across disks.</li>
          <li>Recovers the target index as though it were a closed index which had just been re-opened. Recovers shards to the <code>.routing.allocation.initial_recovery._id</code> index setting.</li>
          </ul>
          <p>IMPORTANT: Indices can only be shrunk if they satisfy the following requirements:</p>
          <ul>
          <li>The target index must not exist.</li>
          <li>The source index must have more primary shards than the target index.</li>
          <li>The number of primary shards in the target index must be a factor of the number of primary shards in the source index. The source index must have more primary shards than the target index.</li>
          <li>The index must not contain more than 2,147,483,519 documents in total across all shards that will be shrunk into a single shard on the target index as this is the maximum number of docs that can fit into a single shard.</li>
          <li>The node handling the shrink process must have sufficient free disk space to accommodate a second copy of the existing index.</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-shrink>`_

        :param index: Name of the source index to shrink.
        :param target: Name of the target index to create.
        :param aliases: The key is the alias name. Index alias names support date math.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param settings: Configuration options for the target index.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`).
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if target in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'target'")
        __path_parts: t.Dict[str, str] = {
            "index": _quote(index),
            "target": _quote(target),
        }
        __path = f'/{__path_parts["index"]}/_shrink/{__path_parts["target"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        if not __body:
            if aliases is not None:
                __body["aliases"] = aliases
            if settings is not None:
                __body["settings"] = settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.shrink",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def simulate_index_template(
        self,
        *,
        name: str,
        cause: t.Optional[str] = None,
        create: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        include_defaults: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Simulate an index.
          Get the index configuration that would be applied to the specified index from an existing index template.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-simulate-index-template>`_

        :param name: Name of the index to simulate
        :param cause: User defined reason for dry-run creating the new template for simulation
            purposes
        :param create: Whether the index template we optionally defined in the body should
            only be dry-run added if new or can also replace an existing one
        :param include_defaults: If true, returns all relevant default configurations
            for the index template.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        """
        if name in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'name'")
        __path_parts: t.Dict[str, str] = {"name": _quote(name)}
        __path = f'/_index_template/_simulate_index/{__path_parts["name"]}'
        __query: t.Dict[str, t.Any] = {}
        if cause is not None:
            __query["cause"] = cause
        if create is not None:
            __query["create"] = create
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.simulate_index_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "allow_auto_create",
            "composed_of",
            "data_stream",
            "deprecated",
            "ignore_missing_component_templates",
            "index_patterns",
            "meta",
            "priority",
            "template",
            "version",
        ),
        parameter_aliases={"_meta": "meta"},
    )
    def simulate_template(
        self,
        *,
        name: t.Optional[str] = None,
        allow_auto_create: t.Optional[bool] = None,
        cause: t.Optional[str] = None,
        composed_of: t.Optional[t.Sequence[str]] = None,
        create: t.Optional[bool] = None,
        data_stream: t.Optional[t.Mapping[str, t.Any]] = None,
        deprecated: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_missing_component_templates: t.Optional[t.Sequence[str]] = None,
        include_defaults: t.Optional[bool] = None,
        index_patterns: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        meta: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        priority: t.Optional[int] = None,
        template: t.Optional[t.Mapping[str, t.Any]] = None,
        version: t.Optional[int] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Simulate an index template.
          Get the index configuration that would be applied by a particular index template.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-simulate-template>`_

        :param name: Name of the index template to simulate. To test a template configuration
            before you add it to the cluster, omit this parameter and specify the template
            configuration in the request body.
        :param allow_auto_create: This setting overrides the value of the `action.auto_create_index`
            cluster setting. If set to `true` in a template, then indices can be automatically
            created using that template even if auto-creation of indices is disabled
            via `actions.auto_create_index`. If set to `false`, then indices or data
            streams matching the template must always be explicitly created, and may
            never be automatically created.
        :param cause: User defined reason for dry-run creating the new template for simulation
            purposes
        :param composed_of: An ordered list of component template names. Component templates
            are merged in the order specified, meaning that the last component template
            specified has the highest precedence.
        :param create: If true, the template passed in the body is only used if no existing
            templates match the same index patterns. If false, the simulation uses the
            template with the highest priority. Note that the template is not permanently
            added or updated in either case; it is only used for the simulation.
        :param data_stream: If this object is included, the template is used to create
            data streams and their backing indices. Supports an empty object. Data streams
            require a matching index template with a `data_stream` object.
        :param deprecated: Marks this index template as deprecated. When creating or
            updating a non-deprecated index template that uses deprecated components,
            Elasticsearch will emit a deprecation warning.
        :param ignore_missing_component_templates: The configuration option ignore_missing_component_templates
            can be used when an index template references a component template that might
            not exist
        :param include_defaults: If true, returns all relevant default configurations
            for the index template.
        :param index_patterns: Array of wildcard (`*`) expressions used to match the
            names of data streams and indices during creation.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param meta: Optional user metadata about the index template. May have any contents.
            This map is not automatically generated by Elasticsearch.
        :param priority: Priority to determine index template precedence when a new data
            stream or index is created. The index template with the highest priority
            is chosen. If no priority is specified the template is treated as though
            it is of priority 0 (lowest priority). This number is not automatically generated
            by Elasticsearch.
        :param template: Template to be applied. It may optionally include an `aliases`,
            `mappings`, or `settings` configuration.
        :param version: Version number used to manage index templates externally. This
            number is not automatically generated by Elasticsearch.
        """
        __path_parts: t.Dict[str, str]
        if name not in SKIP_IN_PATH:
            __path_parts = {"name": _quote(name)}
            __path = f'/_index_template/_simulate/{__path_parts["name"]}'
        else:
            __path_parts = {}
            __path = "/_index_template/_simulate"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if cause is not None:
            __query["cause"] = cause
        if create is not None:
            __query["create"] = create
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if include_defaults is not None:
            __query["include_defaults"] = include_defaults
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if allow_auto_create is not None:
                __body["allow_auto_create"] = allow_auto_create
            if composed_of is not None:
                __body["composed_of"] = composed_of
            if data_stream is not None:
                __body["data_stream"] = data_stream
            if deprecated is not None:
                __body["deprecated"] = deprecated
            if ignore_missing_component_templates is not None:
                __body["ignore_missing_component_templates"] = (
                    ignore_missing_component_templates
                )
            if index_patterns is not None:
                __body["index_patterns"] = index_patterns
            if meta is not None:
                __body["_meta"] = meta
            if priority is not None:
                __body["priority"] = priority
            if template is not None:
                __body["template"] = template
            if version is not None:
                __body["version"] = version
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.simulate_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("aliases", "settings"),
    )
    def split(
        self,
        *,
        index: str,
        target: str,
        aliases: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        settings: t.Optional[t.Mapping[str, t.Any]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Split an index.
          Split an index into a new index with more primary shards.</p>
          <ul>
          <li>
          <p>Before you can split an index:</p>
          </li>
          <li>
          <p>The index must be read-only.</p>
          </li>
          <li>
          <p>The cluster health status must be green.</p>
          </li>
          </ul>
          <p>You can do make an index read-only with the following request using the add index block API:</p>
          <pre><code>PUT /my_source_index/_block/write
          </code></pre>
          <p>The current write index on a data stream cannot be split.
          In order to split the current write index, the data stream must first be rolled over so that a new write index is created and then the previous write index can be split.</p>
          <p>The number of times the index can be split (and the number of shards that each original shard can be split into) is determined by the <code>index.number_of_routing_shards</code> setting.
          The number of routing shards specifies the hashing space that is used internally to distribute documents across shards with consistent hashing.
          For instance, a 5 shard index with <code>number_of_routing_shards</code> set to 30 (5 x 2 x 3) could be split by a factor of 2 or 3.</p>
          <p>A split operation:</p>
          <ul>
          <li>Creates a new target index with the same definition as the source index, but with a larger number of primary shards.</li>
          <li>Hard-links segments from the source index into the target index. If the file system doesn't support hard-linking, all segments are copied into the new index, which is a much more time consuming process.</li>
          <li>Hashes all documents again, after low level files are created, to delete documents that belong to a different shard.</li>
          <li>Recovers the target index as though it were a closed index which had just been re-opened.</li>
          </ul>
          <p>IMPORTANT: Indices can only be split if they satisfy the following requirements:</p>
          <ul>
          <li>The target index must not exist.</li>
          <li>The source index must have fewer primary shards than the target index.</li>
          <li>The number of primary shards in the target index must be a multiple of the number of primary shards in the source index.</li>
          <li>The node handling the split process must have sufficient free disk space to accommodate a second copy of the existing index.</li>
          </ul>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-split>`_

        :param index: Name of the source index to split.
        :param target: Name of the target index to create.
        :param aliases: Aliases for the resulting index.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param settings: Configuration options for the target index.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`).
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if target in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'target'")
        __path_parts: t.Dict[str, str] = {
            "index": _quote(index),
            "target": _quote(target),
        }
        __path = f'/{__path_parts["index"]}/_split/{__path_parts["target"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        if not __body:
            if aliases is not None:
                __body["aliases"] = aliases
            if settings is not None:
                __body["settings"] = settings
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.split",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def stats(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        metric: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        completion_fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        fielddata_fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        forbid_closed_indices: t.Optional[bool] = None,
        groups: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        include_segment_file_sizes: t.Optional[bool] = None,
        include_unloaded_segments: t.Optional[bool] = None,
        level: t.Optional[
            t.Union[str, t.Literal["cluster", "indices", "shards"]]
        ] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get index statistics.
          For data streams, the API retrieves statistics for the stream's backing indices.</p>
          <p>By default, the returned statistics are index-level with <code>primaries</code> and <code>total</code> aggregations.
          <code>primaries</code> are the values for only the primary shards.
          <code>total</code> are the accumulated values for both primary and replica shards.</p>
          <p>To get shard-level statistics, set the <code>level</code> parameter to <code>shards</code>.</p>
          <p>NOTE: When moving to another node, the shard-level statistics for a shard are cleared.
          Although the shard is no longer part of the node, that node retains any node-level statistics to which the shard contributed.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-stats>`_

        :param index: A comma-separated list of index names; use `_all` or empty string
            to perform the operation on all indices
        :param metric: Limit the information returned the specific metrics.
        :param completion_fields: Comma-separated list or wildcard expressions of fields
            to include in fielddata and suggest statistics.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param fielddata_fields: Comma-separated list or wildcard expressions of fields
            to include in fielddata statistics.
        :param fields: Comma-separated list or wildcard expressions of fields to include
            in the statistics.
        :param forbid_closed_indices: If true, statistics are not collected from closed
            indices.
        :param groups: Comma-separated list of search groups to include in the search
            statistics.
        :param include_segment_file_sizes: If true, the call reports the aggregated disk
            usage of each one of the Lucene index files (only applies if segment stats
            are requested).
        :param include_unloaded_segments: If true, the response includes information
            from segments that are not loaded into memory.
        :param level: Indicates whether statistics are aggregated at the cluster, index,
            or shard level.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH and metric not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index), "metric": _quote(metric)}
            __path = f'/{__path_parts["index"]}/_stats/{__path_parts["metric"]}'
        elif index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_stats'
        elif metric not in SKIP_IN_PATH:
            __path_parts = {"metric": _quote(metric)}
            __path = f'/_stats/{__path_parts["metric"]}'
        else:
            __path_parts = {}
            __path = "/_stats"
        __query: t.Dict[str, t.Any] = {}
        if completion_fields is not None:
            __query["completion_fields"] = completion_fields
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if fielddata_fields is not None:
            __query["fielddata_fields"] = fielddata_fields
        if fields is not None:
            __query["fields"] = fields
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if forbid_closed_indices is not None:
            __query["forbid_closed_indices"] = forbid_closed_indices
        if groups is not None:
            __query["groups"] = groups
        if human is not None:
            __query["human"] = human
        if include_segment_file_sizes is not None:
            __query["include_segment_file_sizes"] = include_segment_file_sizes
        if include_unloaded_segments is not None:
            __query["include_unloaded_segments"] = include_unloaded_segments
        if level is not None:
            __query["level"] = level
        if pretty is not None:
            __query["pretty"] = pretty
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="indices.stats",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("actions",),
    )
    def update_aliases(
        self,
        *,
        actions: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update an alias.
          Adds a data stream or index to an alias.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-update-aliases>`_

        :param actions: Actions to perform.
        :param master_timeout: Period to wait for a connection to the master node. If
            no response is received before the timeout expires, the request fails and
            returns an error.
        :param timeout: Period to wait for a response. If no response is received before
            the timeout expires, the request fails and returns an error.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_aliases"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if master_timeout is not None:
            __query["master_timeout"] = master_timeout
        if pretty is not None:
            __query["pretty"] = pretty
        if timeout is not None:
            __query["timeout"] = timeout
        if not __body:
            if actions is not None:
                __body["actions"] = actions
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.update_aliases",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("query",),
    )
    def validate_query(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        all_shards: t.Optional[bool] = None,
        allow_no_indices: t.Optional[bool] = None,
        analyze_wildcard: t.Optional[bool] = None,
        analyzer: t.Optional[str] = None,
        default_operator: t.Optional[t.Union[str, t.Literal["and", "or"]]] = None,
        df: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        expand_wildcards: t.Optional[
            t.Union[
                t.Sequence[
                    t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]]
                ],
                t.Union[str, t.Literal["all", "closed", "hidden", "none", "open"]],
            ]
        ] = None,
        explain: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        lenient: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        q: t.Optional[str] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        rewrite: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Validate a query.
          Validates a query without running it.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-indices-validate-query>`_

        :param index: Comma-separated list of data streams, indices, and aliases to search.
            Supports wildcards (`*`). To search all data streams or indices, omit this
            parameter or use `*` or `_all`.
        :param all_shards: If `true`, the validation is executed on all shards instead
            of one random shard per index.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices.
        :param analyze_wildcard: If `true`, wildcard and prefix queries are analyzed.
        :param analyzer: Analyzer to use for the query string. This parameter can only
            be used when the `q` query string parameter is specified.
        :param default_operator: The default operator for query string query: `AND` or
            `OR`.
        :param df: Field to use as default where no field prefix is given in the query
            string. This parameter can only be used when the `q` query string parameter
            is specified.
        :param expand_wildcards: Type of index that wildcard patterns can match. If the
            request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams. Supports comma-separated values, such
            as `open,hidden`.
        :param explain: If `true`, the response returns detailed information if an error
            has occurred.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param lenient: If `true`, format-based query failures (such as providing text
            to a numeric field) in the query string will be ignored.
        :param q: Query in the Lucene query string syntax.
        :param query: Query in the Lucene query string syntax.
        :param rewrite: If `true`, returns a more detailed explanation showing the actual
            Lucene query that will be executed.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_validate/query'
        else:
            __path_parts = {}
            __path = "/_validate/query"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if all_shards is not None:
            __query["all_shards"] = all_shards
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if analyze_wildcard is not None:
            __query["analyze_wildcard"] = analyze_wildcard
        if analyzer is not None:
            __query["analyzer"] = analyzer
        if default_operator is not None:
            __query["default_operator"] = default_operator
        if df is not None:
            __query["df"] = df
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if explain is not None:
            __query["explain"] = explain
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if lenient is not None:
            __query["lenient"] = lenient
        if pretty is not None:
            __query["pretty"] = pretty
        if q is not None:
            __query["q"] = q
        if rewrite is not None:
            __query["rewrite"] = rewrite
        if not __body:
            if query is not None:
                __body["query"] = query
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="indices.validate_query",
            path_parts=__path_parts,
        )
