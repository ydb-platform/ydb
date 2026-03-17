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

from typing import (
    Any,
    Collection,
    Dict,
    Mapping,
    MutableMapping,
    Optional,
    Tuple,
    Union,
)

from .utils import NamespacedClient

class AsyncSearchClient(NamespacedClient):
    def delete(
        self,
        *,
        id: Any,
        pretty: Optional[bool] = ...,
        human: Optional[bool] = ...,
        error_trace: Optional[bool] = ...,
        format: Optional[str] = ...,
        filter_path: Optional[Union[str, Collection[str]]] = ...,
        request_timeout: Optional[Union[int, float]] = ...,
        ignore: Optional[Union[int, Collection[int]]] = ...,
        opaque_id: Optional[str] = ...,
        http_auth: Optional[Union[str, Tuple[str, str]]] = ...,
        api_key: Optional[Union[str, Tuple[str, str]]] = ...,
        params: Optional[MutableMapping[str, Any]] = ...,
        headers: Optional[MutableMapping[str, str]] = ...,
    ) -> Dict[str, Any]: ...
    def get(
        self,
        *,
        id: Any,
        keep_alive: Optional[Any] = ...,
        typed_keys: Optional[bool] = ...,
        wait_for_completion_timeout: Optional[Any] = ...,
        pretty: Optional[bool] = ...,
        human: Optional[bool] = ...,
        error_trace: Optional[bool] = ...,
        format: Optional[str] = ...,
        filter_path: Optional[Union[str, Collection[str]]] = ...,
        request_timeout: Optional[Union[int, float]] = ...,
        ignore: Optional[Union[int, Collection[int]]] = ...,
        opaque_id: Optional[str] = ...,
        http_auth: Optional[Union[str, Tuple[str, str]]] = ...,
        api_key: Optional[Union[str, Tuple[str, str]]] = ...,
        params: Optional[MutableMapping[str, Any]] = ...,
        headers: Optional[MutableMapping[str, str]] = ...,
    ) -> Dict[str, Any]: ...
    def submit(
        self,
        *,
        body: Optional[Mapping[str, Any]] = ...,
        index: Optional[Any] = ...,
        _source: Optional[Any] = ...,
        _source_excludes: Optional[Any] = ...,
        _source_includes: Optional[Any] = ...,
        allow_no_indices: Optional[bool] = ...,
        allow_partial_search_results: Optional[bool] = ...,
        analyze_wildcard: Optional[bool] = ...,
        analyzer: Optional[Any] = ...,
        batched_reduce_size: Optional[Any] = ...,
        default_operator: Optional[Any] = ...,
        df: Optional[Any] = ...,
        docvalue_fields: Optional[Any] = ...,
        expand_wildcards: Optional[Any] = ...,
        explain: Optional[bool] = ...,
        from_: Optional[Any] = ...,
        ignore_throttled: Optional[bool] = ...,
        ignore_unavailable: Optional[bool] = ...,
        keep_alive: Optional[Any] = ...,
        keep_on_completion: Optional[bool] = ...,
        lenient: Optional[bool] = ...,
        max_concurrent_shard_requests: Optional[Any] = ...,
        preference: Optional[Any] = ...,
        q: Optional[Any] = ...,
        request_cache: Optional[bool] = ...,
        routing: Optional[Any] = ...,
        search_type: Optional[Any] = ...,
        seq_no_primary_term: Optional[bool] = ...,
        size: Optional[Any] = ...,
        sort: Optional[Any] = ...,
        stats: Optional[Any] = ...,
        stored_fields: Optional[Any] = ...,
        suggest_field: Optional[Any] = ...,
        suggest_mode: Optional[Any] = ...,
        suggest_size: Optional[Any] = ...,
        suggest_text: Optional[Any] = ...,
        terminate_after: Optional[Any] = ...,
        timeout: Optional[Any] = ...,
        track_scores: Optional[bool] = ...,
        track_total_hits: Optional[bool] = ...,
        typed_keys: Optional[bool] = ...,
        version: Optional[bool] = ...,
        wait_for_completion_timeout: Optional[Any] = ...,
        pretty: Optional[bool] = ...,
        human: Optional[bool] = ...,
        error_trace: Optional[bool] = ...,
        format: Optional[str] = ...,
        filter_path: Optional[Union[str, Collection[str]]] = ...,
        request_timeout: Optional[Union[int, float]] = ...,
        ignore: Optional[Union[int, Collection[int]]] = ...,
        opaque_id: Optional[str] = ...,
        http_auth: Optional[Union[str, Tuple[str, str]]] = ...,
        api_key: Optional[Union[str, Tuple[str, str]]] = ...,
        params: Optional[MutableMapping[str, Any]] = ...,
        headers: Optional[MutableMapping[str, str]] = ...,
    ) -> Dict[str, Any]: ...
    def status(
        self,
        *,
        id: Any,
        pretty: Optional[bool] = ...,
        human: Optional[bool] = ...,
        error_trace: Optional[bool] = ...,
        format: Optional[str] = ...,
        filter_path: Optional[Union[str, Collection[str]]] = ...,
        request_timeout: Optional[Union[int, float]] = ...,
        ignore: Optional[Union[int, Collection[int]]] = ...,
        opaque_id: Optional[str] = ...,
        http_auth: Optional[Union[str, Tuple[str, str]]] = ...,
        api_key: Optional[Union[str, Tuple[str, str]]] = ...,
        params: Optional[MutableMapping[str, Any]] = ...,
        headers: Optional[MutableMapping[str, str]] = ...,
    ) -> Dict[str, Any]: ...
