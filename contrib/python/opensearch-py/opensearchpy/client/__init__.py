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


# ------------------------------------------------------------------------------------------
# THIS CODE IS AUTOMATICALLY GENERATED AND MANUAL EDITS WILL BE LOST
#
# To contribute, kindly make modifications in the opensearch-py client generator
# or in the OpenSearch API specification, and run `nox -rs generate`. See DEVELOPER_GUIDE.md
# and https://github.com/opensearch-project/opensearch-api-specification for details.
# -----------------------------------------------------------------------------------------+


import logging
from typing import Any, Type

from ..transport import Transport, TransportError
from .cat import CatClient
from .client import Client
from .cluster import ClusterClient
from .dangling_indices import DanglingIndicesClient
from .features import FeaturesClient
from .http import HttpClient
from .indices import IndicesClient
from .ingest import IngestClient
from .ingestion import IngestionClient
from .insights import InsightsClient
from .list import ListClient
from .nodes import NodesClient
from .plugins import PluginsClient
from .remote import RemoteClient
from .remote_store import RemoteStoreClient
from .search_pipeline import SearchPipelineClient
from .security import SecurityClient
from .snapshot import SnapshotClient
from .tasks import TasksClient
from .utils import SKIP_IN_PATH, _bulk_body, _make_path, query_params
from .wlm import WlmClient

logger = logging.getLogger("opensearch")


class OpenSearch(Client):
    """
    OpenSearch client. Provides a straightforward mapping from
    Python to OpenSearch REST endpoints.

    The instance has attributes ``cat``, ``cluster``, ``indices``, ``ingest``,
    ``nodes``, ``snapshot`` and ``tasks`` that provide access to instances of
    :class:`~opensearchpy.client.CatClient`,
    :class:`~opensearchpy.client.ClusterClient`,
    :class:`~opensearchpy.client.IndicesClient`,
    :class:`~opensearchpy.client.IngestClient`,
    :class:`~opensearchpy.client.NodesClient`,
    :class:`~opensearchpy.client.SnapshotClient` and
    :class:`~opensearchpy.client.TasksClient` respectively. This is the
    preferred (and only supported) way to get access to those classes and their
    methods.

    You can specify your own connection class which should be used by providing
    the ``connection_class`` parameter::

        # create connection to localhost using the ThriftConnection
        client = OpenSearch(connection_class=ThriftConnection)

    If you want to turn on sniffing you have several options (described
    in :class:`~opensearchpy.Transport`)::

        # create connection that will automatically inspect the cluster to get
        # the list of active nodes. Start with nodes running on
        # 'opensearchnode1' and 'opensearchnode2'
        client = OpenSearch(
            ['opensearchnode1', 'opensearchnode2'],
            # sniff before doing anything
            sniff_on_start=True,
            # refresh nodes after a node fails to respond
            sniff_on_connection_fail=True,
            # and also every 60 seconds
            sniffer_timeout=60
        )

    Different hosts can have different parameters, use a dictionary per node to
    specify those::

        # connect to localhost directly and another node using SSL on port 443
        # and an url_prefix. Note that ``port`` needs to be an int.
        client = OpenSearch([
            {'host': 'localhost'},
            {'host': 'othernode', 'port': 443, 'url_prefix': 'opensearch', 'use_ssl': True},
        ])

    If using SSL, there are several parameters that control how we deal with
    certificates (see :class:`~opensearchpy.AIOHttpConnection` for
    detailed description of the options)::

        client = OpenSearch(
            ['localhost:443', 'other_host:443'],
            # turn on SSL
            use_ssl=True,
            # make sure we verify SSL certificates
            verify_certs=True,
            # provide a path to CA certs on disk
            ca_certs='/path/to/CA_certs'
        )

    If using SSL, but don't verify the certs, a warning message is showed
    optionally (see :class:`~opensearchpy.AIOHttpConnection` for
    detailed description of the options)::

        client = OpenSearch(
            ['localhost:443', 'other_host:443'],
            # turn on SSL
            use_ssl=True,
            # no verify SSL certificates
            verify_certs=False,
            # don't verify the hostname in the certificate
            ssl_assert_hostname=False,
            # don't show warnings about ssl certs verification
            ssl_show_warn=False
        )

    SSL client authentication is supported
    (see :class:`~opensearchpy.AIOHttpConnection` for
    detailed description of the options)::

        client = OpenSearch(
            ['localhost:443', 'other_host:443'],
            # turn on SSL
            use_ssl=True,
            # make sure we verify SSL certificates
            verify_certs=True,
            # provide a path to CA certs on disk
            ca_certs='/path/to/CA_certs',
            # PEM formatted SSL client certificate
            client_cert='/path/to/clientcert.pem',
            # PEM formatted SSL client key
            client_key='/path/to/clientkey.pem'
        )

    Alternatively you can use RFC-1738 formatted URLs, as long as they are not
    in conflict with other options::

        client = OpenSearch(
            [
                'http://user:secret@localhost:9200/',
                'https://user:secret@other_host:443/production'
            ],
            verify_certs=True
        )

    By default, `JSONSerializer
    <https://github.com/opensearch-project/opensearch-py/blob/master/opensearch/serializer.py#L24>`_
    is used to encode all outgoing requests.
    However, you can implement your own custom serializer::

        from opensearchpy.serializer import JSONSerializer

        class SetEncoder(JSONSerializer):
            def default(self, obj):
                if isinstance(obj, set):
                    return list(obj)
                if isinstance(obj, Something):
                    return 'CustomSomethingRepresentation'
                return JSONSerializer.default(self, obj)

        client = OpenSearch(serializer=SetEncoder())

    """

    def __init__(
        self,
        hosts: Any = None,
        transport_class: Type[Transport] = Transport,
        **kwargs: Any,
    ) -> None:
        """
        :arg hosts: list of nodes, or a single node, we should connect to.
            Node should be a dictionary ({"host": "localhost", "port": 9200}),
            the entire dictionary will be passed to the :class:`~opensearchpy.Connection`
            class as kwargs, or a string in the format of ``host[:port]`` which will be
            translated to a dictionary automatically.  If no value is given the
            :class:`~opensearchpy.Connection` class defaults will be used.

        :arg transport_class: :class:`~opensearchpy.Transport` subclass to use.

        :arg kwargs: any additional arguments will be passed on to the
            :class:`~opensearchpy.Transport` class and, subsequently, to the
            :class:`~opensearchpy.Connection` instances.
        """
        super().__init__(hosts, transport_class, **kwargs)

        # namespaced clients for compatibility with API names
        self.ingestion = IngestionClient(self)
        self.wlm = WlmClient(self)
        self.list = ListClient(self)
        self.insights = InsightsClient(self)
        self.search_pipeline = SearchPipelineClient(self)
        self.cat = CatClient(self)
        self.cluster = ClusterClient(self)
        self.dangling_indices = DanglingIndicesClient(self)
        self.indices = IndicesClient(self)
        self.ingest = IngestClient(self)
        self.nodes = NodesClient(self)
        self.remote = RemoteClient(self)
        self.security = SecurityClient(self)
        self.snapshot = SnapshotClient(self)
        self.tasks = TasksClient(self)
        self.remote_store = RemoteStoreClient(self)

        self.features = FeaturesClient(self)
        self.plugins = PluginsClient(self)
        self.http = HttpClient(self)

    def __repr__(self) -> Any:
        try:
            # get a list of all connections
            cons: Any = self.transport.hosts
            # truncate to 5 if there are too many
            if len(cons) > 5:
                cons = cons[:5] + ["..."]
            return f"<{self.__class__.__name__}({cons})>"
        except Exception:
            # probably operating on custom transport and connection_pool, ignore
            return super().__repr__()

    def __enter__(self) -> Any:
        if hasattr(self.transport, "_async_call"):
            self.transport._async_call()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    def close(self) -> None:
        """Closes the Transport and all internal connections"""
        self.transport.close()

    # AUTO-GENERATED-API-DEFINITIONS #
    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def ping(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns whether the cluster is running.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        try:
            return self.transport.perform_request(
                "HEAD", "/", params=params, headers=headers
            )
        except TransportError:
            return False

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def info(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns basic information about the cluster.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", "/", params=params, headers=headers
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "pipeline",
        "pretty",
        "refresh",
        "routing",
        "source",
        "timeout",
        "version",
        "version_type",
        "wait_for_active_shards",
    )
    def create(
        self,
        *,
        index: Any,
        id: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates a new document in the index.  Returns a 409 response when a document
        with a same ID already exists in the index.


        :arg index: Name of the data stream or index to target. If the
            target doesn't exist and matches the name or wildcard (`*`) pattern of
            an index template with a `data_stream` definition, this request creates
            the data stream. If the target doesn't exist and doesn't match a data
            stream template, this request creates the index.
        :arg id: The unique identifier for the document.
        :arg body: The document
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pipeline: ID of the pipeline to use to preprocess incoming
            documents. If the index has a default ingest pipeline specified, then
            setting the value to `_none` disables the default ingest pipeline for
            this request. If a final pipeline is configured it will always run,
            regardless of the value of this parameter.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg refresh: If `true`, OpenSearch refreshes the affected
            shards to make this operation visible to search, if `wait_for` then wait
            for a refresh to make this operation visible to search, if `false` do
            nothing with refreshes. Valid values: `true`, `false`, `wait_for`.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period the request waits for the following
            operations: automatic index creation, dynamic mapping updates, waiting
            for active shards.
        :arg version: Explicit version number for concurrency control.
            The specified version must match the current version of the document for
            the request to succeed.
        :arg version_type: The specific version type: `external`,
            `external_gte`. Valid choices are external, external_gte, force,
            internal.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        """
        for param in (index, id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        path = _make_path(index, "_create", id)

        return self.transport.perform_request(
            "PUT", path, params=params, headers=headers, body=body
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "if_primary_term",
        "if_seq_no",
        "op_type",
        "pipeline",
        "pretty",
        "refresh",
        "require_alias",
        "routing",
        "source",
        "timeout",
        "version",
        "version_type",
        "wait_for_active_shards",
    )
    def index(
        self,
        *,
        index: Any,
        body: Any,
        id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates or updates a document in an index.


        :arg index: Name of the data stream or index to target.
        :arg body: The document
        :arg id: The unique identifier for the document.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg if_primary_term: Only perform the operation if the document
            has this primary term.
        :arg if_seq_no: Only perform the operation if the document has
            this sequence number.
        :arg op_type: Set to create to only index the document if it
            does not already exist (put if absent). If a document with the specified
            `_id` already exists, the indexing operation will fail. Same as using
            the `<index>/_create` endpoint. Valid values: `index`, `create`. If
            document id is specified, it defaults to `index`. Otherwise, it defaults
            to `create`.
        :arg pipeline: ID of the pipeline to use to preprocess incoming
            documents. If the index has a default ingest pipeline specified, then
            setting the value to `_none` disables the default ingest pipeline for
            this request. If a final pipeline is configured it will always run,
            regardless of the value of this parameter.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg refresh: If `true`, OpenSearch refreshes the affected
            shards to make this operation visible to search, if `wait_for` then wait
            for a refresh to make this operation visible to search, if `false` do
            nothing with refreshes. Valid values: `true`, `false`, `wait_for`.
        :arg require_alias: If `true`, the destination must be an index
            alias. Default is false.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period the request waits for the following
            operations: automatic index creation, dynamic mapping updates, waiting
            for active shards.
        :arg version: Explicit version number for concurrency control.
            The specified version must match the current version of the document for
            the request to succeed.
        :arg version_type: The specific version type: `external`,
            `external_gte`. Valid choices are external, external_gte, force,
            internal.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to all or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        """
        for param in (index, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "POST" if id in SKIP_IN_PATH else "PUT",
            _make_path(index, "_doc", id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "error_trace",
        "filter_path",
        "human",
        "pipeline",
        "pretty",
        "refresh",
        "require_alias",
        "routing",
        "source",
        "timeout",
        "wait_for_active_shards",
    )
    def bulk(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to perform multiple index/update/delete operations in a single request.


        :arg body: The operation definition and data (action-data
            pairs), separated by newlines
        :arg index: Name of the data stream, index, or index alias to
            perform bulk actions on.
        :arg _source: `true` or `false` to return the `_source` field or
            not, or a list of fields to return.
        :arg _source_excludes: A comma-separated list of source fields
            to exclude from the response.
        :arg _source_includes: A comma-separated list of source fields
            to include in the response.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pipeline: ID of the pipeline to use to preprocess incoming
            documents. If the index has a default ingest pipeline specified, then
            setting the value to `_none` disables the default ingest pipeline for
            this request. If a final pipeline is configured it will always run,
            regardless of the value of this parameter.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg refresh: If `true`, OpenSearch refreshes the affected
            shards to make this operation visible to search, if `wait_for` then wait
            for a refresh to make this operation visible to search, if `false` do
            nothing with refreshes. Valid values: `true`, `false`, `wait_for`.
        :arg require_alias: If `true`, the request's actions must target
            an index alias. Default is false.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period each action waits for the following
            operations: automatic index creation, dynamic mapping updates, waiting
            for active shards.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to all or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        body = _bulk_body(self.transport.serializer, body)
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_bulk"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def clear_scroll(
        self,
        *,
        body: Any = None,
        scroll_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Explicitly clears the search context for a scroll.


        :arg body: A comma-separated list of scroll IDs to clear if none
            was specified using the `scroll_id` parameter
        :arg scroll_id: A comma-separated list of scroll IDs to clear.
            To clear all scroll IDs, use `_all`.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if scroll_id in SKIP_IN_PATH and body in SKIP_IN_PATH:
            raise ValueError("You need to supply scroll_id or body.")
        elif scroll_id and not body:
            body = {"scroll_id": [scroll_id]}
        elif scroll_id:
            params["scroll_id"] = scroll_id

        return self.transport.perform_request(
            "DELETE", "/_search/scroll", params=params, headers=headers, body=body
        )

    @query_params(
        "allow_no_indices",
        "analyze_wildcard",
        "analyzer",
        "default_operator",
        "df",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_throttled",
        "ignore_unavailable",
        "lenient",
        "min_score",
        "preference",
        "pretty",
        "q",
        "routing",
        "source",
        "terminate_after",
    )
    def count(
        self,
        *,
        body: Any = None,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns number of documents matching a query.


        :arg body: Query to restrict the results specified with the
            Query DSL (optional)
        :arg index: A comma-separated list of data streams, indexes, and
            aliases to search. Supports wildcards (`*`). To search all data streams
            and indexes, omit this parameter or use `*` or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes.
        :arg analyze_wildcard: If `true`, wildcard and prefix queries
            are analyzed. This parameter can only be used when the `q` query string
            parameter is specified. Default is false.
        :arg analyzer: Analyzer to use for the query string. This
            parameter can only be used when the `q` query string parameter is
            specified.
        :arg default_operator: The default operator for query string
            query: `AND` or `OR`. This parameter can only be used when the `q` query
            string parameter is specified. Valid choices are and, or.
        :arg df: Field to use as default where no field prefix is given
            in the query string. This parameter can only be used when the `q` query
            string parameter is specified.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Specifies the type of index that wildcard
            expressions can match. Supports comma-separated values. Valid choices
            are all, closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_throttled: If `true`, concrete, expanded or aliased
            indexes are ignored when frozen.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg lenient: If `true`, format-based query failures (such as
            providing text to a numeric field) in the query string will be ignored.
        :arg min_score: Sets the minimum `_score` value that documents
            must have to be included in the result.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg q: Query in the Lucene query string syntax.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg terminate_after: Maximum number of documents to collect for
            each shard. If a query reaches this limit, OpenSearch terminates the
            query early. OpenSearch collects documents before sorting.
        """
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_count"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "if_primary_term",
        "if_seq_no",
        "pretty",
        "refresh",
        "routing",
        "source",
        "timeout",
        "version",
        "version_type",
        "wait_for_active_shards",
    )
    def delete(
        self,
        *,
        index: Any,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Removes a document from the index.


        :arg index: Name of the target index.
        :arg id: The unique identifier for the document.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg if_primary_term: Only perform the operation if the document
            has this primary term.
        :arg if_seq_no: Only perform the operation if the document has
            this sequence number.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg refresh: If `true`, OpenSearch refreshes the affected
            shards to make this operation visible to search, if `wait_for` then wait
            for a refresh to make this operation visible to search, if `false` do
            nothing with refreshes. Valid values: `true`, `false`, `wait_for`.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for active shards.
        :arg version: Explicit version number for concurrency control.
            The specified version must match the current version of the document for
            the request to succeed.
        :arg version_type: The specific version type: `external`,
            `external_gte`. Valid choices are external, external_gte, force,
            internal.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "DELETE", _make_path(index, "_doc", id), params=params, headers=headers
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "allow_no_indices",
        "analyze_wildcard",
        "analyzer",
        "conflicts",
        "default_operator",
        "df",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "from_",
        "human",
        "ignore_unavailable",
        "lenient",
        "max_docs",
        "preference",
        "pretty",
        "q",
        "refresh",
        "request_cache",
        "requests_per_second",
        "routing",
        "scroll",
        "scroll_size",
        "search_timeout",
        "search_type",
        "size",
        "slices",
        "sort",
        "source",
        "stats",
        "terminate_after",
        "timeout",
        "version",
        "wait_for_active_shards",
        "wait_for_completion",
    )
    def delete_by_query(
        self,
        *,
        index: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes documents matching the provided query.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases to search. Supports wildcards (`*`). To search all data streams
            or indexes, omit this parameter or use `*` or `_all`.
        :arg body: The search definition using the Query DSL
        :arg _source: Set to `true` or `false` to return the `_source`
            field or not, or a list of fields to return.
        :arg _source_excludes: List of fields to exclude from the
            returned `_source` field.
        :arg _source_includes: List of fields to extract and return from
            the `_source` field.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting `foo*,bar*`
            returns an error if an index starts with `foo` but no index starts with
            `bar`.
        :arg analyze_wildcard: If `true`, wildcard and prefix queries
            are analyzed. Default is false.
        :arg analyzer: Analyzer to use for the query string.
        :arg conflicts: What to do if delete by query hits version
            conflicts: `abort` or `proceed`. Valid choices are abort, proceed.
        :arg default_operator: The default operator for query string
            query: `AND` or `OR`. Valid choices are and, or.
        :arg df: Field to use as default where no field prefix is given
            in the query string.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg from_: Starting offset. Default is 0.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg lenient: If `true`, format-based query failures (such as
            providing text to a numeric field) in the query string will be ignored.
        :arg max_docs: Maximum number of documents to process. Defaults
            to all documents.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg q: Query in the Lucene query string syntax.
        :arg refresh: If `true`, OpenSearch refreshes all shards
            involved in the delete by query after the request completes. Valid
            choices are false, true, wait_for.
        :arg request_cache: If `true`, the request cache is used for
            this request. Defaults to the index-level setting.
        :arg requests_per_second: The throttle for this request in sub-
            requests per second. Default is 0.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg scroll: Period to retain the search context for scrolling.
        :arg scroll_size: Size of the scroll request that powers the
            operation. Default is 100.
        :arg search_timeout: Explicit timeout for each search request.
            Defaults to no timeout.
        :arg search_type: The type of the search operation. Available
            options: `query_then_fetch`, `dfs_query_then_fetch`. Valid choices are
            dfs_query_then_fetch, query_then_fetch.
        :arg size: Deprecated, use `max_docs` instead.
        :arg slices: The number of slices this task should be divided
            into.
        :arg sort: A comma-separated list of <field>:<direction> pairs.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg stats: Specific `tag` of the request for logging and
            statistical purposes.
        :arg terminate_after: Maximum number of documents to collect for
            each shard. If a query reaches this limit, OpenSearch terminates the
            query early. OpenSearch collects documents before sorting. Use with
            caution. OpenSearch applies this parameter to each shard handling the
            request. When possible, let OpenSearch perform early termination
            automatically. Avoid specifying this parameter for requests that target
            data streams with backing indexes across multiple data tiers.
        :arg timeout: Period each deletion request waits for active
            shards.
        :arg version: If `true`, returns the document version as part of
            a hit.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to all or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        :arg wait_for_completion: If `true`, the request blocks until
            the operation is complete. Default is True.
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        for param in (index, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "POST",
            _make_path(index, "_delete_by_query"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace", "filter_path", "human", "pretty", "requests_per_second", "source"
    )
    def delete_by_query_rethrottle(
        self,
        *,
        task_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Changes the number of requests per second for a particular Delete By Query
        operation.


        :arg task_id: The ID for the task.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg requests_per_second: The throttle for this request in sub-
            requests per second.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if task_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'task_id'.")

        return self.transport.perform_request(
            "POST",
            _make_path("_delete_by_query", task_id, "_rethrottle"),
            params=params,
            headers=headers,
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def delete_script(
        self,
        *,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes a script.


        :arg id: Identifier for the stored script or search template.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "DELETE", _make_path("_scripts", id), params=params, headers=headers
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "error_trace",
        "filter_path",
        "human",
        "preference",
        "pretty",
        "realtime",
        "refresh",
        "routing",
        "source",
        "stored_fields",
        "version",
        "version_type",
    )
    def exists(
        self,
        *,
        index: Any,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about whether a document exists in an index.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases. Supports wildcards (`*`).
        :arg id: Identifier of the document.
        :arg _source: `true` or `false` to return the `_source` field or
            not, or a list of fields to return.
        :arg _source_excludes: A comma-separated list of source fields
            to exclude in the response.
        :arg _source_includes: A comma-separated list of source fields
            to include in the response.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg realtime: If `true`, the request is real time as opposed to
            near real time.
        :arg refresh: If `true`, OpenSearch refreshes all shards
            involved in the delete by query after the request completes. Valid
            choices are false, true, wait_for.
        :arg routing: Target the specified primary shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg stored_fields: List of stored fields to return as part of a
            hit. If no fields are specified, no stored fields are included in the
            response. If this field is specified, the `_source` parameter defaults
            to false.
        :arg version: Explicit version number for concurrency control.
            The specified version must match the current version of the document for
            the request to succeed.
        :arg version_type: The specific version type: `external`,
            `external_gte`. Valid choices are external, external_gte, force,
            internal.
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "HEAD", _make_path(index, "_doc", id), params=params, headers=headers
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "error_trace",
        "filter_path",
        "human",
        "preference",
        "pretty",
        "realtime",
        "refresh",
        "routing",
        "source",
        "version",
        "version_type",
    )
    def exists_source(
        self,
        *,
        index: Any,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about whether a document source exists in an index.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases. Supports wildcards (`*`).
        :arg id: Identifier of the document.
        :arg _source: `true` or `false` to return the `_source` field or
            not, or a list of fields to return.
        :arg _source_excludes: A comma-separated list of source fields
            to exclude in the response.
        :arg _source_includes: A comma-separated list of source fields
            to include in the response.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg realtime: If `true`, the request is real time as opposed to
            near real time.
        :arg refresh: If `true`, OpenSearch refreshes all shards
            involved in the delete by query after the request completes. Valid
            choices are false, true, wait_for.
        :arg routing: Target the specified primary shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg version: Explicit version number for concurrency control.
            The specified version must match the current version of the document for
            the request to succeed.
        :arg version_type: The specific version type: `external`,
            `external_gte`. Valid choices are external, external_gte, force,
            internal.
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        path = _make_path(index, "_source", id)

        return self.transport.perform_request(
            "HEAD", path, params=params, headers=headers
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "analyze_wildcard",
        "analyzer",
        "default_operator",
        "df",
        "error_trace",
        "filter_path",
        "human",
        "lenient",
        "preference",
        "pretty",
        "q",
        "routing",
        "source",
        "stored_fields",
    )
    def explain(
        self,
        *,
        index: Any,
        id: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about why a specific document matches (or doesn't match) a
        query.


        :arg index: Index names used to limit the request. Only a single
            index name can be provided to this parameter.
        :arg id: Defines the document ID.
        :arg body: The query definition using the Query DSL
        :arg _source: Set to `true` or `false` to return the `_source`
            field or not, or a list of fields to return.
        :arg _source_excludes: A comma-separated list of source fields
            to exclude from the response.
        :arg _source_includes: A comma-separated list of source fields
            to include in the response.
        :arg analyze_wildcard: If `true`, wildcard and prefix queries
            are analyzed. Default is false.
        :arg analyzer: Analyzer to use for the query string. This
            parameter can only be used when the `q` query string parameter is
            specified.
        :arg default_operator: The default operator for query string
            query: `AND` or `OR`. Valid choices are and, or.
        :arg df: Field to use as default where no field prefix is given
            in the query string. Default is _all.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg lenient: If `true`, format-based query failures (such as
            providing text to a numeric field) in the query string will be ignored.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg q: Query in the Lucene query string syntax.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg stored_fields: A comma-separated list of stored fields to
            return in the response.
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        path = _make_path(index, "_explain", id)

        return self.transport.perform_request(
            "POST", path, params=params, headers=headers, body=body
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "fields",
        "filter_path",
        "human",
        "ignore_unavailable",
        "include_unmapped",
        "pretty",
        "source",
    )
    def field_caps(
        self,
        *,
        body: Any = None,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns the information about the capabilities of fields among multiple
        indexes.


        :arg body: An index filter specified with the Query DSL
        :arg index: A comma-separated list of data streams, indexes, and
            aliases used to limit the request. Supports wildcards (*). To target all
            data streams and indexes, omit this parameter or use * or `_all`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting `foo*,bar*`
            returns an error if an index starts with foo but no index starts with
            bar.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: The type of index that wildcard patterns
            can match. If the request can target data streams, this argument
            determines whether wildcard expressions match hidden data streams.
            Supports comma-separated values, such as `open,hidden`. Valid choices
            are all, closed, hidden, none, open.
        :arg fields: A comma-separated list of fields to retrieve
            capabilities for. Wildcard (`*`) expressions are supported.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `true`, missing or closed indexes
            are not included in the response.
        :arg include_unmapped: If `true`, unmapped fields are included
            in the response. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_field_caps"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "error_trace",
        "filter_path",
        "human",
        "preference",
        "pretty",
        "realtime",
        "refresh",
        "routing",
        "source",
        "stored_fields",
        "version",
        "version_type",
    )
    def get(
        self,
        *,
        index: Any,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns a document.


        :arg index: The name of the index containing the document.
        :arg id: The unique identifier of the document.
        :arg _source: Set to `true` or `false` to return the `_source`
            field or not, or a list of fields to return.
        :arg _source_excludes: A comma-separated list of source fields
            to exclude in the response.
        :arg _source_includes: A comma-separated list of source fields
            to include in the response.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg realtime: If `true`, the request is real time as opposed to
            near real time.
        :arg refresh: If `true`, OpenSearch refreshes the affected
            shards to make this operation visible to search. If `false`, do nothing
            with refreshes. Valid choices are false, true, wait_for.
        :arg routing: Target the specified primary shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg stored_fields: List of stored fields to return as part of a
            hit. If no fields are specified, no stored fields are included in the
            response. If this field is specified, the `_source` parameter defaults
            to false.
        :arg version: Explicit version number for concurrency control.
            The specified version must match the current version of the document for
            the request to succeed.
        :arg version_type: The specific version type: `internal`,
            `external`, `external_gte`. Valid choices are external, external_gte,
            force, internal.
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "GET", _make_path(index, "_doc", id), params=params, headers=headers
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
    )
    def get_script(
        self,
        *,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns a script.


        :arg id: Identifier for the stored script or search template.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Specify timeout for connection
            to master
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'id'.")

        return self.transport.perform_request(
            "GET", _make_path("_scripts", id), params=params, headers=headers
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "error_trace",
        "filter_path",
        "human",
        "preference",
        "pretty",
        "realtime",
        "refresh",
        "routing",
        "source",
        "version",
        "version_type",
    )
    def get_source(
        self,
        *,
        index: Any,
        id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns the source of a document.


        :arg index: The name of the index containing the document.
        :arg id: The unique identifier of the document.
        :arg _source: Set to `true` or `false` to return the `_source`
            field or not, or a list of fields to return.
        :arg _source_excludes: A comma-separated list of source fields
            to exclude in the response.
        :arg _source_includes: A comma-separated list of source fields
            to include in the response.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg realtime: Boolean) If `true`, the request is real time as
            opposed to near real time.
        :arg refresh: If `true`, OpenSearch refreshes the affected
            shards to make this operation visible to search. If `false`, do nothing
            with refreshes. Valid choices are false, true, wait_for.
        :arg routing: Target the specified primary shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg version: Explicit version number for concurrency control.
            The specified version must match the current version of the document for
            the request to succeed.
        :arg version_type: The specific version type. One of `internal`,
            `external`, `external_gte`. Valid choices are external, external_gte,
            force, internal.
        """
        for param in (index, id):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        path = _make_path(index, "_source", id)

        return self.transport.perform_request(
            "GET", path, params=params, headers=headers
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "error_trace",
        "filter_path",
        "human",
        "preference",
        "pretty",
        "realtime",
        "refresh",
        "routing",
        "source",
        "stored_fields",
    )
    def mget(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to get multiple documents in one request.


        :arg body: Document identifiers; can be either `docs`
            (containing full document information) or `ids` (when index is provided
            in the URL.
        :arg index: The name of the index to retrieve documents from
            when `ids` are specified, or when a document in the `docs` array does
            not specify an index.
        :arg _source: Set to `true` or `false` to return the `_source`
            field or not, or a list of fields to return.
        :arg _source_excludes: A comma-separated list of source fields
            to exclude from the response. You can also use this parameter to exclude
            fields from the subset specified in `_source_includes` query parameter.
        :arg _source_includes: A comma-separated list of source fields
            to include in the response. If this parameter is specified, only these
            source fields are returned. You can exclude fields from this subset
            using the `_source_excludes` query parameter. If the `_source` parameter
            is `false`, this parameter is ignored.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg realtime: If `true`, the request is real time as opposed to
            near real time.
        :arg refresh: If `true`, the request refreshes relevant shards
            before retrieving documents. Valid choices are false, true, wait_for.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg stored_fields: If `true`, retrieves the document fields
            stored in the index rather than the document `_source`.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "POST",
            _make_path(index, "_mget"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "ccs_minimize_roundtrips",
        "error_trace",
        "filter_path",
        "human",
        "max_concurrent_searches",
        "max_concurrent_shard_requests",
        "pre_filter_shard_size",
        "pretty",
        "rest_total_hits_as_int",
        "search_type",
        "source",
        "typed_keys",
    )
    def msearch(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to execute several search operations in one request.


        :arg body: The request definitions (metadata-search request
            definition pairs), separated by newlines
        :arg index: A comma-separated list of data streams, indexes, and
            index aliases to search.
        :arg ccs_minimize_roundtrips: If `true`, network round-trips
            between the coordinating node and remote clusters are minimized for
            cross-cluster search requests. Default is True.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg max_concurrent_searches: Maximum number of concurrent
            searches the multi search API can execute.
        :arg max_concurrent_shard_requests: Maximum number of concurrent
            shard requests that each sub-search request executes per node. Default
            is 5.
        :arg pre_filter_shard_size: Defines a threshold that enforces a
            pre-filter roundtrip to prefilter search shards based on query rewriting
            if the number of shards the search request expands to exceeds the
            threshold. This filter roundtrip can limit the number of shards
            significantly if for instance a shard can not match any documents based
            on its rewrite method i.e., if date filters are mandatory to match but
            the shard bounds and the query are disjoint.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg rest_total_hits_as_int: If `true`, `hits.total` are
            returned as an integer in the response. Defaults to false, which returns
            an object. Default is false.
        :arg search_type: Indicates whether global term and document
            frequencies should be used when scoring returned documents. Valid
            choices are dfs_query_then_fetch, query_then_fetch.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg typed_keys: Specifies whether aggregation and suggester
            names should be prefixed by their respective types in the response.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        body = _bulk_body(self.transport.serializer, body)
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_msearch"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "ccs_minimize_roundtrips",
        "error_trace",
        "filter_path",
        "human",
        "max_concurrent_searches",
        "pretty",
        "rest_total_hits_as_int",
        "search_type",
        "source",
        "typed_keys",
    )
    def msearch_template(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to execute several search template operations in one request.


        :arg body: The request definitions (metadata-search request
            definition pairs), separated by newlines
        :arg index: A comma-separated list of data streams, indexes, and
            aliases to search. Supports wildcards (`*`). To search all data streams
            and indexes, omit this parameter or use `*`.
        :arg ccs_minimize_roundtrips: If `true`, network round-trips are
            minimized for cross-cluster search requests. Default is True.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg max_concurrent_searches: Maximum number of concurrent
            searches the API can run.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg rest_total_hits_as_int: If `true`, the response returns
            `hits.total` as an integer. If `false`, it returns `hits.total` as an
            object. Default is false.
        :arg search_type: The type of the search operation. Available
            options: `query_then_fetch`, `dfs_query_then_fetch`. Valid choices are
            dfs_query_then_fetch, query_then_fetch.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg typed_keys: If `true`, the response prefixes aggregation
            and suggester names with their respective types.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        body = _bulk_body(self.transport.serializer, body)
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_msearch", "template"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace",
        "field_statistics",
        "fields",
        "filter_path",
        "human",
        "ids",
        "offsets",
        "payloads",
        "positions",
        "preference",
        "pretty",
        "realtime",
        "routing",
        "source",
        "term_statistics",
        "version",
        "version_type",
    )
    def mtermvectors(
        self,
        *,
        body: Any = None,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns multiple termvectors in one request.


        :arg body: Define ids, documents, parameters or a list of
            parameters per document here. You must at least provide a list of
            document ids. See documentation.
        :arg index: The name of the index that contains the document.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg field_statistics: If `true`, the response includes the
            document count, sum of document frequencies, and sum of total term
            frequencies. Default is True.
        :arg fields: A comma-separated list or a wildcard expression
            specifying the fields to include in the statistics. Used as the default
            list unless a specific field list is provided in the `completion_fields`
            or `fielddata_fields` parameters.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ids: A comma-separated list of documents IDs. You must
            provide either the `docs` field in the request body or specify `ids` as
            a query parameter or in the request body.
        :arg offsets: If `true`, the response includes term offsets.
            Default is True.
        :arg payloads: If `true`, the response includes term payloads.
            Default is True.
        :arg positions: If `true`, the response includes term positions.
            Default is True.
        :arg preference: Specifies the node or shard on which the
            operation should be performed. See [preference query
            parameter]({{site.url}}{{site.baseurl}}/api-reference/search-
            apis/search/#the-preference-query-parameter) for a list of available
            options. By default the requests are routed randomly to available shard
            copies (primary or replica), with no guarantee of consistency across
            repeated queries.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg realtime: If `true`, the request is real time as opposed to
            near real time. Default is True.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg term_statistics: If `true`, the response includes term
            frequency and document frequency. Default is false.
        :arg version: If `true`, returns the document version as part of
            a hit.
        :arg version_type: The specific version type. Valid choices are
            external, external_gte, force, internal.
        """
        path = _make_path(index, "_mtermvectors")

        return self.transport.perform_request(
            "POST", path, params=params, headers=headers, body=body
        )

    @query_params(
        "cluster_manager_timeout",
        "error_trace",
        "filter_path",
        "human",
        "master_timeout",
        "pretty",
        "source",
        "timeout",
    )
    def put_script(
        self,
        *,
        id: Any,
        body: Any,
        context: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates or updates a script.


        :arg id: Identifier for the stored script or search template.
            Must be unique within the cluster.
        :arg body: The document
        :arg context: Context in which the script or search template
            should run. To prevent errors, the API immediately compiles the script
            or template in this context.
        :arg cluster_manager_timeout: Operation timeout for connection
            to cluster-manager node.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg master_timeout (Deprecated: To promote inclusive language,
            use `cluster_manager_timeout` instead.): Period to wait for a connection
            to the cluster-manager node. If no response is received before the
            timeout expires, the request fails and returns an error.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for a response. If no response is
            received before the timeout expires, the request fails and returns an
            error.
        """
        for param in (id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        return self.transport.perform_request(
            "PUT",
            _make_path("_scripts", id, context),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "pretty",
        "search_type",
        "source",
    )
    def rank_eval(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to evaluate the quality of ranked search results over a set of typical
        search queries.


        :arg body: The ranking evaluation search definition, including
            search requests, document ratings and ranking metric definition.
        :arg index: A comma-separated list of data streams, indexes, and
            index aliases used to limit the request. Wildcard (`*`) expressions are
            supported. To target all data streams and indexes in a cluster, omit
            this parameter or use `_all` or `*`.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting `foo*,bar*`
            returns an error if an index starts with `foo` but no index starts with
            `bar`.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indexes that are open, closed or both. Valid choices are all,
            closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `true`, missing or closed indexes
            are not included in the response.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg search_type: Search operation type Valid choices are
            dfs_query_then_fetch, query_then_fetch.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "POST",
            _make_path(index, "_rank_eval"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "max_docs",
        "pretty",
        "refresh",
        "requests_per_second",
        "require_alias",
        "scroll",
        "slices",
        "source",
        "timeout",
        "wait_for_active_shards",
        "wait_for_completion",
    )
    def reindex(
        self,
        *,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to copy documents from one index to another, optionally filtering the
        source documents by a query, changing the destination index settings, or
        fetching the documents from a remote cluster.


        :arg body: The search definition using the Query DSL and the
            prototype for the index request.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg max_docs: Maximum number of documents to process. By
            default, all documents.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg refresh: If `true`, the request refreshes affected shards
            to make this operation visible to search. Valid choices are false, true,
            wait_for.
        :arg requests_per_second: The throttle for this request in sub-
            requests per second. Defaults to no throttle. Default is 0.
        :arg scroll: Specifies how long a consistent view of the index
            should be maintained for scrolled search.
        :arg slices: The number of slices this task should be divided
            into. Defaults to 1 slice, meaning the task isn't sliced into subtasks.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period each indexing waits for automatic index
            creation, dynamic mapping updates, and waiting for active shards.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        :arg wait_for_completion: If `true`, the request blocks until
            the operation is complete. Default is True.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "POST", "/_reindex", params=params, headers=headers, body=body
        )

    @query_params(
        "error_trace", "filter_path", "human", "pretty", "requests_per_second", "source"
    )
    def reindex_rethrottle(
        self,
        *,
        task_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Changes the number of requests per second for a particular reindex operation.


        :arg task_id: Identifier for the task.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg requests_per_second: The throttle for this request in sub-
            requests per second.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if task_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'task_id'.")

        return self.transport.perform_request(
            "POST",
            _make_path("_reindex", task_id, "_rethrottle"),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def render_search_template(
        self,
        *,
        body: Any = None,
        id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to use the Mustache language to pre-render a search definition.


        :arg body: The search definition template and its parameters.
        :arg id: ID of the search template to render. If no `source` is
            specified, this or the `id` request body parameter is required.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST",
            _make_path("_render", "template", id),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def scripts_painless_execute(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows an arbitrary script to be executed and a result to be returned.


        :arg body: The script to execute
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST",
            "/_scripts/painless/_execute",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace",
        "filter_path",
        "human",
        "pretty",
        "rest_total_hits_as_int",
        "scroll",
        "source",
    )
    def scroll(
        self,
        *,
        body: Any = None,
        scroll_id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to retrieve a large numbers of results from a single search request.


        :arg scroll_id: The scroll ID for scrolled search
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg rest_total_hits_as_int: If `true`, the API response's
            `hit.total` property is returned as an integer. If `false`, the API
            response's `hit.total` property is returned as an object. Default is
            false.
        :arg scroll: Period to retain the search context for scrolling.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if scroll_id in SKIP_IN_PATH and body in SKIP_IN_PATH:
            raise ValueError("You need to supply scroll_id or body.")
        elif scroll_id and not body:
            body = {"scroll_id": scroll_id}
        elif scroll_id:
            params["scroll_id"] = scroll_id

        return self.transport.perform_request(
            "POST", "/_search/scroll", params=params, headers=headers, body=body
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "allow_no_indices",
        "allow_partial_search_results",
        "analyze_wildcard",
        "analyzer",
        "batched_reduce_size",
        "cancel_after_time_interval",
        "ccs_minimize_roundtrips",
        "default_operator",
        "df",
        "docvalue_fields",
        "error_trace",
        "expand_wildcards",
        "explain",
        "filter_path",
        "from_",
        "human",
        "ignore_throttled",
        "ignore_unavailable",
        "include_named_queries_score",
        "lenient",
        "max_concurrent_shard_requests",
        "phase_took",
        "pre_filter_shard_size",
        "preference",
        "pretty",
        "q",
        "request_cache",
        "rest_total_hits_as_int",
        "routing",
        "scroll",
        "search_pipeline",
        "search_type",
        "seq_no_primary_term",
        "size",
        "sort",
        "source",
        "stats",
        "stored_fields",
        "suggest_field",
        "suggest_mode",
        "suggest_size",
        "suggest_text",
        "terminate_after",
        "timeout",
        "track_scores",
        "track_total_hits",
        "typed_keys",
        "verbose_pipeline",
        "version",
    )
    def search(
        self,
        *,
        body: Any = None,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns results matching a query.


        :arg body: The search definition using the Query DSL
        :arg index: A comma-separated list of data streams, indexes, and
            aliases to search. Supports wildcards (`*`). To search all data streams
            and indexes, omit this parameter or use `*` or `_all`.
        :arg _source: Indicates which source fields are returned for
            matching documents. These fields are returned in the `hits._source`
            property of the search response. Valid values are: `true` to return the
            entire document source; `false` to not return the document source;
            `<string>` to return the source fields that are specified as a comma-
            separated list (supports wildcard (`*`) patterns).
        :arg _source_excludes: A comma-separated list of source fields
            to exclude from the response. You can also use this parameter to exclude
            fields from the subset specified in `_source_includes` query parameter.
            If the `_source` parameter is `false`, this parameter is ignored.
        :arg _source_includes: A comma-separated list of source fields
            to include in the response. If this parameter is specified, only these
            source fields are returned. You can exclude fields from this subset
            using the `_source_excludes` query parameter. If the `_source` parameter
            is `false`, this parameter is ignored.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting `foo*,bar*`
            returns an error if an index starts with `foo` but no index starts with
            `bar`.
        :arg allow_partial_search_results: If `true`, returns partial
            results if there are shard request timeouts or shard failures. If
            `false`, returns an error with no partial results. Default is True.
        :arg analyze_wildcard: If `true`, wildcard and prefix queries
            are analyzed. This parameter can only be used when the q query string
            parameter is specified. Default is false.
        :arg analyzer: Analyzer to use for the query string. This
            parameter can only be used when the q query string parameter is
            specified.
        :arg batched_reduce_size: The number of shard results that
            should be reduced at once on the coordinating node. This value should be
            used as a protection mechanism to reduce the memory overhead per search
            request if the potential number of shards in the request can be large.
            Default is 512.
        :arg cancel_after_time_interval: The time after which the search
            request will be canceled. Request-level parameter takes precedence over
            `cancel_after_time_interval` cluster setting.
        :arg ccs_minimize_roundtrips: If `true`, network round-trips
            between the coordinating node and the remote clusters are minimized when
            executing cross-cluster search (CCS) requests. Default is True.
        :arg default_operator: The default operator for query string
            query: AND or OR. This parameter can only be used when the `q` query
            string parameter is specified. Valid choices are and, or.
        :arg df: Field to use as default where no field prefix is given
            in the query string. This parameter can only be used when the q query
            string parameter is specified.
        :arg docvalue_fields: A comma-separated list of fields to return
            as the docvalue representation for each hit.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid choices are all, closed,
            hidden, none, open.
        :arg explain: If `true`, returns detailed information about
            score computation as part of a hit.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg from_: Starting document offset. Needs to be non-negative.
            By default, you cannot page through more than 10,000 hits using the
            `from` and `size` parameters. To page through more hits, use the
            `search_after` parameter. Default is 0.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_throttled: If `true`, concrete, expanded or aliased
            indexes will be ignored when frozen.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg include_named_queries_score: Indicates whether
            `hit.matched_queries` should be rendered as a map that includes the name
            of the matched query associated with its score (true) or as an array
            containing the name of the matched queries (false) Default is false.
        :arg lenient: If `true`, format-based query failures (such as
            providing text to a numeric field) in the query string will be ignored.
            This parameter can only be used when the `q` query string parameter is
            specified.
        :arg max_concurrent_shard_requests: Defines the number of
            concurrent shard requests per node this search executes concurrently.
            This value should be used to limit the impact of the search on the
            cluster in order to limit the number of concurrent shard requests.
            Default is 5.
        :arg phase_took: Indicates whether to return phase-level `took`
            time values in the response. Default is false.
        :arg pre_filter_shard_size: Defines a threshold that enforces a
            pre-filter roundtrip to prefilter search shards based on query rewriting
            if the number of shards the search request expands to exceeds the
            threshold. This filter roundtrip can limit the number of shards
            significantly if for instance a shard can not match any documents based
            on its rewrite method (if date filters are mandatory to match but the
            shard bounds and the query are disjoint). When unspecified, the pre-
            filter phase is executed if any of these conditions is met: the request
            targets more than 128 shards; the request targets one or more read-only
            index; the primary sort of the query targets an indexed field.
        :arg preference: Nodes and shards used for the search. By
            default, OpenSearch selects from eligible nodes and shards using
            adaptive replica selection, accounting for allocation awareness. Valid
            values are: `_only_local` to run the search only on shards on the local
            node; `_local` to, if possible, run the search on shards on the local
            node, or if not, select shards using the default method;
            `_only_nodes:<node-id>,<node-id>` to run the search on only the
            specified nodes IDs, where, if suitable shards exist on more than one
            selected node, use shards on those nodes using the default method, or if
            none of the specified nodes are available, select shards from any
            available node using the default method; `_prefer_nodes:<node-id>,<node-
            id>` to if possible, run the search on the specified nodes IDs, or if
            not, select shards using the default method; `_shards:<shard>,<shard>`
            to run the search only on the specified shards; `<custom-string>` (any
            string that does not start with `_`) to route searches with the same
            `<custom-string>` to the same shards in the same order. Default is
            random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg q: Query in the Lucene query string syntax using query
            parameter search. Query parameter searches do not support the full
            OpenSearch Query DSL but are handy for testing.
        :arg request_cache: If `true`, the caching of search results is
            enabled for requests where `size` is `0`. Defaults to index level
            settings.
        :arg rest_total_hits_as_int: Indicates whether `hits.total`
            should be rendered as an integer or an object in the rest search
            response. Default is false.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg scroll: Period to retain the search context for scrolling.
            See Scroll search results. By default, this value cannot exceed `1d` (24
            hours). You can change this limit using the `search.max_keep_alive`
            cluster-level setting.
        :arg search_pipeline: Customizable sequence of processing stages
            applied to search queries.
        :arg search_type: How distributed term frequencies are
            calculated for relevance scoring. Valid choices are
            dfs_query_then_fetch, query_then_fetch.
        :arg seq_no_primary_term: If `true`, returns sequence number and
            primary term of the last modification of each hit.
        :arg size: Defines the number of hits to return. By default, you
            cannot page through more than 10,000 hits using the `from` and `size`
            parameters. To page through more hits, use the `search_after` parameter.
            Default is 10.
        :arg sort: A comma-separated list of <field>:<direction> pairs.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg stats: Specific `tag` of the request for logging and
            statistical purposes.
        :arg stored_fields: A comma-separated list of stored fields to
            return as part of a hit. If no fields are specified, no stored fields
            are included in the response. If this field is specified, the `_source`
            parameter defaults to `false`. You can pass `_source: true` to return
            both source fields and stored fields in the search response.
        :arg suggest_field: Specifies which field to use for
            suggestions.
        :arg suggest_mode: Specifies the suggest mode. This parameter
            can only be used when the `suggest_field` and `suggest_text` query
            string parameters are specified. Valid choices are always, missing,
            popular.
        :arg suggest_size: Number of suggestions to return. This
            parameter can only be used when the `suggest_field` and `suggest_text`
            query string parameters are specified.
        :arg suggest_text: The source text for which the suggestions
            should be returned. This parameter can only be used when the
            `suggest_field` and `suggest_text` query string parameters are
            specified.
        :arg terminate_after: Maximum number of documents to collect for
            each shard. If a query reaches this limit, OpenSearch terminates the
            query early. OpenSearch collects documents before sorting. Use with
            caution. OpenSearch applies this parameter to each shard handling the
            request. When possible, let OpenSearch perform early termination
            automatically. Avoid specifying this parameter for requests that target
            data streams with backing indexes across multiple data tiers. If set to
            `0` (default), the query does not terminate early.
        :arg timeout: Specifies the period of time to wait for a
            response from each shard. If no response is received before the timeout
            expires, the request fails and returns an error.
        :arg track_scores: If `true`, calculate and return document
            scores, even if the scores are not used for sorting.
        :arg track_total_hits: Number of hits matching the query to
            count accurately. If `true`, the exact number of hits is returned at the
            cost of some performance. If `false`, the response does not include the
            total number of hits matching the query.
        :arg typed_keys: If `true`, aggregation and suggester names are
            be prefixed by their respective types in the response.
        :arg verbose_pipeline: Enables or disables verbose mode for the
            search pipeline. When verbose mode is enabled, detailed information
            about each processor in the search pipeline is included in the search
            response. This includes the processor name, execution status, input,
            output, and time taken for processing. This parameter is primarily
            intended for debugging purposes, allowing users to track how data flows
            and transforms through the search pipeline.
        :arg version: If `true`, returns document version as part of a
            hit.
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        return self.transport.perform_request(
            "POST",
            _make_path(index, "_search"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "allow_no_indices",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "ignore_unavailable",
        "local",
        "preference",
        "pretty",
        "routing",
        "source",
    )
    def search_shards(
        self,
        *,
        body: Any = None,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information about the indexes and shards that a search request would be
        executed against.


        :arg index: Returns the indexes and shards that a search request
            would be executed against.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting `foo*,bar*`
            returns an error if an index starts with `foo` but no index starts with
            `bar`.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg local: If `true`, the request retrieves information from
            the local node only. Default is false.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "POST",
            _make_path(index, "_search_shards"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "allow_no_indices",
        "ccs_minimize_roundtrips",
        "error_trace",
        "expand_wildcards",
        "explain",
        "filter_path",
        "human",
        "ignore_throttled",
        "ignore_unavailable",
        "phase_took",
        "preference",
        "pretty",
        "profile",
        "rest_total_hits_as_int",
        "routing",
        "scroll",
        "search_pipeline",
        "search_type",
        "source",
        "typed_keys",
    )
    def search_template(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to use the Mustache language to pre-render a search definition.


        :arg body: The search definition template and its parameters.
        :arg index: A comma-separated list of data streams, indexes, and
            aliases to search. Supports wildcards (*).
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting `foo*,bar*`
            returns an error if an index starts with `foo` but no index starts with
            `bar`.
        :arg ccs_minimize_roundtrips: If `true`, network round-trips are
            minimized for cross-cluster search requests. Default is True.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg explain: If `true`, the response includes additional
            details about score computation as part of a hit.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_throttled: If `true`, specified concrete, expanded,
            or aliased indexes are not included in the response when throttled.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg phase_took: Indicates whether to return phase-level `took`
            time values in the response. Default is false.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg profile: If `true`, the query execution is profiled.
        :arg rest_total_hits_as_int: If `true`, `hits.total` are
            rendered as an integer in the response. Default is false.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg scroll: Specifies how long a consistent view of the index
            should be maintained for scrolled search.
        :arg search_pipeline: Customizable sequence of processing stages
            applied to search queries.
        :arg search_type: The type of the search operation. Valid
            choices are dfs_query_then_fetch, query_then_fetch.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg typed_keys: If `true`, the response prefixes aggregation
            and suggester names with their respective types.
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        return self.transport.perform_request(
            "POST",
            _make_path(index, "_search", "template"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace",
        "field_statistics",
        "fields",
        "filter_path",
        "human",
        "offsets",
        "payloads",
        "positions",
        "preference",
        "pretty",
        "realtime",
        "routing",
        "source",
        "term_statistics",
        "version",
        "version_type",
    )
    def termvectors(
        self,
        *,
        index: Any,
        body: Any = None,
        id: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns information and statistics about terms in the fields of a particular
        document.


        :arg index: The name of the index containing the document.
        :arg body: Define parameters and or supply a document to get
            termvectors for. See documentation.
        :arg id: The unique identifier of the document.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg field_statistics: If `true`, the response includes the
            document count, sum of document frequencies, and sum of total term
            frequencies. Default is True.
        :arg fields: A comma-separated list or a wildcard expression
            specifying the fields to include in the statistics. Used as the default
            list unless a specific field list is provided in the `completion_fields`
            or `fielddata_fields` parameters.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg offsets: If `true`, the response includes term offsets.
            Default is True.
        :arg payloads: If `true`, the response includes term payloads.
            Default is True.
        :arg positions: If `true`, the response includes term positions.
            Default is True.
        :arg preference: Specifies the node or shard on which the
            operation should be performed. See [preference query
            parameter]({{site.url}}{{site.baseurl}}/api-reference/search-
            apis/search/#the-preference-query-parameter) for a list of available
            options. By default the requests are routed randomly to available shard
            copies (primary or replica), with no guarantee of consistency across
            repeated queries.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg realtime: If `true`, the request is real time as opposed to
            near real time. Default is True.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg term_statistics: If `true`, the response includes term
            frequency and document frequency. Default is false.
        :arg version: If `true`, returns the document version as part of
            a hit.
        :arg version_type: The specific version type. Valid choices are
            external, external_gte, force, internal.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        path = _make_path(index, "_termvectors", id)

        return self.transport.perform_request(
            "POST", path, params=params, headers=headers, body=body
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "error_trace",
        "filter_path",
        "human",
        "if_primary_term",
        "if_seq_no",
        "lang",
        "pretty",
        "refresh",
        "require_alias",
        "retry_on_conflict",
        "routing",
        "source",
        "timeout",
        "wait_for_active_shards",
    )
    def update(
        self,
        *,
        index: Any,
        id: Any,
        body: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Updates a document with a script or partial document.


        :arg index: The name of the index
        :arg id: Document ID
        :arg body: The request definition requires either `script` or
            partial `doc`
        :arg _source: Set to `false` to disable source retrieval. You
            can also specify a comma-separated list of the fields you want to
            retrieve.
        :arg _source_excludes: Specify the source fields you want to
            exclude.
        :arg _source_includes: Specify the source fields you want to
            retrieve.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg if_primary_term: Only perform the operation if the document
            has this primary term.
        :arg if_seq_no: Only perform the operation if the document has
            this sequence number.
        :arg lang: The script language. Default is painless.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg refresh: If 'true', OpenSearch refreshes the affected
            shards to make this operation visible to search, if `wait_for` then wait
            for a refresh to make this operation visible to search, if `false` do
            nothing with refreshes. Valid choices are false, true, wait_for.
        :arg require_alias: If `true`, the destination must be an index
            alias. Default is false.
        :arg retry_on_conflict: Specify how many times should the
            operation be retried when a conflict occurs. Default is 0.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period to wait for dynamic mapping updates and
            active shards. This guarantees OpenSearch waits for at least the timeout
            before failing. The actual wait time could be longer, particularly when
            multiple waits occur.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operations. Set to 'all' or
            any positive integer up to the total number of shards in the index
            (number_of_replicas+1). Defaults to 1 meaning the primary shard.
        """
        for param in (index, id, body):
            if param in SKIP_IN_PATH:
                raise ValueError("Empty value passed for a required argument.")

        path = _make_path(index, "_update", id)

        return self.transport.perform_request(
            "POST", path, params=params, headers=headers, body=body
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "allow_no_indices",
        "analyze_wildcard",
        "analyzer",
        "conflicts",
        "default_operator",
        "df",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "from_",
        "human",
        "ignore_unavailable",
        "lenient",
        "max_docs",
        "pipeline",
        "preference",
        "pretty",
        "q",
        "refresh",
        "request_cache",
        "requests_per_second",
        "routing",
        "scroll",
        "scroll_size",
        "search_timeout",
        "search_type",
        "size",
        "slices",
        "sort",
        "source",
        "stats",
        "terminate_after",
        "timeout",
        "version",
        "wait_for_active_shards",
        "wait_for_completion",
    )
    def update_by_query(
        self,
        *,
        index: Any,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Performs an update on every document in the index without changing the source,
        for example to pick up a mapping change.


        :arg index: A comma-separated list of data streams, indexes, and
            aliases to search. Supports wildcards (`*`). To search all data streams
            or indexes, omit this parameter or use `*` or `_all`.
        :arg body: The search definition using the Query DSL
        :arg _source: Set to `true` or `false` to return the `_source`
            field or not, or a list of fields to return.
        :arg _source_excludes: List of fields to exclude from the
            returned `_source` field.
        :arg _source_includes: List of fields to extract and return from
            the `_source` field.
        :arg allow_no_indices: If `false`, the request returns an error
            if any wildcard expression, index alias, or `_all` value targets only
            missing or closed indexes. This behavior applies even if the request
            targets other open indexes. For example, a request targeting `foo*,bar*`
            returns an error if an index starts with `foo` but no index starts with
            `bar`.
        :arg analyze_wildcard: If `true`, wildcard and prefix queries
            are analyzed. Default is false.
        :arg analyzer: Analyzer to use for the query string.
        :arg conflicts: What to do if update by query hits version
            conflicts: `abort` or `proceed`. Valid choices are abort, proceed.
        :arg default_operator: The default operator for query string
            query: `AND` or `OR`. Valid choices are and, or.
        :arg df: Field to use as default where no field prefix is given
            in the query string.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Type of index that wildcard patterns can
            match. If the request can target data streams, this argument determines
            whether wildcard expressions match hidden data streams. Supports comma-
            separated values, such as `open,hidden`. Valid values are: `all`,
            `open`, `closed`, `hidden`, `none`.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg from_: Starting offset. Default is 0.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg ignore_unavailable: If `false`, the request returns an
            error if it targets a missing or closed index.
        :arg lenient: If `true`, format-based query failures (such as
            providing text to a numeric field) in the query string will be ignored.
        :arg max_docs: Maximum number of documents to process. Defaults
            to all documents.
        :arg pipeline: ID of the pipeline to use to preprocess incoming
            documents. If the index has a default ingest pipeline specified, then
            setting the value to `_none` disables the default ingest pipeline for
            this request. If a final pipeline is configured it will always run,
            regardless of the value of this parameter.
        :arg preference: Specifies the node or shard the operation
            should be performed on. Random by default. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg q: Query in the Lucene query string syntax.
        :arg refresh: If `true`, OpenSearch refreshes affected shards to
            make the operation visible to search. Valid choices are false, true,
            wait_for.
        :arg request_cache: If `true`, the request cache is used for
            this request.
        :arg requests_per_second: The throttle for this request in sub-
            requests per second. Default is 0.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg scroll: Period to retain the search context for scrolling.
        :arg scroll_size: Size of the scroll request that powers the
            operation. Default is 100.
        :arg search_timeout: Explicit timeout for each search request.
        :arg search_type: The type of the search operation. Available
            options: `query_then_fetch`, `dfs_query_then_fetch`. Valid choices are
            dfs_query_then_fetch, query_then_fetch.
        :arg size: Deprecated, use `max_docs` instead.
        :arg slices: The number of slices this task should be divided
            into.
        :arg sort: A comma-separated list of <field>:<direction> pairs.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg stats: Specific `tag` of the request for logging and
            statistical purposes.
        :arg terminate_after: Maximum number of documents to collect for
            each shard. If a query reaches this limit, OpenSearch terminates the
            query early. OpenSearch collects documents before sorting. Use with
            caution. OpenSearch applies this parameter to each shard handling the
            request. When possible, let OpenSearch perform early termination
            automatically. Avoid specifying this parameter for requests that target
            data streams with backing indexes across multiple data tiers.
        :arg timeout: Period each update request waits for the following
            operations: dynamic mapping updates, waiting for active shards.
        :arg version: If `true`, returns the document version as part of
            a hit.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to `all` or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        :arg wait_for_completion: If `true`, the request blocks until
            the operation is complete. Default is True.
        """
        # from is a reserved word so it cannot be used, use from_ instead
        if "from_" in params:
            params["from"] = params.pop("from_")

        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "POST",
            _make_path(index, "_update_by_query"),
            params=params,
            headers=headers,
            body=body,
        )

    @query_params(
        "error_trace", "filter_path", "human", "pretty", "requests_per_second", "source"
    )
    def update_by_query_rethrottle(
        self,
        *,
        task_id: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Changes the number of requests per second for a particular Update By Query
        operation.


        :arg task_id: The ID for the task.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg requests_per_second: The throttle for this request in sub-
            requests per second.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if task_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'task_id'.")

        return self.transport.perform_request(
            "POST",
            _make_path("_update_by_query", task_id, "_rethrottle"),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_script_context(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns all script contexts.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", "/_script_context", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_script_languages(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Returns available script types, languages and contexts.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", "/_script_language", params=params, headers=headers
        )

    @query_params(
        "allow_partial_pit_creation",
        "error_trace",
        "expand_wildcards",
        "filter_path",
        "human",
        "keep_alive",
        "preference",
        "pretty",
        "routing",
        "source",
    )
    def create_pit(
        self,
        *,
        index: Any,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Creates point in time context.


        :arg index: A comma-separated list of indexes; use `_all` or
            empty string to perform the operation on all indexes.
        :arg allow_partial_pit_creation: Allow if point in time can be
            created with partial failures.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg expand_wildcards: Whether to expand wildcard expression to
            concrete indexes that are open, closed or both. Valid choices are all,
            closed, hidden, none, open.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg keep_alive: Specify the keep alive for point in time.
        :arg preference: Specify the node or shard the operation should
            be performed on. Default is random.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg routing: A comma-separated list of specific routing values.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'index'.")

        return self.transport.perform_request(
            "POST",
            _make_path(index, "_search", "point_in_time"),
            params=params,
            headers=headers,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_all_pits(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes all active point in time searches.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "DELETE", "/_search/point_in_time/_all", params=params, headers=headers
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def delete_pit(
        self,
        *,
        body: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Deletes one or more point in time searches based on the IDs passed.


        :arg body: The point-in-time ids to be deleted
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "DELETE",
            "/_search/point_in_time",
            params=params,
            headers=headers,
            body=body,
        )

    @query_params("error_trace", "filter_path", "human", "pretty", "source")
    def get_all_pits(
        self,
        *,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Lists all active point in time searches.


        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        """
        return self.transport.perform_request(
            "GET", "/_search/point_in_time/_all", params=params, headers=headers
        )

    @query_params(
        "_source",
        "_source_excludes",
        "_source_includes",
        "batch_interval",
        "batch_size",
        "error_trace",
        "filter_path",
        "human",
        "pipeline",
        "pretty",
        "refresh",
        "require_alias",
        "routing",
        "source",
        "timeout",
        "wait_for_active_shards",
    )
    def bulk_stream(
        self,
        *,
        body: Any,
        index: Any = None,
        params: Any = None,
        headers: Any = None,
    ) -> Any:
        """
        Allows to perform multiple index/update/delete operations using request
        response streaming.


        :arg body: The operation definition and data (action-data
            pairs), separated by newlines
        :arg index: Name of the data stream, index, or index alias to
            perform bulk actions on.
        :arg _source: `true` or `false` to return the `_source` field or
            not, or a list of fields to return.
        :arg _source_excludes: A comma-separated list of source fields
            to exclude from the response.
        :arg _source_includes: A comma-separated list of source fields
            to include in the response.
        :arg batch_interval: Specifies for how long bulk operations
            should be accumulated into a batch before sending the batch to data
            nodes.
        :arg batch_size: Specifies how many bulk operations should be
            accumulated into a batch before sending the batch to data nodes.
        :arg error_trace: Whether to include the stack trace of returned
            errors. Default is false.
        :arg filter_path: A comma-separated list of filters used to
            filter the response. Use wildcards to match any field or part of a
            field's name. To exclude fields, use `-`.
        :arg human: Whether to return human-readable values for
            statistics. Default is false.
        :arg pipeline: ID of the pipeline to use to preprocess incoming
            documents. If the index has a default ingest pipeline specified, then
            setting the value to `_none` disables the default ingest pipeline for
            this request. If a final pipeline is configured it will always run,
            regardless of the value of this parameter.
        :arg pretty: Whether to pretty-format the returned JSON
            response. Default is false.
        :arg refresh: If `true`, OpenSearch refreshes the affected
            shards to make this operation visible to search, if `wait_for` then wait
            for a refresh to make this operation visible to search, if `false` do
            nothing with refreshes. Valid values: `true`, `false`, `wait_for`.
        :arg require_alias: If `true`, the request's actions must target
            an index alias. Default is false.
        :arg routing: A custom value used to route operations to a
            specific shard.
        :arg source: The URL-encoded request definition. Useful for
            libraries that do not accept a request body for non-POST requests.
        :arg timeout: Period each action waits for the following
            operations: automatic index creation, dynamic mapping updates, waiting
            for active shards.
        :arg wait_for_active_shards: The number of shard copies that
            must be active before proceeding with the operation. Set to all or any
            positive integer up to the total number of shards in the index
            (`number_of_replicas+1`).
        """
        if body in SKIP_IN_PATH:
            raise ValueError("Empty value passed for a required argument 'body'.")

        body = _bulk_body(self.transport.serializer, body)
        return self.transport.perform_request(
            "PUT",
            _make_path(index, "_bulk", "stream"),
            params=params,
            headers=headers,
            body=body,
        )
