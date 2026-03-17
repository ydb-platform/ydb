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


import logging
import typing as t

from elastic_transport import (
    BaseNode,
    BinaryApiResponse,
    HeadApiResponse,
    NodeConfig,
    NodePool,
    NodeSelector,
    ObjectApiResponse,
    Serializer,
    Transport,
)
from elastic_transport.client_utils import DEFAULT, DefaultType

from ...exceptions import ApiError, TransportError
from ...serializer import DEFAULT_SERIALIZERS
from ._base import (
    BaseClient,
    create_sniff_callback,
    default_sniff_callback,
    resolve_auth_headers,
)
from .async_search import AsyncSearchClient
from .autoscaling import AutoscalingClient
from .cat import CatClient
from .ccr import CcrClient
from .cluster import ClusterClient
from .connector import ConnectorClient
from .dangling_indices import DanglingIndicesClient
from .enrich import EnrichClient
from .eql import EqlClient
from .esql import EsqlClient
from .features import FeaturesClient
from .fleet import FleetClient
from .graph import GraphClient
from .ilm import IlmClient
from .indices import IndicesClient
from .inference import InferenceClient
from .ingest import IngestClient
from .license import LicenseClient
from .logstash import LogstashClient
from .migration import MigrationClient
from .ml import MlClient
from .monitoring import MonitoringClient
from .nodes import NodesClient
from .query_rules import QueryRulesClient
from .rollup import RollupClient
from .search_application import SearchApplicationClient
from .searchable_snapshots import SearchableSnapshotsClient
from .security import SecurityClient
from .shutdown import ShutdownClient
from .simulate import SimulateClient
from .slm import SlmClient
from .snapshot import SnapshotClient
from .sql import SqlClient
from .ssl import SslClient
from .synonyms import SynonymsClient
from .tasks import TasksClient
from .text_structure import TextStructureClient
from .transform import TransformClient
from .utils import (
    _TYPE_HOSTS,
    CLIENT_META_SERVICE,
    SKIP_IN_PATH,
    Stability,
    _quote,
    _rewrite_parameters,
    _stability_warning,
    client_node_configs,
    is_requests_http_auth,
    is_requests_node_class,
)
from .watcher import WatcherClient
from .xpack import XPackClient

logger = logging.getLogger("elasticsearch")


SelfType = t.TypeVar("SelfType", bound="Elasticsearch")


class Elasticsearch(BaseClient):
    """
    Elasticsearch low-level client. Provides a straightforward mapping from
    Python to Elasticsearch REST APIs.

    The client instance has additional attributes to update APIs in different
    namespaces such as ``async_search``, ``indices``, ``security``, and more:

    .. code-block:: python

        client = Elasticsearch("http://localhost:9200")

        # Get Document API
        client.get(index="*", id="1")

        # Get Index API
        client.indices.get(index="*")

    Transport options can be set on the client constructor or using
    the :meth:`~elasticsearch.Elasticsearch.options` method:

    .. code-block:: python

        # Set 'api_key' on the constructor
        client = Elasticsearch(
            "http://localhost:9200",
            api_key="api_key",
        )
        client.search(...)

        # Set 'api_key' per request
        client.options(api_key="api_key").search(...)
    """

    def __init__(
        self,
        hosts: t.Optional[_TYPE_HOSTS] = None,
        *,
        # API
        cloud_id: t.Optional[str] = None,
        api_key: t.Optional[t.Union[str, t.Tuple[str, str]]] = None,
        basic_auth: t.Optional[t.Union[str, t.Tuple[str, str]]] = None,
        bearer_auth: t.Optional[str] = None,
        opaque_id: t.Optional[str] = None,
        # Node
        headers: t.Union[DefaultType, t.Mapping[str, str]] = DEFAULT,
        connections_per_node: t.Union[DefaultType, int] = DEFAULT,
        http_compress: t.Union[DefaultType, bool] = DEFAULT,
        verify_certs: t.Union[DefaultType, bool] = DEFAULT,
        ca_certs: t.Union[DefaultType, str] = DEFAULT,
        client_cert: t.Union[DefaultType, str] = DEFAULT,
        client_key: t.Union[DefaultType, str] = DEFAULT,
        ssl_assert_hostname: t.Union[DefaultType, str] = DEFAULT,
        ssl_assert_fingerprint: t.Union[DefaultType, str] = DEFAULT,
        ssl_version: t.Union[DefaultType, int] = DEFAULT,
        ssl_context: t.Union[DefaultType, t.Any] = DEFAULT,
        ssl_show_warn: t.Union[DefaultType, bool] = DEFAULT,
        # Transport
        transport_class: t.Type[Transport] = Transport,
        request_timeout: t.Union[DefaultType, None, float] = DEFAULT,
        node_class: t.Union[DefaultType, t.Type[BaseNode]] = DEFAULT,
        node_pool_class: t.Union[DefaultType, t.Type[NodePool]] = DEFAULT,
        randomize_nodes_in_pool: t.Union[DefaultType, bool] = DEFAULT,
        node_selector_class: t.Union[DefaultType, t.Type[NodeSelector]] = DEFAULT,
        dead_node_backoff_factor: t.Union[DefaultType, float] = DEFAULT,
        max_dead_node_backoff: t.Union[DefaultType, float] = DEFAULT,
        serializer: t.Optional[Serializer] = None,
        serializers: t.Union[DefaultType, t.Mapping[str, Serializer]] = DEFAULT,
        default_mimetype: str = "application/json",
        max_retries: t.Union[DefaultType, int] = DEFAULT,
        retry_on_status: t.Union[DefaultType, int, t.Collection[int]] = DEFAULT,
        retry_on_timeout: t.Union[DefaultType, bool] = DEFAULT,
        sniff_on_start: t.Union[DefaultType, bool] = DEFAULT,
        sniff_before_requests: t.Union[DefaultType, bool] = DEFAULT,
        sniff_on_node_failure: t.Union[DefaultType, bool] = DEFAULT,
        sniff_timeout: t.Union[DefaultType, None, float] = DEFAULT,
        min_delay_between_sniffing: t.Union[DefaultType, None, float] = DEFAULT,
        sniffed_node_callback: t.Optional[
            t.Callable[[t.Dict[str, t.Any], NodeConfig], t.Optional[NodeConfig]]
        ] = None,
        meta_header: t.Union[DefaultType, bool] = DEFAULT,
        http_auth: t.Union[DefaultType, t.Any] = DEFAULT,
        # Internal use only
        _transport: t.Optional[Transport] = None,
    ) -> None:
        if hosts is None and cloud_id is None and _transport is None:
            raise ValueError("Either 'hosts' or 'cloud_id' must be specified")

        if serializer is not None:
            if serializers is not DEFAULT:
                raise ValueError(
                    "Can't specify both 'serializer' and 'serializers' parameters "
                    "together. Instead only specify one of the other."
                )
            serializers = {default_mimetype: serializer}

        # Setting min_delay_between_sniffing=True implies sniff_before_requests=True
        if min_delay_between_sniffing is not DEFAULT:
            sniff_before_requests = True

        sniffing_options = (
            sniff_timeout,
            sniff_on_start,
            sniff_before_requests,
            sniff_on_node_failure,
            sniffed_node_callback,
            min_delay_between_sniffing,
            sniffed_node_callback,
        )
        if cloud_id is not None and any(
            x is not DEFAULT and x is not None for x in sniffing_options
        ):
            raise ValueError(
                "Sniffing should not be enabled when connecting to Elastic Cloud"
            )

        sniff_callback = None
        if sniffed_node_callback is not None:
            sniff_callback = create_sniff_callback(
                sniffed_node_callback=sniffed_node_callback
            )
        elif (
            sniff_on_start is True
            or sniff_before_requests is True
            or sniff_on_node_failure is True
        ):
            sniff_callback = default_sniff_callback

        if _transport is None:
            requests_session_auth = None
            if http_auth is not None and http_auth is not DEFAULT:
                if is_requests_http_auth(http_auth):
                    # If we're using custom requests authentication
                    # then we need to alert the user that they also
                    # need to use 'node_class=requests'.
                    if not is_requests_node_class(node_class):
                        raise ValueError(
                            "Using a custom 'requests.auth.AuthBase' class for "
                            "'http_auth' must be used with node_class='requests'"
                        )

                    # Reset 'http_auth' to DEFAULT so it's not consumed below.
                    requests_session_auth = http_auth
                    http_auth = DEFAULT

            node_configs = client_node_configs(
                hosts,
                cloud_id=cloud_id,
                requests_session_auth=requests_session_auth,
                connections_per_node=connections_per_node,
                http_compress=http_compress,
                verify_certs=verify_certs,
                ca_certs=ca_certs,
                client_cert=client_cert,
                client_key=client_key,
                ssl_assert_hostname=ssl_assert_hostname,
                ssl_assert_fingerprint=ssl_assert_fingerprint,
                ssl_version=ssl_version,
                ssl_context=ssl_context,
                ssl_show_warn=ssl_show_warn,
            )
            transport_kwargs: t.Dict[str, t.Any] = {}
            if node_class is not DEFAULT:
                transport_kwargs["node_class"] = node_class
            if node_pool_class is not DEFAULT:
                transport_kwargs["node_pool_class"] = node_pool_class
            if randomize_nodes_in_pool is not DEFAULT:
                transport_kwargs["randomize_nodes_in_pool"] = randomize_nodes_in_pool
            if node_selector_class is not DEFAULT:
                transport_kwargs["node_selector_class"] = node_selector_class
            if dead_node_backoff_factor is not DEFAULT:
                transport_kwargs["dead_node_backoff_factor"] = dead_node_backoff_factor
            if max_dead_node_backoff is not DEFAULT:
                transport_kwargs["max_dead_node_backoff"] = max_dead_node_backoff
            if meta_header is not DEFAULT:
                transport_kwargs["meta_header"] = meta_header

            transport_serializers = DEFAULT_SERIALIZERS.copy()
            if serializers is not DEFAULT:
                transport_serializers.update(serializers)

                # Override compatibility serializers from their non-compat mimetypes too.
                # So we use the same serializer for requests and responses.
                for mime_subtype in ("json", "x-ndjson"):
                    if f"application/{mime_subtype}" in serializers:
                        compat_mimetype = (
                            f"application/vnd.elasticsearch+{mime_subtype}"
                        )
                        if compat_mimetype not in serializers:
                            transport_serializers[compat_mimetype] = serializers[
                                f"application/{mime_subtype}"
                            ]

            transport_kwargs["serializers"] = transport_serializers

            transport_kwargs["default_mimetype"] = default_mimetype
            if sniff_on_start is not DEFAULT:
                transport_kwargs["sniff_on_start"] = sniff_on_start
            if sniff_before_requests is not DEFAULT:
                transport_kwargs["sniff_before_requests"] = sniff_before_requests
            if sniff_on_node_failure is not DEFAULT:
                transport_kwargs["sniff_on_node_failure"] = sniff_on_node_failure
            if sniff_timeout is not DEFAULT:
                transport_kwargs["sniff_timeout"] = sniff_timeout
            if min_delay_between_sniffing is not DEFAULT:
                transport_kwargs["min_delay_between_sniffing"] = (
                    min_delay_between_sniffing
                )

            _transport = transport_class(
                node_configs,
                client_meta_service=CLIENT_META_SERVICE,
                sniff_callback=sniff_callback,
                **transport_kwargs,
            )

            super().__init__(_transport)

            # These are set per-request so are stored separately.
            self._request_timeout = request_timeout
            self._max_retries = max_retries
            self._retry_on_timeout = retry_on_timeout
            if isinstance(retry_on_status, int):
                retry_on_status = (retry_on_status,)
            self._retry_on_status = retry_on_status

        else:
            super().__init__(_transport)

        if headers is not DEFAULT and headers is not None:
            self._headers.update(headers)
        if opaque_id is not DEFAULT and opaque_id is not None:  # type: ignore[comparison-overlap]
            self._headers["x-opaque-id"] = opaque_id
        self._headers = resolve_auth_headers(
            self._headers,
            http_auth=http_auth,
            api_key=api_key,
            basic_auth=basic_auth,
            bearer_auth=bearer_auth,
        )

        # namespaced clients for compatibility with API names
        self.async_search = AsyncSearchClient(self)
        self.autoscaling = AutoscalingClient(self)
        self.cat = CatClient(self)
        self.cluster = ClusterClient(self)
        self.connector = ConnectorClient(self)
        self.fleet = FleetClient(self)
        self.features = FeaturesClient(self)
        self.indices = IndicesClient(self)
        self.inference = InferenceClient(self)
        self.ingest = IngestClient(self)
        self.nodes = NodesClient(self)
        self.snapshot = SnapshotClient(self)
        self.tasks = TasksClient(self)

        self.xpack = XPackClient(self)
        self.ccr = CcrClient(self)
        self.dangling_indices = DanglingIndicesClient(self)
        self.enrich = EnrichClient(self)
        self.eql = EqlClient(self)
        self.esql = EsqlClient(self)
        self.graph = GraphClient(self)
        self.ilm = IlmClient(self)
        self.license = LicenseClient(self)
        self.logstash = LogstashClient(self)
        self.migration = MigrationClient(self)
        self.ml = MlClient(self)
        self.monitoring = MonitoringClient(self)
        self.query_rules = QueryRulesClient(self)
        self.rollup = RollupClient(self)
        self.search_application = SearchApplicationClient(self)
        self.searchable_snapshots = SearchableSnapshotsClient(self)
        self.security = SecurityClient(self)
        self.slm = SlmClient(self)
        self.simulate = SimulateClient(self)
        self.shutdown = ShutdownClient(self)
        self.sql = SqlClient(self)
        self.ssl = SslClient(self)
        self.synonyms = SynonymsClient(self)
        self.text_structure = TextStructureClient(self)
        self.transform = TransformClient(self)
        self.watcher = WatcherClient(self)

    def __repr__(self) -> str:
        try:
            # get a list of all connections
            nodes = [node.base_url for node in self.transport.node_pool.all()]
            # truncate to 5 if there are too many
            if len(nodes) > 5:
                nodes = nodes[:5] + ["..."]
            return f"<{self.__class__.__name__}({nodes})>"
        except Exception:
            # probably operating on custom transport and connection_pool, ignore
            return super().__repr__()

    def __enter__(self) -> "Elasticsearch":
        try:
            # All this to avoid a Mypy error when using unasync.
            getattr(self.transport, "_async_call")()
        except AttributeError:
            pass
        return self

    def __exit__(self, *_: t.Any) -> None:
        self.close()

    def options(
        self: SelfType,
        *,
        opaque_id: t.Union[DefaultType, str] = DEFAULT,
        api_key: t.Union[DefaultType, str, t.Tuple[str, str]] = DEFAULT,
        basic_auth: t.Union[DefaultType, str, t.Tuple[str, str]] = DEFAULT,
        bearer_auth: t.Union[DefaultType, str] = DEFAULT,
        headers: t.Union[DefaultType, t.Mapping[str, str]] = DEFAULT,
        request_timeout: t.Union[DefaultType, t.Optional[float]] = DEFAULT,
        ignore_status: t.Union[DefaultType, int, t.Collection[int]] = DEFAULT,
        max_retries: t.Union[DefaultType, int] = DEFAULT,
        retry_on_status: t.Union[DefaultType, int, t.Collection[int]] = DEFAULT,
        retry_on_timeout: t.Union[DefaultType, bool] = DEFAULT,
    ) -> SelfType:
        client = type(self)(_transport=self.transport)

        resolved_headers = headers if headers is not DEFAULT else None
        resolved_headers = resolve_auth_headers(
            headers=resolved_headers,
            api_key=api_key,
            basic_auth=basic_auth,
            bearer_auth=bearer_auth,
        )
        resolved_opaque_id = opaque_id if opaque_id is not DEFAULT else None
        if resolved_opaque_id:
            resolved_headers["x-opaque-id"] = resolved_opaque_id

        if resolved_headers:
            new_headers = self._headers.copy()
            new_headers.update(resolved_headers)
            client._headers = new_headers
        else:
            client._headers = self._headers.copy()

        if request_timeout is not DEFAULT:
            client._request_timeout = request_timeout
        else:
            client._request_timeout = self._request_timeout

        if ignore_status is not DEFAULT:
            if isinstance(ignore_status, int):
                ignore_status = (ignore_status,)
            client._ignore_status = ignore_status
        else:
            client._ignore_status = self._ignore_status

        if max_retries is not DEFAULT:
            if not isinstance(max_retries, int):
                raise TypeError("'max_retries' must be of type 'int'")
            client._max_retries = max_retries
        else:
            client._max_retries = self._max_retries

        if retry_on_status is not DEFAULT:
            if isinstance(retry_on_status, int):
                retry_on_status = (retry_on_status,)
            client._retry_on_status = retry_on_status
        else:
            client._retry_on_status = self._retry_on_status

        if retry_on_timeout is not DEFAULT:
            if not isinstance(retry_on_timeout, bool):
                raise TypeError("'retry_on_timeout' must be of type 'bool'")
            client._retry_on_timeout = retry_on_timeout
        else:
            client._retry_on_timeout = self._retry_on_timeout

        return client

    def close(self) -> None:
        """Closes the Transport and all internal connections"""
        self.transport.close()

    @_rewrite_parameters()
    def ping(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[t.List[str], str]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> bool:
        """
        Returns True if a successful response returns from the info() API,
        otherwise returns False. This API call can fail either at the transport
        layer (due to connection errors or timeouts) or from a non-2XX HTTP response
        (due to authentication or authorization issues).

        If you want to discover why the request failed you should use the ``info()`` API.

        `<https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html>`_
        """
        __path = "/"
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
        try:
            self.perform_request("HEAD", __path, params=__query, headers=__headers)
            return True
        except (ApiError, TransportError):
            return False

    # AUTO-GENERATED-API-DEFINITIONS #

    @_rewrite_parameters(
        body_name="operations",
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def bulk(
        self,
        *,
        operations: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        index: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        include_source_on_error: t.Optional[bool] = None,
        list_executed_pipelines: t.Optional[bool] = None,
        pipeline: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        require_alias: t.Optional[bool] = None,
        require_data_stream: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        source: t.Optional[t.Union[bool, t.Union[str, t.Sequence[str]]]] = None,
        source_excludes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        source_includes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Bulk index or delete documents.
          Perform multiple <code>index</code>, <code>create</code>, <code>delete</code>, and <code>update</code> actions in a single request.
          This reduces overhead and can greatly increase indexing speed.</p>
          <p>If the Elasticsearch security features are enabled, you must have the following index privileges for the target data stream, index, or index alias:</p>
          <ul>
          <li>To use the <code>create</code> action, you must have the <code>create_doc</code>, <code>create</code>, <code>index</code>, or <code>write</code> index privilege. Data streams support only the <code>create</code> action.</li>
          <li>To use the <code>index</code> action, you must have the <code>create</code>, <code>index</code>, or <code>write</code> index privilege.</li>
          <li>To use the <code>delete</code> action, you must have the <code>delete</code> or <code>write</code> index privilege.</li>
          <li>To use the <code>update</code> action, you must have the <code>index</code> or <code>write</code> index privilege.</li>
          <li>To automatically create a data stream or index with a bulk API request, you must have the <code>auto_configure</code>, <code>create_index</code>, or <code>manage</code> index privilege.</li>
          <li>To make the result of a bulk operation visible to search using the <code>refresh</code> parameter, you must have the <code>maintenance</code> or <code>manage</code> index privilege.</li>
          </ul>
          <p>Automatic data stream creation requires a matching index template with data stream enabled.</p>
          <p>The actions are specified in the request body using a newline delimited JSON (NDJSON) structure:</p>
          <pre><code>action_and_meta_data\\n
          optional_source\\n
          action_and_meta_data\\n
          optional_source\\n
          ....
          action_and_meta_data\\n
          optional_source\\n
          </code></pre>
          <p>The <code>index</code> and <code>create</code> actions expect a source on the next line and have the same semantics as the <code>op_type</code> parameter in the standard index API.
          A <code>create</code> action fails if a document with the same ID already exists in the target
          An <code>index</code> action adds or replaces a document as necessary.</p>
          <p>NOTE: Data streams support only the <code>create</code> action.
          To update or delete a document in a data stream, you must target the backing index containing the document.</p>
          <p>An <code>update</code> action expects that the partial doc, upsert, and script and its options are specified on the next line.</p>
          <p>A <code>delete</code> action does not expect a source on the next line and has the same semantics as the standard delete API.</p>
          <p>NOTE: The final line of data must end with a newline character (<code>\\n</code>).
          Each newline character may be preceded by a carriage return (<code>\\r</code>).
          When sending NDJSON data to the <code>_bulk</code> endpoint, use a <code>Content-Type</code> header of <code>application/json</code> or <code>application/x-ndjson</code>.
          Because this format uses literal newline characters (<code>\\n</code>) as delimiters, make sure that the JSON actions and sources are not pretty printed.</p>
          <p>If you provide a target in the request path, it is used for any actions that don't explicitly specify an <code>_index</code> argument.</p>
          <p>A note on the format: the idea here is to make processing as fast as possible.
          As some of the actions are redirected to other shards on other nodes, only <code>action_meta_data</code> is parsed on the receiving node side.</p>
          <p>Client libraries using this protocol should try and strive to do something similar on the client side, and reduce buffering as much as possible.</p>
          <p>There is no &quot;correct&quot; number of actions to perform in a single bulk request.
          Experiment with different settings to find the optimal size for your particular workload.
          Note that Elasticsearch limits the maximum size of a HTTP request to 100mb by default so clients must ensure that no request exceeds this size.
          It is not possible to index a single document that exceeds the size limit, so you must pre-process any such documents into smaller pieces before sending them to Elasticsearch.
          For instance, split documents into pages or chapters before indexing them, or store raw binary data in a system outside Elasticsearch and replace the raw data with a link to the external system in the documents that you send to Elasticsearch.</p>
          <p><strong>Client suppport for bulk requests</strong></p>
          <p>Some of the officially supported clients provide helpers to assist with bulk requests and reindexing:</p>
          <ul>
          <li>Go: Check out <code>esutil.BulkIndexer</code></li>
          <li>Perl: Check out <code>Search::Elasticsearch::Client::5_0::Bulk</code> and <code>Search::Elasticsearch::Client::5_0::Scroll</code></li>
          <li>Python: Check out <code>elasticsearch.helpers.*</code></li>
          <li>JavaScript: Check out <code>client.helpers.*</code></li>
          <li>.NET: Check out <code>BulkAllObservable</code></li>
          <li>PHP: Check out bulk indexing.</li>
          <li>Ruby: Check out <code>Elasticsearch::Helpers::BulkHelper</code></li>
          </ul>
          <p><strong>Submitting bulk requests with cURL</strong></p>
          <p>If you're providing text file input to <code>curl</code>, you must use the <code>--data-binary</code> flag instead of plain <code>-d</code>.
          The latter doesn't preserve newlines. For example:</p>
          <pre><code>$ cat requests
          { &quot;index&quot; : { &quot;_index&quot; : &quot;test&quot;, &quot;_id&quot; : &quot;1&quot; } }
          { &quot;field1&quot; : &quot;value1&quot; }
          $ curl -s -H &quot;Content-Type: application/x-ndjson&quot; -XPOST localhost:9200/_bulk --data-binary &quot;@requests&quot;; echo
          {&quot;took&quot;:7, &quot;errors&quot;: false, &quot;items&quot;:[{&quot;index&quot;:{&quot;_index&quot;:&quot;test&quot;,&quot;_id&quot;:&quot;1&quot;,&quot;_version&quot;:1,&quot;result&quot;:&quot;created&quot;,&quot;forced_refresh&quot;:false}}]}
          </code></pre>
          <p><strong>Optimistic concurrency control</strong></p>
          <p>Each <code>index</code> and <code>delete</code> action within a bulk API call may include the <code>if_seq_no</code> and <code>if_primary_term</code> parameters in their respective action and meta data lines.
          The <code>if_seq_no</code> and <code>if_primary_term</code> parameters control how operations are run, based on the last modification to existing documents. See Optimistic concurrency control for more details.</p>
          <p><strong>Versioning</strong></p>
          <p>Each bulk item can include the version value using the <code>version</code> field.
          It automatically follows the behavior of the index or delete operation based on the <code>_version</code> mapping.
          It also support the <code>version_type</code>.</p>
          <p><strong>Routing</strong></p>
          <p>Each bulk item can include the routing value using the <code>routing</code> field.
          It automatically follows the behavior of the index or delete operation based on the <code>_routing</code> mapping.</p>
          <p>NOTE: Data streams do not support custom routing unless they were created with the <code>allow_custom_routing</code> setting enabled in the template.</p>
          <p><strong>Wait for active shards</strong></p>
          <p>When making bulk calls, you can set the <code>wait_for_active_shards</code> parameter to require a minimum number of shard copies to be active before starting to process the bulk request.</p>
          <p><strong>Refresh</strong></p>
          <p>Control when the changes made by this request are visible to search.</p>
          <p>NOTE: Only the shards that receive the bulk request will be affected by refresh.
          Imagine a <code>_bulk?refresh=wait_for</code> request with three documents in it that happen to be routed to different shards in an index with five shards.
          The request will only wait for those three shards to refresh.
          The other two shards that make up the index do not participate in the <code>_bulk</code> request at all.</p>
          <p>You might want to disable the refresh interval temporarily to improve indexing throughput for large bulk requests.
          Refer to the linked documentation for step-by-step instructions using the index settings API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk>`_

        :param operations:
        :param index: The name of the data stream, index, or index alias to perform bulk
            actions on.
        :param include_source_on_error: True or false if to include the document source
            in the error message in case of parsing errors.
        :param list_executed_pipelines: If `true`, the response will include the ingest
            pipelines that were run for each index or create.
        :param pipeline: The pipeline identifier to use to preprocess incoming documents.
            If the index has a default ingest pipeline specified, setting the value to
            `_none` turns off the default ingest pipeline for this request. If a final
            pipeline is configured, it will always run regardless of the value of this
            parameter.
        :param refresh: If `true`, Elasticsearch refreshes the affected shards to make
            this operation visible to search. If `wait_for`, wait for a refresh to make
            this operation visible to search. If `false`, do nothing with refreshes.
            Valid values: `true`, `false`, `wait_for`.
        :param require_alias: If `true`, the request's actions must target an index alias.
        :param require_data_stream: If `true`, the request's actions must target a data
            stream (existing or to be created).
        :param routing: A custom value that is used to route operations to a specific
            shard.
        :param source: Indicates whether to return the `_source` field (`true` or `false`)
            or contains a list of fields to return.
        :param source_excludes: A comma-separated list of source fields to exclude from
            the response. You can also use this parameter to exclude fields from the
            subset specified in `_source_includes` query parameter. If the `_source`
            parameter is `false`, this parameter is ignored.
        :param source_includes: A comma-separated list of source fields to include in
            the response. If this parameter is specified, only these source fields are
            returned. You can exclude fields from this subset using the `_source_excludes`
            query parameter. If the `_source` parameter is `false`, this parameter is
            ignored.
        :param timeout: The period each action waits for the following operations: automatic
            index creation, dynamic mapping updates, and waiting for active shards. The
            default is `1m` (one minute), which guarantees Elasticsearch waits for at
            least the timeout before failing. The actual wait time could be longer, particularly
            when multiple waits occur.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`). The
            default is `1`, which waits for each primary shard to be active.
        """
        if operations is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'operations' and 'body', one of them should be set."
            )
        elif operations is not None and body is not None:
            raise ValueError("Cannot set both 'operations' and 'body'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_bulk'
        else:
            __path_parts = {}
            __path = "/_bulk"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if include_source_on_error is not None:
            __query["include_source_on_error"] = include_source_on_error
        if list_executed_pipelines is not None:
            __query["list_executed_pipelines"] = list_executed_pipelines
        if pipeline is not None:
            __query["pipeline"] = pipeline
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if require_alias is not None:
            __query["require_alias"] = require_alias
        if require_data_stream is not None:
            __query["require_data_stream"] = require_data_stream
        if routing is not None:
            __query["routing"] = routing
        if source is not None:
            __query["_source"] = source
        if source_excludes is not None:
            __query["_source_excludes"] = source_excludes
        if source_includes is not None:
            __query["_source_includes"] = source_includes
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        __body = operations if operations is not None else body
        __headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="bulk",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("scroll_id",),
    )
    def clear_scroll(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        scroll_id: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Clear a scrolling search.
          Clear the search context and results for a scrolling search.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-clear-scroll>`_

        :param scroll_id: The scroll IDs to clear. To clear all scroll IDs, use `_all`.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_search/scroll"
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
            if scroll_id is not None:
                __body["scroll_id"] = scroll_id
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="clear_scroll",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("id",),
    )
    def close_point_in_time(
        self,
        *,
        id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Close a point in time.
          A point in time must be opened explicitly before being used in search requests.
          The <code>keep_alive</code> parameter tells Elasticsearch how long it should persist.
          A point in time is automatically closed when the <code>keep_alive</code> period has elapsed.
          However, keeping points in time has a cost; close them as soon as they are no longer required for search requests.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-open-point-in-time>`_

        :param id: The ID of the point-in-time.
        """
        if id is None and body is None:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_pit"
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
            if id is not None:
                __body["id"] = id
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/json"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="close_point_in_time",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("query",),
    )
    def count(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
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
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ignore_throttled: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        lenient: t.Optional[bool] = None,
        min_score: t.Optional[float] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        q: t.Optional[str] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        routing: t.Optional[str] = None,
        terminate_after: t.Optional[int] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Count search results.
          Get the number of documents matching a query.</p>
          <p>The query can be provided either by using a simple query string as a parameter, or by defining Query DSL within the request body.
          The query is optional. When no query is provided, the API uses <code>match_all</code> to count all the documents.</p>
          <p>The count API supports multi-target syntax. You can run a single count API search across multiple data streams and indices.</p>
          <p>The operation is broadcast across all shards.
          For each shard ID group, a replica is chosen and the search is run against it.
          This means that replicas increase the scalability of the count.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-count>`_

        :param index: A comma-separated list of data streams, indices, and aliases to
            search. It supports wildcards (`*`). To search all data streams and indices,
            omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param analyze_wildcard: If `true`, wildcard and prefix queries are analyzed.
            This parameter can be used only when the `q` query string parameter is specified.
        :param analyzer: The analyzer to use for the query string. This parameter can
            be used only when the `q` query string parameter is specified.
        :param default_operator: The default operator for query string query: `AND` or
            `OR`. This parameter can be used only when the `q` query string parameter
            is specified.
        :param df: The field to use as a default when no field prefix is given in the
            query string. This parameter can be used only when the `q` query string parameter
            is specified.
        :param expand_wildcards: The type of index that wildcard patterns can match.
            If the request can target data streams, this argument determines whether
            wildcard expressions match hidden data streams. It supports comma-separated
            values, such as `open,hidden`.
        :param ignore_throttled: If `true`, concrete, expanded, or aliased indices are
            ignored when frozen.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param lenient: If `true`, format-based query failures (such as providing text
            to a numeric field) in the query string will be ignored. This parameter can
            be used only when the `q` query string parameter is specified.
        :param min_score: The minimum `_score` value that documents must have to be included
            in the result.
        :param preference: The node or shard the operation should be performed on. By
            default, it is random.
        :param q: The query in Lucene query string syntax. This parameter cannot be used
            with a request body.
        :param query: Defines the search query using Query DSL. A request body query
            cannot be used with the `q` query string parameter.
        :param routing: A custom value used to route operations to a specific shard.
        :param terminate_after: The maximum number of documents to collect for each shard.
            If a query reaches this limit, Elasticsearch terminates the query early.
            Elasticsearch collects documents before sorting. IMPORTANT: Use with caution.
            Elasticsearch applies this parameter to each shard handling the request.
            When possible, let Elasticsearch perform early termination automatically.
            Avoid specifying this parameter for requests that target data streams with
            backing indices across multiple data tiers.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_count'
        else:
            __path_parts = {}
            __path = "/_count"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
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
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if ignore_throttled is not None:
            __query["ignore_throttled"] = ignore_throttled
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if lenient is not None:
            __query["lenient"] = lenient
        if min_score is not None:
            __query["min_score"] = min_score
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if q is not None:
            __query["q"] = q
        if routing is not None:
            __query["routing"] = routing
        if terminate_after is not None:
            __query["terminate_after"] = terminate_after
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
            endpoint_id="count",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="document",
    )
    def create(
        self,
        *,
        index: str,
        id: str,
        document: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        include_source_on_error: t.Optional[bool] = None,
        pipeline: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        require_alias: t.Optional[bool] = None,
        require_data_stream: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        version: t.Optional[int] = None,
        version_type: t.Optional[
            t.Union[str, t.Literal["external", "external_gte", "force", "internal"]]
        ] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create a new document in the index.</p>
          <p>You can index a new JSON document with the <code>/&lt;target&gt;/_doc/</code> or <code>/&lt;target&gt;/_create/&lt;_id&gt;</code> APIs
          Using <code>_create</code> guarantees that the document is indexed only if it does not already exist.
          It returns a 409 response when a document with a same ID already exists in the index.
          To update an existing document, you must use the <code>/&lt;target&gt;/_doc/</code> API.</p>
          <p>If the Elasticsearch security features are enabled, you must have the following index privileges for the target data stream, index, or index alias:</p>
          <ul>
          <li>To add a document using the <code>PUT /&lt;target&gt;/_create/&lt;_id&gt;</code> or <code>POST /&lt;target&gt;/_create/&lt;_id&gt;</code> request formats, you must have the <code>create_doc</code>, <code>create</code>, <code>index</code>, or <code>write</code> index privilege.</li>
          <li>To automatically create a data stream or index with this API request, you must have the <code>auto_configure</code>, <code>create_index</code>, or <code>manage</code> index privilege.</li>
          </ul>
          <p>Automatic data stream creation requires a matching index template with data stream enabled.</p>
          <p><strong>Automatically create data streams and indices</strong></p>
          <p>If the request's target doesn't exist and matches an index template with a <code>data_stream</code> definition, the index operation automatically creates the data stream.</p>
          <p>If the target doesn't exist and doesn't match a data stream template, the operation automatically creates the index and applies any matching index templates.</p>
          <p>NOTE: Elasticsearch includes several built-in index templates. To avoid naming collisions with these templates, refer to index pattern documentation.</p>
          <p>If no mapping exists, the index operation creates a dynamic mapping.
          By default, new fields and objects are automatically added to the mapping if needed.</p>
          <p>Automatic index creation is controlled by the <code>action.auto_create_index</code> setting.
          If it is <code>true</code>, any index can be created automatically.
          You can modify this setting to explicitly allow or block automatic creation of indices that match specified patterns or set it to <code>false</code> to turn off automatic index creation entirely.
          Specify a comma-separated list of patterns you want to allow or prefix each pattern with <code>+</code> or <code>-</code> to indicate whether it should be allowed or blocked.
          When a list is specified, the default behaviour is to disallow.</p>
          <p>NOTE: The <code>action.auto_create_index</code> setting affects the automatic creation of indices only.
          It does not affect the creation of data streams.</p>
          <p><strong>Routing</strong></p>
          <p>By default, shard placement — or routing — is controlled by using a hash of the document's ID value.
          For more explicit control, the value fed into the hash function used by the router can be directly specified on a per-operation basis using the <code>routing</code> parameter.</p>
          <p>When setting up explicit mapping, you can also use the <code>_routing</code> field to direct the index operation to extract the routing value from the document itself.
          This does come at the (very minimal) cost of an additional document parsing pass.
          If the <code>_routing</code> mapping is defined and set to be required, the index operation will fail if no routing value is provided or extracted.</p>
          <p>NOTE: Data streams do not support custom routing unless they were created with the <code>allow_custom_routing</code> setting enabled in the template.</p>
          <p><strong>Distributed</strong></p>
          <p>The index operation is directed to the primary shard based on its route and performed on the actual node containing this shard.
          After the primary shard completes the operation, if needed, the update is distributed to applicable replicas.</p>
          <p><strong>Active shards</strong></p>
          <p>To improve the resiliency of writes to the system, indexing operations can be configured to wait for a certain number of active shard copies before proceeding with the operation.
          If the requisite number of active shard copies are not available, then the write operation must wait and retry, until either the requisite shard copies have started or a timeout occurs.
          By default, write operations only wait for the primary shards to be active before proceeding (that is to say <code>wait_for_active_shards</code> is <code>1</code>).
          This default can be overridden in the index settings dynamically by setting <code>index.write.wait_for_active_shards</code>.
          To alter this behavior per operation, use the <code>wait_for_active_shards request</code> parameter.</p>
          <p>Valid values are all or any positive integer up to the total number of configured copies per shard in the index (which is <code>number_of_replicas</code>+1).
          Specifying a negative value or a number greater than the number of shard copies will throw an error.</p>
          <p>For example, suppose you have a cluster of three nodes, A, B, and C and you create an index index with the number of replicas set to 3 (resulting in 4 shard copies, one more copy than there are nodes).
          If you attempt an indexing operation, by default the operation will only ensure the primary copy of each shard is available before proceeding.
          This means that even if B and C went down and A hosted the primary shard copies, the indexing operation would still proceed with only one copy of the data.
          If <code>wait_for_active_shards</code> is set on the request to <code>3</code> (and all three nodes are up), the indexing operation will require 3 active shard copies before proceeding.
          This requirement should be met because there are 3 active nodes in the cluster, each one holding a copy of the shard.
          However, if you set <code>wait_for_active_shards</code> to <code>all</code> (or to <code>4</code>, which is the same in this situation), the indexing operation will not proceed as you do not have all 4 copies of each shard active in the index.
          The operation will timeout unless a new node is brought up in the cluster to host the fourth copy of the shard.</p>
          <p>It is important to note that this setting greatly reduces the chances of the write operation not writing to the requisite number of shard copies, but it does not completely eliminate the possibility, because this check occurs before the write operation starts.
          After the write operation is underway, it is still possible for replication to fail on any number of shard copies but still succeed on the primary.
          The <code>_shards</code> section of the API response reveals the number of shard copies on which replication succeeded and failed.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create>`_

        :param index: The name of the data stream or index to target. If the target doesn't
            exist and matches the name or wildcard (`*`) pattern of an index template
            with a `data_stream` definition, this request creates the data stream. If
            the target doesn't exist and doesn’t match a data stream template, this request
            creates the index.
        :param id: A unique identifier for the document. To automatically generate a
            document ID, use the `POST /<target>/_doc/` request format.
        :param document:
        :param include_source_on_error: True or false if to include the document source
            in the error message in case of parsing errors.
        :param pipeline: The ID of the pipeline to use to preprocess incoming documents.
            If the index has a default ingest pipeline specified, setting the value to
            `_none` turns off the default ingest pipeline for this request. If a final
            pipeline is configured, it will always run regardless of the value of this
            parameter.
        :param refresh: If `true`, Elasticsearch refreshes the affected shards to make
            this operation visible to search. If `wait_for`, it waits for a refresh to
            make this operation visible to search. If `false`, it does nothing with refreshes.
        :param require_alias: If `true`, the destination must be an index alias.
        :param require_data_stream: If `true`, the request's actions must target a data
            stream (existing or to be created).
        :param routing: A custom value that is used to route operations to a specific
            shard.
        :param timeout: The period the request waits for the following operations: automatic
            index creation, dynamic mapping updates, waiting for active shards. Elasticsearch
            waits for at least the specified timeout period before failing. The actual
            wait time could be longer, particularly when multiple waits occur. This parameter
            is useful for situations where the primary shard assigned to perform the
            operation might not be available when the operation runs. Some reasons for
            this might be that the primary shard is currently recovering from a gateway
            or undergoing relocation. By default, the operation will wait on the primary
            shard to become available for at least 1 minute before failing and responding
            with an error. The actual wait time could be longer, particularly when multiple
            waits occur.
        :param version: The explicit version number for concurrency control. It must
            be a non-negative long number.
        :param version_type: The version type.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. You can set it to `all` or any positive
            integer up to the total number of shards in the index (`number_of_replicas+1`).
            The default value of `1` means it waits for each primary shard to be active.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        if document is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'document' and 'body', one of them should be set."
            )
        elif document is not None and body is not None:
            raise ValueError("Cannot set both 'document' and 'body'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "id": _quote(id)}
        __path = f'/{__path_parts["index"]}/_create/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if include_source_on_error is not None:
            __query["include_source_on_error"] = include_source_on_error
        if pipeline is not None:
            __query["pipeline"] = pipeline
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if require_alias is not None:
            __query["require_alias"] = require_alias
        if require_data_stream is not None:
            __query["require_data_stream"] = require_data_stream
        if routing is not None:
            __query["routing"] = routing
        if timeout is not None:
            __query["timeout"] = timeout
        if version is not None:
            __query["version"] = version
        if version_type is not None:
            __query["version_type"] = version_type
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        __body = document if document is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="create",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete(
        self,
        *,
        index: str,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        if_primary_term: t.Optional[int] = None,
        if_seq_no: t.Optional[int] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        routing: t.Optional[str] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        version: t.Optional[int] = None,
        version_type: t.Optional[
            t.Union[str, t.Literal["external", "external_gte", "force", "internal"]]
        ] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a document.</p>
          <p>Remove a JSON document from the specified index.</p>
          <p>NOTE: You cannot send deletion requests directly to a data stream.
          To delete a document in a data stream, you must target the backing index containing the document.</p>
          <p><strong>Optimistic concurrency control</strong></p>
          <p>Delete operations can be made conditional and only be performed if the last modification to the document was assigned the sequence number and primary term specified by the <code>if_seq_no</code> and <code>if_primary_term</code> parameters.
          If a mismatch is detected, the operation will result in a <code>VersionConflictException</code> and a status code of <code>409</code>.</p>
          <p><strong>Versioning</strong></p>
          <p>Each document indexed is versioned.
          When deleting a document, the version can be specified to make sure the relevant document you are trying to delete is actually being deleted and it has not changed in the meantime.
          Every write operation run on a document, deletes included, causes its version to be incremented.
          The version number of a deleted document remains available for a short time after deletion to allow for control of concurrent operations.
          The length of time for which a deleted document's version remains available is determined by the <code>index.gc_deletes</code> index setting.</p>
          <p><strong>Routing</strong></p>
          <p>If routing is used during indexing, the routing value also needs to be specified to delete a document.</p>
          <p>If the <code>_routing</code> mapping is set to <code>required</code> and no routing value is specified, the delete API throws a <code>RoutingMissingException</code> and rejects the request.</p>
          <p>For example:</p>
          <pre><code>DELETE /my-index-000001/_doc/1?routing=shard-1
          </code></pre>
          <p>This request deletes the document with ID 1, but it is routed based on the user.
          The document is not deleted if the correct routing is not specified.</p>
          <p><strong>Distributed</strong></p>
          <p>The delete operation gets hashed into a specific shard ID.
          It then gets redirected into the primary shard within that ID group and replicated (if needed) to shard replicas within that ID group.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete>`_

        :param index: The name of the target index.
        :param id: A unique identifier for the document.
        :param if_primary_term: Only perform the operation if the document has this primary
            term.
        :param if_seq_no: Only perform the operation if the document has this sequence
            number.
        :param refresh: If `true`, Elasticsearch refreshes the affected shards to make
            this operation visible to search. If `wait_for`, it waits for a refresh to
            make this operation visible to search. If `false`, it does nothing with refreshes.
        :param routing: A custom value used to route operations to a specific shard.
        :param timeout: The period to wait for active shards. This parameter is useful
            for situations where the primary shard assigned to perform the delete operation
            might not be available when the delete operation runs. Some reasons for this
            might be that the primary shard is currently recovering from a store or undergoing
            relocation. By default, the delete operation will wait on the primary shard
            to become available for up to 1 minute before failing and responding with
            an error.
        :param version: An explicit version number for concurrency control. It must match
            the current version of the document for the request to succeed.
        :param version_type: The version type.
        :param wait_for_active_shards: The minimum number of shard copies that must be
            active before proceeding with the operation. You can set it to `all` or any
            positive integer up to the total number of shards in the index (`number_of_replicas+1`).
            The default value of `1` means it waits for each primary shard to be active.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "id": _quote(id)}
        __path = f'/{__path_parts["index"]}/_doc/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if if_primary_term is not None:
            __query["if_primary_term"] = if_primary_term
        if if_seq_no is not None:
            __query["if_seq_no"] = if_seq_no
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if routing is not None:
            __query["routing"] = routing
        if timeout is not None:
            __query["timeout"] = timeout
        if version is not None:
            __query["version"] = version
        if version_type is not None:
            __query["version_type"] = version_type
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "DELETE",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="delete",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("max_docs", "query", "slice", "sort"),
        parameter_aliases={"from": "from_"},
    )
    def delete_by_query(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        analyze_wildcard: t.Optional[bool] = None,
        analyzer: t.Optional[str] = None,
        conflicts: t.Optional[t.Union[str, t.Literal["abort", "proceed"]]] = None,
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
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        lenient: t.Optional[bool] = None,
        max_docs: t.Optional[int] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        q: t.Optional[str] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        refresh: t.Optional[bool] = None,
        request_cache: t.Optional[bool] = None,
        requests_per_second: t.Optional[float] = None,
        routing: t.Optional[str] = None,
        scroll: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        scroll_size: t.Optional[int] = None,
        search_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        search_type: t.Optional[
            t.Union[str, t.Literal["dfs_query_then_fetch", "query_then_fetch"]]
        ] = None,
        slice: t.Optional[t.Mapping[str, t.Any]] = None,
        slices: t.Optional[t.Union[int, t.Union[str, t.Literal["auto"]]]] = None,
        sort: t.Optional[
            t.Union[
                t.Sequence[t.Union[str, t.Mapping[str, t.Any]]],
                t.Union[str, t.Mapping[str, t.Any]],
            ]
        ] = None,
        stats: t.Optional[t.Sequence[str]] = None,
        terminate_after: t.Optional[int] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        version: t.Optional[bool] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
        wait_for_completion: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete documents.</p>
          <p>Deletes documents that match the specified query.</p>
          <p>If the Elasticsearch security features are enabled, you must have the following index privileges for the target data stream, index, or alias:</p>
          <ul>
          <li><code>read</code></li>
          <li><code>delete</code> or <code>write</code></li>
          </ul>
          <p>You can specify the query criteria in the request URI or the request body using the same syntax as the search API.
          When you submit a delete by query request, Elasticsearch gets a snapshot of the data stream or index when it begins processing the request and deletes matching documents using internal versioning.
          If a document changes between the time that the snapshot is taken and the delete operation is processed, it results in a version conflict and the delete operation fails.</p>
          <p>NOTE: Documents with a version equal to 0 cannot be deleted using delete by query because internal versioning does not support 0 as a valid version number.</p>
          <p>While processing a delete by query request, Elasticsearch performs multiple search requests sequentially to find all of the matching documents to delete.
          A bulk delete request is performed for each batch of matching documents.
          If a search or bulk request is rejected, the requests are retried up to 10 times, with exponential back off.
          If the maximum retry limit is reached, processing halts and all failed requests are returned in the response.
          Any delete requests that completed successfully still stick, they are not rolled back.</p>
          <p>You can opt to count version conflicts instead of halting and returning by setting <code>conflicts</code> to <code>proceed</code>.
          Note that if you opt to count version conflicts the operation could attempt to delete more documents from the source than <code>max_docs</code> until it has successfully deleted <code>max_docs documents</code>, or it has gone through every document in the source query.</p>
          <p><strong>Throttling delete requests</strong></p>
          <p>To control the rate at which delete by query issues batches of delete operations, you can set <code>requests_per_second</code> to any positive decimal number.
          This pads each batch with a wait time to throttle the rate.
          Set <code>requests_per_second</code> to <code>-1</code> to disable throttling.</p>
          <p>Throttling uses a wait time between batches so that the internal scroll requests can be given a timeout that takes the request padding into account.
          The padding time is the difference between the batch size divided by the <code>requests_per_second</code> and the time spent writing.
          By default the batch size is <code>1000</code>, so if <code>requests_per_second</code> is set to <code>500</code>:</p>
          <pre><code>target_time = 1000 / 500 per second = 2 seconds
          wait_time = target_time - write_time = 2 seconds - .5 seconds = 1.5 seconds
          </code></pre>
          <p>Since the batch is issued as a single <code>_bulk</code> request, large batch sizes cause Elasticsearch to create many requests and wait before starting the next set.
          This is &quot;bursty&quot; instead of &quot;smooth&quot;.</p>
          <p><strong>Slicing</strong></p>
          <p>Delete by query supports sliced scroll to parallelize the delete process.
          This can improve efficiency and provide a convenient way to break the request down into smaller parts.</p>
          <p>Setting <code>slices</code> to <code>auto</code> lets Elasticsearch choose the number of slices to use.
          This setting will use one slice per shard, up to a certain limit.
          If there are multiple source data streams or indices, it will choose the number of slices based on the index or backing index with the smallest number of shards.
          Adding slices to the delete by query operation creates sub-requests which means it has some quirks:</p>
          <ul>
          <li>You can see these requests in the tasks APIs. These sub-requests are &quot;child&quot; tasks of the task for the request with slices.</li>
          <li>Fetching the status of the task for the request with slices only contains the status of completed slices.</li>
          <li>These sub-requests are individually addressable for things like cancellation and rethrottling.</li>
          <li>Rethrottling the request with <code>slices</code> will rethrottle the unfinished sub-request proportionally.</li>
          <li>Canceling the request with <code>slices</code> will cancel each sub-request.</li>
          <li>Due to the nature of <code>slices</code> each sub-request won't get a perfectly even portion of the documents. All documents will be addressed, but some slices may be larger than others. Expect larger slices to have a more even distribution.</li>
          <li>Parameters like <code>requests_per_second</code> and <code>max_docs</code> on a request with <code>slices</code> are distributed proportionally to each sub-request. Combine that with the earlier point about distribution being uneven and you should conclude that using <code>max_docs</code> with <code>slices</code> might not result in exactly <code>max_docs</code> documents being deleted.</li>
          <li>Each sub-request gets a slightly different snapshot of the source data stream or index though these are all taken at approximately the same time.</li>
          </ul>
          <p>If you're slicing manually or otherwise tuning automatic slicing, keep in mind that:</p>
          <ul>
          <li>Query performance is most efficient when the number of slices is equal to the number of shards in the index or backing index. If that number is large (for example, 500), choose a lower number as too many <code>slices</code> hurts performance. Setting <code>slices</code> higher than the number of shards generally does not improve efficiency and adds overhead.</li>
          <li>Delete performance scales linearly across available resources with the number of slices.</li>
          </ul>
          <p>Whether query or delete performance dominates the runtime depends on the documents being reindexed and cluster resources.</p>
          <p><strong>Cancel a delete by query operation</strong></p>
          <p>Any delete by query can be canceled using the task cancel API. For example:</p>
          <pre><code>POST _tasks/r1A2WoRbTwKZ516z6NEs5A:36619/_cancel
          </code></pre>
          <p>The task ID can be found by using the get tasks API.</p>
          <p>Cancellation should happen quickly but might take a few seconds.
          The get task status API will continue to list the delete by query task until this task checks that it has been cancelled and terminates itself.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete-by-query>`_

        :param index: A comma-separated list of data streams, indices, and aliases to
            search. It supports wildcards (`*`). To search all data streams or indices,
            omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param analyze_wildcard: If `true`, wildcard and prefix queries are analyzed.
            This parameter can be used only when the `q` query string parameter is specified.
        :param analyzer: Analyzer to use for the query string. This parameter can be
            used only when the `q` query string parameter is specified.
        :param conflicts: What to do if delete by query hits version conflicts: `abort`
            or `proceed`.
        :param default_operator: The default operator for query string query: `AND` or
            `OR`. This parameter can be used only when the `q` query string parameter
            is specified.
        :param df: The field to use as default where no field prefix is given in the
            query string. This parameter can be used only when the `q` query string parameter
            is specified.
        :param expand_wildcards: The type of index that wildcard patterns can match.
            If the request can target data streams, this argument determines whether
            wildcard expressions match hidden data streams. It supports comma-separated
            values, such as `open,hidden`.
        :param from_: Skips the specified number of documents.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param lenient: If `true`, format-based query failures (such as providing text
            to a numeric field) in the query string will be ignored. This parameter can
            be used only when the `q` query string parameter is specified.
        :param max_docs: The maximum number of documents to delete.
        :param preference: The node or shard the operation should be performed on. It
            is random by default.
        :param q: A query in the Lucene query string syntax.
        :param query: The documents to delete specified with Query DSL.
        :param refresh: If `true`, Elasticsearch refreshes all shards involved in the
            delete by query after the request completes. This is different than the delete
            API's `refresh` parameter, which causes just the shard that received the
            delete request to be refreshed. Unlike the delete API, it does not support
            `wait_for`.
        :param request_cache: If `true`, the request cache is used for this request.
            Defaults to the index-level setting.
        :param requests_per_second: The throttle for this request in sub-requests per
            second.
        :param routing: A custom value used to route operations to a specific shard.
        :param scroll: The period to retain the search context for scrolling.
        :param scroll_size: The size of the scroll request that powers the operation.
        :param search_timeout: The explicit timeout for each search request. It defaults
            to no timeout.
        :param search_type: The type of the search operation. Available options include
            `query_then_fetch` and `dfs_query_then_fetch`.
        :param slice: Slice the request manually using the provided slice ID and total
            number of slices.
        :param slices: The number of slices this task should be divided into.
        :param sort: A sort object that specifies the order of deleted documents.
        :param stats: The specific `tag` of the request for logging and statistical purposes.
        :param terminate_after: The maximum number of documents to collect for each shard.
            If a query reaches this limit, Elasticsearch terminates the query early.
            Elasticsearch collects documents before sorting. Use with caution. Elasticsearch
            applies this parameter to each shard handling the request. When possible,
            let Elasticsearch perform early termination automatically. Avoid specifying
            this parameter for requests that target data streams with backing indices
            across multiple data tiers.
        :param timeout: The period each deletion request waits for active shards.
        :param version: If `true`, returns the document version as part of a hit.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`). The
            `timeout` value controls how long each write request waits for unavailable
            shards to become available.
        :param wait_for_completion: If `true`, the request blocks until the operation
            is complete. If `false`, Elasticsearch performs some preflight checks, launches
            the request, and returns a task you can use to cancel or get the status of
            the task. Elasticsearch creates a record of this task as a document at `.tasks/task/${taskId}`.
            When you are done with a task, you should delete the task document so Elasticsearch
            can reclaim the space.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_delete_by_query'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        # The 'sort' parameter with a colon can't be encoded to the body.
        if sort is not None and (
            (isinstance(sort, str) and ":" in sort)
            or (
                isinstance(sort, (list, tuple))
                and all(isinstance(_x, str) for _x in sort)
                and any(":" in _x for _x in sort)
            )
        ):
            __query["sort"] = sort
            sort = None
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if analyze_wildcard is not None:
            __query["analyze_wildcard"] = analyze_wildcard
        if analyzer is not None:
            __query["analyzer"] = analyzer
        if conflicts is not None:
            __query["conflicts"] = conflicts
        if default_operator is not None:
            __query["default_operator"] = default_operator
        if df is not None:
            __query["df"] = df
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if from_ is not None:
            __query["from"] = from_
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if lenient is not None:
            __query["lenient"] = lenient
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if q is not None:
            __query["q"] = q
        if refresh is not None:
            __query["refresh"] = refresh
        if request_cache is not None:
            __query["request_cache"] = request_cache
        if requests_per_second is not None:
            __query["requests_per_second"] = requests_per_second
        if routing is not None:
            __query["routing"] = routing
        if scroll is not None:
            __query["scroll"] = scroll
        if scroll_size is not None:
            __query["scroll_size"] = scroll_size
        if search_timeout is not None:
            __query["search_timeout"] = search_timeout
        if search_type is not None:
            __query["search_type"] = search_type
        if slices is not None:
            __query["slices"] = slices
        if stats is not None:
            __query["stats"] = stats
        if terminate_after is not None:
            __query["terminate_after"] = terminate_after
        if timeout is not None:
            __query["timeout"] = timeout
        if version is not None:
            __query["version"] = version
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        if wait_for_completion is not None:
            __query["wait_for_completion"] = wait_for_completion
        if not __body:
            if max_docs is not None:
                __body["max_docs"] = max_docs
            if query is not None:
                __body["query"] = query
            if slice is not None:
                __body["slice"] = slice
            if sort is not None:
                __body["sort"] = sort
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="delete_by_query",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete_by_query_rethrottle(
        self,
        *,
        task_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        requests_per_second: t.Optional[float] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Throttle a delete by query operation.</p>
          <p>Change the number of requests per second for a particular delete by query operation.
          Rethrottling that speeds up the query takes effect immediately but rethrotting that slows down the query takes effect after completing the current batch to prevent scroll timeouts.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete-by-query-rethrottle>`_

        :param task_id: The ID for the task.
        :param requests_per_second: The throttle for this request in sub-requests per
            second. To disable throttling, set it to `-1`.
        """
        if task_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_id'")
        __path_parts: t.Dict[str, str] = {"task_id": _quote(task_id)}
        __path = f'/_delete_by_query/{__path_parts["task_id"]}/_rethrottle'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if requests_per_second is not None:
            __query["requests_per_second"] = requests_per_second
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="delete_by_query_rethrottle",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def delete_script(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Delete a script or search template.
          Deletes a stored script or search template.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-delete-script>`_

        :param id: The identifier for the stored script or search template.
        :param master_timeout: The period to wait for a connection to the master node.
            If no response is received before the timeout expires, the request fails
            and returns an error. It can also be set to `-1` to indicate that the request
            should never timeout.
        :param timeout: The period to wait for a response. If no response is received
            before the timeout expires, the request fails and returns an error. It can
            also be set to `-1` to indicate that the request should never timeout.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_scripts/{__path_parts["id"]}'
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
            endpoint_id="delete_script",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def exists(
        self,
        *,
        index: str,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        realtime: t.Optional[bool] = None,
        refresh: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        source: t.Optional[t.Union[bool, t.Union[str, t.Sequence[str]]]] = None,
        source_excludes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        source_includes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        stored_fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        version: t.Optional[int] = None,
        version_type: t.Optional[
            t.Union[str, t.Literal["external", "external_gte", "force", "internal"]]
        ] = None,
    ) -> HeadApiResponse:
        """
        .. raw:: html

          <p>Check a document.</p>
          <p>Verify that a document exists.
          For example, check to see if a document with the <code>_id</code> 0 exists:</p>
          <pre><code>HEAD my-index-000001/_doc/0
          </code></pre>
          <p>If the document exists, the API returns a status code of <code>200 - OK</code>.
          If the document doesn’t exist, the API returns <code>404 - Not Found</code>.</p>
          <p><strong>Versioning support</strong></p>
          <p>You can use the <code>version</code> parameter to check the document only if its current version is equal to the specified one.</p>
          <p>Internally, Elasticsearch has marked the old document as deleted and added an entirely new document.
          The old version of the document doesn't disappear immediately, although you won't be able to access it.
          Elasticsearch cleans up deleted documents in the background as you continue to index more data.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get>`_

        :param index: A comma-separated list of data streams, indices, and aliases. It
            supports wildcards (`*`).
        :param id: A unique document identifier.
        :param preference: The node or shard the operation should be performed on. By
            default, the operation is randomized between the shard replicas. If it is
            set to `_local`, the operation will prefer to be run on a local allocated
            shard when possible. If it is set to a custom value, the value is used to
            guarantee that the same shards will be used for the same custom value. This
            can help with "jumping values" when hitting different shards in different
            refresh states. A sample value can be something like the web session ID or
            the user name.
        :param realtime: If `true`, the request is real-time as opposed to near-real-time.
        :param refresh: If `true`, the request refreshes the relevant shards before retrieving
            the document. Setting it to `true` should be done after careful thought and
            verification that this does not cause a heavy load on the system (and slow
            down indexing).
        :param routing: A custom value used to route operations to a specific shard.
        :param source: Indicates whether to return the `_source` field (`true` or `false`)
            or lists the fields to return.
        :param source_excludes: A comma-separated list of source fields to exclude from
            the response. You can also use this parameter to exclude fields from the
            subset specified in `_source_includes` query parameter. If the `_source`
            parameter is `false`, this parameter is ignored.
        :param source_includes: A comma-separated list of source fields to include in
            the response. If this parameter is specified, only these source fields are
            returned. You can exclude fields from this subset using the `_source_excludes`
            query parameter. If the `_source` parameter is `false`, this parameter is
            ignored.
        :param stored_fields: A comma-separated list of stored fields to return as part
            of a hit. If no fields are specified, no stored fields are included in the
            response. If this field is specified, the `_source` parameter defaults to
            `false`.
        :param version: Explicit version number for concurrency control. The specified
            version must match the current version of the document for the request to
            succeed.
        :param version_type: The version type.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "id": _quote(id)}
        __path = f'/{__path_parts["index"]}/_doc/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if realtime is not None:
            __query["realtime"] = realtime
        if refresh is not None:
            __query["refresh"] = refresh
        if routing is not None:
            __query["routing"] = routing
        if source is not None:
            __query["_source"] = source
        if source_excludes is not None:
            __query["_source_excludes"] = source_excludes
        if source_includes is not None:
            __query["_source_includes"] = source_includes
        if stored_fields is not None:
            __query["stored_fields"] = stored_fields
        if version is not None:
            __query["version"] = version
        if version_type is not None:
            __query["version_type"] = version_type
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "HEAD",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="exists",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def exists_source(
        self,
        *,
        index: str,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        realtime: t.Optional[bool] = None,
        refresh: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        source: t.Optional[t.Union[bool, t.Union[str, t.Sequence[str]]]] = None,
        source_excludes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        source_includes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        version: t.Optional[int] = None,
        version_type: t.Optional[
            t.Union[str, t.Literal["external", "external_gte", "force", "internal"]]
        ] = None,
    ) -> HeadApiResponse:
        """
        .. raw:: html

          <p>Check for a document source.</p>
          <p>Check whether a document source exists in an index.
          For example:</p>
          <pre><code>HEAD my-index-000001/_source/1
          </code></pre>
          <p>A document's source is not available if it is disabled in the mapping.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get>`_

        :param index: A comma-separated list of data streams, indices, and aliases. It
            supports wildcards (`*`).
        :param id: A unique identifier for the document.
        :param preference: The node or shard the operation should be performed on. By
            default, the operation is randomized between the shard replicas.
        :param realtime: If `true`, the request is real-time as opposed to near-real-time.
        :param refresh: If `true`, the request refreshes the relevant shards before retrieving
            the document. Setting it to `true` should be done after careful thought and
            verification that this does not cause a heavy load on the system (and slow
            down indexing).
        :param routing: A custom value used to route operations to a specific shard.
        :param source: Indicates whether to return the `_source` field (`true` or `false`)
            or lists the fields to return.
        :param source_excludes: A comma-separated list of source fields to exclude in
            the response.
        :param source_includes: A comma-separated list of source fields to include in
            the response.
        :param version: The version number for concurrency control. It must match the
            current version of the document for the request to succeed.
        :param version_type: The version type.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "id": _quote(id)}
        __path = f'/{__path_parts["index"]}/_source/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if realtime is not None:
            __query["realtime"] = realtime
        if refresh is not None:
            __query["refresh"] = refresh
        if routing is not None:
            __query["routing"] = routing
        if source is not None:
            __query["_source"] = source
        if source_excludes is not None:
            __query["_source_excludes"] = source_excludes
        if source_includes is not None:
            __query["_source_includes"] = source_includes
        if version is not None:
            __query["version"] = version
        if version_type is not None:
            __query["version_type"] = version_type
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "HEAD",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="exists_source",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("query",),
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def explain(
        self,
        *,
        index: str,
        id: str,
        analyze_wildcard: t.Optional[bool] = None,
        analyzer: t.Optional[str] = None,
        default_operator: t.Optional[t.Union[str, t.Literal["and", "or"]]] = None,
        df: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        lenient: t.Optional[bool] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        q: t.Optional[str] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        routing: t.Optional[str] = None,
        source: t.Optional[t.Union[bool, t.Union[str, t.Sequence[str]]]] = None,
        source_excludes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        source_includes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        stored_fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Explain a document match result.
          Get information about why a specific document matches, or doesn't match, a query.
          It computes a score explanation for a query and a specific document.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-explain>`_

        :param index: Index names that are used to limit the request. Only a single index
            name can be provided to this parameter.
        :param id: The document identifier.
        :param analyze_wildcard: If `true`, wildcard and prefix queries are analyzed.
            This parameter can be used only when the `q` query string parameter is specified.
        :param analyzer: The analyzer to use for the query string. This parameter can
            be used only when the `q` query string parameter is specified.
        :param default_operator: The default operator for query string query: `AND` or
            `OR`. This parameter can be used only when the `q` query string parameter
            is specified.
        :param df: The field to use as default where no field prefix is given in the
            query string. This parameter can be used only when the `q` query string parameter
            is specified.
        :param lenient: If `true`, format-based query failures (such as providing text
            to a numeric field) in the query string will be ignored. This parameter can
            be used only when the `q` query string parameter is specified.
        :param preference: The node or shard the operation should be performed on. It
            is random by default.
        :param q: The query in the Lucene query string syntax.
        :param query: Defines the search definition using the Query DSL.
        :param routing: A custom value used to route operations to a specific shard.
        :param source: `True` or `false` to return the `_source` field or not or a list
            of fields to return.
        :param source_excludes: A comma-separated list of source fields to exclude from
            the response. You can also use this parameter to exclude fields from the
            subset specified in `_source_includes` query parameter. If the `_source`
            parameter is `false`, this parameter is ignored.
        :param source_includes: A comma-separated list of source fields to include in
            the response. If this parameter is specified, only these source fields are
            returned. You can exclude fields from this subset using the `_source_excludes`
            query parameter. If the `_source` parameter is `false`, this parameter is
            ignored.
        :param stored_fields: A comma-separated list of stored fields to return in the
            response.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "id": _quote(id)}
        __path = f'/{__path_parts["index"]}/_explain/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
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
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if lenient is not None:
            __query["lenient"] = lenient
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if q is not None:
            __query["q"] = q
        if routing is not None:
            __query["routing"] = routing
        if source is not None:
            __query["_source"] = source
        if source_excludes is not None:
            __query["_source_excludes"] = source_excludes
        if source_includes is not None:
            __query["_source_includes"] = source_includes
        if stored_fields is not None:
            __query["stored_fields"] = stored_fields
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
            endpoint_id="explain",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("fields", "index_filter", "runtime_mappings"),
    )
    def field_caps(
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
        fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        filters: t.Optional[str] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        include_empty_fields: t.Optional[bool] = None,
        include_unmapped: t.Optional[bool] = None,
        index_filter: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        runtime_mappings: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        types: t.Optional[t.Sequence[str]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get the field capabilities.</p>
          <p>Get information about the capabilities of fields among multiple indices.</p>
          <p>For data streams, the API returns field capabilities among the stream’s backing indices.
          It returns runtime fields like any other field.
          For example, a runtime field with a type of keyword is returned the same as any other field that belongs to the <code>keyword</code> family.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-field-caps>`_

        :param index: A comma-separated list of data streams, indices, and aliases used
            to limit the request. Supports wildcards (*). To target all data streams
            and indices, omit this parameter or use * or _all.
        :param allow_no_indices: If false, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with foo but no index starts with bar.
        :param expand_wildcards: The type of index that wildcard patterns can match.
            If the request can target data streams, this argument determines whether
            wildcard expressions match hidden data streams. Supports comma-separated
            values, such as `open,hidden`.
        :param fields: A list of fields to retrieve capabilities for. Wildcard (`*`)
            expressions are supported.
        :param filters: A comma-separated list of filters to apply to the response.
        :param ignore_unavailable: If `true`, missing or closed indices are not included
            in the response.
        :param include_empty_fields: If false, empty fields are not included in the response.
        :param include_unmapped: If true, unmapped fields are included in the response.
        :param index_filter: Filter indices if the provided query rewrites to `match_none`
            on every shard. IMPORTANT: The filtering is done on a best-effort basis,
            it uses index statistics and mappings to rewrite queries to `match_none`
            instead of fully running the request. For instance a range query over a date
            field can rewrite to `match_none` if all documents within a shard (including
            deleted documents) are outside of the provided range. However, not all queries
            can rewrite to `match_none` so this API may return an index even if the provided
            filter matches no document.
        :param runtime_mappings: Define ad-hoc runtime fields in the request similar
            to the way it is done in search requests. These fields exist only as part
            of the query and take precedence over fields defined with the same name in
            the index mappings.
        :param types: A comma-separated list of field types to include. Any fields that
            do not match one of these types will be excluded from the results. It defaults
            to empty, meaning that all field types are returned.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_field_caps'
        else:
            __path_parts = {}
            __path = "/_field_caps"
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
        if filters is not None:
            __query["filters"] = filters
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if include_empty_fields is not None:
            __query["include_empty_fields"] = include_empty_fields
        if include_unmapped is not None:
            __query["include_unmapped"] = include_unmapped
        if pretty is not None:
            __query["pretty"] = pretty
        if types is not None:
            __query["types"] = types
        if not __body:
            if fields is not None:
                __body["fields"] = fields
            if index_filter is not None:
                __body["index_filter"] = index_filter
            if runtime_mappings is not None:
                __body["runtime_mappings"] = runtime_mappings
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
            endpoint_id="field_caps",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def get(
        self,
        *,
        index: str,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        force_synthetic_source: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        realtime: t.Optional[bool] = None,
        refresh: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        source: t.Optional[t.Union[bool, t.Union[str, t.Sequence[str]]]] = None,
        source_excludes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        source_includes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        stored_fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        version: t.Optional[int] = None,
        version_type: t.Optional[
            t.Union[str, t.Literal["external", "external_gte", "force", "internal"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a document by its ID.</p>
          <p>Get a document and its source or stored fields from an index.</p>
          <p>By default, this API is realtime and is not affected by the refresh rate of the index (when data will become visible for search).
          In the case where stored fields are requested with the <code>stored_fields</code> parameter and the document has been updated but is not yet refreshed, the API will have to parse and analyze the source to extract the stored fields.
          To turn off realtime behavior, set the <code>realtime</code> parameter to false.</p>
          <p><strong>Source filtering</strong></p>
          <p>By default, the API returns the contents of the <code>_source</code> field unless you have used the <code>stored_fields</code> parameter or the <code>_source</code> field is turned off.
          You can turn off <code>_source</code> retrieval by using the <code>_source</code> parameter:</p>
          <pre><code>GET my-index-000001/_doc/0?_source=false
          </code></pre>
          <p>If you only need one or two fields from the <code>_source</code>, use the <code>_source_includes</code> or <code>_source_excludes</code> parameters to include or filter out particular fields.
          This can be helpful with large documents where partial retrieval can save on network overhead
          Both parameters take a comma separated list of fields or wildcard expressions.
          For example:</p>
          <pre><code>GET my-index-000001/_doc/0?_source_includes=*.id&amp;_source_excludes=entities
          </code></pre>
          <p>If you only want to specify includes, you can use a shorter notation:</p>
          <pre><code>GET my-index-000001/_doc/0?_source=*.id
          </code></pre>
          <p><strong>Routing</strong></p>
          <p>If routing is used during indexing, the routing value also needs to be specified to retrieve a document.
          For example:</p>
          <pre><code>GET my-index-000001/_doc/2?routing=user1
          </code></pre>
          <p>This request gets the document with ID 2, but it is routed based on the user.
          The document is not fetched if the correct routing is not specified.</p>
          <p><strong>Distributed</strong></p>
          <p>The GET operation is hashed into a specific shard ID.
          It is then redirected to one of the replicas within that shard ID and returns the result.
          The replicas are the primary shard and its replicas within that shard ID group.
          This means that the more replicas you have, the better your GET scaling will be.</p>
          <p><strong>Versioning support</strong></p>
          <p>You can use the <code>version</code> parameter to retrieve the document only if its current version is equal to the specified one.</p>
          <p>Internally, Elasticsearch has marked the old document as deleted and added an entirely new document.
          The old version of the document doesn't disappear immediately, although you won't be able to access it.
          Elasticsearch cleans up deleted documents in the background as you continue to index more data.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get>`_

        :param index: The name of the index that contains the document.
        :param id: A unique document identifier.
        :param force_synthetic_source: Indicates whether the request forces synthetic
            `_source`. Use this parameter to test if the mapping supports synthetic `_source`
            and to get a sense of the worst case performance. Fetches with this parameter
            enabled will be slower than enabling synthetic source natively in the index.
        :param preference: The node or shard the operation should be performed on. By
            default, the operation is randomized between the shard replicas. If it is
            set to `_local`, the operation will prefer to be run on a local allocated
            shard when possible. If it is set to a custom value, the value is used to
            guarantee that the same shards will be used for the same custom value. This
            can help with "jumping values" when hitting different shards in different
            refresh states. A sample value can be something like the web session ID or
            the user name.
        :param realtime: If `true`, the request is real-time as opposed to near-real-time.
        :param refresh: If `true`, the request refreshes the relevant shards before retrieving
            the document. Setting it to `true` should be done after careful thought and
            verification that this does not cause a heavy load on the system (and slow
            down indexing).
        :param routing: A custom value used to route operations to a specific shard.
        :param source: Indicates whether to return the `_source` field (`true` or `false`)
            or lists the fields to return.
        :param source_excludes: A comma-separated list of source fields to exclude from
            the response. You can also use this parameter to exclude fields from the
            subset specified in `_source_includes` query parameter. If the `_source`
            parameter is `false`, this parameter is ignored.
        :param source_includes: A comma-separated list of source fields to include in
            the response. If this parameter is specified, only these source fields are
            returned. You can exclude fields from this subset using the `_source_excludes`
            query parameter. If the `_source` parameter is `false`, this parameter is
            ignored.
        :param stored_fields: A comma-separated list of stored fields to return as part
            of a hit. If no fields are specified, no stored fields are included in the
            response. If this field is specified, the `_source` parameter defaults to
            `false`. Only leaf fields can be retrieved with the `stored_fields` option.
            Object fields can't be returned; if specified, the request fails.
        :param version: The version number for concurrency control. It must match the
            current version of the document for the request to succeed.
        :param version_type: The version type.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "id": _quote(id)}
        __path = f'/{__path_parts["index"]}/_doc/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if force_synthetic_source is not None:
            __query["force_synthetic_source"] = force_synthetic_source
        if human is not None:
            __query["human"] = human
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if realtime is not None:
            __query["realtime"] = realtime
        if refresh is not None:
            __query["refresh"] = refresh
        if routing is not None:
            __query["routing"] = routing
        if source is not None:
            __query["_source"] = source
        if source_excludes is not None:
            __query["_source_excludes"] = source_excludes
        if source_includes is not None:
            __query["_source_includes"] = source_includes
        if stored_fields is not None:
            __query["stored_fields"] = stored_fields
        if version is not None:
            __query["version"] = version
        if version_type is not None:
            __query["version_type"] = version_type
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="get",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_script(
        self,
        *,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        master_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a script or search template.
          Retrieves a stored script or search template.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get-script>`_

        :param id: The identifier for the stored script or search template.
        :param master_timeout: The period to wait for the master node. If the master
            node is not available before the timeout expires, the request fails and returns
            an error. It can also be set to `-1` to indicate that the request should
            never timeout.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"id": _quote(id)}
        __path = f'/_scripts/{__path_parts["id"]}'
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
            endpoint_id="get_script",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_script_context(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get script contexts.</p>
          <p>Get a list of supported script contexts and their methods.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get-script-context>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_script_context"
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
            endpoint_id="get_script_context",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def get_script_languages(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get script languages.</p>
          <p>Get a list of available script types, languages, and contexts.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get-script-languages>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_script_language"
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
            endpoint_id="get_script_languages",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def get_source(
        self,
        *,
        index: str,
        id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        realtime: t.Optional[bool] = None,
        refresh: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        source: t.Optional[t.Union[bool, t.Union[str, t.Sequence[str]]]] = None,
        source_excludes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        source_includes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        version: t.Optional[int] = None,
        version_type: t.Optional[
            t.Union[str, t.Literal["external", "external_gte", "force", "internal"]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get a document's source.</p>
          <p>Get the source of a document.
          For example:</p>
          <pre><code>GET my-index-000001/_source/1
          </code></pre>
          <p>You can use the source filtering parameters to control which parts of the <code>_source</code> are returned:</p>
          <pre><code>GET my-index-000001/_source/1/?_source_includes=*.id&amp;_source_excludes=entities
          </code></pre>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-get>`_

        :param index: The name of the index that contains the document.
        :param id: A unique document identifier.
        :param preference: The node or shard the operation should be performed on. By
            default, the operation is randomized between the shard replicas.
        :param realtime: If `true`, the request is real-time as opposed to near-real-time.
        :param refresh: If `true`, the request refreshes the relevant shards before retrieving
            the document. Setting it to `true` should be done after careful thought and
            verification that this does not cause a heavy load on the system (and slow
            down indexing).
        :param routing: A custom value used to route operations to a specific shard.
        :param source: Indicates whether to return the `_source` field (`true` or `false`)
            or lists the fields to return.
        :param source_excludes: A comma-separated list of source fields to exclude in
            the response.
        :param source_includes: A comma-separated list of source fields to include in
            the response.
        :param version: The version number for concurrency control. It must match the
            current version of the document for the request to succeed.
        :param version_type: The version type.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "id": _quote(id)}
        __path = f'/{__path_parts["index"]}/_source/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if realtime is not None:
            __query["realtime"] = realtime
        if refresh is not None:
            __query["refresh"] = refresh
        if routing is not None:
            __query["routing"] = routing
        if source is not None:
            __query["_source"] = source
        if source_excludes is not None:
            __query["_source_excludes"] = source_excludes
        if source_includes is not None:
            __query["_source_includes"] = source_includes
        if version is not None:
            __query["version"] = version
        if version_type is not None:
            __query["version_type"] = version_type
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="get_source",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def health_report(
        self,
        *,
        feature: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        size: t.Optional[int] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        verbose: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get the cluster health.
          Get a report with the health status of an Elasticsearch cluster.
          The report contains a list of indicators that compose Elasticsearch functionality.</p>
          <p>Each indicator has a health status of: green, unknown, yellow or red.
          The indicator will provide an explanation and metadata describing the reason for its current health status.</p>
          <p>The cluster’s status is controlled by the worst indicator status.</p>
          <p>In the event that an indicator’s status is non-green, a list of impacts may be present in the indicator result which detail the functionalities that are negatively affected by the health issue.
          Each impact carries with it a severity level, an area of the system that is affected, and a simple description of the impact on the system.</p>
          <p>Some health indicators can determine the root cause of a health problem and prescribe a set of steps that can be performed in order to improve the health of the system.
          The root cause and remediation steps are encapsulated in a diagnosis.
          A diagnosis contains a cause detailing a root cause analysis, an action containing a brief description of the steps to take to fix the problem, the list of affected resources (if applicable), and a detailed step-by-step troubleshooting guide to fix the diagnosed problem.</p>
          <p>NOTE: The health indicators perform root cause analysis of non-green health statuses. This can be computationally expensive when called frequently.
          When setting up automated polling of the API for health status, set verbose to false to disable the more expensive analysis logic.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-health-report>`_

        :param feature: A feature of the cluster, as returned by the top-level health
            report API.
        :param size: Limit the number of affected resources the health report API returns.
        :param timeout: Explicit operation timeout.
        :param verbose: Opt-in for more information about the health of the system.
        """
        __path_parts: t.Dict[str, str]
        if feature not in SKIP_IN_PATH:
            __path_parts = {"feature": _quote(feature)}
            __path = f'/_health_report/{__path_parts["feature"]}'
        else:
            __path_parts = {}
            __path = "/_health_report"
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if size is not None:
            __query["size"] = size
        if timeout is not None:
            __query["timeout"] = timeout
        if verbose is not None:
            __query["verbose"] = verbose
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "GET",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="health_report",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="document",
    )
    def index(
        self,
        *,
        index: str,
        document: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Mapping[str, t.Any]] = None,
        id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        if_primary_term: t.Optional[int] = None,
        if_seq_no: t.Optional[int] = None,
        include_source_on_error: t.Optional[bool] = None,
        op_type: t.Optional[t.Union[str, t.Literal["create", "index"]]] = None,
        pipeline: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        require_alias: t.Optional[bool] = None,
        require_data_stream: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        version: t.Optional[int] = None,
        version_type: t.Optional[
            t.Union[str, t.Literal["external", "external_gte", "force", "internal"]]
        ] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Create or update a document in an index.</p>
          <p>Add a JSON document to the specified data stream or index and make it searchable.
          If the target is an index and the document already exists, the request updates the document and increments its version.</p>
          <p>NOTE: You cannot use this API to send update requests for existing documents in a data stream.</p>
          <p>If the Elasticsearch security features are enabled, you must have the following index privileges for the target data stream, index, or index alias:</p>
          <ul>
          <li>To add or overwrite a document using the <code>PUT /&lt;target&gt;/_doc/&lt;_id&gt;</code> request format, you must have the <code>create</code>, <code>index</code>, or <code>write</code> index privilege.</li>
          <li>To add a document using the <code>POST /&lt;target&gt;/_doc/</code> request format, you must have the <code>create_doc</code>, <code>create</code>, <code>index</code>, or <code>write</code> index privilege.</li>
          <li>To automatically create a data stream or index with this API request, you must have the <code>auto_configure</code>, <code>create_index</code>, or <code>manage</code> index privilege.</li>
          </ul>
          <p>Automatic data stream creation requires a matching index template with data stream enabled.</p>
          <p>NOTE: Replica shards might not all be started when an indexing operation returns successfully.
          By default, only the primary is required. Set <code>wait_for_active_shards</code> to change this default behavior.</p>
          <p><strong>Automatically create data streams and indices</strong></p>
          <p>If the request's target doesn't exist and matches an index template with a <code>data_stream</code> definition, the index operation automatically creates the data stream.</p>
          <p>If the target doesn't exist and doesn't match a data stream template, the operation automatically creates the index and applies any matching index templates.</p>
          <p>NOTE: Elasticsearch includes several built-in index templates. To avoid naming collisions with these templates, refer to index pattern documentation.</p>
          <p>If no mapping exists, the index operation creates a dynamic mapping.
          By default, new fields and objects are automatically added to the mapping if needed.</p>
          <p>Automatic index creation is controlled by the <code>action.auto_create_index</code> setting.
          If it is <code>true</code>, any index can be created automatically.
          You can modify this setting to explicitly allow or block automatic creation of indices that match specified patterns or set it to <code>false</code> to turn off automatic index creation entirely.
          Specify a comma-separated list of patterns you want to allow or prefix each pattern with <code>+</code> or <code>-</code> to indicate whether it should be allowed or blocked.
          When a list is specified, the default behaviour is to disallow.</p>
          <p>NOTE: The <code>action.auto_create_index</code> setting affects the automatic creation of indices only.
          It does not affect the creation of data streams.</p>
          <p><strong>Optimistic concurrency control</strong></p>
          <p>Index operations can be made conditional and only be performed if the last modification to the document was assigned the sequence number and primary term specified by the <code>if_seq_no</code> and <code>if_primary_term</code> parameters.
          If a mismatch is detected, the operation will result in a <code>VersionConflictException</code> and a status code of <code>409</code>.</p>
          <p><strong>Routing</strong></p>
          <p>By default, shard placement — or routing — is controlled by using a hash of the document's ID value.
          For more explicit control, the value fed into the hash function used by the router can be directly specified on a per-operation basis using the <code>routing</code> parameter.</p>
          <p>When setting up explicit mapping, you can also use the <code>_routing</code> field to direct the index operation to extract the routing value from the document itself.
          This does come at the (very minimal) cost of an additional document parsing pass.
          If the <code>_routing</code> mapping is defined and set to be required, the index operation will fail if no routing value is provided or extracted.</p>
          <p>NOTE: Data streams do not support custom routing unless they were created with the <code>allow_custom_routing</code> setting enabled in the template.</p>
          <p><strong>Distributed</strong></p>
          <p>The index operation is directed to the primary shard based on its route and performed on the actual node containing this shard.
          After the primary shard completes the operation, if needed, the update is distributed to applicable replicas.</p>
          <p><strong>Active shards</strong></p>
          <p>To improve the resiliency of writes to the system, indexing operations can be configured to wait for a certain number of active shard copies before proceeding with the operation.
          If the requisite number of active shard copies are not available, then the write operation must wait and retry, until either the requisite shard copies have started or a timeout occurs.
          By default, write operations only wait for the primary shards to be active before proceeding (that is to say <code>wait_for_active_shards</code> is <code>1</code>).
          This default can be overridden in the index settings dynamically by setting <code>index.write.wait_for_active_shards</code>.
          To alter this behavior per operation, use the <code>wait_for_active_shards request</code> parameter.</p>
          <p>Valid values are all or any positive integer up to the total number of configured copies per shard in the index (which is <code>number_of_replicas</code>+1).
          Specifying a negative value or a number greater than the number of shard copies will throw an error.</p>
          <p>For example, suppose you have a cluster of three nodes, A, B, and C and you create an index index with the number of replicas set to 3 (resulting in 4 shard copies, one more copy than there are nodes).
          If you attempt an indexing operation, by default the operation will only ensure the primary copy of each shard is available before proceeding.
          This means that even if B and C went down and A hosted the primary shard copies, the indexing operation would still proceed with only one copy of the data.
          If <code>wait_for_active_shards</code> is set on the request to <code>3</code> (and all three nodes are up), the indexing operation will require 3 active shard copies before proceeding.
          This requirement should be met because there are 3 active nodes in the cluster, each one holding a copy of the shard.
          However, if you set <code>wait_for_active_shards</code> to <code>all</code> (or to <code>4</code>, which is the same in this situation), the indexing operation will not proceed as you do not have all 4 copies of each shard active in the index.
          The operation will timeout unless a new node is brought up in the cluster to host the fourth copy of the shard.</p>
          <p>It is important to note that this setting greatly reduces the chances of the write operation not writing to the requisite number of shard copies, but it does not completely eliminate the possibility, because this check occurs before the write operation starts.
          After the write operation is underway, it is still possible for replication to fail on any number of shard copies but still succeed on the primary.
          The <code>_shards</code> section of the API response reveals the number of shard copies on which replication succeeded and failed.</p>
          <p><strong>No operation (noop) updates</strong></p>
          <p>When updating a document by using this API, a new version of the document is always created even if the document hasn't changed.
          If this isn't acceptable use the <code>_update</code> API with <code>detect_noop</code> set to <code>true</code>.
          The <code>detect_noop</code> option isn't available on this API because it doesn’t fetch the old source and isn't able to compare it against the new source.</p>
          <p>There isn't a definitive rule for when noop updates aren't acceptable.
          It's a combination of lots of factors like how frequently your data source sends updates that are actually noops and how many queries per second Elasticsearch runs on the shard receiving the updates.</p>
          <p><strong>Versioning</strong></p>
          <p>Each indexed document is given a version number.
          By default, internal versioning is used that starts at 1 and increments with each update, deletes included.
          Optionally, the version number can be set to an external value (for example, if maintained in a database).
          To enable this functionality, <code>version_type</code> should be set to <code>external</code>.
          The value provided must be a numeric, long value greater than or equal to 0, and less than around <code>9.2e+18</code>.</p>
          <p>NOTE: Versioning is completely real time, and is not affected by the near real time aspects of search operations.
          If no version is provided, the operation runs without any version checks.</p>
          <p>When using the external version type, the system checks to see if the version number passed to the index request is greater than the version of the currently stored document.
          If true, the document will be indexed and the new version number used.
          If the value provided is less than or equal to the stored document's version number, a version conflict will occur and the index operation will fail. For example:</p>
          <pre><code>PUT my-index-000001/_doc/1?version=2&amp;version_type=external
          {
            &quot;user&quot;: {
              &quot;id&quot;: &quot;elkbee&quot;
            }
          }

          In this example, the operation will succeed since the supplied version of 2 is higher than the current document version of 1.
          If the document was already updated and its version was set to 2 or higher, the indexing command will fail and result in a conflict (409 HTTP status code).

          A nice side effect is that there is no need to maintain strict ordering of async indexing operations run as a result of changes to a source database, as long as version numbers from the source database are used.
          Even the simple case of updating the Elasticsearch index using data from a database is simplified if external versioning is used, as only the latest version will be used if the index operations arrive out of order.
          </code></pre>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create>`_

        :param index: The name of the data stream or index to target. If the target doesn't
            exist and matches the name or wildcard (`*`) pattern of an index template
            with a `data_stream` definition, this request creates the data stream. If
            the target doesn't exist and doesn't match a data stream template, this request
            creates the index. You can check for existing targets with the resolve index
            API.
        :param document:
        :param id: A unique identifier for the document. To automatically generate a
            document ID, use the `POST /<target>/_doc/` request format and omit this
            parameter.
        :param if_primary_term: Only perform the operation if the document has this primary
            term.
        :param if_seq_no: Only perform the operation if the document has this sequence
            number.
        :param include_source_on_error: True or false if to include the document source
            in the error message in case of parsing errors.
        :param op_type: Set to `create` to only index the document if it does not already
            exist (put if absent). If a document with the specified `_id` already exists,
            the indexing operation will fail. The behavior is the same as using the `<index>/_create`
            endpoint. If a document ID is specified, this paramater defaults to `index`.
            Otherwise, it defaults to `create`. If the request targets a data stream,
            an `op_type` of `create` is required.
        :param pipeline: The ID of the pipeline to use to preprocess incoming documents.
            If the index has a default ingest pipeline specified, then setting the value
            to `_none` disables the default ingest pipeline for this request. If a final
            pipeline is configured it will always run, regardless of the value of this
            parameter.
        :param refresh: If `true`, Elasticsearch refreshes the affected shards to make
            this operation visible to search. If `wait_for`, it waits for a refresh to
            make this operation visible to search. If `false`, it does nothing with refreshes.
        :param require_alias: If `true`, the destination must be an index alias.
        :param require_data_stream: If `true`, the request's actions must target a data
            stream (existing or to be created).
        :param routing: A custom value that is used to route operations to a specific
            shard.
        :param timeout: The period the request waits for the following operations: automatic
            index creation, dynamic mapping updates, waiting for active shards. This
            parameter is useful for situations where the primary shard assigned to perform
            the operation might not be available when the operation runs. Some reasons
            for this might be that the primary shard is currently recovering from a gateway
            or undergoing relocation. By default, the operation will wait on the primary
            shard to become available for at least 1 minute before failing and responding
            with an error. The actual wait time could be longer, particularly when multiple
            waits occur.
        :param version: An explicit version number for concurrency control. It must be
            a non-negative long number.
        :param version_type: The version type.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. You can set it to `all` or any positive
            integer up to the total number of shards in the index (`number_of_replicas+1`).
            The default value of `1` means it waits for each primary shard to be active.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if document is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'document' and 'body', one of them should be set."
            )
        elif document is not None and body is not None:
            raise ValueError("Cannot set both 'document' and 'body'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH and id not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index), "id": _quote(id)}
            __path = f'/{__path_parts["index"]}/_doc/{__path_parts["id"]}'
            __method = "PUT"
        elif index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_doc'
            __method = "POST"
        else:
            raise ValueError("Couldn't find a path for the given parameters")
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if if_primary_term is not None:
            __query["if_primary_term"] = if_primary_term
        if if_seq_no is not None:
            __query["if_seq_no"] = if_seq_no
        if include_source_on_error is not None:
            __query["include_source_on_error"] = include_source_on_error
        if op_type is not None:
            __query["op_type"] = op_type
        if pipeline is not None:
            __query["pipeline"] = pipeline
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if require_alias is not None:
            __query["require_alias"] = require_alias
        if require_data_stream is not None:
            __query["require_data_stream"] = require_data_stream
        if routing is not None:
            __query["routing"] = routing
        if timeout is not None:
            __query["timeout"] = timeout
        if version is not None:
            __query["version"] = version
        if version_type is not None:
            __query["version_type"] = version_type
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        __body = document if document is not None else body
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            __method,
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="index",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def info(
        self,
        *,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get cluster info.
          Get basic build, version, and cluster information.
          ::: In Serverless, this API is retained for backward compatibility only. Some response fields, such as the version number, should be ignored.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-info>`_
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/"
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
            endpoint_id="info",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("docs", "ids"),
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def mget(
        self,
        *,
        index: t.Optional[str] = None,
        docs: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        force_synthetic_source: t.Optional[bool] = None,
        human: t.Optional[bool] = None,
        ids: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        realtime: t.Optional[bool] = None,
        refresh: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        source: t.Optional[t.Union[bool, t.Union[str, t.Sequence[str]]]] = None,
        source_excludes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        source_includes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        stored_fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get multiple documents.</p>
          <p>Get multiple JSON documents by ID from one or more indices.
          If you specify an index in the request URI, you only need to specify the document IDs in the request body.
          To ensure fast responses, this multi get (mget) API responds with partial results if one or more shards fail.</p>
          <p><strong>Filter source fields</strong></p>
          <p>By default, the <code>_source</code> field is returned for every document (if stored).
          Use the <code>_source</code> and <code>_source_include</code> or <code>source_exclude</code> attributes to filter what fields are returned for a particular document.
          You can include the <code>_source</code>, <code>_source_includes</code>, and <code>_source_excludes</code> query parameters in the request URI to specify the defaults to use when there are no per-document instructions.</p>
          <p><strong>Get stored fields</strong></p>
          <p>Use the <code>stored_fields</code> attribute to specify the set of stored fields you want to retrieve.
          Any requested fields that are not stored are ignored.
          You can include the <code>stored_fields</code> query parameter in the request URI to specify the defaults to use when there are no per-document instructions.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-mget>`_

        :param index: Name of the index to retrieve documents from when `ids` are specified,
            or when a document in the `docs` array does not specify an index.
        :param docs: The documents you want to retrieve. Required if no index is specified
            in the request URI.
        :param force_synthetic_source: Should this request force synthetic _source? Use
            this to test if the mapping supports synthetic _source and to get a sense
            of the worst case performance. Fetches with this enabled will be slower the
            enabling synthetic source natively in the index.
        :param ids: The IDs of the documents you want to retrieve. Allowed when the index
            is specified in the request URI.
        :param preference: Specifies the node or shard the operation should be performed
            on. Random by default.
        :param realtime: If `true`, the request is real-time as opposed to near-real-time.
        :param refresh: If `true`, the request refreshes relevant shards before retrieving
            documents.
        :param routing: Custom value used to route operations to a specific shard.
        :param source: True or false to return the `_source` field or not, or a list
            of fields to return.
        :param source_excludes: A comma-separated list of source fields to exclude from
            the response. You can also use this parameter to exclude fields from the
            subset specified in `_source_includes` query parameter.
        :param source_includes: A comma-separated list of source fields to include in
            the response. If this parameter is specified, only these source fields are
            returned. You can exclude fields from this subset using the `_source_excludes`
            query parameter. If the `_source` parameter is `false`, this parameter is
            ignored.
        :param stored_fields: If `true`, retrieves the document fields stored in the
            index rather than the document `_source`.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_mget'
        else:
            __path_parts = {}
            __path = "/_mget"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if force_synthetic_source is not None:
            __query["force_synthetic_source"] = force_synthetic_source
        if human is not None:
            __query["human"] = human
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if realtime is not None:
            __query["realtime"] = realtime
        if refresh is not None:
            __query["refresh"] = refresh
        if routing is not None:
            __query["routing"] = routing
        if source is not None:
            __query["_source"] = source
        if source_excludes is not None:
            __query["_source_excludes"] = source_excludes
        if source_includes is not None:
            __query["_source_includes"] = source_includes
        if stored_fields is not None:
            __query["stored_fields"] = stored_fields
        if not __body:
            if docs is not None:
                __body["docs"] = docs
            if ids is not None:
                __body["ids"] = ids
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="mget",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="searches",
    )
    def msearch(
        self,
        *,
        searches: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        ccs_minimize_roundtrips: t.Optional[bool] = None,
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
        include_named_queries_score: t.Optional[bool] = None,
        max_concurrent_searches: t.Optional[int] = None,
        max_concurrent_shard_requests: t.Optional[int] = None,
        pre_filter_shard_size: t.Optional[int] = None,
        pretty: t.Optional[bool] = None,
        rest_total_hits_as_int: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        search_type: t.Optional[
            t.Union[str, t.Literal["dfs_query_then_fetch", "query_then_fetch"]]
        ] = None,
        typed_keys: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run multiple searches.</p>
          <p>The format of the request is similar to the bulk API format and makes use of the newline delimited JSON (NDJSON) format.
          The structure is as follows:</p>
          <pre><code>header\\n
          body\\n
          header\\n
          body\\n
          </code></pre>
          <p>This structure is specifically optimized to reduce parsing if a specific search ends up redirected to another node.</p>
          <p>IMPORTANT: The final line of data must end with a newline character <code>\\n</code>.
          Each newline character may be preceded by a carriage return <code>\\r</code>.
          When sending requests to this endpoint the <code>Content-Type</code> header should be set to <code>application/x-ndjson</code>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-msearch>`_

        :param searches:
        :param index: Comma-separated list of data streams, indices, and index aliases
            to search.
        :param allow_no_indices: If false, the request returns an error if any wildcard
            expression, index alias, or _all value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting foo*,bar* returns an error if an index starts
            with foo but no index starts with bar.
        :param ccs_minimize_roundtrips: If true, network roundtrips between the coordinating
            node and remote clusters are minimized for cross-cluster search requests.
        :param expand_wildcards: Type of index that wildcard expressions can match. If
            the request can target data streams, this argument determines whether wildcard
            expressions match hidden data streams.
        :param ignore_throttled: If true, concrete, expanded or aliased indices are ignored
            when frozen.
        :param ignore_unavailable: If true, missing or closed indices are not included
            in the response.
        :param include_named_queries_score: Indicates whether hit.matched_queries should
            be rendered as a map that includes the name of the matched query associated
            with its score (true) or as an array containing the name of the matched queries
            (false) This functionality reruns each named query on every hit in a search
            response. Typically, this adds a small overhead to a request. However, using
            computationally expensive named queries on a large number of hits may add
            significant overhead.
        :param max_concurrent_searches: Maximum number of concurrent searches the multi
            search API can execute. Defaults to `max(1, (# of data nodes * min(search
            thread pool size, 10)))`.
        :param max_concurrent_shard_requests: Maximum number of concurrent shard requests
            that each sub-search request executes per node.
        :param pre_filter_shard_size: Defines a threshold that enforces a pre-filter
            roundtrip to prefilter search shards based on query rewriting if the number
            of shards the search request expands to exceeds the threshold. This filter
            roundtrip can limit the number of shards significantly if for instance a
            shard can not match any documents based on its rewrite method i.e., if date
            filters are mandatory to match but the shard bounds and the query are disjoint.
        :param rest_total_hits_as_int: If true, hits.total are returned as an integer
            in the response. Defaults to false, which returns an object.
        :param routing: Custom routing value used to route search operations to a specific
            shard.
        :param search_type: Indicates whether global term and document frequencies should
            be used when scoring returned documents.
        :param typed_keys: Specifies whether aggregation and suggester names should be
            prefixed by their respective types in the response.
        """
        if searches is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'searches' and 'body', one of them should be set."
            )
        elif searches is not None and body is not None:
            raise ValueError("Cannot set both 'searches' and 'body'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_msearch'
        else:
            __path_parts = {}
            __path = "/_msearch"
        __query: t.Dict[str, t.Any] = {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if ccs_minimize_roundtrips is not None:
            __query["ccs_minimize_roundtrips"] = ccs_minimize_roundtrips
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
        if include_named_queries_score is not None:
            __query["include_named_queries_score"] = include_named_queries_score
        if max_concurrent_searches is not None:
            __query["max_concurrent_searches"] = max_concurrent_searches
        if max_concurrent_shard_requests is not None:
            __query["max_concurrent_shard_requests"] = max_concurrent_shard_requests
        if pre_filter_shard_size is not None:
            __query["pre_filter_shard_size"] = pre_filter_shard_size
        if pretty is not None:
            __query["pretty"] = pretty
        if rest_total_hits_as_int is not None:
            __query["rest_total_hits_as_int"] = rest_total_hits_as_int
        if routing is not None:
            __query["routing"] = routing
        if search_type is not None:
            __query["search_type"] = search_type
        if typed_keys is not None:
            __query["typed_keys"] = typed_keys
        __body = searches if searches is not None else body
        __headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="msearch",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_name="search_templates",
    )
    def msearch_template(
        self,
        *,
        search_templates: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        ccs_minimize_roundtrips: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        max_concurrent_searches: t.Optional[int] = None,
        pretty: t.Optional[bool] = None,
        rest_total_hits_as_int: t.Optional[bool] = None,
        search_type: t.Optional[
            t.Union[str, t.Literal["dfs_query_then_fetch", "query_then_fetch"]]
        ] = None,
        typed_keys: t.Optional[bool] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run multiple templated searches.</p>
          <p>Run multiple templated searches with a single request.
          If you are providing a text file or text input to <code>curl</code>, use the <code>--data-binary</code> flag instead of <code>-d</code> to preserve newlines.
          For example:</p>
          <pre><code>$ cat requests
          { &quot;index&quot;: &quot;my-index&quot; }
          { &quot;id&quot;: &quot;my-search-template&quot;, &quot;params&quot;: { &quot;query_string&quot;: &quot;hello world&quot;, &quot;from&quot;: 0, &quot;size&quot;: 10 }}
          { &quot;index&quot;: &quot;my-other-index&quot; }
          { &quot;id&quot;: &quot;my-other-search-template&quot;, &quot;params&quot;: { &quot;query_type&quot;: &quot;match_all&quot; }}

          $ curl -H &quot;Content-Type: application/x-ndjson&quot; -XGET localhost:9200/_msearch/template --data-binary &quot;@requests&quot;; echo
          </code></pre>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-msearch-template>`_

        :param search_templates:
        :param index: A comma-separated list of data streams, indices, and aliases to
            search. It supports wildcards (`*`). To search all data streams and indices,
            omit this parameter or use `*`.
        :param ccs_minimize_roundtrips: If `true`, network round-trips are minimized
            for cross-cluster search requests.
        :param max_concurrent_searches: The maximum number of concurrent searches the
            API can run.
        :param rest_total_hits_as_int: If `true`, the response returns `hits.total` as
            an integer. If `false`, it returns `hits.total` as an object.
        :param search_type: The type of the search operation.
        :param typed_keys: If `true`, the response prefixes aggregation and suggester
            names with their respective types.
        """
        if search_templates is None and body is None:
            raise ValueError(
                "Empty value passed for parameters 'search_templates' and 'body', one of them should be set."
            )
        elif search_templates is not None and body is not None:
            raise ValueError("Cannot set both 'search_templates' and 'body'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_msearch/template'
        else:
            __path_parts = {}
            __path = "/_msearch/template"
        __query: t.Dict[str, t.Any] = {}
        if ccs_minimize_roundtrips is not None:
            __query["ccs_minimize_roundtrips"] = ccs_minimize_roundtrips
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if max_concurrent_searches is not None:
            __query["max_concurrent_searches"] = max_concurrent_searches
        if pretty is not None:
            __query["pretty"] = pretty
        if rest_total_hits_as_int is not None:
            __query["rest_total_hits_as_int"] = rest_total_hits_as_int
        if search_type is not None:
            __query["search_type"] = search_type
        if typed_keys is not None:
            __query["typed_keys"] = typed_keys
        __body = search_templates if search_templates is not None else body
        __headers = {
            "accept": "application/json",
            "content-type": "application/x-ndjson",
        }
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="msearch_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("docs", "ids"),
    )
    def mtermvectors(
        self,
        *,
        index: t.Optional[str] = None,
        docs: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        error_trace: t.Optional[bool] = None,
        field_statistics: t.Optional[bool] = None,
        fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        ids: t.Optional[t.Sequence[str]] = None,
        offsets: t.Optional[bool] = None,
        payloads: t.Optional[bool] = None,
        positions: t.Optional[bool] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        realtime: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        term_statistics: t.Optional[bool] = None,
        version: t.Optional[int] = None,
        version_type: t.Optional[
            t.Union[str, t.Literal["external", "external_gte", "force", "internal"]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get multiple term vectors.</p>
          <p>Get multiple term vectors with a single request.
          You can specify existing documents by index and ID or provide artificial documents in the body of the request.
          You can specify the index in the request body or request URI.
          The response contains a <code>docs</code> array with all the fetched termvectors.
          Each element has the structure provided by the termvectors API.</p>
          <p><strong>Artificial documents</strong></p>
          <p>You can also use <code>mtermvectors</code> to generate term vectors for artificial documents provided in the body of the request.
          The mapping used is determined by the specified <code>_index</code>.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-mtermvectors>`_

        :param index: The name of the index that contains the documents.
        :param docs: An array of existing or artificial documents.
        :param field_statistics: If `true`, the response includes the document count,
            sum of document frequencies, and sum of total term frequencies.
        :param fields: A comma-separated list or wildcard expressions of fields to include
            in the statistics. It is used as the default list unless a specific field
            list is provided in the `completion_fields` or `fielddata_fields` parameters.
        :param ids: A simplified syntax to specify documents by their ID if they're in
            the same index.
        :param offsets: If `true`, the response includes term offsets.
        :param payloads: If `true`, the response includes term payloads.
        :param positions: If `true`, the response includes term positions.
        :param preference: The node or shard the operation should be performed on. It
            is random by default.
        :param realtime: If true, the request is real-time as opposed to near-real-time.
        :param routing: A custom value used to route operations to a specific shard.
        :param term_statistics: If true, the response includes term frequency and document
            frequency.
        :param version: If `true`, returns the document version as part of a hit.
        :param version_type: The version type.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_mtermvectors'
        else:
            __path_parts = {}
            __path = "/_mtermvectors"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if field_statistics is not None:
            __query["field_statistics"] = field_statistics
        if fields is not None:
            __query["fields"] = fields
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if offsets is not None:
            __query["offsets"] = offsets
        if payloads is not None:
            __query["payloads"] = payloads
        if positions is not None:
            __query["positions"] = positions
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if realtime is not None:
            __query["realtime"] = realtime
        if routing is not None:
            __query["routing"] = routing
        if term_statistics is not None:
            __query["term_statistics"] = term_statistics
        if version is not None:
            __query["version"] = version
        if version_type is not None:
            __query["version_type"] = version_type
        if not __body:
            if docs is not None:
                __body["docs"] = docs
            if ids is not None:
                __body["ids"] = ids
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
            endpoint_id="mtermvectors",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("index_filter",),
    )
    def open_point_in_time(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        keep_alive: t.Union[str, t.Literal[-1], t.Literal[0]],
        allow_partial_search_results: t.Optional[bool] = None,
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
        index_filter: t.Optional[t.Mapping[str, t.Any]] = None,
        max_concurrent_shard_requests: t.Optional[int] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Open a point in time.</p>
          <p>A search request by default runs against the most recent visible data of the target indices,
          which is called point in time. Elasticsearch pit (point in time) is a lightweight view into the
          state of the data as it existed when initiated. In some cases, it’s preferred to perform multiple
          search requests using the same point in time. For example, if refreshes happen between
          <code>search_after</code> requests, then the results of those requests might not be consistent as changes happening
          between searches are only visible to the more recent point in time.</p>
          <p>A point in time must be opened explicitly before being used in search requests.</p>
          <p>A subsequent search request with the <code>pit</code> parameter must not specify <code>index</code>, <code>routing</code>, or <code>preference</code> values as these parameters are copied from the point in time.</p>
          <p>Just like regular searches, you can use <code>from</code> and <code>size</code> to page through point in time search results, up to the first 10,000 hits.
          If you want to retrieve more hits, use PIT with <code>search_after</code>.</p>
          <p>IMPORTANT: The open point in time request and each subsequent search request can return different identifiers; always use the most recently received ID for the next search request.</p>
          <p>When a PIT that contains shard failures is used in a search request, the missing are always reported in the search response as a <code>NoShardAvailableActionException</code> exception.
          To get rid of these exceptions, a new PIT needs to be created so that shards missing from the previous PIT can be handled, assuming they become available in the meantime.</p>
          <p><strong>Keeping point in time alive</strong></p>
          <p>The <code>keep_alive</code> parameter, which is passed to a open point in time request and search request, extends the time to live of the corresponding point in time.
          The value does not need to be long enough to process all data — it just needs to be long enough for the next request.</p>
          <p>Normally, the background merge process optimizes the index by merging together smaller segments to create new, bigger segments.
          Once the smaller segments are no longer needed they are deleted.
          However, open point-in-times prevent the old segments from being deleted since they are still in use.</p>
          <p>TIP: Keeping older segments alive means that more disk space and file handles are needed.
          Ensure that you have configured your nodes to have ample free file handles.</p>
          <p>Additionally, if a segment contains deleted or updated documents then the point in time must keep track of whether each document in the segment was live at the time of the initial search request.
          Ensure that your nodes have sufficient heap space if you have many open point-in-times on an index that is subject to ongoing deletes or updates.
          Note that a point-in-time doesn't prevent its associated indices from being deleted.
          You can check how many point-in-times (that is, search contexts) are open with the nodes stats API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-open-point-in-time>`_

        :param index: A comma-separated list of index names to open point in time; use
            `_all` or empty string to perform the operation on all indices
        :param keep_alive: Extend the length of time that the point in time persists.
        :param allow_partial_search_results: Indicates whether the point in time tolerates
            unavailable shards or shard failures when initially creating the PIT. If
            `false`, creating a point in time request when a shard is missing or unavailable
            will throw an exception. If `true`, the point in time will contain all the
            shards that are available at the time of the request.
        :param expand_wildcards: The type of index that wildcard patterns can match.
            If the request can target data streams, this argument determines whether
            wildcard expressions match hidden data streams. It supports comma-separated
            values, such as `open,hidden`.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param index_filter: Filter indices if the provided query rewrites to `match_none`
            on every shard.
        :param max_concurrent_shard_requests: Maximum number of concurrent shard requests
            that each sub-search request executes per node.
        :param preference: The node or shard the operation should be performed on. By
            default, it is random.
        :param routing: A custom value that is used to route operations to a specific
            shard.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if keep_alive is None and body is None:
            raise ValueError("Empty value passed for parameter 'keep_alive'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_pit'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if keep_alive is not None:
            __query["keep_alive"] = keep_alive
        if allow_partial_search_results is not None:
            __query["allow_partial_search_results"] = allow_partial_search_results
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
        if max_concurrent_shard_requests is not None:
            __query["max_concurrent_shard_requests"] = max_concurrent_shard_requests
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if routing is not None:
            __query["routing"] = routing
        if not __body:
            if index_filter is not None:
                __body["index_filter"] = index_filter
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
            endpoint_id="open_point_in_time",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("script",),
    )
    def put_script(
        self,
        *,
        id: str,
        script: t.Optional[t.Mapping[str, t.Any]] = None,
        context: t.Optional[str] = None,
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

          <p>Create or update a script or search template.
          Creates or updates a stored script or search template.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-put-script>`_

        :param id: The identifier for the stored script or search template. It must be
            unique within the cluster.
        :param script: The script or search template, its parameters, and its language.
        :param context: The context in which the script or search template should run.
            To prevent errors, the API immediately compiles the script or template in
            this context.
        :param master_timeout: The period to wait for a connection to the master node.
            If no response is received before the timeout expires, the request fails
            and returns an error. It can also be set to `-1` to indicate that the request
            should never timeout.
        :param timeout: The period to wait for a response. If no response is received
            before the timeout expires, the request fails and returns an error. It can
            also be set to `-1` to indicate that the request should never timeout.
        """
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        if script is None and body is None:
            raise ValueError("Empty value passed for parameter 'script'")
        __path_parts: t.Dict[str, str]
        if id not in SKIP_IN_PATH and context not in SKIP_IN_PATH:
            __path_parts = {"id": _quote(id), "context": _quote(context)}
            __path = f'/_scripts/{__path_parts["id"]}/{__path_parts["context"]}'
        elif id not in SKIP_IN_PATH:
            __path_parts = {"id": _quote(id)}
            __path = f'/_scripts/{__path_parts["id"]}'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
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
            if script is not None:
                __body["script"] = script
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "PUT",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="put_script",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("requests", "metric"),
    )
    def rank_eval(
        self,
        *,
        requests: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
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
        metric: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        search_type: t.Optional[str] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Evaluate ranked search results.</p>
          <p>Evaluate the quality of ranked search results over a set of typical search queries.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-rank-eval>`_

        :param requests: A set of typical search requests, together with their provided
            ratings.
        :param index: A comma-separated list of data streams, indices, and index aliases
            used to limit the request. Wildcard (`*`) expressions are supported. To target
            all data streams and indices in a cluster, omit this parameter or use `_all`
            or `*`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param expand_wildcards: Whether to expand wildcard expression to concrete indices
            that are open, closed or both.
        :param ignore_unavailable: If `true`, missing or closed indices are not included
            in the response.
        :param metric: Definition of the evaluation metric to calculate.
        :param search_type: Search operation type
        """
        if requests is None and body is None:
            raise ValueError("Empty value passed for parameter 'requests'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_rank_eval'
        else:
            __path_parts = {}
            __path = "/_rank_eval"
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
        if pretty is not None:
            __query["pretty"] = pretty
        if search_type is not None:
            __query["search_type"] = search_type
        if not __body:
            if requests is not None:
                __body["requests"] = requests
            if metric is not None:
                __body["metric"] = metric
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="rank_eval",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("dest", "source", "conflicts", "max_docs", "script", "size"),
    )
    def reindex(
        self,
        *,
        dest: t.Optional[t.Mapping[str, t.Any]] = None,
        source: t.Optional[t.Mapping[str, t.Any]] = None,
        conflicts: t.Optional[t.Union[str, t.Literal["abort", "proceed"]]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        max_docs: t.Optional[int] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[bool] = None,
        requests_per_second: t.Optional[float] = None,
        require_alias: t.Optional[bool] = None,
        script: t.Optional[t.Mapping[str, t.Any]] = None,
        scroll: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        size: t.Optional[int] = None,
        slices: t.Optional[t.Union[int, t.Union[str, t.Literal["auto"]]]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
        wait_for_completion: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Reindex documents.</p>
          <p>Copy documents from a source to a destination.
          You can copy all documents to the destination index or reindex a subset of the documents.
          The source can be any existing index, alias, or data stream.
          The destination must differ from the source.
          For example, you cannot reindex a data stream into itself.</p>
          <p>IMPORTANT: Reindex requires <code>_source</code> to be enabled for all documents in the source.
          The destination should be configured as wanted before calling the reindex API.
          Reindex does not copy the settings from the source or its associated template.
          Mappings, shard counts, and replicas, for example, must be configured ahead of time.</p>
          <p>If the Elasticsearch security features are enabled, you must have the following security privileges:</p>
          <ul>
          <li>The <code>read</code> index privilege for the source data stream, index, or alias.</li>
          <li>The <code>write</code> index privilege for the destination data stream, index, or index alias.</li>
          <li>To automatically create a data stream or index with a reindex API request, you must have the <code>auto_configure</code>, <code>create_index</code>, or <code>manage</code> index privilege for the destination data stream, index, or alias.</li>
          <li>If reindexing from a remote cluster, the <code>source.remote.user</code> must have the <code>monitor</code> cluster privilege and the <code>read</code> index privilege for the source data stream, index, or alias.</li>
          </ul>
          <p>If reindexing from a remote cluster, you must explicitly allow the remote host in the <code>reindex.remote.whitelist</code> setting.
          Automatic data stream creation requires a matching index template with data stream enabled.</p>
          <p>The <code>dest</code> element can be configured like the index API to control optimistic concurrency control.
          Omitting <code>version_type</code> or setting it to <code>internal</code> causes Elasticsearch to blindly dump documents into the destination, overwriting any that happen to have the same ID.</p>
          <p>Setting <code>version_type</code> to <code>external</code> causes Elasticsearch to preserve the <code>version</code> from the source, create any documents that are missing, and update any documents that have an older version in the destination than they do in the source.</p>
          <p>Setting <code>op_type</code> to <code>create</code> causes the reindex API to create only missing documents in the destination.
          All existing documents will cause a version conflict.</p>
          <p>IMPORTANT: Because data streams are append-only, any reindex request to a destination data stream must have an <code>op_type</code> of <code>create</code>.
          A reindex can only add new documents to a destination data stream.
          It cannot update existing documents in a destination data stream.</p>
          <p>By default, version conflicts abort the reindex process.
          To continue reindexing if there are conflicts, set the <code>conflicts</code> request body property to <code>proceed</code>.
          In this case, the response includes a count of the version conflicts that were encountered.
          Note that the handling of other error types is unaffected by the <code>conflicts</code> property.
          Additionally, if you opt to count version conflicts, the operation could attempt to reindex more documents from the source than <code>max_docs</code> until it has successfully indexed <code>max_docs</code> documents into the target or it has gone through every document in the source query.</p>
          <p>It's recommended to reindex on indices with a green status. Reindexing can fail when a node shuts down or crashes.</p>
          <ul>
          <li>When requested with <code>wait_for_completion=true</code> (default), the request fails if the node shuts down.</li>
          <li>When requested with <code>wait_for_completion=false</code>, a task id is returned, for use with the task management APIs. The task may disappear or fail if the node shuts down.
          When retrying a failed reindex operation, it might be necessary to set <code>conflicts=proceed</code> or to first delete the partial destination index.
          Additionally, dry runs, checking disk space, and fetching index recovery information can help address the root cause.</li>
          </ul>
          <p>Refer to the linked documentation for examples of how to reindex documents.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex>`_

        :param dest: The destination you are copying to.
        :param source: The source you are copying from.
        :param conflicts: Indicates whether to continue reindexing even when there are
            conflicts.
        :param max_docs: The maximum number of documents to reindex. By default, all
            documents are reindexed. If it is a value less then or equal to `scroll_size`,
            a scroll will not be used to retrieve the results for the operation. If `conflicts`
            is set to `proceed`, the reindex operation could attempt to reindex more
            documents from the source than `max_docs` until it has successfully indexed
            `max_docs` documents into the target or it has gone through every document
            in the source query.
        :param refresh: If `true`, the request refreshes affected shards to make this
            operation visible to search.
        :param requests_per_second: The throttle for this request in sub-requests per
            second. By default, there is no throttle.
        :param require_alias: If `true`, the destination must be an index alias.
        :param script: The script to run to update the document source or metadata when
            reindexing.
        :param scroll: The period of time that a consistent view of the index should
            be maintained for scrolled search.
        :param size:
        :param slices: The number of slices this task should be divided into. It defaults
            to one slice, which means the task isn't sliced into subtasks. Reindex supports
            sliced scroll to parallelize the reindexing process. This parallelization
            can improve efficiency and provide a convenient way to break the request
            down into smaller parts. NOTE: Reindexing from remote clusters does not support
            manual or automatic slicing. If set to `auto`, Elasticsearch chooses the
            number of slices to use. This setting will use one slice per shard, up to
            a certain limit. If there are multiple sources, it will choose the number
            of slices based on the index or backing index with the smallest number of
            shards.
        :param timeout: The period each indexing waits for automatic index creation,
            dynamic mapping updates, and waiting for active shards. By default, Elasticsearch
            waits for at least one minute before failing. The actual wait time could
            be longer, particularly when multiple waits occur.
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set it to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`). The
            default value is one, which means it waits for each primary shard to be active.
        :param wait_for_completion: If `true`, the request blocks until the operation
            is complete.
        """
        if dest is None and body is None:
            raise ValueError("Empty value passed for parameter 'dest'")
        if source is None and body is None:
            raise ValueError("Empty value passed for parameter 'source'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_reindex"
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
        if requests_per_second is not None:
            __query["requests_per_second"] = requests_per_second
        if require_alias is not None:
            __query["require_alias"] = require_alias
        if scroll is not None:
            __query["scroll"] = scroll
        if slices is not None:
            __query["slices"] = slices
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        if wait_for_completion is not None:
            __query["wait_for_completion"] = wait_for_completion
        if not __body:
            if dest is not None:
                __body["dest"] = dest
            if source is not None:
                __body["source"] = source
            if conflicts is not None:
                __body["conflicts"] = conflicts
            if max_docs is not None:
                __body["max_docs"] = max_docs
            if script is not None:
                __body["script"] = script
            if size is not None:
                __body["size"] = size
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="reindex",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def reindex_rethrottle(
        self,
        *,
        task_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        requests_per_second: t.Optional[float] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Throttle a reindex operation.</p>
          <p>Change the number of requests per second for a particular reindex operation.
          For example:</p>
          <pre><code>POST _reindex/r1A2WoRbTwKZ516z6NEs5A:36619/_rethrottle?requests_per_second=-1
          </code></pre>
          <p>Rethrottling that speeds up the query takes effect immediately.
          Rethrottling that slows down the query will take effect after completing the current batch.
          This behavior prevents scroll timeouts.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-reindex>`_

        :param task_id: The task identifier, which can be found by using the tasks API.
        :param requests_per_second: The throttle for this request in sub-requests per
            second. It can be either `-1` to turn off throttling or any decimal number
            like `1.7` or `12` to throttle to that level.
        """
        if task_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_id'")
        __path_parts: t.Dict[str, str] = {"task_id": _quote(task_id)}
        __path = f'/_reindex/{__path_parts["task_id"]}/_rethrottle'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if requests_per_second is not None:
            __query["requests_per_second"] = requests_per_second
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="reindex_rethrottle",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("file", "params", "source"),
        ignore_deprecated_options={"params"},
    )
    def render_search_template(
        self,
        *,
        id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        file: t.Optional[str] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        params: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        source: t.Optional[t.Union[str, t.Mapping[str, t.Any]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Render a search template.</p>
          <p>Render a search template as a search request body.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-render-search-template>`_

        :param id: The ID of the search template to render. If no `source` is specified,
            this or the `id` request body parameter is required.
        :param file:
        :param params: Key-value pairs used to replace Mustache variables in the template.
            The key is the variable name. The value is the variable value.
        :param source: An inline search template. It supports the same parameters as
            the search API's request body. These parameters also support Mustache variables.
            If no `id` or `<templated-id>` is specified, this parameter is required.
        """
        __path_parts: t.Dict[str, str]
        if id not in SKIP_IN_PATH:
            __path_parts = {"id": _quote(id)}
            __path = f'/_render/template/{__path_parts["id"]}'
        else:
            __path_parts = {}
            __path = "/_render/template"
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
            if file is not None:
                __body["file"] = file
            if params is not None:
                __body["params"] = params
            if source is not None:
                __body["source"] = source
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
            endpoint_id="render_search_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("context", "context_setup", "script"),
    )
    @_stability_warning(Stability.EXPERIMENTAL)
    def scripts_painless_execute(
        self,
        *,
        context: t.Optional[
            t.Union[
                str,
                t.Literal[
                    "boolean_field",
                    "composite_field",
                    "date_field",
                    "double_field",
                    "filter",
                    "geo_point_field",
                    "ip_field",
                    "keyword_field",
                    "long_field",
                    "painless_test",
                    "score",
                ],
            ]
        ] = None,
        context_setup: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        script: t.Optional[t.Mapping[str, t.Any]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run a script.</p>
          <p>Runs a script and returns a result.
          Use this API to build and test scripts, such as when defining a script for a runtime field.
          This API requires very few dependencies and is especially useful if you don't have permissions to write documents on a cluster.</p>
          <p>The API uses several <em>contexts</em>, which control how scripts are run, what variables are available at runtime, and what the return type is.</p>
          <p>Each context requires a script, but additional parameters depend on the context you're using for that script.</p>


        `<https://www.elastic.co/docs/reference/scripting-languages/painless/painless-api-examples>`_

        :param context: The context that the script should run in. NOTE: Result ordering
            in the field contexts is not guaranteed.
        :param context_setup: Additional parameters for the `context`. NOTE: This parameter
            is required for all contexts except `painless_test`, which is the default
            if no value is provided for `context`.
        :param script: The Painless script to run.
        """
        __path_parts: t.Dict[str, str] = {}
        __path = "/_scripts/painless/_execute"
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
            if context is not None:
                __body["context"] = context
            if context_setup is not None:
                __body["context_setup"] = context_setup
            if script is not None:
                __body["script"] = script
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
            endpoint_id="scripts_painless_execute",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("scroll_id", "scroll"),
    )
    def scroll(
        self,
        *,
        scroll_id: t.Optional[str] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        rest_total_hits_as_int: t.Optional[bool] = None,
        scroll: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run a scrolling search.</p>
          <p>IMPORTANT: The scroll API is no longer recommend for deep pagination. If you need to preserve the index state while paging through more than 10,000 hits, use the <code>search_after</code> parameter with a point in time (PIT).</p>
          <p>The scroll API gets large sets of results from a single scrolling search request.
          To get the necessary scroll ID, submit a search API request that includes an argument for the <code>scroll</code> query parameter.
          The <code>scroll</code> parameter indicates how long Elasticsearch should retain the search context for the request.
          The search response returns a scroll ID in the <code>_scroll_id</code> response body parameter.
          You can then use the scroll ID with the scroll API to retrieve the next batch of results for the request.
          If the Elasticsearch security features are enabled, the access to the results of a specific scroll ID is restricted to the user or API key that submitted the search.</p>
          <p>You can also use the scroll API to specify a new scroll parameter that extends or shortens the retention period for the search context.</p>
          <p>IMPORTANT: Results from a scrolling search reflect the state of the index at the time of the initial search request. Subsequent indexing or document changes only affect later search and scroll requests.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-scroll>`_

        :param scroll_id: The scroll ID of the search.
        :param rest_total_hits_as_int: If true, the API response’s hit.total property
            is returned as an integer. If false, the API response’s hit.total property
            is returned as an object.
        :param scroll: The period to retain the search context for scrolling.
        """
        if scroll_id is None and body is None:
            raise ValueError("Empty value passed for parameter 'scroll_id'")
        __path_parts: t.Dict[str, str] = {}
        __path = "/_search/scroll"
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
        if rest_total_hits_as_int is not None:
            __query["rest_total_hits_as_int"] = rest_total_hits_as_int
        if not __body:
            if scroll_id is not None:
                __body["scroll_id"] = scroll_id
            if scroll is not None:
                __body["scroll"] = scroll
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
            endpoint_id="scroll",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "aggregations",
            "aggs",
            "collapse",
            "docvalue_fields",
            "explain",
            "ext",
            "fields",
            "from_",
            "highlight",
            "indices_boost",
            "knn",
            "min_score",
            "pit",
            "post_filter",
            "profile",
            "query",
            "rank",
            "rescore",
            "retriever",
            "runtime_mappings",
            "script_fields",
            "search_after",
            "seq_no_primary_term",
            "size",
            "slice",
            "sort",
            "source",
            "stats",
            "stored_fields",
            "suggest",
            "terminate_after",
            "timeout",
            "track_scores",
            "track_total_hits",
            "version",
        ),
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
            "from": "from_",
        },
    )
    def search(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        aggregations: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        aggs: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        allow_partial_search_results: t.Optional[bool] = None,
        analyze_wildcard: t.Optional[bool] = None,
        analyzer: t.Optional[str] = None,
        batched_reduce_size: t.Optional[int] = None,
        ccs_minimize_roundtrips: t.Optional[bool] = None,
        collapse: t.Optional[t.Mapping[str, t.Any]] = None,
        default_operator: t.Optional[t.Union[str, t.Literal["and", "or"]]] = None,
        df: t.Optional[str] = None,
        docvalue_fields: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
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
        ext: t.Optional[t.Mapping[str, t.Any]] = None,
        fields: t.Optional[t.Sequence[t.Mapping[str, t.Any]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        force_synthetic_source: t.Optional[bool] = None,
        from_: t.Optional[int] = None,
        highlight: t.Optional[t.Mapping[str, t.Any]] = None,
        human: t.Optional[bool] = None,
        ignore_throttled: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        include_named_queries_score: t.Optional[bool] = None,
        indices_boost: t.Optional[t.Sequence[t.Mapping[str, float]]] = None,
        knn: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        lenient: t.Optional[bool] = None,
        max_concurrent_shard_requests: t.Optional[int] = None,
        min_score: t.Optional[float] = None,
        pit: t.Optional[t.Mapping[str, t.Any]] = None,
        post_filter: t.Optional[t.Mapping[str, t.Any]] = None,
        pre_filter_shard_size: t.Optional[int] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        profile: t.Optional[bool] = None,
        q: t.Optional[str] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        rank: t.Optional[t.Mapping[str, t.Any]] = None,
        request_cache: t.Optional[bool] = None,
        rescore: t.Optional[
            t.Union[t.Mapping[str, t.Any], t.Sequence[t.Mapping[str, t.Any]]]
        ] = None,
        rest_total_hits_as_int: t.Optional[bool] = None,
        retriever: t.Optional[t.Mapping[str, t.Any]] = None,
        routing: t.Optional[str] = None,
        runtime_mappings: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        script_fields: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        scroll: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        search_after: t.Optional[
            t.Sequence[t.Union[None, bool, float, int, str]]
        ] = None,
        search_type: t.Optional[
            t.Union[str, t.Literal["dfs_query_then_fetch", "query_then_fetch"]]
        ] = None,
        seq_no_primary_term: t.Optional[bool] = None,
        size: t.Optional[int] = None,
        slice: t.Optional[t.Mapping[str, t.Any]] = None,
        sort: t.Optional[
            t.Union[
                t.Sequence[t.Union[str, t.Mapping[str, t.Any]]],
                t.Union[str, t.Mapping[str, t.Any]],
            ]
        ] = None,
        source: t.Optional[t.Union[bool, t.Mapping[str, t.Any]]] = None,
        source_excludes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        source_includes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        stats: t.Optional[t.Sequence[str]] = None,
        stored_fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        suggest: t.Optional[t.Mapping[str, t.Any]] = None,
        suggest_field: t.Optional[str] = None,
        suggest_mode: t.Optional[
            t.Union[str, t.Literal["always", "missing", "popular"]]
        ] = None,
        suggest_size: t.Optional[int] = None,
        suggest_text: t.Optional[str] = None,
        terminate_after: t.Optional[int] = None,
        timeout: t.Optional[str] = None,
        track_scores: t.Optional[bool] = None,
        track_total_hits: t.Optional[t.Union[bool, int]] = None,
        typed_keys: t.Optional[bool] = None,
        version: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run a search.</p>
          <p>Get search hits that match the query defined in the request.
          You can provide search queries using the <code>q</code> query string parameter or the request body.
          If both are specified, only the query parameter is used.</p>
          <p>If the Elasticsearch security features are enabled, you must have the read index privilege for the target data stream, index, or alias. For cross-cluster search, refer to the documentation about configuring CCS privileges.
          To search a point in time (PIT) for an alias, you must have the <code>read</code> index privilege for the alias's data streams or indices.</p>
          <p><strong>Search slicing</strong></p>
          <p>When paging through a large number of documents, it can be helpful to split the search into multiple slices to consume them independently with the <code>slice</code> and <code>pit</code> properties.
          By default the splitting is done first on the shards, then locally on each shard.
          The local splitting partitions the shard into contiguous ranges based on Lucene document IDs.</p>
          <p>For instance if the number of shards is equal to 2 and you request 4 slices, the slices 0 and 2 are assigned to the first shard and the slices 1 and 3 are assigned to the second shard.</p>
          <p>IMPORTANT: The same point-in-time ID should be used for all slices.
          If different PIT IDs are used, slices can overlap and miss documents.
          This situation can occur because the splitting criterion is based on Lucene document IDs, which are not stable across changes to the index.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search>`_

        :param index: A comma-separated list of data streams, indices, and aliases to
            search. It supports wildcards (`*`). To search all data streams and indices,
            omit this parameter or use `*` or `_all`.
        :param aggregations: Defines the aggregations that are run as part of the search
            request.
        :param aggs: Defines the aggregations that are run as part of the search request.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param allow_partial_search_results: If `true` and there are shard request timeouts
            or shard failures, the request returns partial results. If `false`, it returns
            an error with no partial results. To override the default behavior, you can
            set the `search.default_allow_partial_results` cluster setting to `false`.
        :param analyze_wildcard: If `true`, wildcard and prefix queries are analyzed.
            This parameter can be used only when the `q` query string parameter is specified.
        :param analyzer: The analyzer to use for the query string. This parameter can
            be used only when the `q` query string parameter is specified.
        :param batched_reduce_size: The number of shard results that should be reduced
            at once on the coordinating node. If the potential number of shards in the
            request can be large, this value should be used as a protection mechanism
            to reduce the memory overhead per search request.
        :param ccs_minimize_roundtrips: If `true`, network round-trips between the coordinating
            node and the remote clusters are minimized when running cross-cluster search
            (CCS) requests.
        :param collapse: Collapses search results the values of the specified field.
        :param default_operator: The default operator for the query string query: `AND`
            or `OR`. This parameter can be used only when the `q` query string parameter
            is specified.
        :param df: The field to use as a default when no field prefix is given in the
            query string. This parameter can be used only when the `q` query string parameter
            is specified.
        :param docvalue_fields: An array of wildcard (`*`) field patterns. The request
            returns doc values for field names matching these patterns in the `hits.fields`
            property of the response.
        :param expand_wildcards: The type of index that wildcard patterns can match.
            If the request can target data streams, this argument determines whether
            wildcard expressions match hidden data streams. It supports comma-separated
            values such as `open,hidden`.
        :param explain: If `true`, the request returns detailed information about score
            computation as part of a hit.
        :param ext: Configuration of search extensions defined by Elasticsearch plugins.
        :param fields: An array of wildcard (`*`) field patterns. The request returns
            values for field names matching these patterns in the `hits.fields` property
            of the response.
        :param force_synthetic_source: Should this request force synthetic _source? Use
            this to test if the mapping supports synthetic _source and to get a sense
            of the worst case performance. Fetches with this enabled will be slower the
            enabling synthetic source natively in the index.
        :param from_: The starting document offset, which must be non-negative. By default,
            you cannot page through more than 10,000 hits using the `from` and `size`
            parameters. To page through more hits, use the `search_after` parameter.
        :param highlight: Specifies the highlighter to use for retrieving highlighted
            snippets from one or more fields in your search results.
        :param ignore_throttled: If `true`, concrete, expanded or aliased indices will
            be ignored when frozen.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param include_named_queries_score: If `true`, the response includes the score
            contribution from any named queries. This functionality reruns each named
            query on every hit in a search response. Typically, this adds a small overhead
            to a request. However, using computationally expensive named queries on a
            large number of hits may add significant overhead.
        :param indices_boost: Boost the `_score` of documents from specified indices.
            The boost value is the factor by which scores are multiplied. A boost value
            greater than `1.0` increases the score. A boost value between `0` and `1.0`
            decreases the score.
        :param knn: The approximate kNN search to run.
        :param lenient: If `true`, format-based query failures (such as providing text
            to a numeric field) in the query string will be ignored. This parameter can
            be used only when the `q` query string parameter is specified.
        :param max_concurrent_shard_requests: The number of concurrent shard requests
            per node that the search runs concurrently. This value should be used to
            limit the impact of the search on the cluster in order to limit the number
            of concurrent shard requests.
        :param min_score: The minimum `_score` for matching documents. Documents with
            a lower `_score` are not included in search results and results collected
            by aggregations.
        :param pit: Limit the search to a point in time (PIT). If you provide a PIT,
            you cannot specify an `<index>` in the request path.
        :param post_filter: Use the `post_filter` parameter to filter search results.
            The search hits are filtered after the aggregations are calculated. A post
            filter has no impact on the aggregation results.
        :param pre_filter_shard_size: A threshold that enforces a pre-filter roundtrip
            to prefilter search shards based on query rewriting if the number of shards
            the search request expands to exceeds the threshold. This filter roundtrip
            can limit the number of shards significantly if for instance a shard can
            not match any documents based on its rewrite method (if date filters are
            mandatory to match but the shard bounds and the query are disjoint). When
            unspecified, the pre-filter phase is executed if any of these conditions
            is met: * The request targets more than 128 shards. * The request targets
            one or more read-only index. * The primary sort of the query targets an indexed
            field.
        :param preference: The nodes and shards used for the search. By default, Elasticsearch
            selects from eligible nodes and shards using adaptive replica selection,
            accounting for allocation awareness. Valid values are: * `_only_local` to
            run the search only on shards on the local node. * `_local` to, if possible,
            run the search on shards on the local node, or if not, select shards using
            the default method. * `_only_nodes:<node-id>,<node-id>` to run the search
            on only the specified nodes IDs. If suitable shards exist on more than one
            selected node, use shards on those nodes using the default method. If none
            of the specified nodes are available, select shards from any available node
            using the default method. * `_prefer_nodes:<node-id>,<node-id>` to if possible,
            run the search on the specified nodes IDs. If not, select shards using the
            default method. * `_shards:<shard>,<shard>` to run the search only on the
            specified shards. You can combine this value with other `preference` values.
            However, the `_shards` value must come first. For example: `_shards:2,3|_local`.
            * `<custom-string>` (any string that does not start with `_`) to route searches
            with the same `<custom-string>` to the same shards in the same order.
        :param profile: Set to `true` to return detailed timing information about the
            execution of individual components in a search request. NOTE: This is a debugging
            tool and adds significant overhead to search execution.
        :param q: A query in the Lucene query string syntax. Query parameter searches
            do not support the full Elasticsearch Query DSL but are handy for testing.
            IMPORTANT: This parameter overrides the query parameter in the request body.
            If both parameters are specified, documents matching the query request body
            parameter are not returned.
        :param query: The search definition using the Query DSL.
        :param rank: The Reciprocal Rank Fusion (RRF) to use.
        :param request_cache: If `true`, the caching of search results is enabled for
            requests where `size` is `0`. It defaults to index level settings.
        :param rescore: Can be used to improve precision by reordering just the top (for
            example 100 - 500) documents returned by the `query` and `post_filter` phases.
        :param rest_total_hits_as_int: Indicates whether `hits.total` should be rendered
            as an integer or an object in the rest search response.
        :param retriever: A retriever is a specification to describe top documents returned
            from a search. A retriever replaces other elements of the search API that
            also return top documents such as `query` and `knn`.
        :param routing: A custom value that is used to route operations to a specific
            shard.
        :param runtime_mappings: One or more runtime fields in the search request. These
            fields take precedence over mapped fields with the same name.
        :param script_fields: Retrieve a script evaluation (based on different fields)
            for each hit.
        :param scroll: The period to retain the search context for scrolling. By default,
            this value cannot exceed `1d` (24 hours). You can change this limit by using
            the `search.max_keep_alive` cluster-level setting.
        :param search_after: Used to retrieve the next page of hits using a set of sort
            values from the previous page.
        :param search_type: Indicates how distributed term frequencies are calculated
            for relevance scoring.
        :param seq_no_primary_term: If `true`, the request returns sequence number and
            primary term of the last modification of each hit.
        :param size: The number of hits to return, which must not be negative. By default,
            you cannot page through more than 10,000 hits using the `from` and `size`
            parameters. To page through more hits, use the `search_after` property.
        :param slice: Split a scrolled search into multiple slices that can be consumed
            independently.
        :param sort: A comma-separated list of <field>:<direction> pairs.
        :param source: The source fields that are returned for matching documents. These
            fields are returned in the `hits._source` property of the search response.
            If the `stored_fields` property is specified, the `_source` property defaults
            to `false`. Otherwise, it defaults to `true`.
        :param source_excludes: A comma-separated list of source fields to exclude from
            the response. You can also use this parameter to exclude fields from the
            subset specified in `_source_includes` query parameter. If the `_source`
            parameter is `false`, this parameter is ignored.
        :param source_includes: A comma-separated list of source fields to include in
            the response. If this parameter is specified, only these source fields are
            returned. You can exclude fields from this subset using the `_source_excludes`
            query parameter. If the `_source` parameter is `false`, this parameter is
            ignored.
        :param stats: The stats groups to associate with the search. Each group maintains
            a statistics aggregation for its associated searches. You can retrieve these
            stats using the indices stats API.
        :param stored_fields: A comma-separated list of stored fields to return as part
            of a hit. If no fields are specified, no stored fields are included in the
            response. If this field is specified, the `_source` property defaults to
            `false`. You can pass `_source: true` to return both source fields and stored
            fields in the search response.
        :param suggest: Defines a suggester that provides similar looking terms based
            on a provided text.
        :param suggest_field: The field to use for suggestions.
        :param suggest_mode: The suggest mode. This parameter can be used only when the
            `suggest_field` and `suggest_text` query string parameters are specified.
        :param suggest_size: The number of suggestions to return. This parameter can
            be used only when the `suggest_field` and `suggest_text` query string parameters
            are specified.
        :param suggest_text: The source text for which the suggestions should be returned.
            This parameter can be used only when the `suggest_field` and `suggest_text`
            query string parameters are specified.
        :param terminate_after: The maximum number of documents to collect for each shard.
            If a query reaches this limit, Elasticsearch terminates the query early.
            Elasticsearch collects documents before sorting. IMPORTANT: Use with caution.
            Elasticsearch applies this property to each shard handling the request. When
            possible, let Elasticsearch perform early termination automatically. Avoid
            specifying this property for requests that target data streams with backing
            indices across multiple data tiers. If set to `0` (default), the query does
            not terminate early.
        :param timeout: The period of time to wait for a response from each shard. If
            no response is received before the timeout expires, the request fails and
            returns an error. Defaults to no timeout.
        :param track_scores: If `true`, calculate and return document scores, even if
            the scores are not used for sorting.
        :param track_total_hits: Number of hits matching the query to count accurately.
            If `true`, the exact number of hits is returned at the cost of some performance.
            If `false`, the response does not include the total number of hits matching
            the query.
        :param typed_keys: If `true`, aggregation and suggester names are be prefixed
            by their respective types in the response.
        :param version: If `true`, the request returns the document version as part of
            a hit.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_search'
        else:
            __path_parts = {}
            __path = "/_search"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        # The 'sort' parameter with a colon can't be encoded to the body.
        if sort is not None and (
            (isinstance(sort, str) and ":" in sort)
            or (
                isinstance(sort, (list, tuple))
                and all(isinstance(_x, str) for _x in sort)
                and any(":" in _x for _x in sort)
            )
        ):
            __query["sort"] = sort
            sort = None
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if allow_partial_search_results is not None:
            __query["allow_partial_search_results"] = allow_partial_search_results
        if analyze_wildcard is not None:
            __query["analyze_wildcard"] = analyze_wildcard
        if analyzer is not None:
            __query["analyzer"] = analyzer
        if batched_reduce_size is not None:
            __query["batched_reduce_size"] = batched_reduce_size
        if ccs_minimize_roundtrips is not None:
            __query["ccs_minimize_roundtrips"] = ccs_minimize_roundtrips
        if default_operator is not None:
            __query["default_operator"] = default_operator
        if df is not None:
            __query["df"] = df
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if expand_wildcards is not None:
            __query["expand_wildcards"] = expand_wildcards
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if force_synthetic_source is not None:
            __query["force_synthetic_source"] = force_synthetic_source
        if human is not None:
            __query["human"] = human
        if ignore_throttled is not None:
            __query["ignore_throttled"] = ignore_throttled
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if include_named_queries_score is not None:
            __query["include_named_queries_score"] = include_named_queries_score
        if lenient is not None:
            __query["lenient"] = lenient
        if max_concurrent_shard_requests is not None:
            __query["max_concurrent_shard_requests"] = max_concurrent_shard_requests
        if pre_filter_shard_size is not None:
            __query["pre_filter_shard_size"] = pre_filter_shard_size
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if q is not None:
            __query["q"] = q
        if request_cache is not None:
            __query["request_cache"] = request_cache
        if rest_total_hits_as_int is not None:
            __query["rest_total_hits_as_int"] = rest_total_hits_as_int
        if routing is not None:
            __query["routing"] = routing
        if scroll is not None:
            __query["scroll"] = scroll
        if search_type is not None:
            __query["search_type"] = search_type
        if source_excludes is not None:
            __query["_source_excludes"] = source_excludes
        if source_includes is not None:
            __query["_source_includes"] = source_includes
        if suggest_field is not None:
            __query["suggest_field"] = suggest_field
        if suggest_mode is not None:
            __query["suggest_mode"] = suggest_mode
        if suggest_size is not None:
            __query["suggest_size"] = suggest_size
        if suggest_text is not None:
            __query["suggest_text"] = suggest_text
        if typed_keys is not None:
            __query["typed_keys"] = typed_keys
        if not __body:
            if aggregations is not None:
                __body["aggregations"] = aggregations
            if aggs is not None:
                __body["aggs"] = aggs
            if collapse is not None:
                __body["collapse"] = collapse
            if docvalue_fields is not None:
                __body["docvalue_fields"] = docvalue_fields
            if explain is not None:
                __body["explain"] = explain
            if ext is not None:
                __body["ext"] = ext
            if fields is not None:
                __body["fields"] = fields
            if from_ is not None:
                __body["from"] = from_
            if highlight is not None:
                __body["highlight"] = highlight
            if indices_boost is not None:
                __body["indices_boost"] = indices_boost
            if knn is not None:
                __body["knn"] = knn
            if min_score is not None:
                __body["min_score"] = min_score
            if pit is not None:
                __body["pit"] = pit
            if post_filter is not None:
                __body["post_filter"] = post_filter
            if profile is not None:
                __body["profile"] = profile
            if query is not None:
                __body["query"] = query
            if rank is not None:
                __body["rank"] = rank
            if rescore is not None:
                __body["rescore"] = rescore
            if retriever is not None:
                __body["retriever"] = retriever
            if runtime_mappings is not None:
                __body["runtime_mappings"] = runtime_mappings
            if script_fields is not None:
                __body["script_fields"] = script_fields
            if search_after is not None:
                __body["search_after"] = search_after
            if seq_no_primary_term is not None:
                __body["seq_no_primary_term"] = seq_no_primary_term
            if size is not None:
                __body["size"] = size
            if slice is not None:
                __body["slice"] = slice
            if sort is not None:
                __body["sort"] = sort
            if source is not None:
                __body["_source"] = source
            if stats is not None:
                __body["stats"] = stats
            if stored_fields is not None:
                __body["stored_fields"] = stored_fields
            if suggest is not None:
                __body["suggest"] = suggest
            if terminate_after is not None:
                __body["terminate_after"] = terminate_after
            if timeout is not None:
                __body["timeout"] = timeout
            if track_scores is not None:
                __body["track_scores"] = track_scores
            if track_total_hits is not None:
                __body["track_total_hits"] = track_total_hits
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
            endpoint_id="search",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "aggs",
            "buffer",
            "exact_bounds",
            "extent",
            "fields",
            "grid_agg",
            "grid_precision",
            "grid_type",
            "query",
            "runtime_mappings",
            "size",
            "sort",
            "track_total_hits",
            "with_labels",
        ),
    )
    def search_mvt(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        field: str,
        zoom: int,
        x: int,
        y: int,
        aggs: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        buffer: t.Optional[int] = None,
        error_trace: t.Optional[bool] = None,
        exact_bounds: t.Optional[bool] = None,
        extent: t.Optional[int] = None,
        fields: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        grid_agg: t.Optional[t.Union[str, t.Literal["geohex", "geotile"]]] = None,
        grid_precision: t.Optional[int] = None,
        grid_type: t.Optional[
            t.Union[str, t.Literal["centroid", "grid", "point"]]
        ] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        runtime_mappings: t.Optional[t.Mapping[str, t.Mapping[str, t.Any]]] = None,
        size: t.Optional[int] = None,
        sort: t.Optional[
            t.Union[
                t.Sequence[t.Union[str, t.Mapping[str, t.Any]]],
                t.Union[str, t.Mapping[str, t.Any]],
            ]
        ] = None,
        track_total_hits: t.Optional[t.Union[bool, int]] = None,
        with_labels: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> BinaryApiResponse:
        """
        .. raw:: html

          <p>Search a vector tile.</p>
          <p>Search a vector tile for geospatial values.
          Before using this API, you should be familiar with the Mapbox vector tile specification.
          The API returns results as a binary mapbox vector tile.</p>
          <p>Internally, Elasticsearch translates a vector tile search API request into a search containing:</p>
          <ul>
          <li>A <code>geo_bounding_box</code> query on the <code>&lt;field&gt;</code>. The query uses the <code>&lt;zoom&gt;/&lt;x&gt;/&lt;y&gt;</code> tile as a bounding box.</li>
          <li>A <code>geotile_grid</code> or <code>geohex_grid</code> aggregation on the <code>&lt;field&gt;</code>. The <code>grid_agg</code> parameter determines the aggregation type. The aggregation uses the <code>&lt;zoom&gt;/&lt;x&gt;/&lt;y&gt;</code> tile as a bounding box.</li>
          <li>Optionally, a <code>geo_bounds</code> aggregation on the <code>&lt;field&gt;</code>. The search only includes this aggregation if the <code>exact_bounds</code> parameter is <code>true</code>.</li>
          <li>If the optional parameter <code>with_labels</code> is <code>true</code>, the internal search will include a dynamic runtime field that calls the <code>getLabelPosition</code> function of the geometry doc value. This enables the generation of new point features containing suggested geometry labels, so that, for example, multi-polygons will have only one label.</li>
          </ul>
          <p>The API returns results as a binary Mapbox vector tile.
          Mapbox vector tiles are encoded as Google Protobufs (PBF). By default, the tile contains three layers:</p>
          <ul>
          <li>A <code>hits</code> layer containing a feature for each <code>&lt;field&gt;</code> value matching the <code>geo_bounding_box</code> query.</li>
          <li>An <code>aggs</code> layer containing a feature for each cell of the <code>geotile_grid</code> or <code>geohex_grid</code>. The layer only contains features for cells with matching data.</li>
          <li>A meta layer containing:
          <ul>
          <li>A feature containing a bounding box. By default, this is the bounding box of the tile.</li>
          <li>Value ranges for any sub-aggregations on the <code>geotile_grid</code> or <code>geohex_grid</code>.</li>
          <li>Metadata for the search.</li>
          </ul>
          </li>
          </ul>
          <p>The API only returns features that can display at its zoom level.
          For example, if a polygon feature has no area at its zoom level, the API omits it.
          The API returns errors as UTF-8 encoded JSON.</p>
          <p>IMPORTANT: You can specify several options for this API as either a query parameter or request body parameter.
          If you specify both parameters, the query parameter takes precedence.</p>
          <p><strong>Grid precision for geotile</strong></p>
          <p>For a <code>grid_agg</code> of <code>geotile</code>, you can use cells in the <code>aggs</code> layer as tiles for lower zoom levels.
          <code>grid_precision</code> represents the additional zoom levels available through these cells. The final precision is computed by as follows: <code>&lt;zoom&gt; + grid_precision</code>.
          For example, if <code>&lt;zoom&gt;</code> is 7 and <code>grid_precision</code> is 8, then the <code>geotile_grid</code> aggregation will use a precision of 15.
          The maximum final precision is 29.
          The <code>grid_precision</code> also determines the number of cells for the grid as follows: <code>(2^grid_precision) x (2^grid_precision)</code>.
          For example, a value of 8 divides the tile into a grid of 256 x 256 cells.
          The <code>aggs</code> layer only contains features for cells with matching data.</p>
          <p><strong>Grid precision for geohex</strong></p>
          <p>For a <code>grid_agg</code> of <code>geohex</code>, Elasticsearch uses <code>&lt;zoom&gt;</code> and <code>grid_precision</code> to calculate a final precision as follows: <code>&lt;zoom&gt; + grid_precision</code>.</p>
          <p>This precision determines the H3 resolution of the hexagonal cells produced by the <code>geohex</code> aggregation.
          The following table maps the H3 resolution for each precision.
          For example, if <code>&lt;zoom&gt;</code> is 3 and <code>grid_precision</code> is 3, the precision is 6.
          At a precision of 6, hexagonal cells have an H3 resolution of 2.
          If <code>&lt;zoom&gt;</code> is 3 and <code>grid_precision</code> is 4, the precision is 7.
          At a precision of 7, hexagonal cells have an H3 resolution of 3.</p>
          <table>
          <thead>
          <tr>
          <th>Precision</th>
          <th>Unique tile bins</th>
          <th>H3 resolution</th>
          <th>Unique hex bins</th>
          <th>Ratio</th>
          </tr>
          </thead>
          <tbody>
          <tr>
          <td>1</td>
          <td>4</td>
          <td>0</td>
          <td>122</td>
          <td>30.5</td>
          </tr>
          <tr>
          <td>2</td>
          <td>16</td>
          <td>0</td>
          <td>122</td>
          <td>7.625</td>
          </tr>
          <tr>
          <td>3</td>
          <td>64</td>
          <td>1</td>
          <td>842</td>
          <td>13.15625</td>
          </tr>
          <tr>
          <td>4</td>
          <td>256</td>
          <td>1</td>
          <td>842</td>
          <td>3.2890625</td>
          </tr>
          <tr>
          <td>5</td>
          <td>1024</td>
          <td>2</td>
          <td>5882</td>
          <td>5.744140625</td>
          </tr>
          <tr>
          <td>6</td>
          <td>4096</td>
          <td>2</td>
          <td>5882</td>
          <td>1.436035156</td>
          </tr>
          <tr>
          <td>7</td>
          <td>16384</td>
          <td>3</td>
          <td>41162</td>
          <td>2.512329102</td>
          </tr>
          <tr>
          <td>8</td>
          <td>65536</td>
          <td>3</td>
          <td>41162</td>
          <td>0.6280822754</td>
          </tr>
          <tr>
          <td>9</td>
          <td>262144</td>
          <td>4</td>
          <td>288122</td>
          <td>1.099098206</td>
          </tr>
          <tr>
          <td>10</td>
          <td>1048576</td>
          <td>4</td>
          <td>288122</td>
          <td>0.2747745514</td>
          </tr>
          <tr>
          <td>11</td>
          <td>4194304</td>
          <td>5</td>
          <td>2016842</td>
          <td>0.4808526039</td>
          </tr>
          <tr>
          <td>12</td>
          <td>16777216</td>
          <td>6</td>
          <td>14117882</td>
          <td>0.8414913416</td>
          </tr>
          <tr>
          <td>13</td>
          <td>67108864</td>
          <td>6</td>
          <td>14117882</td>
          <td>0.2103728354</td>
          </tr>
          <tr>
          <td>14</td>
          <td>268435456</td>
          <td>7</td>
          <td>98825162</td>
          <td>0.3681524172</td>
          </tr>
          <tr>
          <td>15</td>
          <td>1073741824</td>
          <td>8</td>
          <td>691776122</td>
          <td>0.644266719</td>
          </tr>
          <tr>
          <td>16</td>
          <td>4294967296</td>
          <td>8</td>
          <td>691776122</td>
          <td>0.1610666797</td>
          </tr>
          <tr>
          <td>17</td>
          <td>17179869184</td>
          <td>9</td>
          <td>4842432842</td>
          <td>0.2818666889</td>
          </tr>
          <tr>
          <td>18</td>
          <td>68719476736</td>
          <td>10</td>
          <td>33897029882</td>
          <td>0.4932667053</td>
          </tr>
          <tr>
          <td>19</td>
          <td>274877906944</td>
          <td>11</td>
          <td>237279209162</td>
          <td>0.8632167343</td>
          </tr>
          <tr>
          <td>20</td>
          <td>1099511627776</td>
          <td>11</td>
          <td>237279209162</td>
          <td>0.2158041836</td>
          </tr>
          <tr>
          <td>21</td>
          <td>4398046511104</td>
          <td>12</td>
          <td>1660954464122</td>
          <td>0.3776573213</td>
          </tr>
          <tr>
          <td>22</td>
          <td>17592186044416</td>
          <td>13</td>
          <td>11626681248842</td>
          <td>0.6609003122</td>
          </tr>
          <tr>
          <td>23</td>
          <td>70368744177664</td>
          <td>13</td>
          <td>11626681248842</td>
          <td>0.165225078</td>
          </tr>
          <tr>
          <td>24</td>
          <td>281474976710656</td>
          <td>14</td>
          <td>81386768741882</td>
          <td>0.2891438866</td>
          </tr>
          <tr>
          <td>25</td>
          <td>1125899906842620</td>
          <td>15</td>
          <td>569707381193162</td>
          <td>0.5060018015</td>
          </tr>
          <tr>
          <td>26</td>
          <td>4503599627370500</td>
          <td>15</td>
          <td>569707381193162</td>
          <td>0.1265004504</td>
          </tr>
          <tr>
          <td>27</td>
          <td>18014398509482000</td>
          <td>15</td>
          <td>569707381193162</td>
          <td>0.03162511259</td>
          </tr>
          <tr>
          <td>28</td>
          <td>72057594037927900</td>
          <td>15</td>
          <td>569707381193162</td>
          <td>0.007906278149</td>
          </tr>
          <tr>
          <td>29</td>
          <td>288230376151712000</td>
          <td>15</td>
          <td>569707381193162</td>
          <td>0.001976569537</td>
          </tr>
          </tbody>
          </table>
          <p>Hexagonal cells don't align perfectly on a vector tile.
          Some cells may intersect more than one vector tile.
          To compute the H3 resolution for each precision, Elasticsearch compares the average density of hexagonal bins at each resolution with the average density of tile bins at each zoom level.
          Elasticsearch uses the H3 resolution that is closest to the corresponding geotile density.</p>
          <p>Learn how to use the vector tile search API with practical examples in the <a href="https://www.elastic.co/docs/reference/elasticsearch/rest-apis/vector-tile-search">Vector tile search examples</a> guide.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-mvt>`_

        :param index: Comma-separated list of data streams, indices, or aliases to search
        :param field: Field containing geospatial data to return
        :param zoom: Zoom level for the vector tile to search
        :param x: X coordinate for the vector tile to search
        :param y: Y coordinate for the vector tile to search
        :param aggs: Sub-aggregations for the geotile_grid. It supports the following
            aggregation types: - `avg` - `boxplot` - `cardinality` - `extended stats`
            - `max` - `median absolute deviation` - `min` - `percentile` - `percentile-rank`
            - `stats` - `sum` - `value count` The aggregation names can't start with
            `_mvt_`. The `_mvt_` prefix is reserved for internal aggregations.
        :param buffer: The size, in pixels, of a clipping buffer outside the tile. This
            allows renderers to avoid outline artifacts from geometries that extend past
            the extent of the tile.
        :param exact_bounds: If `false`, the meta layer's feature is the bounding box
            of the tile. If `true`, the meta layer's feature is a bounding box resulting
            from a `geo_bounds` aggregation. The aggregation runs on <field> values that
            intersect the `<zoom>/<x>/<y>` tile with `wrap_longitude` set to `false`.
            The resulting bounding box may be larger than the vector tile.
        :param extent: The size, in pixels, of a side of the tile. Vector tiles are square
            with equal sides.
        :param fields: The fields to return in the `hits` layer. It supports wildcards
            (`*`). This parameter does not support fields with array values. Fields with
            array values may return inconsistent results.
        :param grid_agg: The aggregation used to create a grid for the `field`.
        :param grid_precision: Additional zoom levels available through the aggs layer.
            For example, if `<zoom>` is `7` and `grid_precision` is `8`, you can zoom
            in up to level 15. Accepts 0-8. If 0, results don't include the aggs layer.
        :param grid_type: Determines the geometry type for features in the aggs layer.
            In the aggs layer, each feature represents a `geotile_grid` cell. If `grid,
            each feature is a polygon of the cells bounding box. If `point`, each feature
            is a Point that is the centroid of the cell.
        :param query: The query DSL used to filter documents for the search.
        :param runtime_mappings: Defines one or more runtime fields in the search request.
            These fields take precedence over mapped fields with the same name.
        :param size: The maximum number of features to return in the hits layer. Accepts
            0-10000. If 0, results don't include the hits layer.
        :param sort: Sort the features in the hits layer. By default, the API calculates
            a bounding box for each feature. It sorts features based on this box's diagonal
            length, from longest to shortest.
        :param track_total_hits: The number of hits matching the query to count accurately.
            If `true`, the exact number of hits is returned at the cost of some performance.
            If `false`, the response does not include the total number of hits matching
            the query.
        :param with_labels: If `true`, the hits and aggs layers will contain additional
            point features representing suggested label positions for the original features.
            * `Point` and `MultiPoint` features will have one of the points selected.
            * `Polygon` and `MultiPolygon` features will have a single point generated,
            either the centroid, if it is within the polygon, or another point within
            the polygon selected from the sorted triangle-tree. * `LineString` features
            will likewise provide a roughly central point selected from the triangle-tree.
            * The aggregation results will provide one central point for each aggregation
            bucket. All attributes from the original features will also be copied to
            the new label features. In addition, the new features will be distinguishable
            using the tag `_mvt_label_position`.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if field in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'field'")
        if zoom in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'zoom'")
        if x in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'x'")
        if y in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'y'")
        __path_parts: t.Dict[str, str] = {
            "index": _quote(index),
            "field": _quote(field),
            "zoom": _quote(zoom),
            "x": _quote(x),
            "y": _quote(y),
        }
        __path = f'/{__path_parts["index"]}/_mvt/{__path_parts["field"]}/{__path_parts["zoom"]}/{__path_parts["x"]}/{__path_parts["y"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        # The 'sort' parameter with a colon can't be encoded to the body.
        if sort is not None and (
            (isinstance(sort, str) and ":" in sort)
            or (
                isinstance(sort, (list, tuple))
                and all(isinstance(_x, str) for _x in sort)
                and any(":" in _x for _x in sort)
            )
        ):
            __query["sort"] = sort
            sort = None
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if not __body:
            if aggs is not None:
                __body["aggs"] = aggs
            if buffer is not None:
                __body["buffer"] = buffer
            if exact_bounds is not None:
                __body["exact_bounds"] = exact_bounds
            if extent is not None:
                __body["extent"] = extent
            if fields is not None:
                __body["fields"] = fields
            if grid_agg is not None:
                __body["grid_agg"] = grid_agg
            if grid_precision is not None:
                __body["grid_precision"] = grid_precision
            if grid_type is not None:
                __body["grid_type"] = grid_type
            if query is not None:
                __body["query"] = query
            if runtime_mappings is not None:
                __body["runtime_mappings"] = runtime_mappings
            if size is not None:
                __body["size"] = size
            if sort is not None:
                __body["sort"] = sort
            if track_total_hits is not None:
                __body["track_total_hits"] = track_total_hits
            if with_labels is not None:
                __body["with_labels"] = with_labels
        if not __body:
            __body = None  # type: ignore[assignment]
        __headers = {"accept": "application/vnd.mapbox-vector-tile"}
        if __body is not None:
            __headers["content-type"] = "application/json"
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="search_mvt",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def search_shards(
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
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get the search shards.</p>
          <p>Get the indices and shards that a search request would be run against.
          This information can be useful for working out issues or planning optimizations with routing and shard preferences.
          When filtered aliases are used, the filter is returned as part of the <code>indices</code> section.</p>
          <p>If the Elasticsearch security features are enabled, you must have the <code>view_index_metadata</code> or <code>manage</code> index privilege for the target data stream, index, or alias.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-shards>`_

        :param index: A comma-separated list of data streams, indices, and aliases to
            search. It supports wildcards (`*`). To search all data streams and indices,
            omit this parameter or use `*` or `_all`.
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
        :param local: If `true`, the request retrieves information from the local node
            only.
        :param master_timeout: The period to wait for a connection to the master node.
            If the master node is not available before the timeout expires, the request
            fails and returns an error. IT can also be set to `-1` to indicate that the
            request should never timeout.
        :param preference: The node or shard the operation should be performed on. It
            is random by default.
        :param routing: A custom value used to route operations to a specific shard.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_search_shards'
        else:
            __path_parts = {}
            __path = "/_search_shards"
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
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if routing is not None:
            __query["routing"] = routing
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="search_shards",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("explain", "id", "params", "profile", "source"),
        ignore_deprecated_options={"params"},
    )
    def search_template(
        self,
        *,
        index: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        allow_no_indices: t.Optional[bool] = None,
        ccs_minimize_roundtrips: t.Optional[bool] = None,
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
        id: t.Optional[str] = None,
        ignore_throttled: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        params: t.Optional[t.Mapping[str, t.Any]] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        profile: t.Optional[bool] = None,
        rest_total_hits_as_int: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        scroll: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        search_type: t.Optional[
            t.Union[str, t.Literal["dfs_query_then_fetch", "query_then_fetch"]]
        ] = None,
        source: t.Optional[t.Union[str, t.Mapping[str, t.Any]]] = None,
        typed_keys: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Run a search with a search template.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-search-template>`_

        :param index: A comma-separated list of data streams, indices, and aliases to
            search. It supports wildcards (`*`).
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param ccs_minimize_roundtrips: If `true`, network round-trips are minimized
            for cross-cluster search requests.
        :param expand_wildcards: The type of index that wildcard patterns can match.
            If the request can target data streams, this argument determines whether
            wildcard expressions match hidden data streams. Supports comma-separated
            values, such as `open,hidden`.
        :param explain: If `true`, returns detailed information about score calculation
            as part of each hit. If you specify both this and the `explain` query parameter,
            the API uses only the query parameter.
        :param id: The ID of the search template to use. If no `source` is specified,
            this parameter is required.
        :param ignore_throttled: If `true`, specified concrete, expanded, or aliased
            indices are not included in the response when throttled.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param params: Key-value pairs used to replace Mustache variables in the template.
            The key is the variable name. The value is the variable value.
        :param preference: The node or shard the operation should be performed on. It
            is random by default.
        :param profile: If `true`, the query execution is profiled.
        :param rest_total_hits_as_int: If `true`, `hits.total` is rendered as an integer
            in the response. If `false`, it is rendered as an object.
        :param routing: A custom value used to route operations to a specific shard.
        :param scroll: Specifies how long a consistent view of the index should be maintained
            for scrolled search.
        :param search_type: The type of the search operation.
        :param source: An inline search template. Supports the same parameters as the
            search API's request body. It also supports Mustache variables. If no `id`
            is specified, this parameter is required.
        :param typed_keys: If `true`, the response prefixes aggregation and suggester
            names with their respective types.
        """
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_search/template'
        else:
            __path_parts = {}
            __path = "/_search/template"
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if allow_no_indices is not None:
            __query["allow_no_indices"] = allow_no_indices
        if ccs_minimize_roundtrips is not None:
            __query["ccs_minimize_roundtrips"] = ccs_minimize_roundtrips
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
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if rest_total_hits_as_int is not None:
            __query["rest_total_hits_as_int"] = rest_total_hits_as_int
        if routing is not None:
            __query["routing"] = routing
        if scroll is not None:
            __query["scroll"] = scroll
        if search_type is not None:
            __query["search_type"] = search_type
        if typed_keys is not None:
            __query["typed_keys"] = typed_keys
        if not __body:
            if explain is not None:
                __body["explain"] = explain
            if id is not None:
                __body["id"] = id
            if params is not None:
                __body["params"] = params
            if profile is not None:
                __body["profile"] = profile
            if source is not None:
                __body["source"] = source
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="search_template",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "field",
            "case_insensitive",
            "index_filter",
            "search_after",
            "size",
            "string",
            "timeout",
        ),
    )
    def terms_enum(
        self,
        *,
        index: str,
        field: t.Optional[str] = None,
        case_insensitive: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        index_filter: t.Optional[t.Mapping[str, t.Any]] = None,
        pretty: t.Optional[bool] = None,
        search_after: t.Optional[str] = None,
        size: t.Optional[int] = None,
        string: t.Optional[str] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get terms in an index.</p>
          <p>Discover terms that match a partial string in an index.
          This API is designed for low-latency look-ups used in auto-complete scenarios.</p>
          <blockquote>
          <p>info
          The terms enum API may return terms from deleted documents. Deleted documents are initially only marked as deleted. It is not until their segments are merged that documents are actually deleted. Until that happens, the terms enum API will return terms from these documents.</p>
          </blockquote>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-terms-enum>`_

        :param index: A comma-separated list of data streams, indices, and index aliases
            to search. Wildcard (`*`) expressions are supported. To search all data streams
            or indices, omit this parameter or use `*` or `_all`.
        :param field: The string to match at the start of indexed terms. If not provided,
            all terms in the field are considered.
        :param case_insensitive: When `true`, the provided search string is matched against
            index terms without case sensitivity.
        :param index_filter: Filter an index shard if the provided query rewrites to
            `match_none`.
        :param search_after: The string after which terms in the index should be returned.
            It allows for a form of pagination if the last result from one request is
            passed as the `search_after` parameter for a subsequent request.
        :param size: The number of matching terms to return.
        :param string: The string to match at the start of indexed terms. If it is not
            provided, all terms in the field are considered. > info > The prefix string
            cannot be larger than the largest possible keyword value, which is Lucene's
            term byte-length limit of 32766.
        :param timeout: The maximum length of time to spend collecting results. If the
            timeout is exceeded the `complete` flag set to `false` in the response and
            the results may be partial or empty.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if field is None and body is None:
            raise ValueError("Empty value passed for parameter 'field'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_terms_enum'
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
            if field is not None:
                __body["field"] = field
            if case_insensitive is not None:
                __body["case_insensitive"] = case_insensitive
            if index_filter is not None:
                __body["index_filter"] = index_filter
            if search_after is not None:
                __body["search_after"] = search_after
            if size is not None:
                __body["size"] = size
            if string is not None:
                __body["string"] = string
            if timeout is not None:
                __body["timeout"] = timeout
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
            endpoint_id="terms_enum",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "doc",
            "field_statistics",
            "fields",
            "filter",
            "offsets",
            "payloads",
            "per_field_analyzer",
            "positions",
            "routing",
            "term_statistics",
            "version",
            "version_type",
        ),
    )
    def termvectors(
        self,
        *,
        index: str,
        id: t.Optional[str] = None,
        doc: t.Optional[t.Mapping[str, t.Any]] = None,
        error_trace: t.Optional[bool] = None,
        field_statistics: t.Optional[bool] = None,
        fields: t.Optional[t.Sequence[str]] = None,
        filter: t.Optional[t.Mapping[str, t.Any]] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        offsets: t.Optional[bool] = None,
        payloads: t.Optional[bool] = None,
        per_field_analyzer: t.Optional[t.Mapping[str, str]] = None,
        positions: t.Optional[bool] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        realtime: t.Optional[bool] = None,
        routing: t.Optional[str] = None,
        term_statistics: t.Optional[bool] = None,
        version: t.Optional[int] = None,
        version_type: t.Optional[
            t.Union[str, t.Literal["external", "external_gte", "force", "internal"]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Get term vector information.</p>
          <p>Get information and statistics about terms in the fields of a particular document.</p>
          <p>You can retrieve term vectors for documents stored in the index or for artificial documents passed in the body of the request.
          You can specify the fields you are interested in through the <code>fields</code> parameter or by adding the fields to the request body.
          For example:</p>
          <pre><code>GET /my-index-000001/_termvectors/1?fields=message
          </code></pre>
          <p>Fields can be specified using wildcards, similar to the multi match query.</p>
          <p>Term vectors are real-time by default, not near real-time.
          This can be changed by setting <code>realtime</code> parameter to <code>false</code>.</p>
          <p>You can request three types of values: <em>term information</em>, <em>term statistics</em>, and <em>field statistics</em>.
          By default, all term information and field statistics are returned for all fields but term statistics are excluded.</p>
          <p><strong>Term information</strong></p>
          <ul>
          <li>term frequency in the field (always returned)</li>
          <li>term positions (<code>positions: true</code>)</li>
          <li>start and end offsets (<code>offsets: true</code>)</li>
          <li>term payloads (<code>payloads: true</code>), as base64 encoded bytes</li>
          </ul>
          <p>If the requested information wasn't stored in the index, it will be computed on the fly if possible.
          Additionally, term vectors could be computed for documents not even existing in the index, but instead provided by the user.</p>
          <blockquote>
          <p>warn
          Start and end offsets assume UTF-16 encoding is being used. If you want to use these offsets in order to get the original text that produced this token, you should make sure that the string you are taking a sub-string of is also encoded using UTF-16.</p>
          </blockquote>
          <p><strong>Behaviour</strong></p>
          <p>The term and field statistics are not accurate.
          Deleted documents are not taken into account.
          The information is only retrieved for the shard the requested document resides in.
          The term and field statistics are therefore only useful as relative measures whereas the absolute numbers have no meaning in this context.
          By default, when requesting term vectors of artificial documents, a shard to get the statistics from is randomly selected.
          Use <code>routing</code> only to hit a particular shard.
          Refer to the linked documentation for detailed examples of how to use this API.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-termvectors>`_

        :param index: The name of the index that contains the document.
        :param id: A unique identifier for the document.
        :param doc: An artificial document (a document not present in the index) for
            which you want to retrieve term vectors.
        :param field_statistics: If `true`, the response includes: * The document count
            (how many documents contain this field). * The sum of document frequencies
            (the sum of document frequencies for all terms in this field). * The sum
            of total term frequencies (the sum of total term frequencies of each term
            in this field).
        :param fields: A list of fields to include in the statistics. It is used as the
            default list unless a specific field list is provided in the `completion_fields`
            or `fielddata_fields` parameters.
        :param filter: Filter terms based on their tf-idf scores. This could be useful
            in order find out a good characteristic vector of a document. This feature
            works in a similar manner to the second phase of the More Like This Query.
        :param offsets: If `true`, the response includes term offsets.
        :param payloads: If `true`, the response includes term payloads.
        :param per_field_analyzer: Override the default per-field analyzer. This is useful
            in order to generate term vectors in any fashion, especially when using artificial
            documents. When providing an analyzer for a field that already stores term
            vectors, the term vectors will be regenerated.
        :param positions: If `true`, the response includes term positions.
        :param preference: The node or shard the operation should be performed on. It
            is random by default.
        :param realtime: If true, the request is real-time as opposed to near-real-time.
        :param routing: A custom value that is used to route operations to a specific
            shard.
        :param term_statistics: If `true`, the response includes: * The total term frequency
            (how often a term occurs in all documents). * The document frequency (the
            number of documents containing the current term). By default these values
            are not returned since term statistics can have a serious performance impact.
        :param version: If `true`, returns the document version as part of a hit.
        :param version_type: The version type.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str]
        if index not in SKIP_IN_PATH and id not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index), "id": _quote(id)}
            __path = f'/{__path_parts["index"]}/_termvectors/{__path_parts["id"]}'
        elif index not in SKIP_IN_PATH:
            __path_parts = {"index": _quote(index)}
            __path = f'/{__path_parts["index"]}/_termvectors'
        else:
            raise ValueError("Couldn't find a path for the given parameters")
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if realtime is not None:
            __query["realtime"] = realtime
        if not __body:
            if doc is not None:
                __body["doc"] = doc
            if field_statistics is not None:
                __body["field_statistics"] = field_statistics
            if fields is not None:
                __body["fields"] = fields
            if filter is not None:
                __body["filter"] = filter
            if offsets is not None:
                __body["offsets"] = offsets
            if payloads is not None:
                __body["payloads"] = payloads
            if per_field_analyzer is not None:
                __body["per_field_analyzer"] = per_field_analyzer
            if positions is not None:
                __body["positions"] = positions
            if routing is not None:
                __body["routing"] = routing
            if term_statistics is not None:
                __body["term_statistics"] = term_statistics
            if version is not None:
                __body["version"] = version
            if version_type is not None:
                __body["version_type"] = version_type
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
            endpoint_id="termvectors",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=(
            "detect_noop",
            "doc",
            "doc_as_upsert",
            "script",
            "scripted_upsert",
            "source",
            "upsert",
        ),
        parameter_aliases={
            "_source": "source",
            "_source_excludes": "source_excludes",
            "_source_includes": "source_includes",
        },
    )
    def update(
        self,
        *,
        index: str,
        id: str,
        detect_noop: t.Optional[bool] = None,
        doc: t.Optional[t.Mapping[str, t.Any]] = None,
        doc_as_upsert: t.Optional[bool] = None,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        if_primary_term: t.Optional[int] = None,
        if_seq_no: t.Optional[int] = None,
        include_source_on_error: t.Optional[bool] = None,
        lang: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        refresh: t.Optional[
            t.Union[bool, str, t.Literal["false", "true", "wait_for"]]
        ] = None,
        require_alias: t.Optional[bool] = None,
        retry_on_conflict: t.Optional[int] = None,
        routing: t.Optional[str] = None,
        script: t.Optional[t.Mapping[str, t.Any]] = None,
        scripted_upsert: t.Optional[bool] = None,
        source: t.Optional[t.Union[bool, t.Mapping[str, t.Any]]] = None,
        source_excludes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        source_includes: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        upsert: t.Optional[t.Mapping[str, t.Any]] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update a document.</p>
          <p>Update a document by running a script or passing a partial document.</p>
          <p>If the Elasticsearch security features are enabled, you must have the <code>index</code> or <code>write</code> index privilege for the target index or index alias.</p>
          <p>The script can update, delete, or skip modifying the document.
          The API also supports passing a partial document, which is merged into the existing document.
          To fully replace an existing document, use the index API.
          This operation:</p>
          <ul>
          <li>Gets the document (collocated with the shard) from the index.</li>
          <li>Runs the specified script.</li>
          <li>Indexes the result.</li>
          </ul>
          <p>The document must still be reindexed, but using this API removes some network roundtrips and reduces chances of version conflicts between the GET and the index operation.</p>
          <p>The <code>_source</code> field must be enabled to use this API.
          In addition to <code>_source</code>, you can access the following variables through the <code>ctx</code> map: <code>_index</code>, <code>_type</code>, <code>_id</code>, <code>_version</code>, <code>_routing</code>, and <code>_now</code> (the current timestamp).
          For usage examples such as partial updates, upserts, and scripted updates, see the External documentation.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update>`_

        :param index: The name of the target index. By default, the index is created
            automatically if it doesn't exist.
        :param id: A unique identifier for the document to be updated.
        :param detect_noop: If `true`, the `result` in the response is set to `noop`
            (no operation) when there are no changes to the document.
        :param doc: A partial update to an existing document. If both `doc` and `script`
            are specified, `doc` is ignored.
        :param doc_as_upsert: If `true`, use the contents of 'doc' as the value of 'upsert'.
            NOTE: Using ingest pipelines with `doc_as_upsert` is not supported.
        :param if_primary_term: Only perform the operation if the document has this primary
            term.
        :param if_seq_no: Only perform the operation if the document has this sequence
            number.
        :param include_source_on_error: True or false if to include the document source
            in the error message in case of parsing errors.
        :param lang: The script language.
        :param refresh: If 'true', Elasticsearch refreshes the affected shards to make
            this operation visible to search. If 'wait_for', it waits for a refresh to
            make this operation visible to search. If 'false', it does nothing with refreshes.
        :param require_alias: If `true`, the destination must be an index alias.
        :param retry_on_conflict: The number of times the operation should be retried
            when a conflict occurs.
        :param routing: A custom value used to route operations to a specific shard.
        :param script: The script to run to update the document.
        :param scripted_upsert: If `true`, run the script whether or not the document
            exists.
        :param source: If `false`, turn off source retrieval. You can also specify a
            comma-separated list of the fields you want to retrieve.
        :param source_excludes: The source fields you want to exclude.
        :param source_includes: The source fields you want to retrieve.
        :param timeout: The period to wait for the following operations: dynamic mapping
            updates and waiting for active shards. Elasticsearch waits for at least the
            timeout period before failing. The actual wait time could be longer, particularly
            when multiple waits occur.
        :param upsert: If the document does not already exist, the contents of 'upsert'
            are inserted as a new document. If the document exists, the 'script' is run.
        :param wait_for_active_shards: The number of copies of each shard that must be
            active before proceeding with the operation. Set to 'all' or any positive
            integer up to the total number of shards in the index (`number_of_replicas`+1).
            The default value of `1` means it waits for each primary shard to be active.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        if id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'id'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index), "id": _quote(id)}
        __path = f'/{__path_parts["index"]}/_update/{__path_parts["id"]}'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if if_primary_term is not None:
            __query["if_primary_term"] = if_primary_term
        if if_seq_no is not None:
            __query["if_seq_no"] = if_seq_no
        if include_source_on_error is not None:
            __query["include_source_on_error"] = include_source_on_error
        if lang is not None:
            __query["lang"] = lang
        if pretty is not None:
            __query["pretty"] = pretty
        if refresh is not None:
            __query["refresh"] = refresh
        if require_alias is not None:
            __query["require_alias"] = require_alias
        if retry_on_conflict is not None:
            __query["retry_on_conflict"] = retry_on_conflict
        if routing is not None:
            __query["routing"] = routing
        if source_excludes is not None:
            __query["_source_excludes"] = source_excludes
        if source_includes is not None:
            __query["_source_includes"] = source_includes
        if timeout is not None:
            __query["timeout"] = timeout
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        if not __body:
            if detect_noop is not None:
                __body["detect_noop"] = detect_noop
            if doc is not None:
                __body["doc"] = doc
            if doc_as_upsert is not None:
                __body["doc_as_upsert"] = doc_as_upsert
            if script is not None:
                __body["script"] = script
            if scripted_upsert is not None:
                __body["scripted_upsert"] = scripted_upsert
            if source is not None:
                __body["_source"] = source
            if upsert is not None:
                __body["upsert"] = upsert
        __headers = {"accept": "application/json", "content-type": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            body=__body,
            endpoint_id="update",
            path_parts=__path_parts,
        )

    @_rewrite_parameters(
        body_fields=("conflicts", "max_docs", "query", "script", "slice"),
        parameter_aliases={"from": "from_"},
    )
    def update_by_query(
        self,
        *,
        index: t.Union[str, t.Sequence[str]],
        allow_no_indices: t.Optional[bool] = None,
        analyze_wildcard: t.Optional[bool] = None,
        analyzer: t.Optional[str] = None,
        conflicts: t.Optional[t.Union[str, t.Literal["abort", "proceed"]]] = None,
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
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        from_: t.Optional[int] = None,
        human: t.Optional[bool] = None,
        ignore_unavailable: t.Optional[bool] = None,
        lenient: t.Optional[bool] = None,
        max_docs: t.Optional[int] = None,
        pipeline: t.Optional[str] = None,
        preference: t.Optional[str] = None,
        pretty: t.Optional[bool] = None,
        q: t.Optional[str] = None,
        query: t.Optional[t.Mapping[str, t.Any]] = None,
        refresh: t.Optional[bool] = None,
        request_cache: t.Optional[bool] = None,
        requests_per_second: t.Optional[float] = None,
        routing: t.Optional[str] = None,
        script: t.Optional[t.Mapping[str, t.Any]] = None,
        scroll: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        scroll_size: t.Optional[int] = None,
        search_timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        search_type: t.Optional[
            t.Union[str, t.Literal["dfs_query_then_fetch", "query_then_fetch"]]
        ] = None,
        slice: t.Optional[t.Mapping[str, t.Any]] = None,
        slices: t.Optional[t.Union[int, t.Union[str, t.Literal["auto"]]]] = None,
        sort: t.Optional[t.Sequence[str]] = None,
        stats: t.Optional[t.Sequence[str]] = None,
        terminate_after: t.Optional[int] = None,
        timeout: t.Optional[t.Union[str, t.Literal[-1], t.Literal[0]]] = None,
        version: t.Optional[bool] = None,
        version_type: t.Optional[bool] = None,
        wait_for_active_shards: t.Optional[
            t.Union[int, t.Union[str, t.Literal["all", "index-setting"]]]
        ] = None,
        wait_for_completion: t.Optional[bool] = None,
        body: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Update documents.
          Updates documents that match the specified query.
          If no query is specified, performs an update on every document in the data stream or index without modifying the source, which is useful for picking up mapping changes.</p>
          <p>If the Elasticsearch security features are enabled, you must have the following index privileges for the target data stream, index, or alias:</p>
          <ul>
          <li><code>read</code></li>
          <li><code>index</code> or <code>write</code></li>
          </ul>
          <p>You can specify the query criteria in the request URI or the request body using the same syntax as the search API.</p>
          <p>When you submit an update by query request, Elasticsearch gets a snapshot of the data stream or index when it begins processing the request and updates matching documents using internal versioning.
          When the versions match, the document is updated and the version number is incremented.
          If a document changes between the time that the snapshot is taken and the update operation is processed, it results in a version conflict and the operation fails.
          You can opt to count version conflicts instead of halting and returning by setting <code>conflicts</code> to <code>proceed</code>.
          Note that if you opt to count version conflicts, the operation could attempt to update more documents from the source than <code>max_docs</code> until it has successfully updated <code>max_docs</code> documents or it has gone through every document in the source query.</p>
          <p>NOTE: Documents with a version equal to 0 cannot be updated using update by query because internal versioning does not support 0 as a valid version number.</p>
          <p>While processing an update by query request, Elasticsearch performs multiple search requests sequentially to find all of the matching documents.
          A bulk update request is performed for each batch of matching documents.
          Any query or update failures cause the update by query request to fail and the failures are shown in the response.
          Any update requests that completed successfully still stick, they are not rolled back.</p>
          <p><strong>Refreshing shards</strong></p>
          <p>Specifying the <code>refresh</code> parameter refreshes all shards once the request completes.
          This is different to the update API's <code>refresh</code> parameter, which causes only the shard
          that received the request to be refreshed. Unlike the update API, it does not support
          <code>wait_for</code>.</p>
          <p><strong>Running update by query asynchronously</strong></p>
          <p>If the request contains <code>wait_for_completion=false</code>, Elasticsearch
          performs some preflight checks, launches the request, and returns a
          <a href="https://www.elastic.co/docs/api/doc/elasticsearch/group/endpoint-tasks">task</a> you can use to cancel or get the status of the task.
          Elasticsearch creates a record of this task as a document at <code>.tasks/task/${taskId}</code>.</p>
          <p><strong>Waiting for active shards</strong></p>
          <p><code>wait_for_active_shards</code> controls how many copies of a shard must be active
          before proceeding with the request. See <a href="https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-create#operation-create-wait_for_active_shards"><code>wait_for_active_shards</code></a>
          for details. <code>timeout</code> controls how long each write request waits for unavailable
          shards to become available. Both work exactly the way they work in the
          <a href="https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-bulk">Bulk API</a>. Update by query uses scrolled searches, so you can also
          specify the <code>scroll</code> parameter to control how long it keeps the search context
          alive, for example <code>?scroll=10m</code>. The default is 5 minutes.</p>
          <p><strong>Throttling update requests</strong></p>
          <p>To control the rate at which update by query issues batches of update operations, you can set <code>requests_per_second</code> to any positive decimal number.
          This pads each batch with a wait time to throttle the rate.
          Set <code>requests_per_second</code> to <code>-1</code> to turn off throttling.</p>
          <p>Throttling uses a wait time between batches so that the internal scroll requests can be given a timeout that takes the request padding into account.
          The padding time is the difference between the batch size divided by the <code>requests_per_second</code> and the time spent writing.
          By default the batch size is 1000, so if <code>requests_per_second</code> is set to <code>500</code>:</p>
          <pre><code>target_time = 1000 / 500 per second = 2 seconds
          wait_time = target_time - write_time = 2 seconds - .5 seconds = 1.5 seconds
          </code></pre>
          <p>Since the batch is issued as a single _bulk request, large batch sizes cause Elasticsearch to create many requests and wait before starting the next set.
          This is &quot;bursty&quot; instead of &quot;smooth&quot;.</p>
          <p><strong>Slicing</strong></p>
          <p>Update by query supports sliced scroll to parallelize the update process.
          This can improve efficiency and provide a convenient way to break the request down into smaller parts.</p>
          <p>Setting <code>slices</code> to <code>auto</code> chooses a reasonable number for most data streams and indices.
          This setting will use one slice per shard, up to a certain limit.
          If there are multiple source data streams or indices, it will choose the number of slices based on the index or backing index with the smallest number of shards.</p>
          <p>Adding <code>slices</code> to <code>_update_by_query</code> just automates the manual process of creating sub-requests, which means it has some quirks:</p>
          <ul>
          <li>You can see these requests in the tasks APIs. These sub-requests are &quot;child&quot; tasks of the task for the request with slices.</li>
          <li>Fetching the status of the task for the request with <code>slices</code> only contains the status of completed slices.</li>
          <li>These sub-requests are individually addressable for things like cancellation and rethrottling.</li>
          <li>Rethrottling the request with <code>slices</code> will rethrottle the unfinished sub-request proportionally.</li>
          <li>Canceling the request with slices will cancel each sub-request.</li>
          <li>Due to the nature of slices each sub-request won't get a perfectly even portion of the documents. All documents will be addressed, but some slices may be larger than others. Expect larger slices to have a more even distribution.</li>
          <li>Parameters like <code>requests_per_second</code> and <code>max_docs</code> on a request with slices are distributed proportionally to each sub-request. Combine that with the point above about distribution being uneven and you should conclude that using <code>max_docs</code> with <code>slices</code> might not result in exactly <code>max_docs</code> documents being updated.</li>
          <li>Each sub-request gets a slightly different snapshot of the source data stream or index though these are all taken at approximately the same time.</li>
          </ul>
          <p>If you're slicing manually or otherwise tuning automatic slicing, keep in mind that:</p>
          <ul>
          <li>Query performance is most efficient when the number of slices is equal to the number of shards in the index or backing index. If that number is large (for example, 500), choose a lower number as too many slices hurts performance. Setting slices higher than the number of shards generally does not improve efficiency and adds overhead.</li>
          <li>Update performance scales linearly across available resources with the number of slices.</li>
          </ul>
          <p>Whether query or update performance dominates the runtime depends on the documents being reindexed and cluster resources.
          Refer to the linked documentation for examples of how to update documents using the <code>_update_by_query</code> API:</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query>`_

        :param index: A comma-separated list of data streams, indices, and aliases to
            search. It supports wildcards (`*`). To search all data streams or indices,
            omit this parameter or use `*` or `_all`.
        :param allow_no_indices: If `false`, the request returns an error if any wildcard
            expression, index alias, or `_all` value targets only missing or closed indices.
            This behavior applies even if the request targets other open indices. For
            example, a request targeting `foo*,bar*` returns an error if an index starts
            with `foo` but no index starts with `bar`.
        :param analyze_wildcard: If `true`, wildcard and prefix queries are analyzed.
            This parameter can be used only when the `q` query string parameter is specified.
        :param analyzer: The analyzer to use for the query string. This parameter can
            be used only when the `q` query string parameter is specified.
        :param conflicts: The preferred behavior when update by query hits version conflicts:
            `abort` or `proceed`.
        :param default_operator: The default operator for query string query: `AND` or
            `OR`. This parameter can be used only when the `q` query string parameter
            is specified.
        :param df: The field to use as default where no field prefix is given in the
            query string. This parameter can be used only when the `q` query string parameter
            is specified.
        :param expand_wildcards: The type of index that wildcard patterns can match.
            If the request can target data streams, this argument determines whether
            wildcard expressions match hidden data streams. It supports comma-separated
            values, such as `open,hidden`.
        :param from_: Skips the specified number of documents.
        :param ignore_unavailable: If `false`, the request returns an error if it targets
            a missing or closed index.
        :param lenient: If `true`, format-based query failures (such as providing text
            to a numeric field) in the query string will be ignored. This parameter can
            be used only when the `q` query string parameter is specified.
        :param max_docs: The maximum number of documents to update.
        :param pipeline: The ID of the pipeline to use to preprocess incoming documents.
            If the index has a default ingest pipeline specified, then setting the value
            to `_none` disables the default ingest pipeline for this request. If a final
            pipeline is configured it will always run, regardless of the value of this
            parameter.
        :param preference: The node or shard the operation should be performed on. It
            is random by default.
        :param q: A query in the Lucene query string syntax.
        :param query: The documents to update using the Query DSL.
        :param refresh: If `true`, Elasticsearch refreshes affected shards to make the
            operation visible to search after the request completes. This is different
            than the update API's `refresh` parameter, which causes just the shard that
            received the request to be refreshed.
        :param request_cache: If `true`, the request cache is used for this request.
            It defaults to the index-level setting.
        :param requests_per_second: The throttle for this request in sub-requests per
            second.
        :param routing: A custom value used to route operations to a specific shard.
        :param script: The script to run to update the document source or metadata when
            updating.
        :param scroll: The period to retain the search context for scrolling.
        :param scroll_size: The size of the scroll request that powers the operation.
        :param search_timeout: An explicit timeout for each search request. By default,
            there is no timeout.
        :param search_type: The type of the search operation. Available options include
            `query_then_fetch` and `dfs_query_then_fetch`.
        :param slice: Slice the request manually using the provided slice ID and total
            number of slices.
        :param slices: The number of slices this task should be divided into.
        :param sort: A comma-separated list of <field>:<direction> pairs.
        :param stats: The specific `tag` of the request for logging and statistical purposes.
        :param terminate_after: The maximum number of documents to collect for each shard.
            If a query reaches this limit, Elasticsearch terminates the query early.
            Elasticsearch collects documents before sorting. IMPORTANT: Use with caution.
            Elasticsearch applies this parameter to each shard handling the request.
            When possible, let Elasticsearch perform early termination automatically.
            Avoid specifying this parameter for requests that target data streams with
            backing indices across multiple data tiers.
        :param timeout: The period each update request waits for the following operations:
            dynamic mapping updates, waiting for active shards. By default, it is one
            minute. This guarantees Elasticsearch waits for at least the timeout before
            failing. The actual wait time could be longer, particularly when multiple
            waits occur.
        :param version: If `true`, returns the document version as part of a hit.
        :param version_type: Should the document increment the version number (internal)
            on hit or not (reindex)
        :param wait_for_active_shards: The number of shard copies that must be active
            before proceeding with the operation. Set to `all` or any positive integer
            up to the total number of shards in the index (`number_of_replicas+1`). The
            `timeout` parameter controls how long each write request waits for unavailable
            shards to become available. Both work exactly the way they work in the bulk
            API.
        :param wait_for_completion: If `true`, the request blocks until the operation
            is complete. If `false`, Elasticsearch performs some preflight checks, launches
            the request, and returns a task ID that you can use to cancel or get the
            status of the task. Elasticsearch creates a record of this task as a document
            at `.tasks/task/${taskId}`.
        """
        if index in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'index'")
        __path_parts: t.Dict[str, str] = {"index": _quote(index)}
        __path = f'/{__path_parts["index"]}/_update_by_query'
        __query: t.Dict[str, t.Any] = {}
        __body: t.Dict[str, t.Any] = body if body is not None else {}
        # The 'sort' parameter with a colon can't be encoded to the body.
        if sort is not None and (
            (isinstance(sort, str) and ":" in sort)
            or (
                isinstance(sort, (list, tuple))
                and all(isinstance(_x, str) for _x in sort)
                and any(":" in _x for _x in sort)
            )
        ):
            __query["sort"] = sort
            sort = None
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
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if from_ is not None:
            __query["from"] = from_
        if human is not None:
            __query["human"] = human
        if ignore_unavailable is not None:
            __query["ignore_unavailable"] = ignore_unavailable
        if lenient is not None:
            __query["lenient"] = lenient
        if pipeline is not None:
            __query["pipeline"] = pipeline
        if preference is not None:
            __query["preference"] = preference
        if pretty is not None:
            __query["pretty"] = pretty
        if q is not None:
            __query["q"] = q
        if refresh is not None:
            __query["refresh"] = refresh
        if request_cache is not None:
            __query["request_cache"] = request_cache
        if requests_per_second is not None:
            __query["requests_per_second"] = requests_per_second
        if routing is not None:
            __query["routing"] = routing
        if scroll is not None:
            __query["scroll"] = scroll
        if scroll_size is not None:
            __query["scroll_size"] = scroll_size
        if search_timeout is not None:
            __query["search_timeout"] = search_timeout
        if search_type is not None:
            __query["search_type"] = search_type
        if slices is not None:
            __query["slices"] = slices
        if sort is not None:
            __query["sort"] = sort
        if stats is not None:
            __query["stats"] = stats
        if terminate_after is not None:
            __query["terminate_after"] = terminate_after
        if timeout is not None:
            __query["timeout"] = timeout
        if version is not None:
            __query["version"] = version
        if version_type is not None:
            __query["version_type"] = version_type
        if wait_for_active_shards is not None:
            __query["wait_for_active_shards"] = wait_for_active_shards
        if wait_for_completion is not None:
            __query["wait_for_completion"] = wait_for_completion
        if not __body:
            if conflicts is not None:
                __body["conflicts"] = conflicts
            if max_docs is not None:
                __body["max_docs"] = max_docs
            if query is not None:
                __body["query"] = query
            if script is not None:
                __body["script"] = script
            if slice is not None:
                __body["slice"] = slice
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
            endpoint_id="update_by_query",
            path_parts=__path_parts,
        )

    @_rewrite_parameters()
    def update_by_query_rethrottle(
        self,
        *,
        task_id: str,
        error_trace: t.Optional[bool] = None,
        filter_path: t.Optional[t.Union[str, t.Sequence[str]]] = None,
        human: t.Optional[bool] = None,
        pretty: t.Optional[bool] = None,
        requests_per_second: t.Optional[float] = None,
    ) -> ObjectApiResponse[t.Any]:
        """
        .. raw:: html

          <p>Throttle an update by query operation.</p>
          <p>Change the number of requests per second for a particular update by query operation.
          Rethrottling that speeds up the query takes effect immediately but rethrotting that slows down the query takes effect after completing the current batch to prevent scroll timeouts.</p>


        `<https://www.elastic.co/docs/api/doc/elasticsearch/operation/operation-update-by-query-rethrottle>`_

        :param task_id: The ID for the task.
        :param requests_per_second: The throttle for this request in sub-requests per
            second. To turn off throttling, set it to `-1`.
        """
        if task_id in SKIP_IN_PATH:
            raise ValueError("Empty value passed for parameter 'task_id'")
        __path_parts: t.Dict[str, str] = {"task_id": _quote(task_id)}
        __path = f'/_update_by_query/{__path_parts["task_id"]}/_rethrottle'
        __query: t.Dict[str, t.Any] = {}
        if error_trace is not None:
            __query["error_trace"] = error_trace
        if filter_path is not None:
            __query["filter_path"] = filter_path
        if human is not None:
            __query["human"] = human
        if pretty is not None:
            __query["pretty"] = pretty
        if requests_per_second is not None:
            __query["requests_per_second"] = requests_per_second
        __headers = {"accept": "application/json"}
        return self.perform_request(  # type: ignore[return-value]
            "POST",
            __path,
            params=__query,
            headers=__headers,
            endpoint_id="update_by_query_rethrottle",
            path_parts=__path_parts,
        )
