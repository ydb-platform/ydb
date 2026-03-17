import asyncio
import contextlib
import logging
import random
import time
from enum import IntEnum

import aiokafka.errors as Errors
from aiokafka import __version__
from aiokafka.cluster import ClusterMetadata
from aiokafka.conn import CloseReason, collect_hosts, create_conn
from aiokafka.errors import (
    KafkaConnectionError,
    KafkaError,
    NodeNotReadyError,
    RequestTimedOutError,
    StaleMetadata,
    UnknownTopicOrPartitionError,
)
from aiokafka.protocol.coordination import FindCoordinatorRequest
from aiokafka.protocol.metadata import MetadataRequest
from aiokafka.protocol.produce import ProduceRequest
from aiokafka.util import (
    create_future,
    create_task,
    get_running_loop,
)

__all__ = ["AIOKafkaClient"]


log = logging.getLogger("aiokafka")


class ConnectionGroup(IntEnum):
    DEFAULT = 0
    COORDINATION = 1


class CoordinationType(IntEnum):
    GROUP = 0
    TRANSACTION = 1


class AIOKafkaClient:
    """Initialize an asynchronous kafka client

    Keyword Arguments:
        bootstrap_servers: 'host[:port]' string (or list of 'host[:port]'
            strings) that the consumer should contact to bootstrap initial
            cluster metadata. This does not have to be the full node list.
            It just needs to have at least one broker that will respond to
            Metadata API Request. Default port is 9092. If no servers are
            specified, will default to localhost:9092.
        client_id (str): a name for this client. This string is passed in
            each request to servers and can be used to identify specific
            server-side log entries that correspond to this client. Also
            submitted to GroupCoordinator for logging with respect to
            consumer group administration. Default: 'aiokafka-{ver}'
        request_timeout_ms (int): Client request timeout in milliseconds.
            Default: 40000.
        metadata_max_age_ms (int): The period of time in milliseconds after
            which we force a refresh of metadata even if we haven't seen
            any partition leadership changes to proactively discover any
            new brokers or partitions. Default: 300000
        retry_backoff_ms (int): Milliseconds to backoff when retrying on
            errors. Default: 100.
        security_protocol (str): Protocol used to communicate with brokers.
            Valid values are: PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL.
            Default: PLAINTEXT.
        ssl_context (ssl.SSLContext): pre-configured SSLContext for wrapping
            socket connections. For more information see :ref:`ssl_auth`.
            Default: None.
        connections_max_idle_ms (int): Close idle connections after the number
            of milliseconds specified by this config. Specifying `None` will
            disable idle checks. Default: 540000 (9 minutes).
    """

    def __init__(
        self,
        *,
        loop=None,
        bootstrap_servers="localhost",
        client_id="aiokafka-" + __version__,
        metadata_max_age_ms=300000,
        request_timeout_ms=40000,
        retry_backoff_ms=100,
        ssl_context=None,
        security_protocol="PLAINTEXT",
        connections_max_idle_ms=540000,
        sasl_mechanism="PLAIN",
        sasl_plain_username=None,
        sasl_plain_password=None,
        sasl_kerberos_service_name="kafka",
        sasl_kerberos_domain_name=None,
        sasl_oauth_token_provider=None,
    ):
        if loop is None:
            loop = get_running_loop()

        if security_protocol not in ("SSL", "PLAINTEXT", "SASL_PLAINTEXT", "SASL_SSL"):
            raise ValueError("`security_protocol` should be SSL or PLAINTEXT")
        if security_protocol in ["SSL", "SASL_SSL"] and ssl_context is None:
            raise ValueError("`ssl_context` is mandatory if security_protocol=='SSL'")
        if security_protocol in ["SASL_SSL", "SASL_PLAINTEXT"]:
            if sasl_mechanism not in (
                "PLAIN",
                "GSSAPI",
                "SCRAM-SHA-256",
                "SCRAM-SHA-512",
                "OAUTHBEARER",
            ):
                raise ValueError(
                    "only `PLAIN`, `GSSAPI`, `SCRAM-SHA-256`, "
                    "`SCRAM-SHA-512` and `OAUTHBEARER`"
                    "sasl_mechanism are supported "
                    "at the moment"
                )
            if sasl_mechanism == "PLAIN" and (
                sasl_plain_username is None or sasl_plain_password is None
            ):
                raise ValueError(
                    "sasl_plain_username and sasl_plain_password required for "
                    "PLAIN sasl"
                )

        self._bootstrap_servers = bootstrap_servers
        self._client_id = client_id
        self._metadata_max_age_ms = metadata_max_age_ms
        self._request_timeout_ms = request_timeout_ms
        self._security_protocol = security_protocol
        self._ssl_context = ssl_context
        self._retry_backoff = retry_backoff_ms / 1000
        self._connections_max_idle_ms = connections_max_idle_ms
        self._sasl_mechanism = sasl_mechanism
        self._sasl_plain_username = sasl_plain_username
        self._sasl_plain_password = sasl_plain_password
        self._sasl_kerberos_service_name = sasl_kerberos_service_name
        self._sasl_kerberos_domain_name = sasl_kerberos_domain_name
        self._sasl_oauth_token_provider = sasl_oauth_token_provider

        self.cluster = ClusterMetadata(metadata_max_age_ms=metadata_max_age_ms)

        self._topics = set()  # empty set will fetch all topic metadata
        self._conns = {}
        self._loop = loop
        self._sync_task = None

        self._md_update_fut = None
        self._md_update_waiter = loop.create_future()
        self._get_conn_lock_value = None

    @property
    def _get_conn_lock(self):
        if self._get_conn_lock_value is None:
            self._get_conn_lock_value = asyncio.Lock()
        return self._get_conn_lock_value

    def __repr__(self):
        return f"<AIOKafkaClient client_id={self._client_id}>"

    @property
    def hosts(self):
        return collect_hosts(self._bootstrap_servers)

    async def close(self):
        if self._sync_task:
            self._sync_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._sync_task
            self._sync_task = None
        # Be careful to wait for graceful closure of all connections, so we
        # process all pending buffers.
        futs = [
            conn.close(reason=CloseReason.SHUTDOWN) for conn in self._conns.values()
        ]
        if futs:
            await asyncio.gather(*futs)

    async def bootstrap(self):
        """Try to to bootstrap initial cluster metadata"""
        assert self._loop is get_running_loop(), (
            "Please create objects with the same loop as running with"
        )

        for host, port, _ in self.hosts:
            log.debug("Attempting to bootstrap via node at %s:%s", host, port)

            try:
                bootstrap_conn = await create_conn(
                    host,
                    port,
                    client_id=self._client_id,
                    request_timeout_ms=self._request_timeout_ms,
                    ssl_context=self._ssl_context,
                    security_protocol=self._security_protocol,
                    max_idle_ms=self._connections_max_idle_ms,
                    sasl_mechanism=self._sasl_mechanism,
                    sasl_plain_username=self._sasl_plain_username,
                    sasl_plain_password=self._sasl_plain_password,
                    sasl_kerberos_service_name=self._sasl_kerberos_service_name,
                    sasl_kerberos_domain_name=self._sasl_kerberos_domain_name,
                    sasl_oauth_token_provider=self._sasl_oauth_token_provider,
                )
            except (OSError, KafkaError, asyncio.TimeoutError) as err:
                log.error('Unable connect to "%s:%s": %s', host, port, err)
                continue

            try:
                metadata = await bootstrap_conn.send(MetadataRequest([]))
            except (KafkaError, asyncio.TimeoutError) as err:
                log.warning(
                    'Unable to request metadata from "%s:%s": %s', host, port, err
                )
                bootstrap_conn.close()
                continue

            self.cluster.update_metadata(metadata)

            # A cluster with no topics can return no broker metadata...
            # In that case, we should keep the bootstrap connection till
            # we get a normal cluster layout.
            if not len(self.cluster.brokers()):
                bootstrap_id = ("bootstrap", ConnectionGroup.DEFAULT)
                self._conns[bootstrap_id] = bootstrap_conn
            else:
                bootstrap_conn.close()

            log.debug("Received cluster metadata: %s", self.cluster)
            break
        else:
            raise KafkaConnectionError(f"Unable to bootstrap from {self.hosts}")

        if self._sync_task is None:
            # starting metadata synchronizer task
            self._sync_task = create_task(self._md_synchronizer())

    async def _md_synchronizer(self):
        """routine (async task) for synchronize cluster metadata every
        `metadata_max_age_ms` milliseconds"""
        while True:
            await asyncio.wait(
                [self._md_update_waiter],
                timeout=self._metadata_max_age_ms / 1000,
            )

            topics = self._topics
            if self._md_update_fut is None:
                self._md_update_fut = create_future()
            ret = await self._metadata_update(self.cluster, topics)
            # If list of topics changed during metadata update we must update
            # it again right away.
            if topics != self._topics:
                continue
            # Earlier this waiter was set before sending metadata_request,
            # but that was to avoid topic list changes being unnoticed, which
            # is handled explicitly now.
            self._md_update_waiter = create_future()

            self._md_update_fut.set_result(ret)
            self._md_update_fut = None

    def get_random_node(self):
        """choice random node from known cluster brokers

        Returns:
            nodeId - identifier of broker
        """
        nodeids = [b.nodeId for b in self.cluster.brokers()]
        if not nodeids:
            return None
        return random.choice(nodeids)

    async def _metadata_update(self, cluster_metadata, topics):
        assert isinstance(cluster_metadata, ClusterMetadata)
        metadata_request = MetadataRequest(list(topics) if topics else None)
        nodeids = [b.nodeId for b in self.cluster.brokers()]
        bootstrap_id = ("bootstrap", ConnectionGroup.DEFAULT)
        if bootstrap_id in self._conns:
            nodeids.append("bootstrap")
        random.shuffle(nodeids)
        for node_id in nodeids:
            conn = await self._get_conn(node_id)

            if conn is None:
                continue
            log.debug(
                "Sending metadata request %s to node %s", metadata_request, node_id
            )

            try:
                metadata = await conn.send(metadata_request)
            except (KafkaError, asyncio.TimeoutError) as err:
                log.warning(
                    "Unable to request metadata from node with id %s: %r", node_id, err
                )
                continue

            # don't update the cluster if there are no valid nodes...the topic
            # we want may still be in the process of being created which means
            # we will get errors and no nodes until it exists
            if not metadata.brokers:
                return False

            cluster_metadata.update_metadata(metadata)

            # We only keep bootstrap connection to update metadata until
            # proper cluster layout is available.
            if bootstrap_id in self._conns and len(self.cluster.brokers()):
                conn = self._conns.pop(bootstrap_id)
                conn.close()

            break
        else:
            log.error("Unable to update metadata from %s", nodeids)
            cluster_metadata.failed_update(None)
            return False
        return True

    def force_metadata_update(self):
        """Update cluster metadata

        Returns:
            True/False - metadata updated or not
        """
        if self._md_update_fut is None:
            # Wake up the `_md_synchronizer` task
            if not self._md_update_waiter.done():
                self._md_update_waiter.set_result(None)
            self._md_update_fut = self._loop.create_future()
        # Metadata will be updated in the background by synchronizer
        return asyncio.shield(self._md_update_fut)

    async def fetch_all_metadata(self):
        cluster_md = ClusterMetadata(metadata_max_age_ms=self._metadata_max_age_ms)
        updated = await self._metadata_update(cluster_md, None)
        if not updated:
            raise KafkaError("Unable to get cluster metadata over all known brokers")
        return cluster_md

    def add_topic(self, topic):
        """Add a topic to the list of topics tracked via metadata.

        Arguments:
            topic (str): topic to track
        """
        if topic in self._topics:
            res = self._loop.create_future()
            res.set_result(True)
        else:
            res = self.force_metadata_update()
        self._topics.add(topic)
        return res

    def set_topics(self, topics):
        """Set specific topics to track for metadata.

        Arguments:
            topics (list of str): topics to track
        """
        assert not isinstance(topics, str)
        if not topics or set(topics).difference(self._topics):
            res = self.force_metadata_update()
        else:
            res = self._loop.create_future()
            res.set_result(True)
        self._topics = set(topics)
        return res

    def _on_connection_closed(self, conn, reason):
        """Callback called when connection is closed"""
        # Connection failures imply that our metadata is stale, so let's
        # refresh
        if reason in [CloseReason.CONNECTION_BROKEN, CloseReason.CONNECTION_TIMEOUT]:
            self.force_metadata_update()

    async def _get_conn(self, node_id, *, group=ConnectionGroup.DEFAULT, no_hint=False):
        "Get or create a connection to a broker using host and port"
        conn_id = (node_id, group)
        if conn_id in self._conns:
            conn = self._conns[conn_id]
            if not conn.connected():
                del self._conns[conn_id]
            else:
                return conn

        try:
            if group == ConnectionGroup.DEFAULT:
                broker = self.cluster.broker_metadata(node_id)
                # XXX: earlier we only did an assert here, but it seems it's
                # possible to get a leader that is for some reason not in
                # metadata.
                # I think requiring metadata should solve this problem
                if broker is None:
                    raise StaleMetadata(f"Broker id {node_id} not in current metadata")
            else:
                broker = self.cluster.coordinator_metadata(node_id)
                assert broker is not None

            log.debug(
                "Initiating connection to node %s at %s:%s",
                node_id,
                broker.host,
                broker.port,
            )

            async with self._get_conn_lock:
                if conn_id in self._conns:
                    return self._conns[conn_id]

                self._conns[conn_id] = await create_conn(
                    broker.host,
                    broker.port,
                    client_id=self._client_id,
                    request_timeout_ms=self._request_timeout_ms,
                    ssl_context=self._ssl_context,
                    security_protocol=self._security_protocol,
                    on_close=self._on_connection_closed,
                    max_idle_ms=self._connections_max_idle_ms,
                    sasl_mechanism=self._sasl_mechanism,
                    sasl_plain_username=self._sasl_plain_username,
                    sasl_plain_password=self._sasl_plain_password,
                    sasl_kerberos_service_name=self._sasl_kerberos_service_name,
                    sasl_kerberos_domain_name=self._sasl_kerberos_domain_name,
                    sasl_oauth_token_provider=self._sasl_oauth_token_provider,
                )
        except (OSError, asyncio.TimeoutError, KafkaError) as err:
            log.error("Unable connect to node with id %s: %s", node_id, err)
            if group == ConnectionGroup.DEFAULT:
                # Connection failures imply that our metadata is stale, so
                # let's refresh
                self.force_metadata_update()
            return None
        else:
            return self._conns[conn_id]

    async def ready(self, node_id, *, group=ConnectionGroup.DEFAULT):
        conn = await self._get_conn(node_id, group=group)
        return conn is not None

    async def send(self, node_id, request, *, group=ConnectionGroup.DEFAULT):
        """Send a request to a specific node.

        Arguments:
            node_id (int): destination node
            request (Request): request to send

        Raises:
            aiokafka.errors.RequestTimedOutError
            aiokafka.errors.NodeNotReadyError
            aiokafka.errors.KafkaConnectionError
            aiokafka.errors.CorrelationIdError

        Returns:
            Future: resolves to Response struct
        """
        if not (await self.ready(node_id, group=group)):
            raise NodeNotReadyError(
                "Attempt to send a request to node"
                f" which is not ready (node id {node_id})."
            )

        # Every request gets a response, except one special case:
        expect_response = True
        if isinstance(request, ProduceRequest) and request.required_acks == 0:
            expect_response = False

        future = self._conns[(node_id, group)].send(
            request, expect_response=expect_response
        )
        try:
            result = await future
        except asyncio.TimeoutError as exc:
            # close connection so it is renewed in next request
            self._conns[(node_id, group)].close(reason=CloseReason.CONNECTION_TIMEOUT)
            raise RequestTimedOutError() from exc
        else:
            return result

    async def _wait_on_metadata(self, topic):
        """
        Wait for cluster metadata including partitions for the given topic to
        be available.

        Arguments:
            topic (str): topic we want metadata for

        Returns:
            set: partition ids for the topic

        Raises:
            UnknownTopicOrPartitionError: if no topic or partitions found
                in cluster metadata
        """
        partitions = self.cluster.partitions_for_topic(topic)
        if partitions is not None:
            return partitions

        # add topic to metadata topic list if it is not there already.
        self.add_topic(topic)

        t0 = time.monotonic()
        while True:
            await self.force_metadata_update()
            partitions = self.cluster.partitions_for_topic(topic)
            if partitions is not None:
                return partitions
            if (time.monotonic() - t0) > (self._request_timeout_ms / 1000):
                raise UnknownTopicOrPartitionError()
            if topic in self.cluster.unauthorized_topics:
                raise Errors.TopicAuthorizationFailedError(topic)
            await asyncio.sleep(self._retry_backoff)

    async def _maybe_wait_metadata(self):
        if self._md_update_fut is not None:
            await asyncio.shield(self._md_update_fut)

    async def coordinator_lookup(self, coordinator_type, coordinator_key):
        """Lookup which node in the cluster is the coordinator for a certain
        role (Transaction coordinator or Group coordinator atm.)
        NOTE: Client keeps track of all coordination nodes separately, as they
        all have different sockets and ids.
        """

        node_id = self.get_random_node()
        assert node_id is not None, "Did we not perform bootstrap?"

        log.debug(
            "Sending FindCoordinator request for key %s to broker %s",
            coordinator_key,
            node_id,
        )

        request = FindCoordinatorRequest(coordinator_key, coordinator_type)
        resp = await self.send(node_id, request)
        log.debug("Received group coordinator response %s", resp)
        error_type = Errors.for_code(resp.error_code)
        if error_type is not Errors.NoError:
            err = error_type()
            raise err
        self.cluster.add_coordinator(
            resp.coordinator_id,
            resp.host,
            resp.port,
            rack=None,
            purpose=(coordinator_type, coordinator_key),
        )
        return resp.coordinator_id
