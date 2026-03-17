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

import asyncio
import logging
import sys
import warnings
from itertools import chain

from ..connection_pool import ConnectionPool
from ..exceptions import (
    AuthenticationException,
    AuthorizationException,
    ConnectionError,
    ConnectionTimeout,
    ElasticsearchWarning,
    SerializationError,
    TransportError,
)
from ..serializer import JSONSerializer
from ..transport import Transport, _ProductChecker, get_host_info
from .compat import get_running_loop
from .http_aiohttp import AIOHttpConnection

logger = logging.getLogger("elasticsearch")


class AsyncTransport(Transport):
    """
    Encapsulation of transport-related to logic. Handles instantiation of the
    individual connections as well as creating a connection pool to hold them.

    Main interface is the `perform_request` method.
    """

    DEFAULT_CONNECTION_CLASS = AIOHttpConnection

    def __init__(
        self,
        hosts,
        connection_class=None,
        connection_pool_class=ConnectionPool,
        host_info_callback=get_host_info,
        sniff_on_start=False,
        sniffer_timeout=None,
        sniff_timeout=0.1,
        sniff_on_connection_fail=False,
        serializer=JSONSerializer(),
        serializers=None,
        default_mimetype="application/json",
        max_retries=3,
        retry_on_status=(502, 503, 504),
        retry_on_timeout=False,
        send_get_body_as="GET",
        meta_header=True,
        **kwargs
    ):
        """
        :arg hosts: list of dictionaries, each containing keyword arguments to
            create a `connection_class` instance
        :arg connection_class: subclass of :class:`~elasticsearch.Connection` to use
        :arg connection_pool_class: subclass of :class:`~elasticsearch.ConnectionPool` to use
        :arg host_info_callback: callback responsible for taking the node information from
            `/_cluster/nodes`, along with already extracted information, and
            producing a list of arguments (same as `hosts` parameter)
        :arg sniff_on_start: flag indicating whether to obtain a list of nodes
            from the cluster at startup time
        :arg sniffer_timeout: number of seconds between automatic sniffs
        :arg sniff_on_connection_fail: flag controlling if connection failure triggers a sniff
        :arg sniff_timeout: timeout used for the sniff request - it should be a
            fast api call and we are talking potentially to more nodes so we want
            to fail quickly. Not used during initial sniffing (if
            ``sniff_on_start`` is on) when the connection still isn't
            initialized.
        :arg serializer: serializer instance
        :arg serializers: optional dict of serializer instances that will be
            used for deserializing data coming from the server. (key is the mimetype)
        :arg default_mimetype: when no mimetype is specified by the server
            response assume this mimetype, defaults to `'application/json'`
        :arg max_retries: maximum number of retries before an exception is propagated
        :arg retry_on_status: set of HTTP status codes on which we should retry
            on a different node. defaults to ``(502, 503, 504)``
        :arg retry_on_timeout: should timeout trigger a retry on different
            node? (default `False`)
        :arg send_get_body_as: for GET requests with body this option allows
            you to specify an alternate way of execution for environments that
            don't support passing bodies with GET requests. If you set this to
            'POST' a POST method will be used instead, if to 'source' then the body
            will be serialized and passed as a query parameter `source`.
        :arg meta_header: If True will send the 'X-Elastic-Client-Meta' HTTP header containing
            simple client metadata. Setting to False will disable the header. Defaults to True.

        Any extra keyword arguments will be passed to the `connection_class`
        when creating and instance unless overridden by that connection's
        options provided as part of the hosts parameter.
        """
        self.sniffing_task = None
        self.loop = None
        self._async_init_called = False
        self._sniff_on_start_event = None  # type: asyncio.Event

        super(AsyncTransport, self).__init__(
            hosts=[],
            connection_class=connection_class,
            connection_pool_class=connection_pool_class,
            host_info_callback=host_info_callback,
            sniff_on_start=False,
            sniffer_timeout=sniffer_timeout,
            sniff_timeout=sniff_timeout,
            sniff_on_connection_fail=sniff_on_connection_fail,
            serializer=serializer,
            serializers=serializers,
            default_mimetype=default_mimetype,
            max_retries=max_retries,
            retry_on_status=retry_on_status,
            retry_on_timeout=retry_on_timeout,
            send_get_body_as=send_get_body_as,
            meta_header=meta_header,
            **kwargs,
        )

        # Don't enable sniffing on Cloud instances.
        if kwargs.get("cloud_id", False):
            sniff_on_start = False

        # Since we defer connections / sniffing to not occur
        # within the constructor we never want to signal to
        # our parent to 'sniff_on_start' or non-empty 'hosts'.
        self.hosts = hosts
        self.sniff_on_start = sniff_on_start

    async def _async_init(self):
        """This is our stand-in for an async constructor. Everything
        that was deferred within __init__() should be done here now.

        This method will only be called once per AsyncTransport instance
        and is called from one of AsyncElasticsearch.__aenter__(),
        AsyncTransport.perform_request() or AsyncTransport.get_connection()
        """
        # Detect the async loop we're running in and set it
        # on all already created HTTP connections.
        self.loop = get_running_loop()
        self.kwargs["loop"] = self.loop

        # Set our 'verified_once' implementation to one that
        # works with 'asyncio' instead of 'threading'
        self._verify_elasticsearch_lock = asyncio.Lock()

        # Now that we have a loop we can create all our HTTP connections...
        self.set_connections(self.hosts)
        self.seed_connections = list(self.connection_pool.connections[:])

        # ... and we can start sniffing in the background.
        if self.sniffing_task is None and self.sniff_on_start:

            # Create an asyncio.Event for future calls to block on
            # until the initial sniffing task completes.
            self._sniff_on_start_event = asyncio.Event()

            try:
                self.last_sniff = self.loop.time()
                self.create_sniff_task(initial=True)

                # Since this is the first one we wait for it to complete
                # in case there's an error it'll get raised here.
                await self.sniffing_task

            # If the task gets cancelled here it likely means the
            # transport got closed.
            except asyncio.CancelledError:
                pass

            # Once we exit this section we want to unblock any _async_calls()
            # that are blocking on our initial sniff attempt regardless of it
            # was successful or not.
            finally:
                self._sniff_on_start_event.set()

    async def _async_call(self):
        """This method is called within any async method of AsyncTransport
        where the transport is not closing. This will check to see if we should
        call our _async_init() or create a new sniffing task
        """
        if not self._async_init_called:
            self._async_init_called = True
            await self._async_init()

        # If the initial sniff_on_start hasn't returned yet
        # then we need to wait for node information to come back
        # or for the task to be cancelled via AsyncTransport.close()
        if self._sniff_on_start_event and not self._sniff_on_start_event.is_set():
            # This is already a no-op if the event is set but we try to
            # avoid an 'await' by checking 'not event.is_set()' above first.
            await self._sniff_on_start_event.wait()

        if self.sniffer_timeout:
            if self.loop.time() >= self.last_sniff + self.sniffer_timeout:
                self.create_sniff_task()

    async def _get_node_info(self, conn, initial):
        try:
            # use small timeout for the sniffing request, should be a fast api call
            _, headers, node_info = await conn.perform_request(
                "GET",
                "/_nodes/_all/http",
                timeout=self.sniff_timeout if not initial else None,
            )
            return self.deserializer.loads(node_info, headers.get("content-type"))
        except Exception:
            pass
        return None

    async def _get_sniff_data(self, initial=False):
        previous_sniff = self.last_sniff

        # reset last_sniff timestamp
        self.last_sniff = self.loop.time()

        # use small timeout for the sniffing request, should be a fast api call
        timeout = self.sniff_timeout if not initial else None

        def _sniff_request(conn):
            return self.loop.create_task(
                conn.perform_request("GET", "/_nodes/_all/http", timeout=timeout)
            )

        # Go through all current connections as well as the
        # seed_connections for good measure
        tasks = []
        for conn in self.connection_pool.connections:
            tasks.append(_sniff_request(conn))
        for conn in self.seed_connections:
            # Ensure that we don't have any duplication within seed_connections.
            if conn in self.connection_pool.connections:
                continue
            tasks.append(_sniff_request(conn))

        done = ()
        try:
            while tasks:
                # The 'loop' keyword is deprecated in 3.8+ so don't
                # pass it to asyncio.wait() unless we're on <=3.7
                wait_kwargs = {"loop": self.loop} if sys.version_info < (3, 8) else {}

                # execute sniff requests in parallel, wait for first to return
                done, tasks = await asyncio.wait(
                    tasks, return_when=asyncio.FIRST_COMPLETED, **wait_kwargs
                )
                # go through all the finished tasks
                for t in done:
                    try:
                        _, headers, node_info = t.result()

                        # Lowercase all the header names for consistency in accessing them.
                        headers = {
                            header.lower(): value for header, value in headers.items()
                        }

                        node_info = self.deserializer.loads(
                            node_info, headers.get("content-type")
                        )
                    except (ConnectionError, SerializationError):
                        continue
                    node_info = list(node_info["nodes"].values())
                    return node_info
            else:
                # no task has finished completely
                raise TransportError("N/A", "Unable to sniff hosts.")
        except Exception:
            # keep the previous value on error
            self.last_sniff = previous_sniff
            raise
        finally:
            # Cancel all the pending tasks
            for task in chain(done, tasks):
                task.cancel()

    async def sniff_hosts(self, initial=False):
        """Either spawns a sniffing_task which does regular sniffing
        over time or does a single sniffing session and awaits the results.
        """
        # Without a loop we can't do anything.
        if not self.loop:
            if initial:
                raise RuntimeError("Event loop not running on initial sniffing task")
            return

        node_info = await self._get_sniff_data(initial)
        hosts = list(filter(None, (self._get_host_info(n) for n in node_info)))

        # we weren't able to get any nodes, maybe using an incompatible
        # transport_schema or host_info_callback blocked all - raise error.
        if not hosts:
            raise TransportError(
                "N/A", "Unable to sniff hosts - no viable hosts found."
            )

        # remember current live connections
        orig_connections = self.connection_pool.connections[:]
        self.set_connections(hosts)
        # close those connections that are not in use any more
        for c in orig_connections:
            if c not in self.connection_pool.connections:
                await c.close()

    def create_sniff_task(self, initial=False):
        """
        Initiate a sniffing task. Make sure we only have one sniff request
        running at any given time. If a finished sniffing request is around,
        collect its result (which can raise its exception).
        """
        if self.sniffing_task and self.sniffing_task.done():
            try:
                if self.sniffing_task is not None:
                    self.sniffing_task.result()
            finally:
                self.sniffing_task = None

        if self.sniffing_task is None:
            self.sniffing_task = self.loop.create_task(self.sniff_hosts(initial))

    def mark_dead(self, connection):
        """
        Mark a connection as dead (failed) in the connection pool. If sniffing
        on failure is enabled this will initiate the sniffing process.

        :arg connection: instance of :class:`~elasticsearch.Connection` that failed
        """
        self.connection_pool.mark_dead(connection)
        if self.sniff_on_connection_fail:
            self.create_sniff_task()

    def get_connection(self):
        return self.connection_pool.get_connection()

    async def perform_request(self, method, url, headers=None, params=None, body=None):
        """
        Perform the actual request. Retrieve a connection from the connection
        pool, pass all the information to it's perform_request method and
        return the data.

        If an exception was raised, mark the connection as failed and retry (up
        to `max_retries` times).

        If the operation was successful and the connection used was previously
        marked as dead, mark it as live, resetting it's failure count.

        :arg method: HTTP method to use
        :arg url: absolute url (without host) to target
        :arg headers: dictionary of headers, will be handed over to the
            underlying :class:`~elasticsearch.Connection` class
        :arg params: dictionary of query parameters, will be handed over to the
            underlying :class:`~elasticsearch.Connection` class for serialization
        :arg body: body of the request, will be serialized using serializer and
            passed to the connection
        """
        await self._async_call()

        method, headers, params, body, ignore, timeout = self._resolve_request_args(
            method, headers, params, body
        )

        # Before we make the actual API call we verify the Elasticsearch instance.
        if self._verified_elasticsearch is None:
            await self._do_verify_elasticsearch(headers=headers, timeout=timeout)

        # If '_verified_elasticsearch' isn't 'True' then we raise an error.
        if self._verified_elasticsearch is not True:
            _ProductChecker.raise_error(self._verified_elasticsearch)

        for attempt in range(self.max_retries + 1):
            connection = self.get_connection()

            try:
                status, headers_response, data = await connection.perform_request(
                    method,
                    url,
                    params,
                    body,
                    headers=headers,
                    ignore=ignore,
                    timeout=timeout,
                )

                # Lowercase all the header names for consistency in accessing them.
                headers_response = {
                    header.lower(): value for header, value in headers_response.items()
                }
            except TransportError as e:
                if method == "HEAD" and e.status_code == 404:
                    return False

                retry = False
                if isinstance(e, ConnectionTimeout):
                    retry = self.retry_on_timeout
                elif isinstance(e, ConnectionError):
                    retry = True
                elif e.status_code in self.retry_on_status:
                    retry = True

                if retry:
                    try:
                        # only mark as dead if we are retrying
                        self.mark_dead(connection)
                    except TransportError:
                        # If sniffing on failure, it could fail too. Catch the
                        # exception not to interrupt the retries.
                        pass
                    # raise exception on last retry
                    if attempt == self.max_retries:
                        raise e
                else:
                    raise e

            else:
                # connection didn't fail, confirm it's live status
                self.connection_pool.mark_live(connection)

                if method == "HEAD":
                    return 200 <= status < 300

                if data:
                    data = self.deserializer.loads(
                        data, headers_response.get("content-type")
                    )
                return data

    async def close(self):
        """
        Explicitly closes connections
        """
        if self.sniffing_task:
            try:
                self.sniffing_task.cancel()
                await self.sniffing_task
            except asyncio.CancelledError:
                pass
            self.sniffing_task = None

        for connection in self.connection_pool.connections:
            await connection.close()

    async def _do_verify_elasticsearch(self, headers, timeout):
        """Verifies that we're connected to an Elasticsearch cluster.
        This is done at least once before the first actual API call
        and makes a single request to the 'GET /' API endpoint and
        check version along with other details of the response.

        If we're unable to verify we're talking to Elasticsearch
        but we're also unable to rule it out due to a permission
        error we instead emit an 'ElasticsearchWarning'.
        """
        # Ensure that there's only one async exec within this section
        # at a time to not emit unnecessary index API calls.
        async with self._verify_elasticsearch_lock:

            # Product check has already been completed while we were
            # waiting our turn, no need to do again.
            if self._verified_elasticsearch is not None:
                return

            headers = {
                header.lower(): value for header, value in (headers or {}).items()
            }
            # We know we definitely want JSON so request it via 'accept'
            headers.setdefault("accept", "application/json")

            info_headers = {}
            info_response = {}
            error = None

            attempted_conns = []
            for conn in chain(self.connection_pool.connections, self.seed_connections):
                # Only attempt once per connection max.
                if conn in attempted_conns:
                    continue
                attempted_conns.append(conn)

                try:
                    _, info_headers, info_response = await conn.perform_request(
                        "GET", "/", headers=headers, timeout=timeout
                    )

                    # Lowercase all the header names for consistency in accessing them.
                    info_headers = {
                        header.lower(): value for header, value in info_headers.items()
                    }

                    info_response = self.deserializer.loads(
                        info_response, mimetype="application/json"
                    )
                    break

                # Previous versions of 7.x Elasticsearch required a specific
                # permission so if we receive HTTP 401/403 we should warn
                # instead of erroring out.
                except (AuthenticationException, AuthorizationException):
                    warnings.warn(
                        (
                            "The client is unable to verify that the server is "
                            "Elasticsearch due security privileges on the server side"
                        ),
                        ElasticsearchWarning,
                        stacklevel=4,
                    )
                    self._verified_elasticsearch = True
                    return

                # This connection didn't work, we'll try another.
                except (ConnectionError, SerializationError, TransportError) as err:
                    if error is None:
                        error = err

            # If we received a connection error and weren't successful
            # anywhere then we re-raise the more appropriate error.
            if error and not info_response:
                raise error

            # Check the information we got back from the index request.
            self._verified_elasticsearch = _ProductChecker.check_product(
                info_headers, info_response
            )
