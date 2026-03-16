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

import re
import time
import warnings
from itertools import chain
from platform import python_version

from ._version import __versionstr__
from .compat import Lock
from .connection import Urllib3HttpConnection
from .connection_pool import ConnectionPool, DummyConnectionPool, EmptyConnectionPool
from .exceptions import (
    AuthenticationException,
    AuthorizationException,
    ConnectionError,
    ConnectionTimeout,
    ElasticsearchWarning,
    SerializationError,
    TransportError,
    UnsupportedProductError,
)
from .serializer import DEFAULT_SERIALIZERS, Deserializer, JSONSerializer
from .utils import _client_meta_version


def get_host_info(node_info, host):
    """
    Simple callback that takes the node info from `/_cluster/nodes` and a
    parsed connection information and return the connection information. If
    `None` is returned this node will be skipped.

    Useful for filtering nodes (by proximity for example) or if additional
    information needs to be provided for the :class:`~elasticsearch.Connection`
    class. By default master only nodes are filtered out since they shouldn't
    typically be used for API operations.

    :arg node_info: node information from `/_cluster/nodes`
    :arg host: connection information (host, port) extracted from the node info
    """
    # ignore master only nodes
    if node_info.get("roles", []) == ["master"]:
        return None
    return host


class Transport(object):
    """
    Encapsulation of transport-related to logic. Handles instantiation of the
    individual connections as well as creating a connection pool to hold them.

    Main interface is the `perform_request` method.
    """

    DEFAULT_CONNECTION_CLASS = Urllib3HttpConnection

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
        if connection_class is None:
            connection_class = self.DEFAULT_CONNECTION_CLASS
        if not isinstance(meta_header, bool):
            raise TypeError("meta_header must be of type bool")

        if send_get_body_as != "GET":
            warnings.warn(
                "The 'send_get_body_as' parameter is no longer necessary "
                "and will be removed in 8.0",
                category=DeprecationWarning,
                stacklevel=2,
            )

        # serialization config
        _serializers = DEFAULT_SERIALIZERS.copy()
        # if a serializer has been specified, use it for deserialization as well
        _serializers[serializer.mimetype] = serializer
        # if custom serializers map has been supplied, override the defaults with it
        if serializers:
            _serializers.update(serializers)
        # create a deserializer with our config
        self.deserializer = Deserializer(_serializers, default_mimetype)

        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout
        self.retry_on_status = retry_on_status
        self.send_get_body_as = send_get_body_as
        self.meta_header = meta_header

        # data serializer
        self.serializer = serializer

        # store all strategies...
        self.connection_pool_class = connection_pool_class
        self.connection_class = connection_class

        # ...save kwargs to be passed to the connections
        self.kwargs = kwargs
        self.hosts = hosts

        # Start with an empty pool specifically for `AsyncTransport`.
        # It should never be used, will be replaced on first call to
        # .set_connections()
        self.connection_pool = EmptyConnectionPool()

        if hosts:
            # ...and instantiate them
            self.set_connections(hosts)
            # retain the original connection instances for sniffing
            self.seed_connections = list(self.connection_pool.connections[:])
        else:
            self.seed_connections = []

        # Don't enable sniffing on Cloud instances.
        if kwargs.get("cloud_id", False):
            sniff_on_start = False
            sniff_on_connection_fail = False

        # sniffing data
        self.sniffer_timeout = sniffer_timeout
        self.sniff_on_start = sniff_on_start
        self.sniff_on_connection_fail = sniff_on_connection_fail
        self.last_sniff = time.time()
        self.sniff_timeout = sniff_timeout

        # callback to construct host dict from data in /_cluster/nodes
        self.host_info_callback = host_info_callback

        if sniff_on_start:
            self.sniff_hosts(True)

        # Create the default metadata for the x-elastic-client-meta
        # HTTP header. Only requires adding the (service, service_version)
        # tuple to the beginning of the client_meta
        self._client_meta = (
            ("es", _client_meta_version(__versionstr__)),
            ("py", _client_meta_version(python_version())),
            ("t", _client_meta_version(__versionstr__)),
        )

        # Grab the 'HTTP_CLIENT_META' property from the connection class
        http_client_meta = getattr(connection_class, "HTTP_CLIENT_META", None)
        if http_client_meta:
            self._client_meta += (http_client_meta,)

        # Tri-state flag that describes what state the verification
        # of whether we're connected to an Elasticsearch cluster or not.
        # The three states are:
        # - 'None': Means we've either not started the verification process
        #   or that the verification is in progress. '_verified_once' ensures
        #   that multiple requests don't kick off multiple verification processes.
        # - 'True': Means we've verified that we're talking to Elasticsearch or
        #   that we can't rule out Elasticsearch due to auth issues. A warning
        #   will be raised if we receive 401/403.
        # - 'int': Means we're talking to an unsupported product, should raise
        #   the corresponding error.
        self._verified_elasticsearch = None

        # Ensures that the ES verification request only fires once and that
        # all requests block until this request returns back.
        self._verify_elasticsearch_lock = Lock()

    def add_connection(self, host):
        """
        Create a new :class:`~elasticsearch.Connection` instance and add it to the pool.

        :arg host: kwargs that will be used to create the instance
        """
        self.hosts.append(host)
        self.set_connections(self.hosts)

    def set_connections(self, hosts):
        """
        Instantiate all the connections and create new connection pool to hold them.
        Tries to identify unchanged hosts and re-use existing
        :class:`~elasticsearch.Connection` instances.

        :arg hosts: same as `__init__`
        """
        # construct the connections
        def _create_connection(host):
            # if this is not the initial setup look at the existing connection
            # options and identify connections that haven't changed and can be
            # kept around.
            if hasattr(self, "connection_pool"):
                for (connection, old_host) in self.connection_pool.connection_opts:
                    if old_host == host:
                        return connection

            # previously unseen params, create new connection
            kwargs = self.kwargs.copy()
            kwargs.update(host)
            return self.connection_class(**kwargs)

        connections = map(_create_connection, hosts)

        connections = list(zip(connections, hosts))
        if len(connections) == 1:
            self.connection_pool = DummyConnectionPool(connections)
        else:
            # pass the hosts dicts to the connection pool to optionally extract parameters from
            self.connection_pool = self.connection_pool_class(
                connections, **self.kwargs
            )

    def get_connection(self):
        """
        Retrieve a :class:`~elasticsearch.Connection` instance from the
        :class:`~elasticsearch.ConnectionPool` instance.
        """
        if self.sniffer_timeout:
            if time.time() >= self.last_sniff + self.sniffer_timeout:
                self.sniff_hosts()
        return self.connection_pool.get_connection()

    def _get_sniff_data(self, initial=False):
        """
        Perform the request to get sniffing information. Returns a list of
        dictionaries (one per node) containing all the information from the
        cluster.

        It also sets the last_sniff attribute in case of a successful attempt.

        In rare cases it might be possible to override this method in your
        custom Transport class to serve data from alternative source like
        configuration management.
        """
        previous_sniff = self.last_sniff

        try:
            # reset last_sniff timestamp
            self.last_sniff = time.time()
            # go through all current connections as well as the
            # seed_connections for good measure
            for c in chain(self.connection_pool.connections, self.seed_connections):
                try:
                    # use small timeout for the sniffing request, should be a fast api call
                    _, headers, node_info = c.perform_request(
                        "GET",
                        "/_nodes/_all/http",
                        timeout=self.sniff_timeout if not initial else None,
                    )

                    # Lowercase all the header names for consistency in accessing them.
                    headers = {
                        header.lower(): value for header, value in headers.items()
                    }

                    node_info = self.deserializer.loads(
                        node_info, headers.get("content-type")
                    )
                    break
                except (ConnectionError, SerializationError):
                    pass
            else:
                raise TransportError("N/A", "Unable to sniff hosts.")
        except Exception:
            # keep the previous value on error
            self.last_sniff = previous_sniff
            raise

        return list(node_info["nodes"].values())

    def _get_host_info(self, host_info):
        host = {}
        address = host_info.get("http", {}).get("publish_address")

        # malformed or no address given
        if not address or ":" not in address:
            return None

        if "/" in address:
            # Support 7.x host/ip:port behavior where http.publish_host has been set.
            fqdn, ipaddress = address.split("/", 1)
            host["host"] = fqdn
            _, host["port"] = ipaddress.rsplit(":", 1)
            host["port"] = int(host["port"])

        else:
            host["host"], host["port"] = address.rsplit(":", 1)
            host["port"] = int(host["port"])

        return self.host_info_callback(host_info, host)

    def sniff_hosts(self, initial=False):
        """
        Obtain a list of nodes from the cluster and create a new connection
        pool using the information retrieved.

        To extract the node connection parameters use the ``nodes_to_host_callback``.

        :arg initial: flag indicating if this is during startup
            (``sniff_on_start``), ignore the ``sniff_timeout`` if ``True``
        """
        node_info = self._get_sniff_data(initial)

        hosts = list(filter(None, (self._get_host_info(n) for n in node_info)))

        # we weren't able to get any nodes or host_info_callback blocked all -
        # raise error.
        if not hosts:
            raise TransportError(
                "N/A", "Unable to sniff hosts - no viable hosts found."
            )

        self.set_connections(hosts)

    def mark_dead(self, connection):
        """
        Mark a connection as dead (failed) in the connection pool. If sniffing
        on failure is enabled this will initiate the sniffing process.

        :arg connection: instance of :class:`~elasticsearch.Connection` that failed
        """
        # mark as dead even when sniffing to avoid hitting this host during the sniff process
        self.connection_pool.mark_dead(connection)
        if self.sniff_on_connection_fail:
            self.sniff_hosts()

    def perform_request(self, method, url, headers=None, params=None, body=None):
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
        method, headers, params, body, ignore, timeout = self._resolve_request_args(
            method, headers, params, body
        )

        # Before we make the actual API call we verify the Elasticsearch instance.
        if self._verified_elasticsearch is None:
            self._do_verify_elasticsearch(headers=headers, timeout=timeout)

        # If '_verified_elasticsearch' isn't 'True' then we raise an error.
        if self._verified_elasticsearch is not True:
            _ProductChecker.raise_error(self._verified_elasticsearch)

        for attempt in range(self.max_retries + 1):
            connection = self.get_connection()

            try:
                status, headers_response, data = connection.perform_request(
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

    def close(self):
        """
        Explicitly closes connections
        """
        self.connection_pool.close()

    def _resolve_request_args(self, method, headers, params, body):
        """Resolves parameters for .perform_request()"""
        if body is not None:
            body = self.serializer.dumps(body)

            # some clients or environments don't support sending GET with body
            if method in ("HEAD", "GET") and self.send_get_body_as != "GET":
                # send it as post instead
                if self.send_get_body_as == "POST":
                    method = "POST"

                # or as source parameter
                elif self.send_get_body_as == "source":
                    if params is None:
                        params = {}
                    params["source"] = body
                    body = None

        if body is not None:
            try:
                body = body.encode("utf-8", "surrogatepass")
            except (UnicodeDecodeError, AttributeError):
                # bytes/str - no need to re-encode
                pass

        ignore = ()
        timeout = None
        if params:
            timeout = params.pop("request_timeout", None)
            ignore = params.pop("ignore", ())
            if isinstance(ignore, int):
                ignore = (ignore,)
            client_meta = params.pop("__elastic_client_meta", ())
        else:
            client_meta = ()

        if self.meta_header:
            headers = headers or {}
            client_meta = self._client_meta + client_meta
            headers["x-elastic-client-meta"] = ",".join(
                "%s=%s" % (k, v) for k, v in client_meta
            )

        return method, headers, params, body, ignore, timeout

    def _do_verify_elasticsearch(self, headers, timeout):
        """Verifies that we're connected to an Elasticsearch cluster.
        This is done at least once before the first actual API call
        and makes a single request to the 'GET /' API endpoint to
        check the version along with other details of the response.

        If we're unable to verify we're talking to Elasticsearch
        but we're also unable to rule it out due to a permission
        error we instead emit an 'ElasticsearchWarning'.
        """
        # Ensure that there's only one thread within this section
        # at a time to not emit unnecessary index API calls.
        with self._verify_elasticsearch_lock:

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
                    _, info_headers, info_response = conn.perform_request(
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
                        stacklevel=5,
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


class _ProductChecker:
    """Class which verifies we're connected to a supported product"""

    # States that can be returned from 'check_product'
    SUCCESS = True
    UNSUPPORTED_PRODUCT = 2
    UNSUPPORTED_DISTRIBUTION = 3

    @classmethod
    def raise_error(cls, state):
        # These states mean the product_check() didn't fail so do nothing.
        if state in (None, True):
            return

        if state == cls.UNSUPPORTED_DISTRIBUTION:
            message = (
                "The client noticed that the server is not "
                "a supported distribution of Elasticsearch"
            )
        else:  # UNSUPPORTED_PRODUCT
            message = (
                "The client noticed that the server is not Elasticsearch "
                "and we do not support this unknown product"
            )
        raise UnsupportedProductError(message)

    @classmethod
    def check_product(cls, headers, response):
        # type: (dict[str, str], dict[str, str]) -> int
        """Verifies that the server we're talking to is Elasticsearch.
        Does this by checking HTTP headers and the deserialized
        response to the 'info' API. Returns one of the states above.
        """
        try:
            version = response.get("version", {})
            version_number = tuple(
                int(x) if x is not None else 999
                for x in re.search(
                    r"^([0-9]+)\.([0-9]+)(?:\.([0-9]+))?", version["number"]
                ).groups()
            )
        except (KeyError, TypeError, ValueError, AttributeError):
            # No valid 'version.number' field, effectively 0.0.0
            version = {}
            version_number = (0, 0, 0)

        # Check all of the fields and headers for missing/valid values.
        try:
            bad_tagline = response.get("tagline", None) != "You Know, for Search"
            bad_build_flavor = version.get("build_flavor", None) != "default"
            bad_product_header = (
                headers.get("x-elastic-product", None) != "Elasticsearch"
            )
        except (AttributeError, TypeError):
            bad_tagline = True
            bad_build_flavor = True
            bad_product_header = True

        # 7.0-7.13 and there's a bad 'tagline' or unsupported 'build_flavor'
        if (7, 0, 0) <= version_number < (7, 14, 0):
            if bad_tagline:
                return cls.UNSUPPORTED_PRODUCT
            elif bad_build_flavor:
                return cls.UNSUPPORTED_DISTRIBUTION

        elif (
            # No version or version less than 6.x
            version_number < (6, 0, 0)
            # 6.x and there's a bad 'tagline'
            or ((6, 0, 0) <= version_number < (7, 0, 0) and bad_tagline)
            # 7.14+ and there's a bad 'X-Elastic-Product' HTTP header
            or ((7, 14, 0) <= version_number and bad_product_header)
        ):
            return cls.UNSUPPORTED_PRODUCT

        return True
