# -*- coding: utf-8 -*-
import abc
import threading
import logging
from concurrent import futures
import collections
import random

from . import connection as connection_impl, issues, resolver, _utilities, tracing
from abc import abstractmethod

from .connection import Connection

logger = logging.getLogger(__name__)


class ConnectionsCache(object):
    def __init__(self, use_all_nodes=False, tracer=tracing.Tracer(None)):
        self.tracer = tracer
        self.lock = threading.RLock()
        self.connections = collections.OrderedDict()
        self.connections_by_node_id = collections.OrderedDict()
        self.outdated = collections.OrderedDict()
        self.subscriptions = set()
        self.preferred = collections.OrderedDict()
        self.logger = logging.getLogger(__name__)
        self.use_all_nodes = use_all_nodes
        self.conn_lst_order = (self.connections,) if self.use_all_nodes else (self.preferred, self.connections)
        self.fast_fail_subscriptions = set()

    def add(self, connection, preferred=False):
        if connection is None:
            return False

        connection.add_cleanup_callback(self.remove)
        with self.lock:
            if preferred:
                self.preferred[connection.endpoint] = connection

            self.connections_by_node_id[connection.node_id] = connection
            self.connections[connection.endpoint] = connection
            subscriptions = list(self.subscriptions)
            self.subscriptions.clear()

            if len(self.connections) > 0:
                self.complete_discovery(None)

        for subscription in subscriptions:
            subscription.set_result(None)
        return True

    def _on_done_callback(self, subscription):
        """
        A done callback for the subscription future
        :param subscription: A subscription
        :return: None
        """
        with self.lock:
            try:
                self.subscriptions.remove(subscription)
            except KeyError:
                return subscription

    @property
    def size(self):
        with self.lock:
            return len(self.connections) - len(self.outdated)

    def already_exists(self, endpoint):
        with self.lock:
            return endpoint in self.connections

    def values(self):
        with self.lock:
            return list(self.connections.values())

    def make_outdated(self, connection):
        with self.lock:
            self.outdated[connection.endpoint] = connection
            return self

    def cleanup_outdated(self):
        with self.lock:
            outdated_connections = list(self.outdated.values())
            for outdated_connection in outdated_connections:
                outdated_connection.close()
        return self

    def cleanup(self):
        with self.lock:
            actual_connections = list(self.connections.values())
            for connection in actual_connections:
                connection.close()

    def complete_discovery(self, error):
        with self.lock:
            for subscription in self.fast_fail_subscriptions:
                if error is None:
                    subscription.set_result(None)
                else:
                    subscription.set_exception(error)

            self.fast_fail_subscriptions.clear()

    def add_fast_fail(self):
        with self.lock:
            subscription = futures.Future()
            if len(self.connections) > 0:
                subscription.set_result(None)
                return subscription

            self.fast_fail_subscriptions.add(subscription)
            return subscription

    def subscribe(self):
        with self.lock:
            subscription = futures.Future()
            if len(self.connections) > 0:
                subscription.set_result(None)
                return subscription
            self.subscriptions.add(subscription)
            subscription.add_done_callback(self._on_done_callback)
            return subscription

    @tracing.with_trace()
    def get(self, preferred_endpoint=None) -> Connection:
        with self.lock:
            if preferred_endpoint is not None and preferred_endpoint.node_id in self.connections_by_node_id:
                return self.connections_by_node_id[preferred_endpoint.node_id]

            if preferred_endpoint is not None and preferred_endpoint.endpoint in self.connections:
                return self.connections[preferred_endpoint]

            for conn_lst in self.conn_lst_order:
                try:
                    endpoint, connection = conn_lst.popitem(last=False)
                    conn_lst[endpoint] = connection
                    tracing.trace(self.tracer, {"found_in_lists": True})
                    return connection
                except KeyError:
                    continue

            raise issues.ConnectionLost("Couldn't find valid connection")

    def remove(self, connection):
        with self.lock:
            self.connections_by_node_id.pop(connection.node_id, None)
            self.preferred.pop(connection.endpoint, None)
            self.connections.pop(connection.endpoint, None)
            self.outdated.pop(connection.endpoint, None)


class Discovery(threading.Thread):
    def __init__(self, store, driver_config):
        """
        A timer thread that implements endpoints discovery logic
        :param store: A store with endpoints
        :param driver_config: An instance of DriverConfig
        """
        super(Discovery, self).__init__()
        self.logger = logger.getChild(self.__class__.__name__)
        self.condition = threading.Condition()
        self.daemon = True
        self._cache = store
        self._driver_config = driver_config
        self._resolver = resolver.DiscoveryEndpointsResolver(self._driver_config)
        self._base_discovery_interval = 60
        self._ready_timeout = 4
        self._discovery_request_timeout = 2
        self._should_stop = threading.Event()
        self._max_size = 9
        self._base_emergency_retry_interval = 1
        self._ssl_required = False
        if driver_config.root_certificates is not None or driver_config.secure_channel:
            self._ssl_required = True

    def discovery_debug_details(self):
        return self._resolver.debug_details()

    def _emergency_retry_interval(self):
        return (1 + random.random()) * self._base_emergency_retry_interval

    def _discovery_interval(self):
        return (1 + random.random()) * self._base_discovery_interval

    def notify_disconnected(self):
        self._send_wake_up()

    def _send_wake_up(self):
        acquired = self.condition.acquire(blocking=False)

        if not acquired:
            return

        self.condition.notify_all()
        self.condition.release()

    def _handle_empty_database(self):
        if self._cache.size > 0:
            return True

        return self._cache.add(
            connection_impl.Connection.ready_factory(
                self._driver_config.endpoint, self._driver_config, self._ready_timeout
            )
        )

    def execute_discovery(self):
        if self._driver_config.database is None:
            return self._handle_empty_database()

        with self._resolver.context_resolve() as resolve_details:
            if resolve_details is None:
                return False

            resolved_endpoints = set(
                endpoint
                for resolved_endpoint in resolve_details.endpoints
                for endpoint, endpoint_options in resolved_endpoint.endpoints_with_options()
            )
            for cached_endpoint in self._cache.values():
                if cached_endpoint.endpoint not in resolved_endpoints:
                    self._cache.make_outdated(cached_endpoint)

            for resolved_endpoint in resolve_details.endpoints:
                if self._ssl_required and not resolved_endpoint.ssl:
                    continue

                if not self._ssl_required and resolved_endpoint.ssl:
                    continue

                preferred = resolve_details.self_location == resolved_endpoint.location

                for (
                    endpoint,
                    endpoint_options,
                ) in resolved_endpoint.endpoints_with_options():
                    if self._cache.size >= self._max_size or self._cache.already_exists(endpoint):
                        continue

                    ready_connection = connection_impl.Connection.ready_factory(
                        endpoint,
                        self._driver_config,
                        self._ready_timeout,
                        endpoint_options=endpoint_options,
                    )
                    self._cache.add(ready_connection, preferred)

        self._cache.cleanup_outdated()

        return self._cache.size > 0

    def stop(self):
        self._should_stop.set()
        self._send_wake_up()

    def run(self):
        with self.condition:
            while True:
                if self._should_stop.is_set():
                    break

                successful = self.execute_discovery()
                if successful:
                    self._cache.complete_discovery(None)
                else:
                    self._cache.complete_discovery(issues.ConnectionFailure(str(self.discovery_debug_details())))

                if self._should_stop.is_set():
                    break

                interval = self._discovery_interval() if successful else self._emergency_retry_interval()
                self.condition.wait(interval)

            self._cache.cleanup()
        self.logger.info("Successfully terminated discovery process")


class IConnectionPool(abc.ABC):
    @abstractmethod
    def __init__(self, driver_config):
        """
        An object that encapsulates discovery logic and provides ability to execute user requests
        on discovered endpoints.
        :param driver_config: An instance of DriverConfig
        """
        pass

    @abstractmethod
    def stop(self, timeout=10):
        """
        Stops underlying discovery process and cleanups
        :param timeout: A timeout to wait for stop completion
        :return: None
        """
        pass

    @abstractmethod
    def wait(self, timeout=None, fail_fast=False):
        """
        Waits for endpoints to be are available to serve user requests
        :param timeout: A timeout to wait in seconds
        :param fail_fast: Should wait fail fast?
        :return: None
        """

    @abstractmethod
    def discovery_debug_details(self):
        """
        Returns debug string about last errors
        :return:
        """
        pass

    @abstractmethod
    def __call__(
        self,
        request,
        stub,
        rpc_name,
        wrap_result=None,
        settings=None,
        wrap_args=(),
        preferred_endpoint=None,
    ):
        """
        Sends request constructed by client library
        :param request: A request constructed by client
        :param stub: A stub instance to wrap channel
        :param rpc_name: A name of RPC to be executed
        :param wrap_result: A callable that intercepts call and wraps received response
        :param settings: An instance of BaseRequestSettings that can be used
        for RPC metadata construction
        :param wrap_args: And arguments to be passed into wrap_result callable
        :return: A result of computation
        """
        pass


class ConnectionPool(IConnectionPool):
    def __init__(self, driver_config):
        """
        An object that encapsulates discovery logic and provides ability to execute user requests
        on discovered endpoints.

        :param driver_config: An instance of DriverConfig
        """
        self._driver_config = driver_config
        self._store = ConnectionsCache(driver_config.use_all_nodes, driver_config.tracer)
        self.tracer = driver_config.tracer
        self._grpc_init = connection_impl.Connection(self._driver_config.endpoint, self._driver_config)
        self._discovery_thread = Discovery(self._store, self._driver_config)
        self._discovery_thread.start()
        self._stopped = False
        self._stop_guard = threading.Lock()

    def stop(self, timeout=10):
        """
        Stops underlying discovery process and cleanups

        :param timeout: A timeout to wait for stop completion
        :return: None
        """
        with self._stop_guard:
            if self._stopped:
                return

            self._stopped = True
            self._discovery_thread.stop()
        self._grpc_init.close()
        self._discovery_thread.join(timeout)

    def async_wait(self, fail_fast=False):
        """
        Returns a future to subscribe on endpoints availability.

        :return: A concurrent.futures.Future instance.
        """
        if fail_fast:
            return self._store.add_fast_fail()
        return self._store.subscribe()

    def wait(self, timeout=None, fail_fast=False):
        """
        Waits for endpoints to be are available to serve user requests

        :param timeout: A timeout to wait in seconds
        :return: None
        """
        if fail_fast:
            self._store.add_fast_fail().result(timeout)
        else:
            self._store.subscribe().result(timeout)

    def _on_disconnected(self, connection):
        """
        Removes bad discovered endpoint and triggers discovery process

        :param connection: A disconnected connection
        :return: None
        """
        connection.close()
        self._discovery_thread.notify_disconnected()

    def discovery_debug_details(self):
        return self._discovery_thread.discovery_debug_details()

    @tracing.with_trace()
    def __call__(
        self,
        request,
        stub,
        rpc_name,
        wrap_result=None,
        settings=None,
        wrap_args=(),
        preferred_endpoint=None,
    ):
        """
        Synchronously sends request constructed by client library

        :param request: A request constructed by client
        :param stub: A stub instance to wrap channel
        :param rpc_name: A name of RPC to be executed
        :param wrap_result: A callable that intercepts call and wraps received response
        :param settings: An instance of BaseRequestSettings that can be used
        for RPC metadata construction
        :param wrap_args: And arguments to be passed into wrap_result callable

        :return: A result of computation
        """
        tracing.trace(self.tracer, {"request": request, "stub": stub, "rpc_name": rpc_name})
        try:
            connection = self._store.get(preferred_endpoint)
        except Exception:
            self._discovery_thread.notify_disconnected()
            raise

        res = connection(
            request,
            stub,
            rpc_name,
            wrap_result,
            settings,
            wrap_args,
            lambda: self._on_disconnected(connection),
        )
        tracing.trace(self.tracer, {"response": res}, trace_level=tracing.TraceLevel.DEBUG)
        return res

    @_utilities.wrap_async_call_exceptions
    def future(
        self,
        request,
        stub,
        rpc_name,
        wrap_result=None,
        settings=None,
        wrap_args=(),
        preferred_endpoint=None,
    ):
        """
        Sends request constructed by client

        :param request: A request constructed by client
        :param stub: A stub instance to wrap channel
        :param rpc_name: A name of RPC to be executed
        :param wrap_result: A callable that intercepts call and wraps received response
        :param settings: An instance of BaseRequestSettings that can be used\
        for RPC metadata construction
        :param wrap_args: And arguments to be passed into wrap_result callable

        :return: A future of computation
        """
        try:
            connection = self._store.get(preferred_endpoint)
        except Exception:
            self._discovery_thread.notify_disconnected()
            raise

        return connection.future(
            request,
            stub,
            rpc_name,
            wrap_result,
            settings,
            wrap_args,
            lambda: self._on_disconnected(connection),
        )

    def __enter__(self):
        """
        In some cases (scripts, for example) this context manager can be used.

        :return:
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()
