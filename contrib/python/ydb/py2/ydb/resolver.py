# -*- coding: utf-8 -*-
import contextlib
import logging
import threading
import random
import itertools
from . import connection as conn_impl, issues, settings as settings_impl, _apis

logger = logging.getLogger(__name__)


class EndpointInfo(object):
    __slots__ = (
        "address",
        "endpoint",
        "location",
        "port",
        "ssl",
        "ipv4_addrs",
        "ipv6_addrs",
        "ssl_target_name_override",
        "node_id",
    )

    def __init__(self, endpoint_info):
        self.address = endpoint_info.address
        self.endpoint = "%s:%s" % (endpoint_info.address, endpoint_info.port)
        self.location = endpoint_info.location
        self.port = endpoint_info.port
        self.ssl = endpoint_info.ssl
        self.ipv4_addrs = tuple(endpoint_info.ip_v4)
        self.ipv6_addrs = tuple(endpoint_info.ip_v6)
        self.ssl_target_name_override = endpoint_info.ssl_target_name_override
        self.node_id = endpoint_info.node_id

    def endpoints_with_options(self):
        ssl_target_name_override = None
        if self.ssl:
            if self.ssl_target_name_override:
                ssl_target_name_override = self.ssl_target_name_override
            elif self.ipv6_addrs or self.ipv4_addrs:
                ssl_target_name_override = self.address

        endpoint_options = conn_impl.EndpointOptions(
            ssl_target_name_override=ssl_target_name_override, node_id=self.node_id
        )

        if self.ipv6_addrs or self.ipv4_addrs:
            for ipv6addr in self.ipv6_addrs:
                yield ("ipv6:[%s]:%s" % (ipv6addr, self.port), endpoint_options)
            for ipv4addr in self.ipv4_addrs:
                yield ("ipv4:%s:%s" % (ipv4addr, self.port), endpoint_options)
        else:
            yield (self.endpoint, endpoint_options)

    def __str__(self):
        return "<Endpoint %s, location %s, ssl: %s>" % (
            self.endpoint,
            self.location,
            self.ssl,
        )

    def __repr__(self):
        return self.__str__()

    def __hash__(self):
        return hash(self.endpoint)

    def __eq__(self, other):
        if not hasattr(other, "endpoint"):
            return False

        return self.endpoint == other.endpoint


def _list_endpoints_request_factory(connection_params):
    request = _apis.ydb_discovery.ListEndpointsRequest()
    request.database = connection_params.database
    return request


class DiscoveryResult(object):
    def __init__(self, self_location, endpoints):
        self.self_location = self_location
        self.endpoints = endpoints

    def __str__(self):
        return "DiscoveryResult <self_location: %s, endpoints %s>" % (
            self.self_location,
            self.endpoints,
        )

    def __repr__(self):
        return self.__str__()

    @classmethod
    def from_response(cls, rpc_state, response, use_all_nodes=False):
        issues._process_response(response.operation)
        message = _apis.ydb_discovery.ListEndpointsResult()
        response.operation.result.Unpack(message)
        unique_local_endpoints = set()
        unique_different_endpoints = set()
        for info in message.endpoints:
            if info.location == message.self_location:
                unique_local_endpoints.add(EndpointInfo(info))
            else:
                unique_different_endpoints.add(EndpointInfo(info))

        result = []
        unique_local_endpoints = list(unique_local_endpoints)
        unique_different_endpoints = list(unique_different_endpoints)
        if use_all_nodes:
            result.extend(unique_local_endpoints)
            result.extend(unique_different_endpoints)
            random.shuffle(result)
        else:
            random.shuffle(unique_local_endpoints)
            random.shuffle(unique_different_endpoints)
            result.extend(unique_local_endpoints)
            result.extend(unique_different_endpoints)

        return cls(message.self_location, result)


class DiscoveryEndpointsResolver(object):
    def __init__(self, driver_config):
        self.logger = logger.getChild(self.__class__.__name__)
        self._driver_config = driver_config
        self._ready_timeout = getattr(
            self._driver_config, "discovery_request_timeout", 10
        )
        self._lock = threading.Lock()
        self._debug_details_history_size = 20
        self._debug_details_items = []
        self._endpoints = []
        self._endpoints.append(driver_config.endpoint)
        self._endpoints.extend(driver_config.endpoints)
        random.shuffle(self._endpoints)
        self._endpoints_iter = itertools.cycle(self._endpoints)

    def _add_debug_details(self, message, *args):
        self.logger.debug(message, *args)
        message = message % args
        with self._lock:
            self._debug_details_items.append(message)
            if len(self._debug_details_items) > self._debug_details_history_size:
                self._debug_details_items.pop()

    def debug_details(self):
        """
        Returns last resolver errors as a debug string.
        """
        with self._lock:
            return "\n".join(self._debug_details_items)

    def resolve(self):
        with self.context_resolve() as result:
            return result

    @contextlib.contextmanager
    def context_resolve(self):
        self.logger.debug("Preparing initial endpoint to resolve endpoints")
        endpoint = next(self._endpoints_iter)
        initial = conn_impl.Connection.ready_factory(
            endpoint, self._driver_config, ready_timeout=self._ready_timeout
        )
        if initial is None:
            self._add_debug_details(
                'Failed to establish connection to YDB discovery endpoint: "%s". Check endpoint correctness.'
                % endpoint
            )
            yield
            return

        self.logger.debug(
            "Resolving endpoints for database %s", self._driver_config.database
        )
        try:
            resolved = initial(
                _list_endpoints_request_factory(self._driver_config),
                _apis.DiscoveryService.Stub,
                _apis.DiscoveryService.ListEndpoints,
                DiscoveryResult.from_response,
                settings=settings_impl.BaseRequestSettings().with_timeout(
                    self._ready_timeout
                ),
                wrap_args=(self._driver_config.use_all_nodes,),
            )

            self._add_debug_details(
                "Resolved endpoints for database %s: %s",
                self._driver_config.database,
                resolved,
            )

            yield resolved
        except Exception as e:

            self._add_debug_details(
                'Failed to resolve endpoints for database %s. Endpoint: "%s". Error details:\n %s',
                self._driver_config.database,
                endpoint,
                e,
            )

            yield

        finally:
            initial.close()
