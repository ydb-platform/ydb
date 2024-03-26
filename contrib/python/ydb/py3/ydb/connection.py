# -*- coding: utf-8 -*-
import logging
import copy
import typing
from concurrent import futures
import uuid
import threading
import collections

from google.protobuf import text_format
import grpc
from . import issues, _apis, _utilities
from . import default_pem

_stubs_list = (
    _apis.TableService.Stub,
    _apis.SchemeService.Stub,
    _apis.DiscoveryService.Stub,
    _apis.CmsService.Stub,
)

logger = logging.getLogger(__name__)
DEFAULT_TIMEOUT = 600
YDB_DATABASE_HEADER = "x-ydb-database"
YDB_TRACE_ID_HEADER = "x-ydb-trace-id"
YDB_REQUEST_TYPE_HEADER = "x-ydb-request-type"

_DEFAULT_MAX_GRPC_MESSAGE_SIZE = 64 * 10**6


def _message_to_string(message):
    """
    Constructs a string representation of provided message or generator
    :param message: A protocol buffer or generator instance
    :return: A string
    """
    try:
        return text_format.MessageToString(message, as_one_line=True)
    except Exception:
        return str(message)


def _log_response(rpc_state, response):
    """
    Writes a message with response into debug logs
    :param rpc_state: A state of rpc
    :param response: A received response
    :return: None
    """
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("%s: response = { %s }", rpc_state, _message_to_string(response))


def _log_request(rpc_state, request):
    """
    Writes a message with request into debug logs
    :param rpc_state: An id of request
    :param request: A received response
    :return: None
    """
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("%s: request = { %s }", rpc_state, _message_to_string(request))


def _rpc_error_handler(
    rpc_state,
    rpc_error: typing.Union[grpc.RpcError, grpc.aio.AioRpcError, grpc.Call, grpc.aio.Call],
    on_disconnected: typing.Callable[[], None] = None,
):
    """
    RPC call error handler, that translates gRPC error into YDB issue
    :param rpc_state: A state of rpc
    :param rpc_error: an underlying rpc error to handle
    :param on_disconnected: a handler to call on disconnected connection
    """
    logger.info("%s: received error, %s", rpc_state, rpc_error)
    if isinstance(rpc_error, (grpc.RpcError, grpc.aio.AioRpcError, grpc.Call, grpc.aio.Call)):
        if rpc_error.code() == grpc.StatusCode.UNAUTHENTICATED:
            return issues.Unauthenticated(rpc_error.details())
        elif rpc_error.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
            return issues.DeadlineExceed("Deadline exceeded on request")
        elif rpc_error.code() == grpc.StatusCode.UNIMPLEMENTED:
            return issues.Unimplemented("Method or feature is not implemented on server!")

    logger.debug("%s: unhandled rpc error, disconnecting channel", rpc_state)
    if on_disconnected is not None:
        on_disconnected()

    return issues.ConnectionLost("Rpc error, reason %s" % str(rpc_error))


def _on_response_callback(rpc_state, call_state_unref, wrap_result=None, on_disconnected=None, wrap_args=()):
    """
    Callback to be executed on received RPC response
    :param rpc_state: A name of RPC
    :param wrap_result: A callable that wraps received response
    :param on_disconnected: A handler to executed on disconnected channel
    :param wrap_args: An arguments to be passed into wrap result callable
    :return: None
    """
    try:
        logger.debug("%s: on response callback started", rpc_state)
        response = rpc_state.rendezvous.result()
        _log_response(rpc_state, response)
        response = response if wrap_result is None else wrap_result(rpc_state, response, *wrap_args)
        rpc_state.result_future.set_result(response)
        logger.debug("%s: on response callback success", rpc_state)
    except grpc.FutureCancelledError as e:
        logger.debug("%s: request execution cancelled", rpc_state)
        if not rpc_state.result_future.cancelled():
            rpc_state.result_future.set_exception(e)

    except grpc.RpcError as rpc_call_error:
        rpc_state.result_future.set_exception(_rpc_error_handler(rpc_state, rpc_call_error, on_disconnected))

    except issues.Error as e:
        logger.info("%s: received exception, %s", rpc_state, str(e))
        rpc_state.result_future.set_exception(e)

    except Exception as e:
        logger.error("%s: received exception, %s", rpc_state, str(e))
        rpc_state.result_future.set_exception(issues.ConnectionLost(str(e)))

    call_state_unref()


def _construct_metadata(driver_config, settings):
    """
    Translates request settings into RPC metadata
    :param driver_config: A driver config
    :param settings: An instance of BaseRequestSettings
    :return: RPC metadata
    """
    metadata = []
    if driver_config.database is not None:
        metadata.append((YDB_DATABASE_HEADER, driver_config.database))

    need_rpc_auth = getattr(settings, "need_rpc_auth", True)
    if driver_config.credentials is not None and need_rpc_auth:
        metadata.extend(driver_config.credentials.auth_metadata())

    if settings is not None:
        if settings.trace_id is not None:
            metadata.append((YDB_TRACE_ID_HEADER, settings.trace_id))
        if settings.request_type is not None:
            metadata.append((YDB_REQUEST_TYPE_HEADER, settings.request_type))
        metadata.extend(getattr(settings, "headers", []))

    metadata.append(_utilities.x_ydb_sdk_build_info_header())
    return metadata


def _get_request_timeout(settings):
    """
    Extracts RPC timeout from request settings
    :param settings: an instance of BaseRequestSettings
    :return: timeout of RPC execution
    """
    if settings is None or settings.timeout is None:
        return DEFAULT_TIMEOUT
    return settings.timeout


class EndpointOptions(object):
    __slots__ = ("ssl_target_name_override", "node_id")

    def __init__(self, ssl_target_name_override=None, node_id=None):
        self.ssl_target_name_override = ssl_target_name_override
        self.node_id = node_id


def _construct_channel_options(driver_config, endpoint_options=None):
    """
    Constructs gRPC channel initialization options
    :param driver_config: A driver config instance
    :param endpoint_options: Endpoint options
    :return: A channel initialization options
    """
    _default_connect_options = [
        ("grpc.max_receive_message_length", _DEFAULT_MAX_GRPC_MESSAGE_SIZE),
        ("grpc.max_send_message_length", _DEFAULT_MAX_GRPC_MESSAGE_SIZE),
        ("grpc.primary_user_agent", driver_config.primary_user_agent),
        (
            "grpc.lb_policy_name",
            getattr(driver_config, "grpc_lb_policy_name", "round_robin"),
        ),
    ]
    if driver_config.grpc_keep_alive_timeout is not None:
        _default_connect_options.extend(
            [
                ("grpc.keepalive_time_ms", driver_config.grpc_keep_alive_timeout >> 3),
                ("grpc.keepalive_timeout_ms", driver_config.grpc_keep_alive_timeout),
                ("grpc.http2.max_pings_without_data", 0),
                ("grpc.keepalive_permit_without_calls", 0),
            ]
        )
    if endpoint_options is not None:
        if endpoint_options.ssl_target_name_override:
            _default_connect_options.append(
                (
                    "grpc.ssl_target_name_override",
                    endpoint_options.ssl_target_name_override,
                )
            )
    if driver_config.channel_options is None:
        return _default_connect_options
    channel_options = copy.deepcopy(driver_config.channel_options)
    custom_options_keys = set(i[0] for i in driver_config.channel_options)
    for item in filter(lambda x: x[0] not in custom_options_keys, _default_connect_options):
        channel_options.append(item)
    return channel_options


class _RpcState(object):
    __slots__ = (
        "rpc",
        "request_id",
        "result_future",
        "rpc_name",
        "endpoint",
        "rendezvous",
        "metadata_kv",
        "endpoint_key",
    )

    def __init__(self, stub_instance, rpc_name, endpoint, endpoint_key):
        """Stores all RPC related data"""
        self.rpc_name = rpc_name
        self.rpc = getattr(stub_instance, rpc_name)
        self.request_id = uuid.uuid4()
        self.endpoint = endpoint
        self.rendezvous = None
        self.metadata_kv = None
        self.endpoint_key = endpoint_key

    def __str__(self):
        return "RpcState(%s, %s, %s)" % (self.rpc_name, self.request_id, self.endpoint)

    def __call__(self, *args, **kwargs):
        """Execute a RPC."""
        try:
            response, rendezvous = self.rpc.with_call(*args, **kwargs)
            self.rendezvous = rendezvous
            return response
        except AttributeError:
            return self.rpc(*args, **kwargs)

    def trailing_metadata(self):
        """Trailing metadata of the call."""
        if self.metadata_kv is None:

            self.metadata_kv = collections.defaultdict(set)
            for metadatum in self.rendezvous.trailing_metadata():
                self.metadata_kv[metadatum.key].add(metadatum.value)

        return self.metadata_kv

    def future(self, *args, **kwargs):
        self.rendezvous = self.rpc.future(*args, **kwargs)
        self.result_future = futures.Future()

        def _cancel_callback(f):
            """forwards cancel to gPRC future"""
            if f.cancelled():
                self.rendezvous.cancel()

        self.rendezvous.add_done_callback(_cancel_callback)
        return self.rendezvous, self.result_future


_nanos_in_second = 10**9


def _set_duration(duration_value, seconds_float):
    duration_value.seconds = int(seconds_float)
    duration_value.nanos = int((seconds_float - int(seconds_float)) * _nanos_in_second)
    return duration_value


def _set_server_timeouts(request, settings, default_value):
    if not hasattr(request, "operation_params"):
        return

    operation_timeout = getattr(settings, "operation_timeout", default_value)
    operation_timeout = default_value if operation_timeout is None else operation_timeout
    cancel_after = getattr(settings, "cancel_after", default_value)
    cancel_after = default_value if cancel_after is None else cancel_after
    _set_duration(request.operation_params.operation_timeout, operation_timeout)
    _set_duration(request.operation_params.cancel_after, cancel_after)


def channel_factory(endpoint, driver_config, channel_provider=None, endpoint_options=None):
    channel_provider = channel_provider if channel_provider is not None else grpc
    options = _construct_channel_options(driver_config, endpoint_options)
    logger.debug("Channel options: {}".format(options))

    if driver_config.root_certificates is None and not driver_config.secure_channel:
        return channel_provider.insecure_channel(
            endpoint, options, compression=getattr(driver_config, "compression", None)
        )

    root_certificates = driver_config.root_certificates
    if root_certificates is None:
        root_certificates = default_pem.load_default_pem()
    credentials = grpc.ssl_channel_credentials(
        root_certificates, driver_config.private_key, driver_config.certificate_chain
    )
    return channel_provider.secure_channel(
        endpoint,
        credentials,
        options,
        compression=getattr(driver_config, "compression", None),
    )


class EndpointKey(object):
    __slots__ = ("endpoint", "node_id")

    def __init__(self, endpoint, node_id):
        self.endpoint = endpoint
        self.node_id = node_id


class Connection(object):
    __slots__ = (
        "endpoint",
        "_channel",
        "_call_states",
        "_stub_instances",
        "_driver_config",
        "_cleanup_callbacks",
        "__weakref__",
        "lock",
        "calls",
        "closing",
        "endpoint_key",
        "node_id",
    )

    def __init__(self, endpoint, driver_config=None, endpoint_options=None):
        """
        Object that wraps gRPC channel and encapsulates gRPC request execution logic
        :param endpoint: endpoint to connect (in pattern host:port), constructed by user or
        discovered by the YDB endpoint discovery mechanism
        :param driver_config: A driver config instance to be used for RPC call interception
        """
        global _stubs_list
        self.endpoint = endpoint
        self.node_id = getattr(endpoint_options, "node_id", None)
        self.endpoint_key = EndpointKey(endpoint, getattr(endpoint_options, "node_id", None))
        self._channel = channel_factory(self.endpoint, driver_config, endpoint_options=endpoint_options)
        self._driver_config = driver_config
        self._call_states = {}
        self._stub_instances = {}
        self._cleanup_callbacks = []
        # pre-initialize stubs
        for stub in _stubs_list:
            self._stub_instances[stub] = stub(self._channel)
        self.lock = threading.RLock()
        self.calls = 0
        self.closing = False

    def _prepare_stub_instance(self, stub):
        if stub not in self._stub_instances:
            self._stub_instances[stub] = stub(self._channel)

    def add_cleanup_callback(self, callback):
        self._cleanup_callbacks.append(callback)

    def _prepare_call(self, stub, rpc_name, request, settings):
        timeout, metadata = _get_request_timeout(settings), _construct_metadata(self._driver_config, settings)
        _set_server_timeouts(request, settings, timeout)
        self._prepare_stub_instance(stub)
        rpc_state = _RpcState(self._stub_instances[stub], rpc_name, self.endpoint, self.endpoint_key)
        logger.debug("%s: creating call state", rpc_state)
        with self.lock:
            if self.closing:
                raise issues.ConnectionLost("Couldn't start call")
            self.calls += 1
            self._call_states[rpc_state.request_id] = rpc_state
        # Call successfully prepared and registered
        _log_request(rpc_state, request)
        return rpc_state, timeout, metadata

    def _finish_call(self, call_state):
        with self.lock:
            self.calls -= 1
            self._call_states.pop(call_state.request_id, None)
            # Call successfully finished
            if self.closing and self.calls == 0:
                # Channel is closing and we have to destroy channel
                self.destroy()

    def future(
        self,
        request,
        stub,
        rpc_name,
        wrap_result=None,
        settings=None,
        wrap_args=(),
        on_disconnected=None,
    ):
        """
        Sends request constructed by client
        :param request: A request constructed by client
        :param stub: A stub instance to wrap channel
        :param rpc_name: A name of RPC to be executed
        :param wrap_result: A callable that intercepts call and wraps received response
        :param settings: An instance of BaseRequestSettings that can be used
        for RPC metadata construction
        :param on_disconnected: A callable to be executed when underlying channel becomes disconnected
        :param wrap_args: And arguments to be passed into wrap_result callable
        :return: A future of computation
        """
        rpc_state, timeout, metadata = self._prepare_call(stub, rpc_name, request, settings)
        rendezvous, result_future = rpc_state.future(
            request,
            timeout,
            metadata,
            compression=getattr(settings, "compression", None),
        )
        rendezvous.add_done_callback(
            lambda resp_future: _on_response_callback(
                rpc_state,
                lambda: self._finish_call(rpc_state),
                wrap_result,
                on_disconnected,
                wrap_args,
            )
        )
        return result_future

    def __call__(
        self,
        request,
        stub,
        rpc_name,
        wrap_result=None,
        settings=None,
        wrap_args=(),
        on_disconnected=None,
    ):
        """
        Synchronously sends request constructed by client library
        :param request: A request constructed by client
        :param stub: A stub instance to wrap channel
        :param rpc_name: A name of RPC to be executed
        :param wrap_result: A callable that intercepts call and wraps received response
        :param settings: An instance of BaseRequestSettings that can be used
        for RPC metadata construction
        :param on_disconnected: A callable to be executed when underlying channel becomes disconnected
        :param wrap_args: And arguments to be passed into wrap_result callable
        :return: A result of computation
        """
        rpc_state, timeout, metadata = self._prepare_call(stub, rpc_name, request, settings)
        try:
            response = rpc_state(
                request,
                timeout,
                metadata,
                compression=getattr(settings, "compression", None),
            )
            _log_response(rpc_state, response)
            return response if wrap_result is None else wrap_result(rpc_state, response, *wrap_args)
        except grpc.RpcError as rpc_error:
            raise _rpc_error_handler(rpc_state, rpc_error, on_disconnected)
        finally:
            self._finish_call(rpc_state)

    @classmethod
    def ready_factory(cls, endpoint, driver_config, ready_timeout=10, endpoint_options=None):
        candidate = cls(endpoint, driver_config, endpoint_options=endpoint_options)
        ready_future = candidate.ready_future()
        try:
            ready_future.result(timeout=ready_timeout)
            return candidate
        except grpc.FutureTimeoutError:
            ready_future.cancel()
            candidate.close()
            return None

        except Exception:
            candidate.close()
            return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """
        Closes the underlying gRPC channel
        :return: None
        """
        logger.info("Closing channel for endpoint %s", self.endpoint)
        with self.lock:
            self.closing = True

            for callback in self._cleanup_callbacks:
                callback(self)

            # potentially we should cancel in-flight calls here but currently
            # it is not required since gRPC can successfully cancel these calls manually.

            if self.calls == 0:
                # everything is cancelled/completed and channel can be destroyed
                self.destroy()

    def destroy(self):
        if hasattr(self, "_channel") and hasattr(self._channel, "close"):
            self._channel.close()

    def ready_future(self):
        """
        Creates a future that tracks underlying gRPC channel is ready
        :return: A Future object that matures when the underlying channel is ready
        to receive request
        """
        return grpc.channel_ready_future(self._channel)
