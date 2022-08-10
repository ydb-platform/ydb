# -*- coding: utf-8 -*-


class BaseRequestSettings(object):
    __slots__ = (
        "trace_id",
        "request_type",
        "timeout",
        "cancel_after",
        "operation_timeout",
        "tracer",
        "compression",
        "headers",
        "need_rpc_auth",
    )

    def __init__(self):
        """
        Request settings to be used for RPC execution
        """
        self.trace_id = None
        self.request_type = None
        self.timeout = None
        self.cancel_after = None
        self.operation_timeout = None
        self.compression = None
        self.need_rpc_auth = True
        self.headers = []

    def make_copy(self):
        return (
            BaseRequestSettings()
            .with_trace_id(self.trace_id)
            .with_request_type(self.request_type)
            .with_timeout(self.timeout)
            .with_cancel_after(self.cancel_after)
            .with_operation_timeout(self.operation_timeout)
            .with_compression(self.compression)
            .with_need_rpc_auth(self.need_rpc_auth)
        )

    def with_compression(self, compression):
        """
        Enables compression for the specific RPC
        :param compression: An RPCCompression enum value.
        :return The self instance.
        """
        self.compression = compression
        return self

    def with_need_rpc_auth(self, need_rpc_auth):
        self.need_rpc_auth = need_rpc_auth
        return self

    def with_header(self, key, value):
        """
        Adds a key-value pair to the request headers.
        :param key: A string with a header key.
        :param value: A string with a header value.
        :return The self instance.
        """
        self.headers.append((key, value))
        return self

    def with_trace_id(self, trace_id):
        """
        Includes trace id for RPC headers
        :param trace_id: A trace id string
        :return: The self instance
        """
        self.trace_id = trace_id
        return self

    def with_request_type(self, request_type):
        """
        Includes request type for RPC headers
        :param request_type: A request type string
        :return: The self instance
        """
        self.request_type = request_type
        return self

    def with_operation_timeout(self, timeout):
        """
        Indicates that client is no longer interested in the result of operation after the specified duration
        starting from the time operation arrives at the server.
        Server will try to stop the execution of operation and if no result is currently available the operation
        will receive TIMEOUT status code, which will be sent back to client if it was waiting for the operation result.
        Timeout of operation does not tell anything about its result, it might be completed successfully
        or cancelled on server.
        :param timeout:
        :return:
        """
        self.operation_timeout = timeout
        return self

    def with_cancel_after(self, timeout):
        """
        Server will try to cancel the operation after the specified duration starting from the time
        the operation arrives at server.
        In case of successful cancellation operation will receive CANCELLED status code, which will be
        sent back to client if it was waiting for the operation result.
        In case when cancellation isn't possible, no action will be performed.
        :param timeout:
        :return:
        """
        self.cancel_after = timeout
        return self

    def with_timeout(self, timeout):
        """
        Client-side timeout to complete request.
        Since YDB doesn't support request cancellation at this moment, this feature should be
        used properly to avoid server overload.
        :param timeout: timeout value in seconds
        :return: The self instance
        """
        self.timeout = timeout
        return self
