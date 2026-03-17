class RPCRequest(object):
    def __init__(self):
        self.unique_id = None
        """A unique ID to remember the request by. Protocol specific, may or
        may not be set. This value should only be set by
        :py:func:`~tinyrpc.RPCProtocol.create_request`.

        The ID allows client to receive responses out-of-order and still allocate
        them to the correct request.

        Only supported if the parent protocol has
        :py:attr:`~tinyrpc.RPCProtocol.supports_out_of_order` set to ``True``.
        """

        self.method = None
        """The name of the method to be called."""

        self.args = []
        """The positional arguments of the method call."""

        self.kwargs = {}
        """The keyword arguments of the method call."""

    def error_respond(self, error):
        """Creates an error response.

        Create a response indicating that the request was parsed correctly,
        but an error has occured trying to fulfill it.

        :param error: An exception or a string describing the error.

        :return: A response or ``None`` to indicate that no error should be sent
                 out.
        """
        raise NotImplementedError()

    def respond(self, result):
        """Create a response.

        Call this to return the result of a successful method invocation.

        This creates and returns an instance of a protocol-specific subclass of
        :py:class:`~tinyrpc.RPCResponse`.

        :param result: Passed on to new response instance.

        :return: A response or ``None`` to indicate this request does not expect a
                 response.
        """
        raise NotImplementedError()

    def serialize(self):
        """Returns a serialization of the request.

        :return: A string to be passed on to a transport.
        """
        raise NotImplementedError()


class RPCBatchRequest(list):
    """Multiple requests batched together.

    A batch request is a subclass of :py:class:`list`. Protocols that support
    multiple requests in a single message use this to group them together.

    Handling a batch requests is done in any order, responses must be gathered
    in a batch response and be in the same order as their respective requests.

    Any item of a batch request is either a request or a subclass of
    :py:class:`~tinyrpc.BadRequestError`, which indicates that there has been
    an error in parsing the request.
    """

    def create_batch_response(self):
        """Creates a response suitable for responding to this request.

        :return: An :py:class:`~tinyrpc.RPCBatchResponse` or ``None``, if no
                 response is expected."""
        raise NotImplementedError()

    def serialize(self):
        raise NotImplementedError()


class RPCResponse(object):
    """RPC call response class.

    Base class for all deriving responses.

    Has an attribute ``result`` containing the result of the RPC call, unless
    an error occured, in which case an attribute ``error`` will contain the
    error message."""

    def __init__(self):
        self.unique_id = None

    def serialize(self):
        """Returns a serialization of the response.

        :return: A reply to be passed on to a transport.
        """
        raise NotImplementedError()


class RPCErrorResponse(RPCResponse):
    pass


class RPCBatchResponse(list):
    """Multiple response from a batch request. See
    :py:class:`~tinyrpc.RPCBatchRequest` on how to handle.

    Items in a batch response need to be
    :py:class:`~tinyrpc.RPCResponse` instances or None, meaning no reply should
    generated for the request.
    """

    def serialize(self):
        """Returns a serialization of the batch response."""
        raise NotImplementedError()


class RPCProtocol(object):
    """Base class for all protocol implementations."""

    supports_out_of_order = False
    """If true, this protocol can receive responses out of order correctly.

    Note that this usually depends on the generation of unique_ids, the
    generation of these may or may not be thread safe, depending on the
    protocol. Ideally, only once instance of RPCProtocol should be used per
    client."""

    raises_errors = True
    """If True, this protocol instance will raise an RPCError exception.

    On receipt of an RPCErrorResponse instance an RPCError exception is raised.
    When this flag is False the RPCErrorResponse object is returned to the caller
    which is then responsible for handling the error."""

    def create_request(self, method, args=None, kwargs=None, one_way=False):
        """Creates a new RPCRequest object.

        It is up to the implementing protocol whether or not ``args``,
        ``kwargs``, one of these, both at once or none of them are supported.

        :param method: The method name to invoke.
        :param args: The positional arguments to call the method with.
        :param kwargs: The keyword arguments to call the method with.
        :param one_way: The request is an update, i.e. it does not expect a
                        reply.
        :return: A new :py:class:`~tinyrpc.RPCRequest` instance.
        """
        raise NotImplementedError()

    def parse_request(self, data):
        """Parses a request given as a string and returns an
        :py:class:`RPCRequest` instance.

        :return: An instanced request.
        """
        raise NotImplementedError()

    def parse_reply(self, data):
        """Parses a reply and returns an :py:class:`RPCResponse` instance.

        :return: An instanced response.
        """
        raise NotImplementedError()


class RPCBatchProtocol(RPCProtocol):
    def create_batch_request(self, requests=None):
        """Create a new :py:class:`tinyrpc.RPCBatchRequest` object.

        :param requests: A list of requests.
        """
        raise NotImplementedError()
