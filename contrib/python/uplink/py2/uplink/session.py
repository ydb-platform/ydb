# Local imports
from uplink import arguments


class Session(object):
    """
    The session of a :class:`~uplink.Consumer` instance.

    Exposes the configuration of a :class:`~uplink.Consumer` instance and
    allows the persistence of certain properties across all requests sent
    from that instance.
    """

    def __init__(self, builder):
        self.__builder = builder
        self.__params = None
        self.__headers = None
        self.__context = None

    def create(self, consumer, definition):
        return self.__builder.build(definition, consumer)

    @property
    def base_url(self):
        """
        The base URL for any requests sent from this consumer instance.
        """
        return self.__builder.base_url

    @property
    def headers(self):
        """
        A dictionary of headers to be sent on each request from this
        consumer instance.
        """
        if self.__headers is None:
            self.__headers = {}
            self.inject(arguments.HeaderMap().with_value(self.__headers))
        return self.__headers

    @property
    def params(self):
        """
        A dictionary of querystring data to attach to each request from
        this consumer instance.
        """
        if self.__params is None:
            self.__params = {}
            self.inject(arguments.QueryMap().with_value(self.__params))
        return self.__params

    @property
    def context(self):
        """
        A dictionary of name-value pairs that are made available to
        request middleware.
        """
        if self.__context is None:
            self.__context = {}
            self.inject(arguments.ContextMap().with_value(self.__context))
        return self.__context

    @property
    def auth(self):
        """The authentication object for this consumer instance."""
        return self.__builder.auth

    @auth.setter
    def auth(self, auth):
        self.__builder.auth = auth

    def inject(self, hook, *more_hooks):
        """
        Add hooks (e.g., functions decorated with either
        :class:`~uplink.response_handler` or
        :class:`~uplink.error_handler`) to the session.
        """
        self.__builder.add_hook(hook, *more_hooks)
