"""
This module implements the built-in class and method decorators and their
handling classes.
"""
# Standard library imports
import functools
import inspect

# Local imports
from uplink import arguments, helpers, hooks, interfaces, utils
from uplink.compat import abc

__all__ = [
    "headers",
    "params",
    "form_url_encoded",
    "multipart",
    "json",
    "timeout",
    "args",
    "response_handler",
    "error_handler",
    "inject",
]


class MethodAnnotationHandlerBuilder(interfaces.AnnotationHandlerBuilder):
    def __init__(self):
        self._class_annotations = list()
        self._method_annotations = list()

    def add_annotation(self, annotation, *args_, **kwargs):
        if kwargs.get("is_class", False):
            self._class_annotations.append(annotation)
        else:
            self._method_annotations.append(annotation)
        super(MethodAnnotationHandlerBuilder, self).add_annotation(annotation)
        return annotation

    def copy(self):
        clone = MethodAnnotationHandlerBuilder()
        clone._class_annotations = list(self._class_annotations)
        clone._method_annotations = list(self._method_annotations)
        return clone

    def build(self):
        return MethodAnnotationHandler(
            self._class_annotations + self._method_annotations
        )


class MethodAnnotationHandler(interfaces.AnnotationHandler):
    def __init__(self, method_annotations):
        self._method_annotations = list(method_annotations)

    @property
    def annotations(self):
        return iter(self._method_annotations)

    def handle_builder(self, request_builder):
        for annotation in self._method_annotations:
            annotation.modify_request(request_builder)


# TODO: Only decorate consumers
class MethodAnnotation(interfaces.Annotation):
    _http_method_blacklist = None
    _http_method_whitelist = None

    @staticmethod
    def _is_consumer_class(c):
        return utils.is_subclass(c, interfaces.Consumer)

    @classmethod
    def supports_http_method(cls, method):
        method = method.upper()
        if cls._http_method_blacklist is not None:
            return method not in cls._http_method_blacklist
        if cls._http_method_whitelist is not None:
            return method in cls._http_method_whitelist
        return True

    @classmethod
    def _is_relevant_for_builder(cls, builder):
        return cls.supports_http_method(builder[1].method)

    @classmethod
    def _is_static_call(cls, *args_, **kwargs):
        if super(MethodAnnotation, cls)._is_static_call(*args_, **kwargs):
            return True
        try:
            is_consumer_class = cls._is_consumer_class(args_[0])
        except IndexError:
            return False
        else:
            return is_consumer_class and not (kwargs or args_[1:])

    def _modify_request_definition(self, builder, kwargs):
        builder.method_handler_builder.add_annotation(self, **kwargs)

    def __call__(self, class_or_builder, **kwargs):
        if self._is_consumer_class(class_or_builder):
            builders = helpers.get_api_definitions(class_or_builder)
            builders = filter(self._is_relevant_for_builder, builders)

            for name, b in builders:
                self(b, is_class=True)
                helpers.set_api_definition(class_or_builder, name, b)
        elif isinstance(class_or_builder, interfaces.RequestDefinitionBuilder):
            self._modify_request_definition(class_or_builder, kwargs)
        return class_or_builder

    def modify_request(self, request_builder):
        pass


class _BaseRequestProperties(MethodAnnotation):
    _property_name = None
    _delimiter = None

    def __init__(self, arg, **kwargs):
        if isinstance(arg, list):
            self._values = dict(self._split(a) for a in arg)
        else:
            self._values = dict(arg, **kwargs)

    def _split(self, arg):
        return map(str.strip, arg.split(self._delimiter))

    def modify_request(self, request_builder):
        """Updates header contents."""
        request_builder.info[self._property_name].update(self._values)


# noinspection PyPep8Naming
class headers(_BaseRequestProperties):
    """
    A decorator that adds static headers for API calls.

    .. code-block:: python

        @headers({"User-Agent": "Uplink-Sample-App"})
        @get("/user")
        def get_user(self):
            \"""Get the current user\"""

    When used as a class decorator, :py:class:`headers` applies to
    all consumer methods bound to the class:

    .. code-block:: python

        @headers({"Accept": "application/vnd.github.v3.full+json"})
        class GitHub(Consumer):
            ...

    :py:class:`headers` takes the same arguments as :py:class:`dict`.

    Args:
        arg: A dict containing header values.
        **kwargs: More header values.
    """

    def __init__(self, arg=None, **kwargs):
        if isinstance(arg, str):
            key, value = self._split(arg)
            arg = {key: value}
        super(headers, self).__init__(arg or {}, **kwargs)

    @property
    def _delimiter(self):
        return ":"

    @property
    def _property_name(self):
        return "headers"


# noinspection PyPep8Naming
class params(_BaseRequestProperties):
    """
    A decorator that adds static query parameters for API calls.

    .. code-block:: python

        @params({"sort": "created"})
        @get("/user")
        def get_user(self):
            \"""Get the current user\"""

    When used as a class decorator, :py:class:`params` applies to
    all consumer methods bound to the class:

    .. code-block:: python

        @params({"client_id": "my-app-client-id"})
        class GitHub(Consumer):
            ...

    :py:class:`params` takes the same arguments as :py:class:`dict`.

    Args:
        arg: A dict containing query parameters.
        **kwargs: More query parameters.
    """

    def __init__(self, arg=None, **kwargs):
        if isinstance(arg, str):
            arg = arg.split("&")
        super(params, self).__init__(arg or {}, **kwargs)

    @property
    def _property_name(self):
        return "params"

    @property
    def _delimiter(self):
        return "="


# noinspection PyPep8Naming
class form_url_encoded(MethodAnnotation):
    """
    URL-encodes the request body.

    Used on POST/PUT/PATCH request. It url-encodes the body of the
    message and sets the appropriate ``Content-Type`` header. Further,
    each field argument should be annotated with
    :py:class:`uplink.Field`.

    Example:
        .. code-block:: python

            @form_url_encoded
            @post("/users/edit")
            def update_user(self, first_name: Field, last_name: Field):
                \"""Update the current user.\"""
    """

    _http_method_blacklist = {"GET"}
    _can_be_static = True

    # XXX: Let `requests` handle building urlencoded syntax.
    # def modify_request(self, request_builder):
    #     request_builder.info.headers(
    #         {"Content-Type": "application/x-www-form-urlencoded"}
    #     )


# noinspection PyPep8Naming
class multipart(MethodAnnotation):
    """
    Sends multipart form data.

    Multipart requests are commonly used to upload files to a server.
    Further, annotate each part argument with :py:class:`Part`.

    Example:

        .. code-block:: python

            @multipart
            @put(/user/photo")
            def update_user(self, photo: Part, description: Part):
                \"""Upload a user profile photo.\"""
    """

    _http_method_blacklist = {"GET"}
    _can_be_static = True

    # XXX: Let `requests` handle building multipart syntax.
    # def modify_request(self, request_builder):
    #     request_builder.info.headers(
    #         {"Content-Type": "multipart/form-data"}
    #     )


# noinspection PyPep8Naming
class json(MethodAnnotation):
    """Use as a decorator to make JSON requests.

    You can annotate a method argument with :py:class:`uplink.Body`,
    which indicates that the argument's value should become the
    request's body. :py:class:`uplink.Body` has to be either a dict or a
    subclass of py:class:`abc.Mapping`.

    Example:
        .. code-block:: python

            @json
            @patch(/user")
            def update_user(self, **info: Body):
                \"""Update the current user.\"""

    You can alternatively use the :py:class:`uplink.Field` annotation to
    specify JSON fields separately, across multiple arguments:

    Example:
        .. code-block:: python

            @json
            @patch(/user")
            def update_user(self, name: Field, email: Field("e-mail")):
                \"""Update the current user.\"""

    Further, to set a nested field, you can specify the path of the
    target field with a tuple of strings as the first argument of
    :py:class:`uplink.Field`.

    Example:
        Consider a consumer method that sends a PATCH request with a JSON
        body of the following format:

        .. code-block:: json
            :emphasize-lines: 3

            {
                user: {
                    name: "<User's Name>"
                },
            }

        The tuple :py:obj:`("user", "name")` specifies the path to the
        highlighted inner field:

        .. code-block:: python
            :emphasize-lines: 5

            @json
            @patch(/user")
            def update_user(
                            self,
                            new_name: Field(("user", "name"))
            ):
                \"""Update the current user.\"""
    """

    _http_method_blacklist = {"GET"}
    _can_be_static = True

    @staticmethod
    def _sequence_path_resolver(path, value, body):
        if not path:
            raise ValueError("Path sequence cannot be empty.")
        for name in path[:-1]:
            body = body.setdefault(name, {})
            if not isinstance(body, abc.Mapping):
                raise ValueError(
                    "Failed to set nested JSON attribute '%s': "
                    "parent field '%s' is not a JSON object." % (path, name)
                )
        body[path[-1]] = value

    def modify_request(self, request_builder):
        """Modifies JSON request."""
        request_builder.add_transaction_hook(self._hook)

    @classmethod
    def set_json_body(cls, request_builder):
        old_body = request_builder.info.pop("data", {})
        if isinstance(old_body, abc.Mapping):
            body = request_builder.info.setdefault("json", {})
            for path in old_body:
                if isinstance(path, tuple):
                    cls._sequence_path_resolver(path, old_body[path], body)
                else:
                    body[path] = old_body[path]
        else:
            request_builder.info.setdefault("json", old_body)

    __hook = None

    @property
    def _hook(self):
        if self.__hook is None:
            self.__hook = hooks.RequestAuditor(self.set_json_body)
        return self.__hook


# noinspection PyPep8Naming
class timeout(MethodAnnotation):
    """
    Time to wait for a server response before giving up.

    When used on other decorators it specifies how long (in secs) a
    decorator should wait before giving up.

    Example:
        .. code-block:: python

            @timeout(60)
            @get("/user/posts")
            def get_posts(self):
                \"""Fetch all posts for the current users.\"""

    When used as a class decorator, :py:class:`timeout` applies to all
    consumer methods bound to the class.

    Args:
        seconds (int): An integer used to indicate how long should the
            request wait.
    """

    def __init__(self, seconds):
        self._seconds = seconds

    def modify_request(self, request_builder):
        """Modifies request timeout."""
        request_builder.info["timeout"] = self._seconds


# noinspection PyPep8Naming
class args(MethodAnnotation):
    """
    Annotate method arguments for Python 2.7 compatibility.

    Arrange annotations in the same order as their corresponding
    function arguments.

    Example:
        .. code-block:: python

            @args(Path, Query)
            @get("/users/{username})
            def get_user(self, username, visibility):
                \"""Get a specific user.\"""

    Use keyword args to target specific method parameters.

    Example:
        .. code-block:: python

            @args(visibility=Query)
            @get("/users/{username})
            def get_user(self, username, visibility):
                \"""Get a specific user.\"""

    Args:
        *annotations: Any number of annotations.
        **more_annotations: More annotations, targeting specific method
           arguments.
    """

    def __init__(self, *annotations, **more_annotations):
        self._annotations = annotations
        self._more_annotations = more_annotations

    def __call__(self, obj):
        if inspect.isfunction(obj):
            handler = arguments.ArgumentAnnotationHandlerBuilder.from_func(obj)
            self._helper(handler)
            return obj
        else:
            return super(args, self).__call__(obj)

    def _helper(self, builder):
        builder.set_annotations(self._annotations, **self._more_annotations)

    def modify_request_definition(self, request_definition_builder):
        """Modifies dynamic requests with given annotations"""
        self._helper(request_definition_builder.argument_handler_builder)


class _InjectableMethodAnnotation(MethodAnnotation):
    def modify_request(self, request_builder):
        request_builder.add_transaction_hook(self)


class _BaseHandlerAnnotation(_InjectableMethodAnnotation):
    def __new__(cls, func=None, *args, **kwargs):
        if func is None:
            return lambda f: cls(f, *args, **kwargs)
        self = super(_BaseHandlerAnnotation, cls).__new__(cls)
        functools.update_wrapper(self, func)
        return self


# noinspection PyPep8Naming
class response_handler(_BaseHandlerAnnotation, hooks.ResponseHandler):
    """
    A decorator for creating custom response handlers.

    To register a function as a custom response handler, decorate the
    function with this class. The decorated function should accept a single
    positional argument, an HTTP response object:

    Example:
        .. code-block:: python

            @response_handler
            def raise_for_status(response):
                response.raise_for_status()
                return response

    Then, to apply custom response handling to a request method, simply
    decorate the method with the registered response handler:

    Example:
        .. code-block:: python

            @raise_for_status
            @get("/user/posts")
            def get_posts(self):
                \"""Fetch all posts for the current users.\"""

    To apply custom response handling on all request methods of a
    :py:class:`uplink.Consumer` subclass, simply decorate the class with
    the registered response handler:

    Example:
        .. code-block:: python

            @raise_for_status
            class GitHub(Consumer):
               ...

    Lastly, the decorator supports the optional argument
    :obj:`requires_consumer`. When this option is set to :obj:`True`,
    the registered callback should accept a reference to the
    :class:`~Consumer` instance as its leading argument:

    Example:
        .. code-block:: python

            @response_handler(requires_consumer=True)
            def raise_for_status(consumer, response):
                ...

    .. versionadded:: 0.4.0
    """


# noinspection PyPep8Naming
class error_handler(_BaseHandlerAnnotation, hooks.ExceptionHandler):
    """
    A decorator for creating custom error handlers.

    To register a function as a custom error handler, decorate the
    function with this class. The decorated function should accept three
    positional arguments: (1) the type of the exception, (2) the
    exception instance raised, and (3) a traceback instance.

    Example:
        .. code-block:: python

            @error_handler
            def raise_api_error(exc_type, exc_val, exc_tb):
                # wrap client error with custom API error
                ...

    Then, to apply custom error handling to a request method, simply
    decorate the method with the registered error handler:

    Example:
        .. code-block:: python

            @raise_api_error
            @get("/user/posts")
            def get_posts(self):
                \"""Fetch all posts for the current users.\"""

    To apply custom error handling on all request methods of a
    :py:class:`uplink.Consumer` subclass, simply decorate the class with
    the registered error handler:

    Example:
        .. code-block:: python

            @raise_api_error
            class GitHub(Consumer):
               ...

    Lastly, the decorator supports the optional argument
    :obj:`requires_consumer`. When this option is set to :obj:`True`,
    the registered callback should accept a reference to the
    :class:`~Consumer` instance as its leading argument:

    Example:
        .. code-block:: python

             @error_handler(requires_consumer=True)
             def raise_api_error(consumer, exc_type, exc_val, exc_tb):
                ...

    .. versionadded:: 0.4.0

    Note:
        Error handlers can not completely suppress exceptions. The
        original exception is thrown if the error handler doesn't throw
        anything.
    """


# noinspection PyPep8Naming
class inject(_InjectableMethodAnnotation, hooks.TransactionHookChain):
    """
    A decorator that applies one or more hooks to a request method.

    .. versionadded:: 0.4.0
    """
