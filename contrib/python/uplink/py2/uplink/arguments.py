"""
This module implements the built-in argument annotations and their
handling classes.
"""
# Standard library imports
import collections
import functools
import inspect

# Local imports
from uplink import exceptions, hooks, interfaces, utils
from uplink.compat import abc
from uplink.converters import keys

__all__ = [
    "Path",
    "Query",
    "QueryMap",
    "Header",
    "HeaderMap",
    "Field",
    "FieldMap",
    "Part",
    "PartMap",
    "Body",
    "Url",
    "Timeout",
    "Context",
]


class ExhaustedArguments(exceptions.AnnotationError):
    message = (
        "Failed to add `%s` to method `%s`, as all arguments have "
        "been annotated."
    )

    def __init__(self, annotation, func):
        self.message = self.message % (annotation, func.__name__)


class ArgumentNotFound(exceptions.AnnotationError):
    message = "`%s` does not match any argument name of method `%s`."

    def __init__(self, name, func):
        self.message = self.message % (name, func.__name__)


class ArgumentAnnotationHandlerBuilder(interfaces.AnnotationHandlerBuilder):
    __ANNOTATION_BUILDER_KEY = "#ANNOTATION_BUILDER_KEY#"

    @classmethod
    def from_func(cls, func):
        if not hasattr(func, cls.__ANNOTATION_BUILDER_KEY):
            spec = utils.get_arg_spec(func)
            handler = cls(func, spec.args)
            setattr(func, cls.__ANNOTATION_BUILDER_KEY, handler)
            handler.set_annotations(spec.annotations)
        return getattr(func, cls.__ANNOTATION_BUILDER_KEY)

    def __init__(self, func, arguments, func_is_method=True):
        self._arguments = arguments[func_is_method:]
        self._annotations = collections.OrderedDict.fromkeys(self._arguments)
        self._defined = 0
        self._func = func
        self._argument_types = {}

    @property
    def missing_arguments(self):
        return (a for a in self._arguments if self._annotations[a] is None)

    @property
    def remaining_args_count(self):
        return len(self._arguments) - self._defined

    def set_annotations(self, annotations=None, **more_annotations):
        if annotations is not None:
            if not isinstance(annotations, abc.Mapping):
                missing = tuple(
                    a
                    for a in self.missing_arguments
                    if a not in more_annotations
                )
                annotations = dict(zip(missing, annotations))
            more_annotations.update(annotations)
        for name in more_annotations:
            self.add_annotation(more_annotations[name], name)

    @staticmethod
    def _is_annotation(annotation):
        cls = interfaces.Annotation
        return utils.is_subclass(annotation, cls) or isinstance(annotation, cls)

    def add_annotation(self, annotation, name=None, *args, **kwargs):
        if self._is_annotation(annotation):
            return self._add_annotation(annotation, name)
        else:
            self._argument_types[name] = annotation

    def _add_annotation(self, annotation, name=None):
        try:
            name = next(self.missing_arguments) if name is None else name
        except StopIteration:
            raise ExhaustedArguments(annotation, self._func)
        if name not in self._annotations:
            raise ArgumentNotFound(name, self._func)
        annotation = self._process_annotation(name, annotation)
        super(ArgumentAnnotationHandlerBuilder, self).add_annotation(annotation)
        self._defined += self._annotations[name] is None
        self._annotations[name] = annotation
        return annotation

    def _process_annotation(self, name, annotation):
        if inspect.isclass(annotation):
            annotation = annotation()
        if isinstance(annotation, TypedArgument) and annotation.type is None:
            annotation.type = self._argument_types.pop(name, None)
        if isinstance(annotation, NamedArgument) and annotation.name is None:
            annotation.name = name
        return annotation

    def is_done(self):
        return self.remaining_args_count == 0

    def copy(self):
        return self

    @property
    def _types(self):
        types = self._annotations
        return ((k, types[k]) for k in types if types[k] is not None)

    def build(self):
        return ArgumentAnnotationHandler(
            self._func, collections.OrderedDict(self._types)
        )


class ArgumentAnnotationHandler(interfaces.AnnotationHandler):
    def __init__(self, func, arguments):
        self._func = func
        self._arguments = arguments

    @property
    def annotations(self):
        return iter(self._arguments.values())

    def get_relevant_arguments(self, call_args):
        annotations = self._arguments
        return ((n, annotations[n]) for n in call_args if n in annotations)

    def handle_call(self, request_builder, args, kwargs):
        call_args = utils.get_call_args(self._func, None, *args, **kwargs)
        self.handle_call_args(request_builder, call_args)

    def handle_call_args(self, request_builder, call_args):
        # TODO: Catch Annotation errors and chain them here + provide context.
        for name, annotation in self.get_relevant_arguments(call_args):
            annotation.modify_request(request_builder, call_args[name])


class ArgumentAnnotation(interfaces.Annotation):
    _can_be_static = True

    def __call__(self, request_definition_builder):
        request_definition_builder.argument_handler_builder.add_annotation(self)
        return request_definition_builder

    def _modify_request(self, request_builder, value):  # pragma: no cover
        pass

    @property
    def type(self):  # pragma: no cover
        return None

    @property
    def converter_key(self):  # pragma: no cover
        raise NotImplementedError

    def modify_request(self, request_builder, value):
        argument_type, converter_key = self.type, self.converter_key
        converter = request_builder.get_converter(converter_key, argument_type)
        converted_value = converter(value) if converter else value
        self._modify_request(request_builder, converted_value)


class TypedArgument(ArgumentAnnotation):
    def __init__(self, type=None):
        self._type = type

    @property
    def type(self):
        return self._type

    @type.setter
    def type(self, type_):
        if self._type is None:
            self._type = type_
        else:
            raise AttributeError("Type is already set.")

    @property
    def converter_key(self):  # pragma: no cover
        raise NotImplementedError


class NamedArgument(TypedArgument):
    _can_be_static = True

    def __init__(self, name=None, type=None):
        self._arg_name = name
        super(NamedArgument, self).__init__(type)

    @property
    def name(self):
        return self._arg_name

    @name.setter
    def name(self, name):
        if self._arg_name is None:
            self._arg_name = name
        else:
            raise AttributeError("Name is already set.")

    @property
    def converter_key(self):  # pragma: no cover
        raise NotImplementedError


class EncodeNoneMixin(object):
    #: Identifies how a `None` value should be encoded in the request.
    _encode_none = None  # type: str

    def _modify_request(self, request_builder, value):  # pragma: no cover
        raise NotImplementedError

    def modify_request(self, request_builder, value):
        if value is None:
            if self._encode_none is None:
                # ignore value if it is None and shouldn't be encoded
                return
            else:
                value = self._encode_none
        super(EncodeNoneMixin, self).modify_request(request_builder, value)


class FuncDecoratorMixin(object):
    @classmethod
    def _is_static_call(cls, *args_, **kwargs):
        if super(FuncDecoratorMixin, cls)._is_static_call(*args_, **kwargs):
            return True
        try:
            is_func = inspect.isfunction(args_[0])
        except IndexError:
            return False
        else:
            return is_func and not (kwargs or args_[1:])

    def __call__(self, obj):
        if inspect.isfunction(obj):
            ArgumentAnnotationHandlerBuilder.from_func(obj).add_annotation(self)
            return obj
        else:
            return super(FuncDecoratorMixin, self).__call__(obj)

    def with_value(self, value):
        """
        Creates an object that can be used with the
        :py:class:`Session.inject` method or
        :py:class:`~uplink.inject` decorator to inject request properties
        with specific values.

        .. versionadded:: 0.4.0
        """
        auditor = functools.partial(self.modify_request, value=value)
        return hooks.RequestAuditor(auditor)


class Path(NamedArgument):
    """
    Substitution of a path variable in a `URI template
    <https://tools.ietf.org/html/rfc6570>`__.

    URI template parameters are enclosed in braces (e.g.,
    :code:`{name}`). To map an argument to a declared URI parameter, use
    the :py:class:`Path` annotation:

    .. code-block:: python

        class TodoService(object):
            @get("/todos/{id}")
            def get_todo(self, todo_id: Path("id")): pass

    Then, invoking :code:`get_todo` with a consumer instance:

    .. code-block:: python

        todo_service.get_todo(100)

    creates an HTTP request with a URL ending in :code:`/todos/100`.

    Note:
        Any unannotated function argument that shares a name with a URL path
        parameter is implicitly annotated with this class at runtime.

        For example, we could simplify the method from the previous
        example by matching the path variable and method argument names:

        .. code-block:: python

            @get("/todos/{id}")
            def get_todo(self, id): pass
    """

    @property
    def converter_key(self):
        return keys.CONVERT_TO_STRING

    def modify_request_definition(self, request_definition_builder):
        request_definition_builder.uri.add_variable(self.name)

    def _modify_request(self, request_builder, value):
        request_builder.set_url_variable({self.name: value})


class Query(FuncDecoratorMixin, EncodeNoneMixin, NamedArgument):
    """
    Set a dynamic query parameter.

    This annotation turns argument values into URL query
    parameters. You can include it as function argument
    annotation, in the format: ``<query argument>: uplink.Query``.

    If the API endpoint you are trying to query uses ``q`` as a query
    parameter, you can add ``q: uplink.Query`` to the consumer method to
    set the ``q`` search term at runtime.

    Example:
        .. code-block:: python

            @get("/search/commits")
            def search(self, search_term: Query("q")):
                \"""Search all commits with the given search term.\"""

        To specify whether or not the query parameter is already URL encoded,
        use the optional :py:obj:`encoded` argument:

        .. code-block:: python

            @get("/search/commits")
            def search(self, search_term: Query("q", encoded=True)):
                \"""Search all commits with the given search term.\"""

        To specify if and how :py:obj:`None` values should be encoded, use the
        optional :py:obj:`encode_none` argument:

        .. code-block:: python

            @get("/search/commits")
            def search(self, search_term: Query("q"),
                       search_order: Query("o", encode_none="null")):
                \"""
                Search all commits with the given search term using the
                optional search order.
                \"""

    Args:
        encoded (:obj:`bool`, optional): Specifies whether the parameter
            :py:obj:`name` and value are already URL encoded.
        encode_none (:obj:`str`, optional): Specifies an optional string with
            which :py:obj:`None` values should be encoded. If not specified,
            parameters with a value of :py:obj:`None` will not be sent.
    """

    class QueryStringEncodingError(exceptions.AnnotationError):
        message = "Failed to join encoded and unencoded query parameters."

    def __init__(self, name=None, encoded=False, type=None, encode_none=None):
        super(Query, self).__init__(name, type)
        self._encoded = encoded
        self._encode_none = encode_none

    @staticmethod
    def _update_params(info, existing, new_params, encoded):
        # TODO: Consider moving some of this to the client backend.
        if encoded:
            params = [] if existing is None else [str(existing)]
            params.extend("%s=%s" % (n, new_params[n]) for n in new_params)
            info["params"] = "&".join(params)
        else:
            info["params"].update(new_params)

    @staticmethod
    def update_params(info, new_params, encoded):
        existing = info.setdefault("params", None if encoded else dict())
        if encoded == isinstance(existing, abc.Mapping):
            raise Query.QueryStringEncodingError()
        Query._update_params(info, existing, new_params, encoded)

    @property
    def converter_key(self):
        """Converts query parameters to the request body."""
        if self._encoded:
            return keys.CONVERT_TO_STRING
        else:
            return keys.Sequence(keys.CONVERT_TO_STRING)

    def _modify_request(self, request_builder, value):
        """Updates request body with the query parameter."""
        self.update_params(
            request_builder.info, {self.name: value}, self._encoded
        )


class QueryMap(FuncDecoratorMixin, TypedArgument):
    """
    A mapping of query arguments.

    If the API you are using accepts multiple query arguments, you can
    include them all in your function method by using the format:
    ``<query argument>: uplink.QueryMap``

    Example:
        .. code-block:: python

            @get("/search/users")
            def search(self, **params: QueryMap):
                \"""Search all users.\"""

    Args:
        encoded (:obj:`bool`, optional): Specifies whether the parameter
            :py:obj:`name` and value are already URL encoded.
    """

    def __init__(self, encoded=False, type=None):
        super(QueryMap, self).__init__(type)
        self._encoded = encoded

    @property
    def converter_key(self):
        """Converts query mapping to request body."""
        if self._encoded:
            return keys.Map(keys.CONVERT_TO_STRING)
        else:
            return keys.Map(keys.Sequence(keys.CONVERT_TO_STRING))

    def _modify_request(self, request_builder, value):
        """Updates request body with the mapping of query args."""
        Query.update_params(request_builder.info, value, self._encoded)


class Header(FuncDecoratorMixin, EncodeNoneMixin, NamedArgument):
    """
    Pass a header as a method argument at runtime.

    While :py:class:`uplink.headers` attaches request headers values
    that are static across all requests sent from the decorated
    consumer method, this annotation turns a method argument into a
    dynamic request header.

    Example:
        .. code-block:: python

            @get("/user")
            def me(self, session_id: Header("Authorization")):
                \"""Get the authenticated user.\"""

        To define an optional header, use the default value of `None`:

            @get("/repositories")
            def fetch_repos(self, auth: Header("Authorization") = None):
                \"""List all public repositories.\"""

        When the argument is not used, the header will not be added
        to the request.
    """

    @property
    def converter_key(self):
        """Converts passed argument to string."""
        return keys.CONVERT_TO_STRING

    def _modify_request(self, request_builder, value):
        """Updates request header contents."""
        request_builder.info["headers"][self.name] = value


class HeaderMap(FuncDecoratorMixin, TypedArgument):
    """Pass a mapping of header fields at runtime."""

    @property
    def converter_key(self):
        """Converts every header field to string"""
        return keys.Map(keys.CONVERT_TO_STRING)

    @classmethod
    def _modify_request(cls, request_builder, value):
        """Updates request header contents."""
        request_builder.info["headers"].update(value)


class Field(NamedArgument):
    """
    Defines a form field to the request body.

    Use together with the decorator :py:class:`uplink.form_url_encoded`
    and annotate each argument accepting a form field with
    :py:class:`uplink.Field`.

    Example::
        .. code-block:: python

            @form_url_encoded
            @post("/users/edit")
            def update_user(self, first_name: Field, last_name: Field):
                \"""Update the current user.\"""
    """

    class FieldAssignmentFailed(exceptions.AnnotationError):
        """Used if the field chosen failed to be defined."""

        message = (
            "Failed to define field '%s' to request body. Another argument "
            "annotation might have overwritten the body entirely."
        )

        def __init__(self, field):
            self.message = self.message % field.name

    @property
    def converter_key(self):
        """Converts type to request body."""
        return keys.CONVERT_TO_REQUEST_BODY

    def _modify_request(self, request_builder, value):
        """Updates the request body with chosen field."""
        try:
            request_builder.info["data"][self.name] = value
        except TypeError:
            # TODO: re-raise with TypeError
            # `data` does not support item assignment
            raise self.FieldAssignmentFailed(self)


class FieldMap(TypedArgument):
    """
    Defines a mapping of form fields to the request body.

    Use together with the decorator :py:class:`uplink.form_url_encoded`
    and annotate each argument accepting a form field with
    :py:class:`uplink.FieldMap`.

    Example:
        .. code-block:: python

            @form_url_encoded
            @post("/user/edit")
            def create_post(self, **user_info: FieldMap):
                \"""Update the current user.\"""
    """

    class FieldMapUpdateFailed(exceptions.AnnotationError):
        """Use when the attempt to update the request body failed."""

        message = (
            "Failed to update request body with field map. Another argument "
            "annotation might have overwritten the body entirely."
        )

    @property
    def converter_key(self):
        """Converts type to request body."""
        return keys.Map(keys.CONVERT_TO_REQUEST_BODY)

    def _modify_request(self, request_builder, value):
        """Updates request body with chosen field mapping."""
        try:
            request_builder.info["data"].update(value)
        except AttributeError:
            # TODO: re-raise with AttributeError
            raise self.FieldMapUpdateFailed()


class Part(NamedArgument):
    """
    Marks an argument as a form part.

    Use together with the decorator :py:class:`uplink.multipart` and
    annotate each form part with :py:class:`uplink.Part`.

    Example:
        .. code-block:: python

            @multipart
            @put(/user/photo")
            def update_user(self, photo: Part, description: Part):
                \"""Upload a user profile photo.\"""
    """

    @property
    def converter_key(self):
        """Converts part to the request body."""
        return keys.CONVERT_TO_REQUEST_BODY

    def _modify_request(self, request_builder, value):
        """Updates the request body with the form part."""
        request_builder.info["files"][self.name] = value


class PartMap(TypedArgument):
    """
    A mapping of form field parts.

    Use together with the decorator :py:class:`uplink.multipart` and
    annotate each part of form parts with :py:class:`uplink.PartMap`

    Example:
        .. code-block:: python

            @multipart
            @put(/user/photo")
            def update_user(self, photo: Part, description: Part):
                \"""Upload a user profile photo.\"""
    """

    @property
    def converter_key(self):
        """Converts each part to the request body."""
        return keys.Map(keys.CONVERT_TO_REQUEST_BODY)

    def _modify_request(self, request_builder, value):
        """Updates request body to with the form parts."""
        request_builder.info["files"].update(value)


class Body(TypedArgument):
    """
    Set the request body at runtime.

    Use together with the decorator :py:class:`uplink.json`. The method
    argument value will become the request's body when annotated
    with :py:class:`uplink.Body`.

    Example:
        .. code-block:: python

            @json
            @patch(/user")
            def update_user(self, **info: Body):
                \"""Update the current user.\"""
    """

    @property
    def converter_key(self):
        """Converts request body."""
        return keys.CONVERT_TO_REQUEST_BODY

    def _modify_request(self, request_builder, value):
        """Updates request body data."""
        request_builder.info["data"] = value


# TODO: Add an integration test that uses arguments.Url
class Url(ArgumentAnnotation):
    """
    Sets a dynamic URL.

    Provides the URL at runtime as a method argument. Drop the decorator
    parameter path from :py:class:`uplink.get` and annotate the
    corresponding argument with :py:class:`uplink.Url`

    Example:
        .. code-block:: python

            @get
            def get(self, endpoint: Url):
                \"""Execute a GET requests against the given endpoint\"""
    """

    class DynamicUrlAssignmentFailed(exceptions.InvalidRequestDefinition):
        """Raised when the attempt to set dynamic url fails."""

        message = "Failed to set dynamic url annotation on `%s`. "

        def __init__(self, request_definition_builder):
            self.message = self.message % request_definition_builder.__name__

    @property
    def converter_key(self):
        """Converts url type to string."""
        return keys.CONVERT_TO_STRING

    def modify_request_definition(self, request_definition_builder):
        """Sets dynamic url."""
        try:
            request_definition_builder.uri.is_dynamic = True
        except ValueError:
            # TODO: re-raise with ValueError
            raise self.DynamicUrlAssignmentFailed(request_definition_builder)

    @classmethod
    def _modify_request(cls, request_builder, value):
        """Updates request url."""
        request_builder.relative_url = value


class Timeout(FuncDecoratorMixin, ArgumentAnnotation):
    """
    Passes a timeout as a method argument at runtime.

    While :py:class:`uplink.timeout` attaches static timeout to all requests
    sent from a consumer method, this class turns a method argument into a
    dynamic timeout value.

    Example:
        .. code-block:: python

            @get("/user/posts")
            def get_posts(self, timeout: Timeout() = 60):
                \"""Fetch all posts for the current users giving up after given
                number of seconds.\"""
    """

    @property
    def type(self):
        return float

    @property
    def converter_key(self):
        """Do not convert passed argument."""
        return keys.Identity()

    def _modify_request(self, request_builder, value):
        """Modifies request timeout."""
        request_builder.info["timeout"] = value


class Context(FuncDecoratorMixin, NamedArgument):
    """
    Defines a name-value pair that is accessible to middleware at
    runtime.

    Request middleware can leverage this annotation to give users
    control over the middleware's behavior.

    Example:
        Consider a custom decorator :obj:`@cache` (this would be a
        subclass of :class:`uplink.decorators.MethodAnnotation`):

        .. code-block:: python

            @cache(hours=3)
            @get("users/user_id")
            def get_user(self, user_id)
                \"""Retrieves a single user.\"""

        As its name suggests, the :obj:`@cache` decorator enables
        caching server responses so that, once a request is cached,
        subsequent identical requests can be served by the cache
        provider rather than adding load to the upstream service.

        Importantly, the writers of the :obj:`@cache` decorators can
        allow users to pass their own cache provider implementation
        through an argument annotated with :class:`Context <uplink.Context>`:

        .. code-block:: python

            @cache(hours=3)
            @get("users/user_id")
            def get_user(self, user_id, cache_provider: Context)
                \"""Retrieves a single user.\"""

        To add a name-value pair to the context of any request made from
        a :class:`Consumer <uplink.Consumer>` instance, you
        can use the :attr:`Consumer.session.context
        <uplink.session.Session.context>` property. Alternatively, you
        can annotate a constructor argument of a :class:`Consumer
        <uplink.Consumer>` subclass with :class:`Context
        <uplink.Context>`, as explained
        :ref:`here <annotating constructor arguments>`.
    """

    @property
    def converter_key(self):
        """Do not convert passed argument."""
        return keys.Identity()

    def _modify_request(self, request_builder, value):
        """Sets the name-value pair in the context."""
        request_builder.context[self.name] = value


class ContextMap(FuncDecoratorMixin, ArgumentAnnotation):
    """
    Defines a mapping of name-value pairs that are accessible to
    middleware at runtime.
    """

    @property
    def converter_key(self):
        """Do not convert passed argument."""
        return keys.Identity()

    def _modify_request(self, request_builder, value):
        """Updates the context with the given name-value pairs."""
        if not isinstance(value, abc.Mapping):
            raise TypeError(
                "ContextMap requires a mapping; got %s instead.", type(value)
            )
        request_builder.context.update(value)
