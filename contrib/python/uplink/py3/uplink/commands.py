# Standard library imports
import functools

# Local imports
from uplink import (
    arguments,
    converters,
    decorators,
    exceptions,
    interfaces,
    returns,
    utils,
)
from uplink.compat import abc

__all__ = ["delete", "get", "head", "patch", "post", "put"]


class MissingArgumentAnnotations(exceptions.InvalidRequestDefinition):
    message = "Missing annotation for argument(s): '%s'."
    implicit_message = " (Implicit path variables: '%s')"

    def __init__(self, missing, path_variables):
        missing, path_variables = list(missing), list(path_variables)
        self.message = self.message % "', '".join(missing)
        if path_variables:  # pragma: no cover
            self.message += self.implicit_message % "', '".join(path_variables)


class MissingUriVariables(exceptions.InvalidRequestDefinition):
    message = "On uri template %s, some variables are not handled: %s"

    def __init__(self, uri, remaining_variables):
        self.message = self.message % (uri, "', '".join(remaining_variables))


class URIDefinitionBuilder(interfaces.UriDefinitionBuilder):
    def __init__(self, uri):
        self._uri = uri
        self._is_dynamic = False
        self._uri_variables = set()

    @property
    def template(self):
        return self._uri

    @property
    def is_static(self):
        return self._uri is not None

    @property
    def is_dynamic(self):
        return self._is_dynamic

    @is_dynamic.setter
    def is_dynamic(self, is_dynamic):
        if self.is_static:
            raise ValueError(
                f"Failed to set dynamic URI as URI is already defined: {self._uri}"
            )
        self._is_dynamic = is_dynamic

    def add_variable(self, name):
        if self.is_static and name not in self.remaining_variables:
            raise ValueError(f"`{name}` is not a variable on the uri {self._uri}")
        self._uri_variables.add(name)

    @property
    def remaining_variables(self):
        return utils.URIBuilder.variables(self._uri) - self._uri_variables

    def build(self):
        if self.remaining_variables:
            raise MissingUriVariables(self._uri, self.remaining_variables)
        return self._uri


class RequestDefinitionBuilder(interfaces.RequestDefinitionBuilder):
    def __init__(
        self,
        func,
        method,
        uri,
        argument_handler_builder,
        method_handler_builder,
    ):
        self._func = func
        self._method = method
        self._uri = uri
        self._argument_handler_builder = argument_handler_builder
        self._method_handler_builder = method_handler_builder
        self._return_type = None

        self._argument_handler_builder.listener = self._notify
        self._method_handler_builder.listener = self._notify

    def _notify(self, annotation):
        annotation.modify_request_definition(self)

    @property
    def method(self):
        return self._method

    @property
    def uri(self):
        return self._uri

    @property
    def argument_handler_builder(self):
        return self._argument_handler_builder

    @property
    def method_handler_builder(self):
        return self._method_handler_builder

    @property
    def return_type(self):
        return self._return_type

    @return_type.setter
    def return_type(self, return_type):
        self._return_type = return_type

    def __call__(self, uri=None, args=()):
        """
        Applies the decorators, HTTP method, and optionally the URI
        of this consumer method to the decorated method.

        This makes the request definition reusable and can help
        minimize duplication across similar consumer methods.

        Examples:
            Define request templates:

            ```python
            from uplink import Consumer, get, json, returns

            @returns.json
            @json
            @get
            def get_json():
                \"""GET request that consumes and produces JSON.\"""

            class GitHub(Consumer):
                @get_json("/users/{user}")
                def get_user(self, user):
                     \"""Fetches a specific GitHub user.\"""
            ```

            Remove duplication across definitions of similar consumer
            methods, whether or not the methods are defined in the same
            class:

            ```python
            from uplink import Consumer, get, params, timeout

            class GitHub(Consumer):
                @timeout(10)
                @get("/users/{user}/repos")
                def get_user_repos(self, user):
                    \"""Retrieves the repos that the user owns.\"""

                # Extends the above method to define a variant:
                @params(type="member")
                @get_user_repos
                def get_repos_for_collaborator(self, user):
                    \"""
                    Retrieves the repos for which the given user is
                    a collaborator.
                    \"""

            class EnhancedGitHub(Github):
                # Updates the return type of an inherited method.
                @GitHub.get_user_repos
                def get_user_repos(self, user) -> List[Repo]:
                    \"""Retrieves the repos that the user owns.\"""
            ```

        Args:
            uri (str, optional): the request's relative path
            args: a list or mapping of function annotations (e.g.
                 `uplink.Path`) corresponding to the decorated
                 function's arguments
        """
        return self.extend(uri, args)

    def extend(self, uri=None, args=()):
        factory = HttpMethodFactory(
            method=self.method, request_definition_builder_factory=self._extend
        )

        if callable(uri):
            return factory(self.uri.template, args)(uri)
        uri = self.uri.template if uri is None else uri
        return factory(uri, args)

    def _extend(self, func, method, uri, arg_handler, _):
        builder = RequestDefinitionBuilder(
            # Extended consumer methods should only inherit the decorators and
            # not any function annotations, since the new method can have a
            # different signature than the original.
            func,
            method,
            uri,
            arg_handler,
            self.method_handler_builder.copy(),
        )
        builder.return_type = self.return_type
        return builder

    def copy(self):
        builder = RequestDefinitionBuilder(
            self._func,
            self._method,
            self._uri,
            self._argument_handler_builder.copy(),
            self._method_handler_builder.copy(),
        )
        builder.return_type = self.return_type
        return builder

    def _auto_fill_remaining_arguments(self):
        uri_vars = set(self.uri.remaining_variables)
        missing = list(self.argument_handler_builder.missing_arguments)
        still_missing = set(missing) - uri_vars

        # Preserve order of function parameters.
        matching = [p for p in missing if p in uri_vars]

        if still_missing:
            raise MissingArgumentAnnotations(still_missing, matching)

        path_vars = dict.fromkeys(matching, arguments.Path)
        self.argument_handler_builder.set_annotations(path_vars)

    def update_wrapper(self, wrapper):
        functools.update_wrapper(wrapper, self._func)

    def build(self):
        if not self._argument_handler_builder.is_done():
            self._auto_fill_remaining_arguments()
        argument_handler = self._argument_handler_builder.build()
        method_handler = self._method_handler_builder.build()
        uri = self._uri.build()
        return RequestDefinition(
            self._method,
            uri,
            self._return_type,
            argument_handler,
            method_handler,
        )


class RequestDefinition(interfaces.RequestDefinition):
    def __init__(self, method, uri, return_type, argument_handler, method_handler):
        self._method = method
        self._uri = uri
        self._return_type = return_type
        self._argument_handler = argument_handler
        self._method_handler = method_handler

    @property
    def argument_annotations(self):
        return tuple(self._argument_handler.annotations)

    @property
    def method_annotations(self):
        return tuple(self._method_handler.annotations)

    def make_converter_registry(self, converters_):
        return converters.ConverterFactoryRegistry(converters_, self)

    def define_request(self, request_builder, func_args, func_kwargs):
        request_builder.method = self._method
        request_builder.relative_url = self._uri
        request_builder.return_type = self._return_type
        self._argument_handler.handle_call(request_builder, func_args, func_kwargs)
        self._method_handler.handle_builder(request_builder)


class HttpMethodFactory:
    def __init__(
        self,
        method,
        request_definition_builder_factory=RequestDefinitionBuilder,
    ):
        self._method = method
        self._request_definition_builder_factory = request_definition_builder_factory

    def __call__(self, uri=None, args=()):
        if callable(uri) and not args:
            return HttpMethod(self._method)(
                uri, self._request_definition_builder_factory
            )
        return functools.partial(
            HttpMethod(self._method, uri, args),
            request_definition_builder_factory=self._request_definition_builder_factory,
        )


class HttpMethod:
    @staticmethod
    def _add_args(obj):
        return obj

    def __init__(self, method, uri=None, args=None):
        self._method = method
        self._uri = uri

        # Register argument annotations
        if args:
            is_map = isinstance(args, abc.Mapping)
            args, kwargs = ((), args) if is_map else (args, {})
            self._add_args = decorators.args(*args, **kwargs)

    def __call__(
        self, func, request_definition_builder_factory=RequestDefinitionBuilder
    ):
        spec = utils.get_arg_spec(func)
        arg_handler = arguments.ArgumentAnnotationHandlerBuilder(func, spec.args)
        builder = request_definition_builder_factory(
            func,
            self._method,
            URIDefinitionBuilder(self._uri),
            arg_handler,
            decorators.MethodAnnotationHandlerBuilder(),
        )

        # Need to add the annotations after constructing the request
        # definition builder so it has a chance to attach its listener.
        arg_handler.set_annotations(spec.annotations)

        # Use return value type hint as expected return type
        if spec.return_annotation is not None:
            builder = returns.schema(spec.return_annotation)(builder)
        return self._add_args(builder)


def get(uri=None, args=()):
    """Creates a decorator that makes the decorated function an HTTP GET request handler.

    Args:
        uri (str, optional): The URI template for the request. Can include path parameters
            in the format {param_name}. If not provided, can be specified when the decorator is used.
        args (tuple|dict, optional): A sequence of argument annotations or mapping of
            argument names to annotations for the handler method.

    Returns:
        A decorator that can be used to define a GET request handler method.

    Examples:
        Basic usage with path parameter:

        ```python
        @get("users/{username}")
        def get_user(self, username):
            \"""Get a specific user.\"""
        ```

        Using query parameters:

        ```python
        @get("users")
        def list_users(self, page: Query, per_page: Query = 30):
            \"""List users with pagination.\"""
        ```
    """
    return HttpMethodFactory("GET").__call__(uri, args)


def head(uri=None, args=()):
    """Creates a decorator that makes the decorated function an HTTP HEAD request handler.

    Args:
        uri (str, optional): The URI template for the request. Can include path parameters
            in the format {param_name}. If not provided, can be specified when the decorator is used.
        args (tuple|dict, optional): A sequence of argument annotations or mapping of
            argument names to annotations for the handler method.

    Returns:
        A decorator that can be used to define a HEAD request handler method.

    Examples:
        Basic usage:

        ```python
        @head("users/{username}")
        def check_user(self, username):
            \"""Check if a user exists.\"""
        ```
    """
    return HttpMethodFactory("HEAD")(uri, args)


def put(uri=None, args=()):
    """Creates a decorator that makes the decorated function an HTTP PUT request handler.

    Args:
        uri (str, optional): The URI template for the request. Can include path parameters
            in the format {param_name}. If not provided, can be specified when the decorator is used.
        args (tuple|dict, optional): A sequence of argument annotations or mapping of
            argument names to annotations for the handler method.

    Returns:
        A decorator that can be used to define a PUT request handler method.

    Examples:
        Using request body:

        ```python
        @put("users/{username}")
        def update_user(self, username, **user_info: Body):
            \"""Update a specific user.\"""
        ```

        Using form fields:

        ```python
        @form_url_encoded
        @put("users/{username}")
        def update_user(self, username, name: Field, email: Field):
            \"""Update a user's information.\"""
        ```
    """
    return HttpMethodFactory("PUT")(uri, args)


def post(uri=None, args=()):
    """Creates a decorator that makes the decorated function an HTTP POST request handler.

    Args:
        uri (str, optional): The URI template for the request. Can include path parameters
            in the format {param_name}. If not provided, can be specified when the decorator is used.
        args (tuple|dict, optional): A sequence of argument annotations or mapping of
            argument names to annotations for the handler method.

    Returns:
        A decorator that can be used to define a POST request handler method.

    Examples:
        Using JSON body:

        ```python
        @json
        @post("users")
        def create_user(self, **user_info: Body):
            \"""Create a new user.\"""
        ```

        Using form data:

        ```python
        @form_url_encoded
        @post("auth/login")
        def login(self, username: Field, password: Field):
            \"""Login with credentials.\"""
        ```
    """
    return HttpMethodFactory("POST")(uri, args)


def patch(uri=None, args=()):
    """Creates a decorator that makes the decorated function an HTTP PATCH request handler.

    Args:
        uri (str, optional): The URI template for the request. Can include path parameters
            in the format {param_name}. If not provided, can be specified when the decorator is used.
        args (tuple|dict, optional): A sequence of argument annotations or mapping of
            argument names to annotations for the handler method.

    Returns:
        A decorator that can be used to define a PATCH request handler method.

    Examples:
        Partial update with JSON:

        ```python
        @json
        @patch("users/{username}")
        def update_user_partially(self, username, **updates: Body):
            \"""Partially update a user.\"""
        ```

        Update specific fields:

        ```python
        @form_url_encoded
        @patch("users/{username}")
        def update_status(self, username, status: Field):
            \"""Update a user's status.\"""
        ```
    """
    return HttpMethodFactory("PATCH")(uri, args)


def delete(uri=None, args=()):
    """Creates a decorator that makes the decorated function an HTTP DELETE request handler.

    Args:
        uri (str, optional): The URI template for the request. Can include path parameters
            in the format {param_name}. If not provided, can be specified when the decorator is used.
        args (tuple|dict, optional): A sequence of argument annotations or mapping of
            argument names to annotations for the handler method.

    Returns:
        A decorator that can be used to define a DELETE request handler method.

    Examples:
        Basic usage:

        ```python
        @delete("users/{username}")
        def delete_user(self, username):
            \"""Delete a specific user.\"""
        ```

        With query parameters:

        ```python
        @delete("users/{username}/posts")
        def delete_posts(self, username, before_date: Query = None):
            \"""Delete user's posts, optionally before a specific date.\"""
        ```
    """
    return HttpMethodFactory("DELETE")(uri, args)
