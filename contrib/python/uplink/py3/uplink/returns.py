# Standard library imports
import sys
import warnings

# Local imports
from uplink import decorators
from uplink.converters import interfaces, keys

__all__ = ["from_json", "json", "schema"]


class ReturnType:
    def __init__(self, decorator, type_):
        self._decorator = decorator
        self._type_ = type_

    @property
    def type(self):
        return self._type_

    @staticmethod
    def with_decorator(old, decorator):
        old_type = None if old is None else old.type
        return ReturnType(decorator, decorator.return_type or old_type)

    def with_strategy(self, strategy):
        return CallableReturnType(self._decorator, self._type_, strategy)

    def is_applicable(self, decorator):
        return self._decorator is decorator


class CallableReturnType(ReturnType):
    def __init__(self, decorator, type_, strategy):
        super().__init__(decorator, type_)
        self._strategy = strategy

    def __call__(self, *args, **kwargs):
        return self._strategy(*args, **kwargs)


class _ReturnsBase(decorators.MethodAnnotation):
    @property
    def return_type(self):  # pragma: no cover
        raise NotImplementedError

    def _make_strategy(self, converter):  # pragma: no cover
        pass

    def _modify_request_definition(self, definition, kwargs):
        super()._modify_request_definition(definition, kwargs)
        definition.return_type = ReturnType.with_decorator(definition.return_type, self)

    def _get_converter(self, request_builder, return_type):  # pragma: no cover
        return request_builder.get_converter(
            keys.CONVERT_FROM_RESPONSE_BODY, return_type.type
        )

    def modify_request(self, request_builder):
        return_type = request_builder.return_type
        if not return_type.is_applicable(self):
            return

        converter = self._get_converter(request_builder, return_type)
        if converter is None:
            return

        # Found a converter that can handle the return type.
        request_builder.return_type = return_type.with_strategy(
            self._make_strategy(converter)
        )


class JsonStrategy:
    # TODO: Consider moving this under json decorator
    # TODO: Support JSON Pointer (https://tools.ietf.org/html/rfc6901)

    def __init__(self, converter, key=()):
        self._converter = converter

        if not isinstance(key, list | tuple):
            key = (key,)
        self._key = key

    def __call__(self, response):
        content = response.json()
        for name in self._key:
            content = content[name]
        return self._converter(content)


# noinspection PyPep8Naming
class json(_ReturnsBase):
    """
    Specifies that the decorated consumer method should return a JSON
    object.

    ```python
    # This method will return a JSON object (e.g., a dict or list)
    @returns.json
    @get("/users/{username}")
    def get_user(self, username):
        \"""Get a specific user.\"""
    ```

    ## Returning a Specific JSON Field

    The `key` argument accepts a string or tuple that
    specifies the path of an internal field in the JSON document.

    For instance, consider an API that returns JSON responses that,
    at the root of the document, contains both the server-retrieved
    data and a list of relevant API errors:

    ```json
    {
        "data": { "user": "prkumar", "id": "140232" },
        "errors": []
    }
    ```

    If returning the list of errors is unnecessary, we can use the
    `key` argument to strictly return the nested field
    `data.id`:

    ```python
    @returns.json(key=("data", "id"))
    @get("/users/{username}")
    def get_user_id(self, username):
        \"""Get a specific user's ID.\"""
    ```

    We can also configure Uplink to convert the field before it's
    returned by also specifying the `type` argument:

    ```python
    @returns.json(key=("data", "id"), type=int)
    @get("/users/{username}")
    def get_user_id(self, username):
        \"""Get a specific user's ID.\"""
    ```

    Added in version 0.5.0
    """

    _can_be_static = True

    class _DummyConverter(interfaces.Converter):
        def convert(self, response):
            return response

    class _CastConverter(interfaces.Converter):
        def __init__(self, cast):
            self._cast = cast

        def convert(self, response):
            return self._cast(response)

    __dummy_converter = _DummyConverter()

    def __init__(self, type=None, key=(), model=None, member=()):
        if model:  # pragma: no cover
            warnings.warn(
                "The `model` argument of @returns.json is deprecated and will "
                "be removed in v1.0.0. Use `type` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        if member:  # pragma: no cover
            warnings.warn(
                "The `member` argument of @returns.json is deprecated and will "
                "be removed in v1.0.0. Use `key` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
        self._type = type or model
        self._key = key or member

    @property
    def return_type(self):
        return self._type

    def _get_converter(self, request_builder, return_type):
        converter = super()._get_converter(request_builder, return_type)

        if converter:
            return converter

        if callable(return_type.type):
            return self._CastConverter(return_type.type)

        # If the return_type cannot be converted, the strategy should directly
        # return the JSON body of the HTTP response, instead of trying to
        # deserialize it into a certain type. In this case, by
        # defaulting the return type to the dummy converter, which
        # implements this pass-through behavior, we ensure that
        # _make_strategy is called.
        return self.__dummy_converter

    def _make_strategy(self, converter):
        return JsonStrategy(converter, self._key)


from_json = json
"""
Specifies that the decorated consumer method should produce
instances of a `type` class using a registered
deserialization strategy (see `uplink.loads.from_json`)

This decorator accepts the same arguments as
`uplink.returns.json`.

Often, a JSON response body represents a schema in your application.
If an existing Python object encapsulates this schema, use the
:py:attr:`type` argument to specify it as the return type:

```python
@returns.from_json(type=User)
@get("/users/{username}")
def get_user(self, username):
    \"""Get a specific user.\"""
```

For Python 3 users, you can alternatively provide a return value
annotation. Hence, the previous code is equivalent to the following
in Python 3:

```python
@returns.from_json
@get("/users/{username}")
def get_user(self, username) -> User:
    \"""Get a specific user.\"""
```

Both usages typically require also registering a converter that
knows how to deserialize the JSON into the specified `type`
(see `uplink.loads.from_json`). This step is unnecessary if
the `type` is defined using a library for which Uplink has
built-in support, such as `marshmallow`.

Added in version 0.6.0
"""


# noinspection PyPep8Naming
class schema(_ReturnsBase):
    """
    Specifies that the function returns a specific type of response.

    The standard way to provide a consumer method's return type is to
    set it as the method's return annotation:

    ```python
    @get("/users/{username}")
    def get_user(self, username) -> UserSchema:
        \"""Get a specific user.\"""
    ```

    Alternatively, you can use this decorator instead:

    ```python
    @returns.schema(UserSchema)
    @get("/users/{username}")
    def get_user(self, username):
        \"""Get a specific user.\"""
    ```

    To have Uplink convert response bodies into the desired type, you
    will need to define an appropriate converter (e.g., using
    [`uplink.loads`][uplink.loads]).

    Added in version 0.5.1
    """

    def __init__(self, type):
        self._schema = type

    @property
    def return_type(self):
        return self._schema

    def _make_strategy(self, converter):
        return converter


class _ModuleProxy:
    __module = sys.modules[__name__]

    schema = model = schema
    json = json
    from_json = from_json
    __all__ = __module.__all__

    def __getattr__(self, item):
        return getattr(self.__module, item)

    def __call__(self, *args, **kwargs):
        return schema(*args, **kwargs)


sys.modules[__name__] = _ModuleProxy()
