# Standard library imports
import functools

# Local imports
from uplink import converters, decorators, install as _install, returns, utils

__all__ = ["loads", "dumps"]

_get_classes = functools.partial(map, type)


class ResponseBodyConverterFactory(converters.Factory):
    def __init__(self, delegate):
        self.create_response_body_converter = delegate


class RequestBodyConverterFactory(converters.Factory):
    def __init__(self, delegate):
        self.create_request_body_converter = delegate


class _Delegate(object):
    def __init__(self, model_class, annotations, func):
        self._model_class = model_class
        self._annotations = annotations
        self._func = func

    def _contains_annotations(self, argument_annotations, method_annotations):
        types = set(_get_classes(argument_annotations))
        types.update(_get_classes(method_annotations))
        return types.issuperset(self._annotations)

    def _is_relevant(self, type_, request_definition):
        return utils.is_subclass(
            type_, self._model_class
        ) and self._contains_annotations(
            request_definition.argument_annotations,
            request_definition.method_annotations,
        )

    def __call__(self, type_, *args, **kwargs):
        if self._is_relevant(type_, *args, **kwargs):
            return functools.partial(self._func, type_)


class _Wrapper(converters.Factory):
    def __init__(self, w, func):
        self.create_response_body_converter = w.create_response_body_converter
        self.create_request_body_converter = w.create_request_body_converter
        self.create_string_converter = w.create_string_converter
        self._func = func

    def __call__(self, *args, **kwargs):
        return self._func(*args, **kwargs)


class _ModelConverterBuilder(object):
    def __init__(self, base_class, annotations=()):
        """
        Args:
            base_class (type): The base model class.
        """
        self._model_class = base_class
        self._annotations = set(annotations)

    def using(self, func):
        """Sets the converter strategy to the given function."""
        delegate = _Delegate(self._model_class, self._annotations, func)
        return self._wrap_delegate(delegate)

    def _wrap_delegate(self, delegate):  # pragma: no cover
        raise NotImplementedError

    def __call__(self, func):
        converter = _Wrapper(self.using(func), func)
        functools.update_wrapper(converter, func)
        return converter

    install = _install

    @classmethod
    def _make_builder(cls, base_class, annotations, *more_annotations):
        annotations = set(annotations)
        annotations.update(more_annotations)
        return cls(base_class=base_class, annotations=annotations)


# noinspection PyPep8Naming
class loads(_ModelConverterBuilder):
    """
    Builds a custom object deserializer.

    This class takes a single argument, the base model class, and
    registers the decorated function as a deserializer for that base
    class and all subclasses.

    Further, the decorated function should accept two positional
    arguments: (1) the encountered type (which can be the given base
    class or a subclass), and (2) the response data.

    .. code-block:: python

        @loads(ModelBase)
        def load_model(model_cls, data):
            ...

    .. versionadded:: v0.5.0
    """

    def _wrap_delegate(self, delegate):
        return ResponseBodyConverterFactory(delegate)

    @classmethod
    def from_json(cls, base_class, annotations=()):
        """
        Builds a custom JSON deserialization strategy.

        This decorator accepts the same arguments and behaves like
        :py:class:`uplink.loads`, except that the second argument of the
        decorated function is a JSON object:

        .. code-block:: python

            @loads.from_json(User)
            def from_json(user_cls, json):
                return user_cls(json["id"], json["username"])

        Notably, only consumer methods that have the expected return
        type (i.e., the given base class or any subclass) and are
        decorated with :py:class:`uplink.returns.from_json` can leverage
        the registered strategy to deserialize JSON responses.

        For example, the following consumer method would leverage the
        :py:func:`from_json` strategy defined above:

        .. code-block:: python

            @returns.from_json
            @get("user")
            def get_user(self) -> User: pass

        .. versionadded:: v0.5.0
        """
        return cls._make_builder(base_class, annotations, returns.json)


# noinspection PyPep8Naming
class dumps(_ModelConverterBuilder):
    """
    Builds a custom object serializer.

    This decorator takes a single argument, the base model class, and
    registers the decorated function as a serializer for that base
    class and all subclasses.

    Further, the decorated function should accept two positional
    arguments: (1) the encountered type (which can be the given base
    class or a subclass), and (2) the encountered instance.

    .. code-block:: python

        @dumps(ModelBase)
        def deserialize_model(model_cls, model_instance):
            ...

    .. versionadded:: v0.5.0
    """

    def _wrap_delegate(self, delegate):
        return RequestBodyConverterFactory(delegate)

    @classmethod
    def to_json(cls, base_class, annotations=()):
        """
        Builds a custom JSON serialization strategy.

        This decorator accepts the same arguments and behaves like
        :py:class:`uplink.dumps`. The only distinction is that the
        decorated function should be JSON serializable.

        .. code-block:: python

            @dumps.to_json(ModelBase)
            def to_json(model_cls, model_instance):
                return model_instance.to_json()

        Notably, only consumer methods that are decorated with
        py:class:`uplink.json` and have one or more argument annotations
        with the expected type (i.e., the given base class or a subclass)
        can leverage the registered strategy.

        For example, the following consumer method would leverage the
        :py:func:`to_json` strategy defined above, given
        :py:class:`User` is a subclass of :py:class:`ModelBase`:

        .. code-block:: python

            @json
            @post("user")
            def change_user_name(self, name: Field(type=User): pass

        .. versionadded:: v0.5.0
        """
        return cls._make_builder(base_class, annotations, decorators.json)
