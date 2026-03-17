import inspect
import sys
from typing import Any, Callable, Dict, Optional, Sequence, Tuple, Type, TypeVar, Union

from django.utils.functional import Promise

# direct import due to https://github.com/microsoft/pyright/issues/3025
if sys.version_info >= (3, 8):
    from typing import Final, Literal
else:
    from typing_extensions import Final, Literal

from rest_framework.fields import Field, empty
from rest_framework.serializers import ListSerializer, Serializer
from rest_framework.settings import api_settings

from drf_spectacular.drainage import (
    error, get_view_method_names, isolate_view_method, set_override, warn,
)
from drf_spectacular.types import OpenApiTypes, _KnownPythonTypes

_ListSerializerType = Union[ListSerializer, Type[ListSerializer]]
_SerializerType = Union[Serializer, Type[Serializer]]
_FieldType = Union[Field, Type[Field]]
_ParameterLocationType = Literal['query', 'path', 'header', 'cookie']
_StrOrPromise = Union[str, Promise]
_SchemaType = Dict[str, Any]
Direction = Literal['request', 'response']


class PolymorphicProxySerializer(Serializer):
    """
    This class is to be used with :func:`@extend_schema <.extend_schema>` to
    signal a request/response might be polymorphic (accepts/returns data
    possibly from different serializers). Usage usually looks like this:

    .. code-block::

        @extend_schema(
            request=PolymorphicProxySerializer(
                component_name='MetaPerson',
                serializers=[
                    LegalPersonSerializer, NaturalPersonSerializer,
                ],
                resource_type_field_name='person_type',
            )
        )
        def create(self, request, *args, **kwargs):
            return Response(...)

    **Beware** that this is not a real serializer and it will raise an AssertionError
    if used in that way. It **cannot** be used in views as ``serializer_class``
    or as field in an actual serializer. It is solely meant for annotation purposes.

    Also make sure that each sub-serializer has a field named after the value of
    ``resource_type_field_name`` (discriminator field). Generated clients will likely
    depend on the existence of this field.

    Setting ``resource_type_field_name`` to ``None`` will remove the discriminator
    altogether. This may be useful in certain situations, but will most likely break
    client generation. Another use-case is explicit control over sub-serializer's ``many``
    attribute. To explicitly control this aspect, you need disable the discriminator with
    ``resource_type_field_name=None`` as well as disable automatic list handling with
    ``many=False``.

    It is **strongly** recommended to pass the ``Serializers`` as **list**,
    and by that let *drf-spectacular* retrieve the field and handle the mapping
    automatically. In special circumstances, the field may not available when
    *drf-spectacular* processes the serializer. In those cases you can explicitly state
    the mapping with ``{'legal': LegalPersonSerializer, ...}``, but it is then your
    responsibility to have a valid mapping.

    It is also permissible to provide a callable with no parameters for ``serializers``,
    such as a lambda that will return an appropriate list or dict when evaluated.
    """
    def __init__(
            self,
            component_name: str,
            serializers: Union[
                Sequence[_SerializerType],
                Dict[str, _SerializerType],
                Callable[[], Sequence[_SerializerType]],
                Callable[[], Dict[str, _SerializerType]]
            ],
            resource_type_field_name: Optional[str],
            many: Optional[bool] = None,
            **kwargs
    ):
        self.component_name = component_name
        self.serializers = serializers
        self.resource_type_field_name = resource_type_field_name
        if self._many is False:  # type: ignore[attr-defined]
            set_override(self, 'many', False)
        # retain kwargs in context for potential anonymous re-init with many=True
        kwargs.setdefault('context', {}).update({
            'component_name': component_name,
            'serializers': serializers,
            'resource_type_field_name': resource_type_field_name
        })
        super().__init__(**kwargs)

    def __new__(cls, *args, **kwargs):
        many = kwargs.pop('many', None)
        if many is True:
            context = kwargs.get('context', {})
            for arg in ['component_name', 'serializers', 'resource_type_field_name']:
                if arg in context:
                    kwargs[arg] = context.pop(arg)  # re-apply retained args
            instance = cls.many_init(*args, **kwargs)
        else:
            instance = super().__new__(cls, *args, **kwargs)
        instance._many = many
        return instance

    @property
    def serializers(self):
        if callable(self._serializers):
            self._serializers = self._serializers()
        return self._serializers

    @serializers.setter
    def serializers(self, value):
        self._serializers = value

    @property
    def data(self):
        self._trap()

    def to_internal_value(self, data):
        self._trap()

    def to_representation(self, instance):
        self._trap()

    def _trap(self):
        raise AssertionError(
            "PolymorphicProxySerializer is an annotation helper and not supposed to "
            "be used for real requests. See documentation for correct usage."
        )


class OpenApiSchemaBase:
    pass


class OpenApiExample(OpenApiSchemaBase):
    """
    Helper class to document a API parameter / request body / response body
    with a concrete example value.

    It is recommended to provide a singular example value, since pagination
    and list responses are handled by drf-spectacular.

    The example will be attached to the operation object where appropriate,
    i.e. where the given ``media_type``, ``status_code`` and modifiers match.
    Example that do not match any scenario are ignored.

    - media_type will default to 'application/json' unless implicitly specified
      through :class:`.OpenApiResponse`
    - status_codes will default to [200, 201] unless implicitly specified
      through :class:`.OpenApiResponse`
    """
    def __init__(
            self,
            name: str,
            value: Any = empty,
            external_value: str = '',
            summary: _StrOrPromise = '',
            description: _StrOrPromise = '',
            request_only: bool = False,
            response_only: bool = False,
            parameter_only: Optional[Tuple[str, _ParameterLocationType]] = None,
            media_type: Optional[str] = None,
            status_codes: Optional[Sequence[Union[str, int]]] = None,
    ):
        self.name = name
        self.summary = summary
        self.description = description
        self.value = value
        self.external_value = external_value
        self.request_only = request_only
        self.response_only = response_only
        self.parameter_only = parameter_only
        self.media_type = media_type
        self.status_codes = status_codes


class OpenApiParameter(OpenApiSchemaBase):
    """
    Helper class to document request query/path/header/cookie parameters.
    Can also be used to document response headers.

    Please note that not all arguments apply to all ``location``/``type``/direction
    variations, e.g. path parameters are ``required=True`` by definition.

    For valid ``style`` choices please consult the
    `OpenAPI specification <https://swagger.io/specification/#style-values>`_.
    """
    QUERY: Final = 'query'
    PATH: Final = 'path'
    HEADER: Final = 'header'
    COOKIE: Final = 'cookie'

    def __init__(
            self,
            name: str,
            type: Union[_SerializerType, _KnownPythonTypes, OpenApiTypes, _SchemaType] = str,
            location: _ParameterLocationType = QUERY,
            required: bool = False,
            description: _StrOrPromise = '',
            enum: Optional[Sequence[Any]] = None,
            pattern: Optional[str] = None,
            deprecated: bool = False,
            style: Optional[str] = None,
            explode: Optional[bool] = None,
            default: Any = None,
            allow_blank: bool = True,
            many: Optional[bool] = None,
            examples: Optional[Sequence[OpenApiExample]] = None,
            extensions: Optional[Dict[str, Any]] = None,
            exclude: bool = False,
            response: Union[bool, Sequence[Union[int, str]]] = False,
    ):
        self.name = name
        self.type = type
        self.location = location
        self.required = required
        self.description = description
        self.enum = enum
        self.pattern = pattern
        self.deprecated = deprecated
        self.style = style
        self.explode = explode
        self.default = default
        self.allow_blank = allow_blank
        self.many = many
        self.examples = examples or []
        self.extensions = extensions
        self.exclude = exclude
        self.response = response


class OpenApiResponse(OpenApiSchemaBase):
    """
    Helper class to bundle a response object (``Serializer``, ``OpenApiType``,
    raw schema, etc) together with a response object description and/or examples.
    Examples can alternatively be provided via :func:`@extend_schema <.extend_schema>`.

    This class is especially helpful for explicitly describing status codes on a
    "Response Object" level.
    """
    def __init__(
            self,
            response: Any = None,
            description: _StrOrPromise = '',
            examples: Optional[Sequence[OpenApiExample]] = None
    ):
        self.response = response
        self.description = description
        self.examples = examples or []


class OpenApiRequest(OpenApiSchemaBase):
    """
    Helper class to combine a request object (``Serializer``, ``OpenApiType``,
    raw schema, etc.) together with an encoding object and/or examples.
    Examples can alternatively be provided via :func:`@extend_schema <.extend_schema>`.

    This class is especially helpful for customizing value encoding for
    ``application/x-www-form-urlencoded`` and ``multipart/*``. The encoding parameter
    takes a dictionary with field names as keys and encoding objects as values.
    Refer to the `specification <https://swagger.io/specification/#encoding-object>`_
    on how to build a valid encoding object.
    """
    def __init__(
            self,
            request: Any = None,
            encoding: Optional[Dict[str, Dict[str, Any]]] = None,
            examples: Optional[Sequence[OpenApiExample]] = None,
    ):
        self.request = request
        self.encoding = encoding
        self.examples = examples or []


F = TypeVar('F', bound=Callable[..., Any])


class OpenApiCallback(OpenApiSchemaBase):
    """
    Helper class to bundle a callback definition. This specifies a view on the callee's
    side, effectively stating the expectations on the receiving end. Please note that this
    particular :func:`@extend_schema <.extend_schema>` instance operates from the perspective
    of the callback origin, which means that ``request`` specifies the outgoing request.

    For convenience sake, we assume the callback sends ``application/json`` and return a ``200``.
    If that is not sufficient, you can use ``request`` and ``responses`` overloads just as you
    normally would.

    :param name: Name under which the this callback is listed in the schema.
    :param path: Path on which the callback operation is performed. To reference request
        body contents, please refer to OpenAPI specification's
        `key expressions <https://swagger.io/specification/#key-expression>`_ for valid choices.
    :param decorator: :func:`@extend_schema <.extend_schema>` decorator that specifies the receiving
        endpoint. In this special context the allowed parameters are ``requests``, ``responses``,
        ``summary``, ``description``, ``deprecated``.
    """
    def __init__(
            self,
            name: _StrOrPromise,
            path: str,
            decorator: Union[Callable[[F], F], Dict[str, Callable[[F], F]], Dict[str, Any]],
    ):
        self.name = name
        self.path = path
        self.decorator = decorator


class OpenApiWebhook(OpenApiSchemaBase):
    """
    Helper class to document webhook definitions. A webhook specifies a possible out-of-band
    request initiated by the API provider and the expected responses from the consumer.

    Please note that this particular :func:`@extend_schema <.extend_schema>` instance operates
    from the perspective of the webhook origin, which means that ``request`` specifies the
    outgoing request.

    For convenience sake, we assume the API provider sends a POST request with a body of type
    ``application/json`` and the receiver responds with ``200`` if the event was successfully
    received.

    :param name: Name under which this webhook is listed in the schema.
    :param decorator: :func:`@extend_schema <.extend_schema>` decorator that specifies the receiving
        endpoint. In this special context the allowed parameters are ``requests``, ``responses``,
        ``summary``, ``description``, ``deprecated``.
    """
    def __init__(
            self,
            name: _StrOrPromise,
            decorator: Union[Callable[[F], F], Dict[str, Callable[[F], F]], Dict[str, Any]],
    ):
        self.name = name
        self.decorator = decorator


def extend_schema(
        operation_id: Optional[str] = None,
        parameters: Optional[Sequence[Union[OpenApiParameter, _SerializerType]]] = None,
        request: Any = empty,
        responses: Any = empty,
        auth: Optional[Sequence[str]] = None,
        description: Optional[_StrOrPromise] = None,
        summary: Optional[_StrOrPromise] = None,
        deprecated: Optional[bool] = None,
        tags: Optional[Sequence[str]] = None,
        filters: Optional[bool] = None,
        exclude: Optional[bool] = None,
        operation: Optional[_SchemaType] = None,
        methods: Optional[Sequence[str]] = None,
        versions: Optional[Sequence[str]] = None,
        examples: Optional[Sequence[OpenApiExample]] = None,
        extensions: Optional[Dict[str, Any]] = None,
        callbacks: Optional[Sequence[OpenApiCallback]] = None,
        external_docs: Optional[Union[Dict[str, str], str]] = None,
) -> Callable[[F], F]:
    """
    Decorator mainly for the "view" method kind. Partially or completely overrides
    what would be otherwise generated by drf-spectacular.

    :param operation_id: replaces the auto-generated operation_id. make sure there
        are no naming collisions.
    :param parameters: list of additional or replacement parameters added to the
        auto-discovered fields.
    :param responses: replaces the discovered Serializer. Takes a variety of
        inputs that can be used individually or combined

        - ``Serializer`` class
        - ``Serializer`` instance (e.g. ``Serializer(many=True)`` for listings)
        - basic types or instances of ``OpenApiTypes``
        - :class:`.OpenApiResponse` for bundling any of the other choices together with
          either a dedicated response description and/or examples.
        - :class:`.PolymorphicProxySerializer` for signaling that
          the operation may yield data from different serializers depending
          on the circumstances.
        - ``dict`` with status codes as keys and one of the above as values.
          Additionally in this case, it is also possible to provide a raw schema dict
          as value.
        - ``dict`` with tuples (status_code, media_type) as keys and one of the above
          as values. Additionally in this case, it is also possible to provide a raw
          schema dict as value.
    :param request: replaces the discovered ``Serializer``. Takes a variety of inputs

        - ``Serializer`` class/instance
        - basic types or instances of ``OpenApiTypes``
        - :class:`.PolymorphicProxySerializer` for signaling that the operation
          accepts a set of different types of objects.
        - ``dict`` with media_type as keys and one of the above as values. Additionally, in
          this case, it is also possible to provide a raw schema dict as value.
    :param auth: replace discovered auth with explicit list of auth methods
    :param description: replaces discovered doc strings
    :param summary: an optional short summary of the description
    :param deprecated: mark operation as deprecated
    :param tags: override default list of tags
    :param filters: ignore list detection and forcefully enable/disable filter discovery
    :param exclude: set True to exclude operation from schema
    :param operation: manually override what auto-discovery would generate. you must
        provide a OpenAPI3-compliant dictionary that gets directly translated to YAML.
    :param methods: scope extend_schema to specific methods. matches all by default.
    :param versions: scope extend_schema to specific API version. matches all by default.
    :param examples: attach request/response examples to the operation
    :param extensions: specification extensions, e.g. ``x-badges``, ``x-code-samples``, etc.
    :param callbacks: associate callbacks with this endpoint
    :param external_docs: Link external documentation. Provide a dict with an "url" key and
        optionally a "description" key. For convenience, if only a string is given it is
        treated as the URL.
    :return:
    """
    if methods is not None:
        methods = [method.upper() for method in methods]

    def decorator(f):
        BaseSchema = (
            # explicit manually set schema or previous view annotation
            getattr(f, 'schema', None)
            # previously set schema with @extend_schema on views methods
            or getattr(f, 'kwargs', {}).get('schema', None)
            # previously set schema with @extend_schema on @api_view
            or getattr(getattr(f, 'cls', None), 'kwargs', {}).get('schema', None)
            # the default
            or api_settings.DEFAULT_SCHEMA_CLASS
        )

        if not inspect.isclass(BaseSchema):
            BaseSchema = BaseSchema.__class__

        def is_in_scope(ext_schema):
            version, _ = ext_schema.view.determine_version(
                ext_schema.view.request,
                **ext_schema.view.kwargs
            )
            version_scope = versions is None or version in versions
            method_scope = methods is None or ext_schema.method in methods
            return method_scope and version_scope

        class ExtendedSchema(BaseSchema):
            def get_operation(self, path, path_regex, path_prefix, method, registry):
                self.method = method.upper()

                if operation is not None and is_in_scope(self):
                    return operation
                return super().get_operation(path, path_regex, path_prefix, method, registry)

            def is_excluded(self):
                if exclude is not None and is_in_scope(self):
                    return exclude
                return super().is_excluded()

            def get_operation_id(self):
                if operation_id and is_in_scope(self):
                    return operation_id
                return super().get_operation_id()

            def get_override_parameters(self):
                if parameters and is_in_scope(self):
                    return super().get_override_parameters() + parameters
                return super().get_override_parameters()

            def get_auth(self):
                if auth is not None and is_in_scope(self):
                    return auth
                return super().get_auth()

            def get_examples(self):
                if examples and is_in_scope(self):
                    return super().get_examples() + examples
                return super().get_examples()

            def get_request_serializer(self):
                if request is not empty and is_in_scope(self):
                    return request
                return super().get_request_serializer()

            def get_response_serializers(self):
                if responses is not empty and is_in_scope(self):
                    return responses
                return super().get_response_serializers()

            def get_description(self):
                if description and is_in_scope(self):
                    return description
                return super().get_description()

            def get_summary(self):
                if summary and is_in_scope(self):
                    return str(summary)
                return super().get_summary()

            def is_deprecated(self):
                if deprecated and is_in_scope(self):
                    return deprecated
                return super().is_deprecated()

            def get_tags(self):
                if tags is not None and is_in_scope(self):
                    return tags
                return super().get_tags()

            def get_extensions(self):
                if extensions and is_in_scope(self):
                    return extensions
                return super().get_extensions()

            def get_filter_backends(self):
                if filters is not None and is_in_scope(self):
                    return getattr(self.view, 'filter_backends', []) if filters else []
                return super().get_filter_backends()

            def get_callbacks(self):
                if callbacks is not None and is_in_scope(self):
                    return callbacks
                return super().get_callbacks()

            def get_external_docs(self):
                if external_docs is not None and is_in_scope(self):
                    return external_docs
                return super().get_external_docs()

        if inspect.isclass(f):
            # either direct decoration of views, or unpacked @api_view from OpenApiViewExtension
            if operation_id is not None or operation is not None:
                error(
                    f'using @extend_schema on viewset class {f.__name__} with parameters '
                    f'operation_id or operation will most likely result in a broken schema.',
                    delayed=f,
                )
            # reorder schema class MRO so that view method annotation takes precedence
            # over view class annotation. only relevant if there is a method annotation
            for view_method_name in get_view_method_names(view=f, schema=BaseSchema):
                if 'schema' not in getattr(getattr(f, view_method_name), 'kwargs', {}):
                    continue
                view_method = isolate_view_method(f, view_method_name)
                view_method.kwargs['schema'] = type(
                    'ExtendedMetaSchema', (view_method.kwargs['schema'], ExtendedSchema), {}
                )
            # persist schema on class to provide annotation to derived view methods.
            # the second purpose is to serve as base for view multi-annotation
            f.schema = ExtendedSchema()
            return f
        elif callable(f) and hasattr(f, 'cls'):
            # 'cls' attr signals that as_view() was called, which only applies to @api_view.
            # keep a "unused" schema reference at root level for multi annotation convenience.
            setattr(f.cls, 'kwargs', {'schema': ExtendedSchema})
            # set schema on method kwargs context to emulate regular view behaviour.
            for method in f.cls.http_method_names:
                setattr(getattr(f.cls, method), 'kwargs', {'schema': ExtendedSchema})
            return f
        elif callable(f):
            # custom actions have kwargs in their context, others don't. create it so our create_view
            # implementation can overwrite the default schema
            if not hasattr(f, 'kwargs'):
                f.kwargs = {}
            # this simulates what @action is actually doing. somewhere along the line in this process
            # the schema is picked up from kwargs and used. it's involved my dear friends.
            # use class instead of instance due to descriptor weakref reverse collisions
            f.kwargs['schema'] = ExtendedSchema
            return f
        else:
            return f

    return decorator


def extend_schema_field(
        field: Union[_SerializerType, _FieldType, OpenApiTypes, _SchemaType, _KnownPythonTypes],
        component_name: Optional[str] = None
) -> Callable[[F], F]:
    """
    Decorator for the "field" kind. Can be used with ``SerializerMethodField`` (annotate the actual
    method) or with custom ``serializers.Field`` implementations.

    If your custom serializer field base class is already the desired type, decoration is not necessary.
    To override the discovered base class type, you can decorate your custom field class.

    Always takes precedence over other mechanisms (e.g. type hints, auto-discovery).

    :param field: accepts a ``Serializer``, :class:`~.types.OpenApiTypes` or raw ``dict``
    :param component_name: signals that the field should be broken out as separate component
    """

    def decorator(f):
        set_override(f, 'field', field)
        set_override(f, 'field_component_name', component_name)
        return f

    return decorator


def extend_schema_serializer(
        many: Optional[bool] = None,
        exclude_fields: Optional[Sequence[str]] = None,
        deprecate_fields: Optional[Sequence[str]] = None,
        examples: Optional[Sequence[OpenApiExample]] = None,
        extensions: Optional[Dict[str, Any]] = None,
        component_name: Optional[str] = None,
        description: Optional[_StrOrPromise] = None,
) -> Callable[[F], F]:
    """
    Decorator for the "serializer" kind. Intended for overriding default serializer behaviour that
    cannot be influenced through :func:`@extend_schema <.extend_schema>`.

    :param many: override how serializer is initialized. Mainly used to coerce the list view detection
        heuristic to acknowledge a non-list serializer.
    :param exclude_fields: fields to ignore while processing the serializer. only affects the
        schema. fields will still be exposed through the API.
    :param deprecate_fields: fields to mark as deprecated while processing the serializer.
    :param examples: define example data to serializer.
    :param extensions: specification extensions, e.g. ``x-is-dynamic``, etc.
    :param component_name: override default class name extraction.
    :param description: override the class docstring for component description
    """
    def decorator(klass):
        if many is not None:
            set_override(klass, 'many', many)
        if exclude_fields:
            set_override(klass, 'exclude_fields', exclude_fields)
        if deprecate_fields:
            set_override(klass, 'deprecate_fields', deprecate_fields)
        if examples:
            set_override(klass, 'examples', examples)
        if extensions:
            set_override(klass, 'extensions', extensions)
        if component_name:
            set_override(klass, 'component_name', component_name)
        if description is not None:
            set_override(klass, 'description', description)
        return klass

    return decorator


def extend_schema_view(**kwargs) -> Callable[[F], F]:
    """
    Convenience decorator for the "view" kind. Intended for annotating derived view methods that
    are are not directly present in the view (usually methods like ``list`` or ``retrieve``).
    Spares you from overriding methods like ``list``, only to perform a super call in the body
    so that you have have something to attach :func:`@extend_schema <.extend_schema>` to.

    This decorator also takes care of safely attaching annotations to derived view methods,
    preventing leakage into unrelated views.

    This decorator also supports custom DRF ``@action`` with the method name as the key.

    :param kwargs: method names as argument names and :func:`@extend_schema <.extend_schema>`
      calls as values
    """
    def decorator(view):
        # special case for @api_view. redirect decoration to enclosed WrappedAPIView
        if callable(view) and hasattr(view, 'cls'):
            extend_schema_view(**kwargs)(view.cls)
            return view

        available_view_methods = get_view_method_names(view)

        for method_name, method_decorator in kwargs.items():
            if method_name not in available_view_methods:
                warn(
                    f'@extend_schema_view argument "{method_name}" was not found on view '
                    f'{view.__name__}. method override for "{method_name}" will be ignored.',
                    delayed=view
                )
                continue

            # the context of derived methods must not be altered, as it belongs to the
            # other view. create a new context so the schema can be safely stored in the
            # wrapped_method. view methods that are not derived can be safely altered.
            if hasattr(method_decorator, '__iter__'):
                for sub_method_decorator in method_decorator:
                    sub_method_decorator(isolate_view_method(view, method_name))
            else:
                method_decorator(isolate_view_method(view, method_name))
        return view

    return decorator


def inline_serializer(name: str, fields: Dict[str, Field], **kwargs) -> Serializer:
    """
    A helper function to create an inline serializer. Primary use is with
    :func:`@extend_schema <.extend_schema>`, where one needs an implicit one-off
    serializer that is not reflected in an actual class.

    :param name: name of the
    :param fields: dict with field names as keys and serializer fields as values
    :param kwargs: optional kwargs for serializer initialization
    """
    serializer_class = type(name, (Serializer,), fields)
    return serializer_class(**kwargs)
