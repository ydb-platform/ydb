from abc import abstractmethod
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Type, Union

from drf_spectacular.plumbing import OpenApiGeneratorExtension
from drf_spectacular.utils import Direction

if TYPE_CHECKING:
    from rest_framework.views import APIView

    from drf_spectacular.openapi import AutoSchema


_SchemaType = Dict[str, Any]


class OpenApiAuthenticationExtension(OpenApiGeneratorExtension['OpenApiAuthenticationExtension']):
    """
    Extension for specifying authentication schemes.

    The common use-case usually consists of setting a ``name`` string and returning a dict from
    ``get_security_definition``. To model a group of headers that go together, set a list
    of names and return a corresponding list of definitions from ``get_security_definition``.

    The view class is available via ``auto_schema.view``, while the original authentication class
    can be accessed via ``self.target``. If you want to override an included extension, be sure to
    set a higher matching priority by setting the class attribute ``priority = 1`` or higher.

    get_security_requirement is expected to return a dict with security object names as keys and a
    scope list as value (usually just []). More than one key in the dict means that each entry is
    required (AND). If you need alternate variations (OR), return a list of those dicts instead.

    ``get_security_definition()`` is expected to return a valid `OpenAPI security scheme object
    <https://spec.openapis.org/oas/v3.0.3#security-scheme-object>`_
    """
    _registry: List[Type['OpenApiAuthenticationExtension']] = []

    name: Union[str, List[str]]

    def get_security_requirement(
            self, auto_schema: 'AutoSchema'
    ) -> Union[Dict[str, List[Any]], List[Dict[str, List[Any]]]]:
        assert self.name, 'name(s) must be specified'
        if isinstance(self.name, str):
            return {self.name: []}
        else:
            return {name: [] for name in self.name}

    @abstractmethod
    def get_security_definition(self, auto_schema: 'AutoSchema') -> Union[_SchemaType, List[_SchemaType]]:
        pass  # pragma: no cover


class OpenApiSerializerExtension(OpenApiGeneratorExtension['OpenApiSerializerExtension']):
    """
    Extension for replacing an insufficient or specifying an unknown Serializer schema.

    The existing implementation of ``map_serializer()`` will generate the same result
    as *drf-spectacular* would. Either augment or replace the generated schema. The
    view instance is available via ``auto_schema.view``, while the original serializer
    can be accessed via ``self.target``.

    ``map_serializer()`` is expected to return a valid `OpenAPI schema object
    <https://spec.openapis.org/oas/v3.0.3#schema-object>`_.
    """
    _registry: List[Type['OpenApiSerializerExtension']] = []

    def get_name(self, auto_schema: 'AutoSchema', direction: Direction) -> Optional[str]:
        """ return str for overriding default name extraction """
        return None

    def get_identity(self, auto_schema: 'AutoSchema', direction: Direction) -> Any:
        """ return anything to compare instances of target. Target will be used by default. """
        return None

    def map_serializer(self, auto_schema: 'AutoSchema', direction: Direction) -> _SchemaType:
        """ override for customized serializer mapping """
        return auto_schema._map_serializer(self.target_class, direction, bypass_extensions=True)


class OpenApiSerializerFieldExtension(OpenApiGeneratorExtension['OpenApiSerializerFieldExtension']):
    """
    Extension for replacing an insufficient or specifying an unknown SerializerField schema.

    To augment the default schema, you can get what *drf-spectacular* would generate with
    ``auto_schema._map_serializer_field(self.target, direction, bypass_extensions=True)``.
    and edit the returned schema at your discretion. Beware that this may still emit
    warnings, in which case manual construction is advisable.

    ``map_serializer_field()`` is expected to return a valid `OpenAPI schema object
    <https://spec.openapis.org/oas/v3.0.3#schema-object>`_.
    """
    _registry: List[Type['OpenApiSerializerFieldExtension']] = []

    def get_name(self) -> Optional[str]:
        """ return str for breaking out field schema into separate named component """
        return None

    @abstractmethod
    def map_serializer_field(self, auto_schema: 'AutoSchema', direction: Direction) -> _SchemaType:
        """ override for customized serializer field mapping """
        pass  # pragma: no cover


class OpenApiViewExtension(OpenApiGeneratorExtension['OpenApiViewExtension']):
    """
    Extension for replacing discovered views with a more schema-appropriate/annotated version.

    ``view_replacement()`` is expected to return a subclass of ``APIView`` (which includes
    ``ViewSet`` et al.). The discovered original view callback can be accessed with
    ``self.target_callback``, while the discovered original view class can be accessed
    with ``self.target`` and can be subclassed if desired.
    """
    _registry: List[Type['OpenApiViewExtension']] = []

    def __init__(self, target_callback):
        super().__init__(target_callback.cls)
        self.target_callback = target_callback

    @classmethod
    def _load_class(cls):
        super()._load_class()
        # special case @api_view: view class is nested in the cls attr of the function object
        if hasattr(cls.target_class, 'cls'):
            cls.target_class = cls.target_class.cls

    @classmethod
    def get_match(cls, target_callback) -> 'Optional[OpenApiViewExtension]':
        for extension in sorted(cls._registry, key=lambda e: e.priority, reverse=True):
            if extension._matches(target_callback.cls):
                return extension(target_callback)
        return None

    @abstractmethod
    def view_replacement(self) -> 'Type[APIView]':
        pass  # pragma: no cover


class OpenApiFilterExtension(OpenApiGeneratorExtension['OpenApiFilterExtension']):
    """
    Extension for specifying a list of filter parameters for a given ``FilterBackend``.

    The original filter class object can be accessed via ``self.target``. The attached view
    is accessible via ``auto_schema.view``.

    ``get_schema_operation_parameters()`` is expected to return either an empty list or a list
    of valid raw `OpenAPI parameter objects
    <https://spec.openapis.org/oas/v3.0.3#parameter-object>`_.
    Using ``drf_spectacular.plumbing.build_parameter_type`` is recommended to generate
    the appropriate raw dict objects.
    """
    _registry: List[Type['OpenApiFilterExtension']] = []

    @abstractmethod
    def get_schema_operation_parameters(self, auto_schema: 'AutoSchema', *args, **kwargs) -> List[_SchemaType]:
        pass  # pragma: no cover
