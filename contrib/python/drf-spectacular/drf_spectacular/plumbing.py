import collections
import functools
import hashlib
import inspect
import json
import re
import sys
import types
import typing
import urllib.parse
from abc import ABCMeta
from collections import OrderedDict, defaultdict
from decimal import Decimal
from enum import Enum
from typing import (
    Any, DefaultDict, Dict, Generic, List, Optional, Sequence, Tuple, Type, TypeVar, Union,
)

if sys.version_info >= (3, 10):
    from typing import TypeGuard  # noqa: F401
else:
    from typing_extensions import TypeGuard  # noqa: F401

import inflection
import uritemplate
from django.apps import apps
from django.db.models.constants import LOOKUP_SEP
from django.db.models.fields.related_descriptors import (
    ForwardManyToOneDescriptor, ManyToManyDescriptor, ReverseManyToOneDescriptor,
    ReverseOneToOneDescriptor,
)
from django.db.models.fields.reverse_related import ForeignObjectRel
from django.db.models.sql.query import Query
from django.urls.converters import get_converters
from django.urls.resolvers import (  # type: ignore[attr-defined]
    _PATH_PARAMETER_COMPONENT_RE, RegexPattern, Resolver404, RoutePattern, URLPattern, URLResolver,
    get_resolver,
)
from django.utils.functional import Promise, cached_property
from django.utils.module_loading import import_string
from django.utils.translation import get_language
from django.utils.translation import gettext_lazy as _
from rest_framework import exceptions, fields, mixins, serializers, versioning
from rest_framework.compat import unicode_http_header
from rest_framework.fields import empty
from rest_framework.settings import api_settings
from rest_framework.test import APIRequestFactory
from rest_framework.utils.encoders import JSONEncoder
from rest_framework.utils.mediatypes import _MediaType
from rest_framework.utils.serializer_helpers import ReturnDict, ReturnList
from uritemplate import URITemplate

from drf_spectacular.drainage import cache, error, get_override, warn
from drf_spectacular.settings import spectacular_settings
from drf_spectacular.types import (
    DJANGO_PATH_CONVERTER_MAPPING, OPENAPI_TYPE_MAPPING, PYTHON_TYPE_MAPPING, OpenApiTypes,
    _KnownPythonTypes,
)
from drf_spectacular.utils import (
    OpenApiExample, OpenApiParameter, OpenApiWebhook, _FieldType, _ListSerializerType,
    _ParameterLocationType, _SchemaType, _SerializerType,
)

try:
    from django.db.models.enums import Choices  # only available in Django>3
except ImportError:
    class Choices:  # type: ignore
        pass

# types.UnionType was added in Python 3.10 for new PEP 604 pipe union syntax
if hasattr(types, 'UnionType'):
    UNION_TYPES: Tuple[Any, ...] = (Union, types.UnionType)
else:
    UNION_TYPES = (Union,)

LITERAL_TYPES: Tuple[Any, ...] = ()
TYPED_DICT_META_TYPES: Tuple[Any, ...] = ()
TYPE_ALIAS_TYPES: Tuple[Any, ...] = ()

if sys.version_info >= (3, 8):
    from typing import Literal as _PyLiteral
    from typing import _TypedDictMeta as _PyTypedDictMeta  # type: ignore[attr-defined]
    LITERAL_TYPES += (_PyLiteral,)
    TYPED_DICT_META_TYPES += (_PyTypedDictMeta,)

if sys.version_info >= (3, 12):
    from typing import TypeAliasType
    TYPE_ALIAS_TYPES += (TypeAliasType,)

try:
    from typing_extensions import Literal as _PxLiteral
    from typing_extensions import _TypedDictMeta as _PxTypedDictMeta  # type: ignore[attr-defined]
    LITERAL_TYPES += (_PxLiteral,)
    TYPED_DICT_META_TYPES += (_PxTypedDictMeta,)
except ImportError:
    pass

if sys.version_info >= (3, 8):
    CACHED_PROPERTY_FUNCS = (functools.cached_property, cached_property)
else:
    CACHED_PROPERTY_FUNCS = (cached_property,)


class _Sentinel:
    pass


class UnableToProceedError(Exception):
    pass


def get_class(obj) -> type:
    return obj if inspect.isclass(obj) else obj.__class__


def force_instance(serializer_or_field):
    if not inspect.isclass(serializer_or_field):
        return serializer_or_field
    elif issubclass(serializer_or_field, (serializers.BaseSerializer, fields.Field)):
        return serializer_or_field()
    else:
        return serializer_or_field


def is_serializer(obj, strict=False) -> TypeGuard[_SerializerType]:

    from drf_spectacular.extensions import OpenApiSerializerExtension
    return (
        isinstance(force_instance(obj), serializers.BaseSerializer)
        or (bool(OpenApiSerializerExtension.get_match(obj)) and not strict)
    )


def is_list_serializer(obj: Any) -> TypeGuard[_ListSerializerType]:
    return isinstance(force_instance(obj), serializers.ListSerializer)


def get_list_serializer(obj: Any):
    return force_instance(obj) if is_list_serializer(obj) else get_class(obj)(many=True, context=obj.context)


def is_list_serializer_customized(obj) -> bool:
    return (
        is_serializer(obj, strict=True)
        and get_class(get_list_serializer(obj)).to_representation  # type: ignore
        is not serializers.ListSerializer.to_representation
    )


def is_basic_serializer(obj: Any) -> TypeGuard[_SerializerType]:
    return is_serializer(obj) and not is_list_serializer(obj)


def is_field(obj: Any) -> TypeGuard[_FieldType]:
    # make sure obj is a serializer field and nothing else.
    # guard against serializers because BaseSerializer(Field)
    return isinstance(force_instance(obj), fields.Field) and not is_serializer(obj)


def is_basic_type(obj: Any, allow_none=True) -> TypeGuard[_KnownPythonTypes]:
    if not isinstance(obj, collections.abc.Hashable):
        return False
    if not allow_none and (obj is None or obj is OpenApiTypes.NONE):
        return False
    return obj in get_openapi_type_mapping() or obj in PYTHON_TYPE_MAPPING


def is_patched_serializer(serializer, direction) -> bool:
    return bool(
        spectacular_settings.COMPONENT_SPLIT_PATCH
        and getattr(serializer, 'partial', None)
        and not getattr(serializer, 'read_only', None)
        and not (spectacular_settings.COMPONENT_SPLIT_REQUEST and direction == 'response')
    )


def is_trivial_string_variation(a: str, b: str) -> bool:
    a = (a or '').strip().lower().replace(' ', '_').replace('-', '_')
    b = (b or '').strip().lower().replace(' ', '_').replace('-', '_')
    return a == b


def assert_basic_serializer(serializer) -> None:
    assert is_basic_serializer(serializer), (
        f'internal assumption violated because we expected a basic serializer here and '
        f'instead got a "{serializer}". This may be the result of another app doing '
        f'some unexpected magic or an invalid internal call. Feel free to report this '
        f'as a bug at https://github.com/tfranzel/drf-spectacular/issues'
    )


@cache
def get_lib_doc_excludes():
    # do not import on package level due to potential import recursion when loading
    # extensions as recommended:  USER's settings.py -> USER EXTENSIONS -> extensions.py
    # -> plumbing.py -> DRF views -> DRF DefaultSchema -> openapi.py - plumbing.py -> Loop
    from rest_framework import generics, views, viewsets
    return [
        object,
        dict,
        Generic,
        views.APIView,
        *[getattr(serializers, c) for c in dir(serializers) if c.endswith('Serializer')],
        *[getattr(viewsets, c) for c in dir(viewsets) if c.endswith('ViewSet')],
        *[getattr(generics, c) for c in dir(generics) if c.endswith('APIView')],
        *[getattr(mixins, c) for c in dir(mixins) if c.endswith('Mixin')],
    ]


def get_view_model(view, emit_warnings=True):
    """
    obtain model from view via view's queryset. try safer view attribute first
    before going through get_queryset(), which may perform arbitrary operations.
    """
    model = getattr(getattr(view, 'queryset', None), 'model', None)

    if model is not None:
        return model

    try:
        return view.get_queryset().model
    except Exception as exc:
        if emit_warnings:
            warn(
                f'Failed to obtain model through view\'s queryset due to raised exception. '
                f'Prevent this either by setting "queryset = Model.objects.none()" on the '
                f'view, checking for "getattr(self, "swagger_fake_view", False)" in '
                f'get_queryset() or by simply using @extend_schema. (Exception: {exc})'
            )


def get_doc(obj) -> str:
    """ get doc string with fallback on obj's base classes (ignoring DRF documentation). """
    def post_cleanup(doc: str) -> str:
        # also clean up trailing whitespace for each line
        return '\n'.join(line.rstrip() for line in doc.rstrip().split('\n'))

    if obj is None:
        return ''
    if not inspect.isclass(obj):
        return post_cleanup(inspect.getdoc(obj) or '')

    def safe_index(lst, item):
        try:
            return lst.index(item)
        except ValueError:
            return len(lst)

    lib_barrier = min(
        safe_index(obj.__mro__, c) for c in spectacular_settings.GET_LIB_DOC_EXCLUDES()
    )
    for cls in obj.__mro__[:lib_barrier]:
        if cls.__doc__:
            return post_cleanup(inspect.cleandoc(cls.__doc__))
    return ''


def get_type_hints(obj) -> Dict[str, Any]:
    """ unpack wrapped partial object and use actual func object """
    if isinstance(obj, functools.partial):
        obj = obj.func
    return typing.get_type_hints(obj)


@cache
def get_openapi_type_mapping():
    return {
        **OPENAPI_TYPE_MAPPING,
        OpenApiTypes.OBJECT: build_generic_type(),
    }


def get_manager(model):
    if not hasattr(model, spectacular_settings.DEFAULT_QUERY_MANAGER):
        error(
            f'Failed to obtain queryset from model "{model.__name__}" because manager '
            f'"{spectacular_settings.DEFAULT_QUERY_MANAGER}" was not found. You may '
            f'need to change the DEFAULT_QUERY_MANAGER setting. bailing.'
        )
    return getattr(model, spectacular_settings.DEFAULT_QUERY_MANAGER)


def build_generic_type():
    if spectacular_settings.GENERIC_ADDITIONAL_PROPERTIES is None:
        return {'type': 'object'}
    elif spectacular_settings.GENERIC_ADDITIONAL_PROPERTIES == 'bool':
        return {'type': 'object', 'additionalProperties': True}
    else:
        return {'type': 'object', 'additionalProperties': {}}


def build_basic_type(obj: Union[_KnownPythonTypes, OpenApiTypes]) -> Optional[_SchemaType]:
    """
    resolve either enum or actual type and yield schema template for modification
    """
    openapi_type_mapping = get_openapi_type_mapping()
    if obj is None or type(obj) is None or obj is OpenApiTypes.NONE:
        return None
    elif obj in openapi_type_mapping:
        return dict(openapi_type_mapping[obj])
    elif obj in PYTHON_TYPE_MAPPING:
        return dict(openapi_type_mapping[PYTHON_TYPE_MAPPING[obj]])  # type: ignore[index]
    else:
        warn(f'could not resolve type for "{obj}". defaulting to "string"')
        return dict(openapi_type_mapping[OpenApiTypes.STR])


def build_array_type(schema: _SchemaType, min_length=None, max_length=None) -> _SchemaType:
    schema = {'type': 'array', 'items': schema}
    if min_length is not None:
        schema['minLength'] = min_length
    if max_length is not None:
        schema['maxLength'] = max_length
    return schema


def build_object_type(
        properties: Optional[_SchemaType] = None,
        required=None,
        description: Optional[str] = None,
        **kwargs
) -> _SchemaType:
    schema: _SchemaType = {'type': 'object'}
    if description:
        schema['description'] = description.strip()
    if properties:
        schema['properties'] = properties
    if 'additionalProperties' in kwargs:
        schema['additionalProperties'] = kwargs.pop('additionalProperties')
    if required:
        schema['required'] = sorted(required)
    schema.update(kwargs)
    return schema


def build_media_type_object(schema, examples=None, encoding=None) -> _SchemaType:
    media_type_object = {'schema': schema}
    if examples:
        media_type_object['examples'] = examples
    if encoding:
        media_type_object['encoding'] = encoding
    return media_type_object


def build_examples_list(examples: Sequence[OpenApiExample]) -> _SchemaType:
    schema = {}
    for example in examples:
        normalized_name = inflection.camelize(example.name.replace(' ', '_'))
        sub_schema = {}
        if example.value is not empty:
            sub_schema['value'] = example.value
        if example.external_value:
            sub_schema['externalValue'] = example.external_value
        if example.summary:
            sub_schema['summary'] = example.summary
        elif normalized_name != example.name:
            sub_schema['summary'] = example.name
        if example.description:
            sub_schema['description'] = example.description
        schema[normalized_name] = sub_schema
    return schema


def build_parameter_type(
        name: str,
        schema: _SchemaType,
        location: _ParameterLocationType,
        required=False,
        description=None,
        enum=None,
        pattern=None,
        deprecated=False,
        explode=None,
        style=None,
        default=None,
        allow_blank=True,
        examples=None,
        extensions=None,
) -> _SchemaType:
    irrelevant_field_meta = ['readOnly', 'writeOnly']
    if location == OpenApiParameter.PATH:
        irrelevant_field_meta += ['nullable', 'default']
    schema = {
        'in': location,
        'name': name,
        'schema': {k: v for k, v in schema.items() if k not in irrelevant_field_meta},
    }
    if description:
        schema['description'] = description
    if required or location == 'path':
        schema['required'] = True
    if deprecated:
        schema['deprecated'] = True
    if explode is not None:
        schema['explode'] = explode
    if style is not None:
        schema['style'] = style
    if enum:
        # in case of array schema, enum makes little sense on the array itself
        if schema['schema'].get('type') == 'array':
            schema['schema']['items']['enum'] = sorted(enum, key=str)
        else:
            schema['schema']['enum'] = sorted(enum, key=str)
    if pattern is not None:
        # in case of array schema, pattern only makes sense on the items
        if schema['schema'].get('type') == 'array':
            schema['schema']['items']['pattern'] = pattern
        else:
            schema['schema']['pattern'] = pattern
    if default is not None and 'default' not in irrelevant_field_meta:
        schema['schema']['default'] = default
    if not allow_blank and schema['schema'].get('type') == 'string':
        schema['schema']['minLength'] = schema['schema'].get('minLength', 1)
    if examples:
        schema['examples'] = examples
    if extensions:
        schema.update(sanitize_specification_extensions(extensions))
    return schema


def build_choice_field(field) -> _SchemaType:
    choices = list(OrderedDict.fromkeys(field.choices))  # preserve order and remove duplicates

    if field.allow_blank and '' not in choices:
        choices.append('')

    if not choices:
        type = None
    elif all(isinstance(choice, bool) for choice in choices):
        type = 'boolean'
    elif all(isinstance(choice, int) for choice in choices):
        type = 'integer'
    elif all(isinstance(choice, (int, float, Decimal)) for choice in choices):  # `number` includes `integer`
        # Ref: https://tools.ietf.org/html/draft-wright-json-schema-validation-00#section-5.21
        type = 'number'
    elif all(isinstance(choice, str) for choice in choices):
        type = 'string'
    else:
        type = None

    if field.allow_null and None not in choices:
        choices.append(None)

    schema: _SchemaType = {
        # The value of `enum` keyword MUST be an array and SHOULD be unique.
        # Ref: https://tools.ietf.org/html/draft-wright-json-schema-validation-00#section-5.20
        'enum': choices
    }

    # If We figured out `type` then and only then we should set it. It must be a string.
    # Ref: https://swagger.io/docs/specification/data-models/data-types/#mixed-type
    # It is optional but it can not be null.
    # Ref: https://tools.ietf.org/html/draft-wright-json-schema-validation-00#section-5.21
    if type:
        schema['type'] = type

    if spectacular_settings.ENUM_GENERATE_CHOICE_DESCRIPTION:
        schema['description'] = build_choice_description_list(field.choices.items())

    schema['x-spec-enum-id'] = list_hash([(k, v) for k, v in field.choices.items() if k not in ('', None)])

    return schema


def build_choice_description_list(choices) -> str:
    return '\n'.join(f'* `{value}` - {label}' for value, label in choices)


def build_bearer_security_scheme_object(header_name, token_prefix, bearer_format=None):
    """ Either build a bearer scheme or a fallback due to OpenAPI 3.0.3 limitations """
    # normalize Django header quirks
    if header_name.startswith('HTTP_'):
        header_name = header_name[5:]
    header_name = header_name.replace('_', '-').capitalize()

    if token_prefix == 'Bearer' and header_name == 'Authorization':
        return {
            'type': 'http',
            'scheme': 'bearer',
            **({'bearerFormat': bearer_format} if bearer_format else {}),
        }
    else:
        return {
            'type': 'apiKey',
            'in': 'header',
            'name': header_name,
            'description': _(
                'Token-based authentication with required prefix "%s"'
            ) % token_prefix
        }


def build_root_object(paths, components, webhooks, version) -> _SchemaType:
    settings = spectacular_settings
    if settings.VERSION and version:
        version = f'{settings.VERSION} ({version})'
    else:
        version = settings.VERSION or version or ''
    root = {
        'openapi': settings.OAS_VERSION,
        'info': {
            'title': settings.TITLE,
            'version': version,
            **sanitize_specification_extensions(settings.EXTENSIONS_INFO),
        },
        'paths': {**paths, **settings.APPEND_PATHS},
        'components': components,
        **sanitize_specification_extensions(settings.EXTENSIONS_ROOT),
    }
    if settings.DESCRIPTION:
        root['info']['description'] = settings.DESCRIPTION
    if settings.TOS:
        root['info']['termsOfService'] = settings.TOS
    if settings.CONTACT:
        root['info']['contact'] = settings.CONTACT
    if settings.LICENSE:
        root['info']['license'] = settings.LICENSE
    if settings.SERVERS:
        root['servers'] = settings.SERVERS
    if settings.TAGS:
        root['tags'] = settings.TAGS
    if settings.EXTERNAL_DOCS:
        root['externalDocs'] = settings.EXTERNAL_DOCS
    if webhooks:
        root['webhooks'] = webhooks
    return root


def safe_ref(schema: _SchemaType) -> _SchemaType:
    """
    ensure that $ref has its own context and does not remove potential sibling
    entries when $ref is substituted. also remove useless singular "allOf" .
    """
    if '$ref' in schema and len(schema) > 1:
        return {'allOf': [{'$ref': schema.pop('$ref')}], **schema}
    if 'allOf' in schema and len(schema) == 1 and len(schema['allOf']) == 1:
        return schema['allOf'][0]
    return schema


def append_meta(schema: _SchemaType, meta: _SchemaType) -> _SchemaType:
    if spectacular_settings.OAS_VERSION.startswith('3.1'):
        schema = schema.copy()
        meta = meta.copy()

        schema_nullable = meta.pop('nullable', None)
        meta_nullable = schema.pop('nullable', None)

        if schema_nullable or meta_nullable:
            if 'type' in schema:
                if isinstance(schema['type'], str):
                    schema['type'] = [schema['type'], 'null']
                else:
                    schema['type'] = [*schema['type'], 'null']
            elif '$ref' in schema:
                schema = {'oneOf': [schema, {'type': 'null'}]}
            elif len(schema) == 1 and 'oneOf' in schema:
                schema['oneOf'].append({'type': 'null'})
            elif not schema:
                schema = {'oneOf': [{}, {'type': 'null'}]}
            else:
                assert False, 'Invalid nullable case'  # pragma: no cover

        # these two aspects were merged in OpenAPI 3.1
        if "exclusiveMinimum" in schema and "minimum" in schema:
            schema["exclusiveMinimum"] = schema.pop("minimum")
        if "exclusiveMaximum" in schema and "maximum" in schema:
            schema["exclusiveMaximum"] = schema.pop("maximum")

    return safe_ref({**schema, **meta})


def _follow_field_source(model, path: List[str]):
    """
        navigate through root model via given navigation path. supports forward/reverse relations.
    """
    field_or_property = getattr(model, path[0], None)

    if len(path) == 1:
        # end of traversal
        if isinstance(field_or_property, property):
            return field_or_property.fget
        elif isinstance(field_or_property, CACHED_PROPERTY_FUNCS):
            return field_or_property.func
        elif callable(field_or_property):
            return field_or_property
        elif isinstance(field_or_property, ManyToManyDescriptor):
            if field_or_property.reverse:
                return field_or_property.rel.target_field  # m2m reverse
            else:
                return field_or_property.field.target_field  # m2m forward
        elif isinstance(field_or_property, ReverseOneToOneDescriptor):
            return field_or_property.related.target_field  # o2o reverse
        elif isinstance(field_or_property, ReverseManyToOneDescriptor):
            return field_or_property.rel.target_field  # foreign reverse
        elif isinstance(field_or_property, ForwardManyToOneDescriptor):
            return field_or_property.field.target_field  # o2o & foreign forward
        else:
            field = model._meta.get_field(path[0])
            if isinstance(field, ForeignObjectRel):
                # case only occurs when relations are traversed in reverse and
                # not via the related_name (default: X_set) but the model name.
                return field.target_field
            else:
                return field
    else:
        if (
            isinstance(field_or_property, (property,) + CACHED_PROPERTY_FUNCS)
            or callable(field_or_property)
        ):
            if isinstance(field_or_property, property):
                target_model = _follow_return_type(field_or_property.fget)
            elif isinstance(field_or_property, CACHED_PROPERTY_FUNCS):
                target_model = _follow_return_type(field_or_property.func)
            else:
                target_model = _follow_return_type(field_or_property)
            if not target_model:
                raise UnableToProceedError(
                    f'could not follow field source through intermediate property "{path[0]}" '
                    f'on model {model}. Please add a type hint on the model\'s property/function '
                    f'to enable traversal of the source path "{".".join(path)}".'
                )
            return _follow_field_source(target_model, path[1:])
        else:
            target_model = model._meta.get_field(path[0]).related_model
            return _follow_field_source(target_model, path[1:])


def _follow_return_type(a_callable):
    target_type = get_type_hints(a_callable).get('return')
    if target_type is None:
        return target_type
    origin, args = _get_type_hint_origin(target_type)
    if origin in UNION_TYPES:
        type_args = [arg for arg in args if arg is not type(None)]  # noqa: E721
        if len(type_args) > 1:
            warn(
                f'could not traverse Union type, because we don\'t know which type to choose '
                f'from {type_args}. Consider terminating "source" on a custom property '
                f'that indicates the expected Optional/Union type. Defaulting to "string"'
            )
            return target_type
        # Optional:
        return type_args[0]
    return target_type


def follow_field_source(model, path, default=None, emit_warnings=True):
    """
    a model traversal chain "foreignkey.foreignkey.value" can either end with an actual model field
    instance "value" or a model property function named "value". differentiate the cases.

    :return: models.Field or function object
    """
    try:
        return _follow_field_source(model, path)
    except UnableToProceedError as e:
        if emit_warnings:
            warn(e)
    except Exception as exc:
        if emit_warnings:
            warn(
                f'could not resolve field on model {model} with path "{".".join(path)}". '
                f'This is likely a custom field that does some unknown magic. Maybe '
                f'consider annotating the field/property? Defaulting to "string". (Exception: {exc})'
            )

    def dummy_property(obj) -> str:  # type: ignore
        pass  # pragma: no cover
    return default or dummy_property


def follow_model_field_lookup(model, lookup):
    """
    Follow a model lookup `foreignkey__foreignkey__field` in the same
    way that Django QuerySet.filter() does, returning the final models.Field.
    """
    query = Query(model)
    lookup_splitted = lookup.split(LOOKUP_SEP)
    _, field, _, _ = query.names_to_path(lookup_splitted, query.get_meta())
    return field


def alpha_operation_sorter(endpoint):
    """ sort endpoints first alphanumerically by path, then by method order """
    path, path_regex, method, callback = endpoint
    method_priority = {
        'GET': 0,
        'POST': 1,
        'PUT': 2,
        'PATCH': 3,
        'DELETE': 4
    }.get(method, 5)

    # Sort foo{arg} after foo/, but before foo/bar
    if path.endswith('/'):
        path = path[:-1] + ' '
    path = path.replace('{', '!')

    return path, method_priority


class ResolvedComponent:
    # OpenAPI 3.0.3
    SCHEMA = 'schemas'
    RESPONSE = 'responses'
    PARAMETER = 'parameters'
    EXAMPLE = 'examples'
    REQUEST_BODY = 'requestBodies'
    HEADER = 'headers'
    SECURITY_SCHEMA = 'securitySchemes'
    LINK = 'links'
    CALLBACK = 'callbacks'
    # OpenAPI 3.1.0+
    PATH_ITEM = 'pathItems'

    def __init__(self, name, type, schema=None, object=None):
        self.name = name
        self.type = type
        self.schema = schema
        self.object = object

    def __bool__(self):
        return bool(self.name and self.type and self.object)

    @property
    def key(self) -> Tuple[str, str]:
        return self.name, self.type

    @property
    def ref(self) -> _SchemaType:
        assert self.__bool__()
        return {'$ref': f'#/components/{self.type}/{self.name}'}


class ComponentIdentity:
    """ A container class to make object/component comparison explicit """
    def __init__(self, obj):
        self.obj = obj

    def __eq__(self, other):
        if isinstance(other, ComponentIdentity):
            return self.obj == other.obj
        return self.obj == other


class ComponentRegistry:
    def __init__(self) -> None:
        self._components: Dict[Tuple[str, str], ResolvedComponent] = {}

    def register(self, component: ResolvedComponent) -> None:
        if component in self:
            warn(
                f'trying to re-register a {component.type} component with name '
                f'{self._components[component.key].name}. this might lead to '
                f'a incorrect schema. Look out for reused names'
            )
        self._components[component.key] = component

    def register_on_missing(self, component: ResolvedComponent) -> None:
        if component not in self:
            self._components[component.key] = component

    def __contains__(self, component):
        if component.key not in self._components:
            return False

        query_obj = component.object
        registry_obj = self._components[component.key].object

        if isinstance(query_obj, ComponentIdentity) or inspect.isclass(query_obj):
            query_id = query_obj
        else:
            query_id = query_obj.__class__

        if isinstance(registry_obj, ComponentIdentity) or inspect.isclass(registry_obj):
            registry_id = registry_obj
        else:
            registry_id = registry_obj.__class__

        suppress_collision_warning = (
            get_override(registry_id, 'suppress_collision_warning', False)
            or get_override(query_id, 'suppress_collision_warning', False)
        )
        if query_id != registry_id and not suppress_collision_warning:
            warn(
                f'Encountered 2 components with identical names "{component.name}" and '
                f'different identities {query_id} and {registry_id}. This will very '
                f'likely result in an incorrect schema. Try renaming one.'
            )
        return True

    def __getitem__(self, key) -> ResolvedComponent:
        if isinstance(key, ResolvedComponent):
            key = key.key
        return self._components[key]

    def __delitem__(self, key):
        if isinstance(key, ResolvedComponent):
            key = key.key
        del self._components[key]

    def build(self, extra_components) -> _SchemaType:
        output: DefaultDict[str, _SchemaType] = defaultdict(dict)
        # build tree from flat registry
        for component in self._components.values():
            output[component.type][component.name] = component.schema
        # add/override extra components
        for extra_type, extra_component_dict in extra_components.items():
            for component_name, component_schema in extra_component_dict.items():
                output[extra_type][component_name] = component_schema
        # sort by component type then by name
        return {
            type: {name: output[type][name] for name in sorted(output[type].keys())}
            for type in sorted(output.keys())
        }


T = TypeVar('T', bound="OpenApiGeneratorExtension")


class OpenApiGeneratorExtension(Generic[T], metaclass=ABCMeta):
    _registry: List[Type[T]] = []
    target_class: Union[None, str, Type[object]] = None
    match_subclasses = False
    priority = 0
    optional = False

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        cls._registry.append(cls)

    def __init__(self, target):
        self.target = target

    @classmethod
    def _load_class(cls):
        try:
            cls.target_class = import_string(cls.target_class)
        except ImportError:
            installed_apps = apps.app_configs.keys()
            if any(cls.target_class.startswith(app + '.') for app in installed_apps) and not cls.optional:
                warn(
                    f'registered extensions {cls.__name__} for "{cls.target_class}" '
                    f'has an installed app but target class was not found.'
                )
            cls.target_class = None
        except Exception as e:  # pragma: no cover
            installed_apps = apps.app_configs.keys()
            if any(cls.target_class.startswith(app + '.') for app in installed_apps):
                warn(
                    f'Unexpected error {e.__class__.__name__} occurred when attempting '
                    f'to import {cls.target_class} for extension {cls.__name__} ({e}).'
                )
            cls.target_class = None

    @classmethod
    def _matches(cls, target: Any) -> bool:
        if isinstance(cls.target_class, str):
            cls._load_class()

        if cls.target_class is None:
            return False  # app not installed
        elif cls.match_subclasses:
            # Targets may trigger customized check through __subclasscheck__. Attempt to be more robust
            try:
                return issubclass(get_class(target), cls.target_class)  # type: ignore
            except TypeError:
                return False
        else:
            return get_class(target) == cls.target_class

    @classmethod
    def get_match(cls, target: Any) -> Optional[T]:
        for extension in sorted(cls._registry, key=lambda e: e.priority, reverse=True):
            if extension._matches(target):
                return extension(target)
        return None


def deep_import_string(string: str) -> Any:
    """ augmented import from string, e.g. MODULE.CLASS/OBJECT.ATTRIBUTE """
    try:
        return import_string(string)
    except ImportError:
        pass
    try:
        *path, attr = string.split('.')
        obj = import_string('.'.join(path))
        return getattr(obj, attr)
    except (ImportError, AttributeError):
        pass


def load_enum_name_overrides():
    return _load_enum_name_overrides(get_language())


@functools.lru_cache()
def _load_enum_name_overrides(language: str):
    overrides = {}
    for name, choices in spectacular_settings.ENUM_NAME_OVERRIDES.items():
        if isinstance(choices, str):
            choices = deep_import_string(choices)
        if not choices:
            warn(
                f'unable to load choice override for {name} from ENUM_NAME_OVERRIDES. '
                f'please check module path string.'
            )
            continue
        if inspect.isclass(choices) and issubclass(choices, Choices):
            choices = choices.choices
        if inspect.isclass(choices) and issubclass(choices, Enum):
            choices = [(c.value, c.name) for c in choices]
        if callable(choices):
            choices = choices()
        normalized_choices = []
        for choice in choices:
            # Allow None values in the simple values list case
            if isinstance(choice, str) or choice is None:
                # TODO warning
                normalized_choices.append((choice, choice))  # simple choice list
            elif isinstance(choice[1], (list, tuple)):
                normalized_choices.extend(choice[1])  # categorized nested choices
            else:
                normalized_choices.append(choice)  # normal 2-tuple form

        # Get all of choice values that should be used in the hash, blank and None values get excluded
        # in the post-processing hook for enum overrides, so we do the same here to ensure the hashes match
        hashable_values = [
            (value, label) for value, label in normalized_choices if value not in ['', None]
        ]
        overrides[list_hash(hashable_values)] = name

    if len(spectacular_settings.ENUM_NAME_OVERRIDES) != len(overrides):
        error(
            'ENUM_NAME_OVERRIDES has duplication issues. Encountered multiple names '
            'for the same choice set. Enum naming might be unexpected.'
        )
    return overrides


def list_hash(lst: Any) -> str:
    return hashlib.sha256(json.dumps(sorted(lst), sort_keys=True, cls=JSONEncoder).encode()).hexdigest()[:16]


def anchor_pattern(pattern: str) -> str:
    if not pattern.startswith('^'):
        pattern = '^' + pattern
    if not pattern.endswith('$'):
        pattern = pattern + '$'
    return pattern


def resolve_django_path_parameter(path_regex, variable, available_formats):
    """
    convert django style path parameters to OpenAPI parameters.
    """
    registered_converters = get_converters()
    for match in _PATH_PARAMETER_COMPONENT_RE.finditer(path_regex):
        converter, parameter = match.group('converter'), match.group('parameter')
        enum_values = None

        if api_settings.SCHEMA_COERCE_PATH_PK and parameter == 'pk':
            parameter = 'id'
        elif spectacular_settings.SCHEMA_COERCE_PATH_PK_SUFFIX and parameter.endswith('_pk'):
            parameter = f'{parameter[:-3]}_id'

        if parameter != variable:
            continue
        # RE also matches untyped patterns (e.g. "<id>")
        if not converter:
            return None

        # special handling for drf_format_suffix
        if converter.startswith('drf_format_suffix_'):
            explicit_formats = converter[len('drf_format_suffix_'):].split('_')
            enum_values = [
                f'.{suffix}' for suffix in explicit_formats if suffix in available_formats
            ]
            converter = 'drf_format_suffix'
        elif converter == 'drf_format_suffix':
            enum_values = [f'.{suffix}' for suffix in available_formats]

        if converter in spectacular_settings.PATH_CONVERTER_OVERRIDES:
            override = spectacular_settings.PATH_CONVERTER_OVERRIDES[converter]
            if is_basic_type(override):
                schema = build_basic_type(override)
            elif isinstance(override, dict):
                schema = dict(override)
            else:
                warn(
                    f'Unable to use path converter override for "{converter}". '
                    f'Please refer to the documentation on how to use this.'
                )
                return None
        elif converter in DJANGO_PATH_CONVERTER_MAPPING:
            schema = build_basic_type(DJANGO_PATH_CONVERTER_MAPPING[converter])
        elif converter in registered_converters:
            # gracious fallback for custom converters that have no override specified.
            schema = build_basic_type(OpenApiTypes.STR)
            schema['pattern'] = anchor_pattern(registered_converters[converter].regex)
        else:
            error(f'Encountered path converter "{converter}" that is unknown to Django.')
            return None

        return build_parameter_type(
            name=variable,
            schema=schema,
            location=OpenApiParameter.PATH,
            enum=enum_values,
        )

    return None


def resolve_regex_path_parameter(path_regex, variable):
    """
    convert regex path parameter to OpenAPI parameter, if pattern is
    explicitly chosen and not the generic non-empty default '[^/.]+'.
    """
    for parameter, pattern in analyze_named_regex_pattern(path_regex).items():
        if api_settings.SCHEMA_COERCE_PATH_PK and parameter == 'pk':
            parameter = 'id'
        elif spectacular_settings.SCHEMA_COERCE_PATH_PK_SUFFIX and parameter.endswith('_pk'):
            parameter = f'{parameter[:-3]}_id'

        if parameter != variable:
            continue
        # do not use default catch-all pattern and defer to model resolution
        if pattern == '[^/.]+':
            return None

        return build_parameter_type(
            name=variable,
            schema=build_basic_type(OpenApiTypes.STR),
            pattern=anchor_pattern(pattern),
            location=OpenApiParameter.PATH,
        )

    return None


def is_versioning_supported(versioning_class) -> bool:
    return issubclass(versioning_class, (
        versioning.URLPathVersioning,
        versioning.NamespaceVersioning,
        versioning.AcceptHeaderVersioning
    ))


def operation_matches_version(view, requested_version) -> bool:
    try:
        version, _ = view.determine_version(view.request, **view.kwargs)
    except exceptions.NotAcceptable:
        return False
    else:
        return str(version) == str(requested_version)


def modify_for_versioning(patterns, method, path, view, requested_version):
    assert view.versioning_class and view.request
    assert requested_version

    view.request.version = requested_version

    if issubclass(view.versioning_class, versioning.URLPathVersioning):
        version_param = view.versioning_class.version_param
        # substitute version variable to emulate request
        path = uritemplate.partial(path, var_dict={version_param: requested_version})
        if isinstance(path, URITemplate):
            path = path.uri
        # emulate router behaviour by injecting substituted variable into view
        view.kwargs[version_param] = requested_version
    elif issubclass(view.versioning_class, versioning.NamespaceVersioning):
        try:
            view.request.resolver_match = get_resolver(
                urlconf=detype_patterns(tuple(patterns)),
            ).resolve(path)
        except Resolver404:
            error(f"namespace versioning path resolution failed for {path}. Path will be ignored.")
    elif issubclass(view.versioning_class, versioning.AcceptHeaderVersioning):
        # Append the version into request accepted_media_type.
        # e.g "application/json; version=1.0"
        # To allow the AcceptHeaderVersioning negotiator going through.
        if not hasattr(view.request, 'accepted_renderer'):
            # Probably a mock request, content negotiation was not performed, so, we do it now.
            negotiated = view.perform_content_negotiation(view.request)
            view.request.accepted_renderer, view.request.accepted_media_type = negotiated
        media_type = _MediaType(view.request.accepted_media_type)
        view.request.accepted_media_type = (
            f'{media_type.full_type}; {view.versioning_class.version_param}={requested_version}'
        )

    return path


def modify_media_types_for_versioning(view, media_types: List[str]) -> List[str]:
    if (
        not view.versioning_class
        or not issubclass(view.versioning_class, versioning.AcceptHeaderVersioning)
    ):
        return media_types

    media_type = _MediaType(view.request.accepted_media_type)
    version = media_type.params.get(view.versioning_class.version_param)  # type: ignore
    version = unicode_http_header(version)

    if not version or version == view.versioning_class.default_version:
        return media_types

    return [
        f'{media_type}; {view.versioning_class.version_param}={version}'
        for media_type in media_types
    ]


def analyze_named_regex_pattern(path: str) -> Dict[str, str]:
    """ safely extract named groups and their pattern from given regex pattern """
    result = {}
    stack = 0
    name_capture, name_buffer = False, ''
    regex_capture, regex_buffer = False, ''
    i = 0
    while i < len(path):
        # estimate state at position i
        skip = False
        if path[i] == '\\':
            ff = 2
        elif path[i:i + 4] == '(?P<':
            skip = True
            name_capture = True
            ff = 4
        elif path[i] in '(' and regex_capture:
            stack += 1
            ff = 1
        elif path[i] == '>' and name_capture:
            assert name_buffer
            name_capture = False
            regex_capture = True
            skip = True
            ff = 1
        elif path[i] in ')' and regex_capture:
            if not stack:
                regex_capture = False
                result[name_buffer] = regex_buffer
                name_buffer, regex_buffer = '', ''
            else:
                stack -= 1
            ff = 1
        else:
            ff = 1
        # fill buffer based on state
        if name_capture and not skip:
            name_buffer += path[i:i + ff]
        elif regex_capture and not skip:
            regex_buffer += path[i:i + ff]
        i += ff
    assert not stack
    return result


@cache
def detype_patterns(patterns):
    """Cache detyped patterns due to the expensive nature of rebuilding URLResolver."""
    return tuple(detype_pattern(pattern) for pattern in patterns)


def detype_pattern(pattern):
    """
    return an equivalent pattern that accepts arbitrary values for path parameters.
    de-typing the path will ease determining a matching route without having properly
    formatted dummy values for all path parameters.
    """
    if isinstance(pattern, URLResolver):
        return URLResolver(
            pattern=detype_pattern(pattern.pattern),
            urlconf_name=[detype_pattern(p) for p in pattern.url_patterns],
            default_kwargs=pattern.default_kwargs,
            app_name=pattern.app_name,
            namespace=pattern.namespace,
        )
    elif isinstance(pattern, URLPattern):
        return URLPattern(
            pattern=detype_pattern(pattern.pattern),
            callback=pattern.callback,
            default_args=pattern.default_args,
            name=pattern.name,
        )
    elif isinstance(pattern, RoutePattern):
        return RoutePattern(
            route=re.sub(r'<\w+:(\w+)>', r'<\1>', pattern._route),
            name=pattern.name,
            is_endpoint=pattern._is_endpoint
        )
    elif isinstance(pattern, RegexPattern):
        detyped_regex = pattern._regex
        for name, regex in analyze_named_regex_pattern(pattern._regex).items():
            detyped_regex = detyped_regex.replace(
                f'(?P<{name}>{regex})',
                f'(?P<{name}>[^/]+)',
            )
        return RegexPattern(
            regex=detyped_regex,
            name=pattern.name,
            is_endpoint=pattern._is_endpoint
        )
    else:
        warn(f'unexpected pattern "{pattern}" encountered while simplifying urlpatterns.')
        return pattern


def normalize_result_object(result):
    """ resolve non-serializable objects like lazy translation strings and OrderedDict """
    if isinstance(result, dict) or isinstance(result, OrderedDict):
        return {k: normalize_result_object(v) for k, v in result.items()}
    if isinstance(result, list) or isinstance(result, tuple):
        return [normalize_result_object(v) for v in result]
    if isinstance(result, Promise):
        return str(result)
    for base_type in [bool, int, float, str]:
        if isinstance(result, base_type):
            return base_type(result)  # coerce basic sub types
    return result


def sanitize_result_object(result):
    # warn about and resolve operationId collisions with suffixes
    operations = defaultdict(list)
    for path, methods in result['paths'].items():
        for method, operation in methods.items():
            operations[operation['operationId']].append((path, method))
    for operation_id, paths in operations.items():
        if len(paths) == 1:
            continue
        warn(f'operationId "{operation_id}" has collisions {paths}. resolving with numeral suffixes.')
        for idx, (path, method) in enumerate(sorted(paths)[1:], start=2):
            suffix = str(idx) if spectacular_settings.CAMELIZE_NAMES else f'_{idx}'
            result['paths'][path][method]['operationId'] += suffix

    return result


def sanitize_specification_extensions(extensions):
    # https://spec.openapis.org/oas/v3.0.3#specification-extensions
    output = {}
    for key, value in extensions.items():
        if not re.match(r'^x-', key):
            warn(f'invalid extension {key!r}. vendor extensions must start with "x-"')
        else:
            output[key] = value
    return output


def camelize_operation(path, operation):
    for path_variable in re.findall(r'\{(\w+)\}', path):
        path = path.replace(
            f'{{{path_variable}}}',
            f'{{{inflection.camelize(path_variable, False)}}}'
        )

    for parameter in operation.get('parameters', []):
        if parameter['in'] == OpenApiParameter.PATH:
            parameter['name'] = inflection.camelize(parameter['name'], False)

    operation['operationId'] = inflection.camelize(operation['operationId'], False)

    return path, operation


def build_mock_request(method, path, view, original_request, **kwargs):
    """ build a mocked request and use original request as reference if available """
    request = getattr(APIRequestFactory(), method.lower())(path=path)
    request = view.initialize_request(request)
    if original_request:
        request.user = original_request.user
        request.auth = original_request.auth
        # ignore headers related to authorization as it has been handled above.
        # also ignore ACCEPT as the MIME type refers to SpectacularAPIView and the
        # version (if available) has already been processed by SpectacularAPIView.
        for name, value in original_request.META.items():
            if not name.startswith('HTTP_'):
                continue
            if name in ['HTTP_ACCEPT', 'HTTP_COOKIE', 'HTTP_AUTHORIZATION']:
                continue
            request.META[name] = value
    return request


def set_query_parameters(url, **kwargs) -> str:
    """ deconstruct url, safely attach query parameters in kwargs, and serialize again """
    url = str(url)  # Force evaluation of reverse_lazy urls
    scheme, netloc, path, params, query, fragment = urllib.parse.urlparse(url)
    query = urllib.parse.parse_qs(query)
    query.update({k: v for k, v in kwargs.items() if v is not None})
    query = urllib.parse.urlencode(query, doseq=True)
    return urllib.parse.urlunparse((scheme, netloc, path, params, query, fragment))


def get_relative_url(url: str) -> str:
    url = str(url)  # Force evaluation of reverse_lazy urls
    scheme, netloc, path, params, query, fragment = urllib.parse.urlparse(url)
    return urllib.parse.urlunparse(('', '', path, params, query, fragment))


def _get_type_hint_origin(hint):
    """ graceful fallback for py 3.8 typing functionality """
    if sys.version_info >= (3, 8):
        return typing.get_origin(hint), typing.get_args(hint)
    else:
        origin = getattr(hint, '__origin__', None)
        args = getattr(hint, '__args__', None)
        origin = {
            typing.List: list,
            typing.Dict: dict,
            typing.Tuple: tuple,
            typing.Set: set,
            typing.FrozenSet: frozenset
        }.get(origin, origin)
        return origin, args


def _resolve_typeddict(hint):
    """resolve required fields for TypedDicts if on 3.9 or above"""
    required = None

    if hasattr(hint, '__required_keys__'):
        required = [h for h in hint.__required_keys__]

    return build_object_type(
        properties={
            k: resolve_type_hint(v) for k, v in get_type_hints(hint).items()
        },
        required=required,
        description=get_doc(hint),
    )


def is_higher_order_type_hint(hint) -> bool:
    return isinstance(hint, (
        getattr(types, 'GenericAlias', _Sentinel),
        getattr(types, 'UnionType', _Sentinel),
        getattr(typing, '_GenericAlias', _Sentinel),
        getattr(typing, '_UnionGenericAlias', _Sentinel),
    ))


def resolve_type_hint(hint):
    """ resolve return value type hints to schema """
    origin, args = _get_type_hint_origin(hint)

    if origin is None and is_basic_type(hint, allow_none=False):
        return build_basic_type(hint)
    elif origin is None and inspect.isclass(hint) and issubclass(hint, tuple):
        # a convoluted way to catch NamedTuple. suggestions welcome.
        if get_type_hints(hint):
            properties = {k: resolve_type_hint(v) for k, v in get_type_hints(hint).items()}
        else:
            properties = {k: build_basic_type(OpenApiTypes.ANY) for k in hint._fields}
        return build_object_type(properties=properties, required=properties.keys())
    elif origin is list or hint is list or hint is ReturnList:
        return build_array_type(
            resolve_type_hint(args[0]) if args else build_basic_type(OpenApiTypes.ANY)
        )
    elif origin is tuple:
        return build_array_type(
            schema=build_basic_type(args[0]),
            max_length=len(args),
            min_length=len(args),
        )
    elif origin is dict or origin is defaultdict or origin is OrderedDict or hint is ReturnDict:
        schema = build_basic_type(OpenApiTypes.OBJECT)
        if args and args[1] is not typing.Any:
            schema['additionalProperties'] = resolve_type_hint(args[1])
        return schema
    elif origin is set:
        return build_array_type(resolve_type_hint(args[0]))
    elif origin is frozenset:
        return build_array_type(resolve_type_hint(args[0]))
    elif origin in LITERAL_TYPES:
        # Literal only works for python >= 3.8 despite typing_extensions, because it
        # behaves slightly different w.r.t. __origin__
        schema = {'enum': list(args)}
        if all(type(args[0]) is type(choice) for choice in args):
            schema.update(build_basic_type(type(args[0])))
        return schema
    elif inspect.isclass(hint) and issubclass(hint, Enum):
        schema = {'enum': [item.value for item in hint]}
        mixin_base_types = [t for t in hint.__mro__ if is_basic_type(t)]
        if mixin_base_types:
            schema.update(build_basic_type(mixin_base_types[0]))
        return schema
    elif isinstance(hint, TYPED_DICT_META_TYPES):
        return _resolve_typeddict(hint)
    elif isinstance(hint, TYPE_ALIAS_TYPES):
        return resolve_type_hint(hint.__value__)
    elif origin in UNION_TYPES:
        type_args = [arg for arg in args if arg is not type(None)]  # noqa: E721
        if len(type_args) > 1:
            schema = {'oneOf': [resolve_type_hint(arg) for arg in type_args]}
        else:
            schema = resolve_type_hint(type_args[0])
        if type(None) in args:
            schema['nullable'] = True
        return schema
    elif origin is collections.abc.Iterable:
        return build_array_type(resolve_type_hint(args[0]))
    else:
        raise UnableToProceedError(hint)


def whitelisted(obj: object, classes: Optional[List[Type[object]]], exact=False) -> bool:
    if classes is None:
        return True
    if exact:
        return obj.__class__ in classes
    else:
        return isinstance(obj, tuple(classes))


def build_mocked_view(method: str, path: str, extend_schema_decorator, registry):
    from rest_framework import parsers, views

    @extend_schema_decorator
    class TmpView(views.APIView):
        parser_classes = [parsers.JSONParser]

    # emulate what Generator would do to setup schema generation.
    view_callable = TmpView.as_view()
    view: views.APIView = view_callable.cls()
    view.request = spectacular_settings.GET_MOCK_REQUEST(
        method.upper(), path, view, None
    )
    view.kwargs = {}
    # prepare AutoSchema with "init" values as if get_operation() was called
    schema: Any = view.schema
    schema.registry = registry
    schema.path = path
    schema.path_regex = path
    schema.path_prefix = ''
    schema.method = method.upper()
    return view


def build_listed_example_value(value: Any, paginator, direction):
    if not paginator or direction == 'request':
        return [value]

    sentinel = object()
    schema = paginator.get_paginated_response_schema(sentinel)

    if schema is sentinel:
        return [value]

    def drilldown_schema_example(schema, sentinel):
        # Recursively travel schema properties to build example.
        if schema is sentinel:
            return [value]
        if 'properties' in schema.keys():
            return {
                field_name: drilldown_schema_example(field_schema, sentinel)
                for field_name, field_schema in schema['properties'].items()
            }
        return schema['example']

    try:
        return drilldown_schema_example(schema, sentinel)
    except (AttributeError, KeyError):
        warn(
            f"OpenApiExample could not be paginated because {paginator.__class__} either "
            f"has an unknown schema structure or the individual pagination fields did not "
            f"provide example values themselves. Using the plain example value as fallback."
        )
        return value


def filter_supported_arguments(func, **kwargs):
    sig = inspect.signature(func)
    return {
        arg: val for arg, val in kwargs.items() if arg in sig.parameters
    }


def build_serializer_context(view) -> typing.Dict[str, Any]:
    try:
        return view.get_serializer_context()
    except:  # noqa
        return {'request': view.request}


def process_webhooks(webhooks: List[OpenApiWebhook], registry: ComponentRegistry):
    """
    Creates a mocked view for every webhook. The given extend_schema decorator then
    specifies the expectations on the receiving end of the callback. Effectively
    simulates a sub-schema from the opposing perspective via a virtual view definition.
    """
    result = {}

    for webhook in webhooks:
        if isinstance(webhook.decorator, dict):
            methods = webhook.decorator
        else:
            methods = {'post': webhook.decorator}

        path_items = {}

        for method, decorator in methods.items():
            # a dict indicates a raw schema; use directly
            if isinstance(decorator, dict):
                path_items[method.lower()] = decorator
                continue

            mocked_view = build_mocked_view(
                method=method,
                path="/",
                extend_schema_decorator=decorator,
                registry=registry,
            )
            operation = {}

            description = mocked_view.schema.get_description()
            if description:
                operation['description'] = description

            summary = mocked_view.schema.get_summary()
            if summary:
                operation['summary'] = summary

            tags = mocked_view.schema.get_tags()
            if tags:
                operation['tags'] = tags

            request_body = mocked_view.schema._get_request_body('response')
            if request_body:
                operation['requestBody'] = request_body

            deprecated = mocked_view.schema.is_deprecated()
            if deprecated:
                operation['deprecated'] = deprecated

            operation['responses'] = mocked_view.schema._get_response_bodies('request')

            extensions = mocked_view.schema.get_extensions()
            if extensions:
                operation.update(sanitize_specification_extensions(extensions))

            path_items[method.lower()] = operation

        result[webhook.name] = path_items

    return result
