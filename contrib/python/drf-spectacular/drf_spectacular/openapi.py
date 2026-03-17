import copy
import functools
import itertools
import re
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union

import uritemplate
from django.core import exceptions as django_exceptions
from django.core import validators
from django.db import models
from django.utils.formats import get_format
from django.utils.translation import gettext_lazy as _
from rest_framework import permissions, renderers, serializers
from rest_framework.fields import _UnvalidatedField, empty
from rest_framework.generics import CreateAPIView, GenericAPIView, ListCreateAPIView
from rest_framework.mixins import ListModelMixin
from rest_framework.schemas.inspectors import ViewInspector
from rest_framework.schemas.utils import get_pk_description
from rest_framework.settings import api_settings
from rest_framework.utils.model_meta import get_field_info
from rest_framework.views import APIView

import drf_spectacular.authentication  # noqa: F403, F401
import drf_spectacular.serializers  # noqa: F403, F401
from drf_spectacular.contrib import *  # noqa: F403, F401
from drf_spectacular.drainage import add_trace_message, error, get_override, has_override, warn
from drf_spectacular.extensions import (
    OpenApiAuthenticationExtension, OpenApiFilterExtension, OpenApiSerializerExtension,
    OpenApiSerializerFieldExtension,
)
from drf_spectacular.plumbing import (
    ComponentRegistry, ResolvedComponent, UnableToProceedError, append_meta,
    assert_basic_serializer, build_array_type, build_basic_type, build_choice_field,
    build_examples_list, build_generic_type, build_listed_example_value, build_media_type_object,
    build_mocked_view, build_object_type, build_parameter_type, build_serializer_context,
    filter_supported_arguments, follow_field_source, follow_model_field_lookup, force_instance,
    get_doc, get_list_serializer, get_manager, get_type_hints, get_view_model, is_basic_serializer,
    is_basic_type, is_field, is_higher_order_type_hint, is_list_serializer,
    is_list_serializer_customized, is_patched_serializer, is_serializer,
    is_trivial_string_variation, modify_media_types_for_versioning, resolve_django_path_parameter,
    resolve_regex_path_parameter, resolve_type_hint, safe_ref, sanitize_specification_extensions,
    whitelisted,
)
from drf_spectacular.settings import spectacular_settings
from drf_spectacular.types import OpenApiTypes
from drf_spectacular.utils import (
    Direction, OpenApiCallback, OpenApiExample, OpenApiParameter, OpenApiRequest, OpenApiResponse,
    _SchemaType, _SerializerType,
)


class AutoSchema(ViewInspector):
    method_mapping = {
        'get': 'retrieve',
        'post': 'create',
        'put': 'update',
        'patch': 'partial_update',
        'delete': 'destroy',
    }

    def get_operation(
            self,
            path: str,
            path_regex: str,
            path_prefix: str,
            method: str,
            registry: ComponentRegistry
    ) -> Optional[_SchemaType]:
        self.registry = registry
        self.path = path
        self.path_regex = path_regex
        self.path_prefix = path_prefix
        self.method = method.upper()

        if self.is_excluded():
            return None

        operation: _SchemaType = {'operationId': self.get_operation_id()}

        description = self.get_description()
        if description:
            operation['description'] = description

        summary = self.get_summary()
        if summary:
            operation['summary'] = summary

        external_docs = self._get_external_docs()
        if external_docs:
            operation['externalDocs'] = external_docs

        parameters = self._get_parameters()
        if parameters:
            operation['parameters'] = parameters

        tags = self.get_tags()
        if tags:
            operation['tags'] = tags

        request_body = self._get_request_body()
        if request_body:
            operation['requestBody'] = request_body

        auth = self.get_auth()
        if auth:
            operation['security'] = auth

        deprecated = self.is_deprecated()
        if deprecated:
            operation['deprecated'] = deprecated

        operation['responses'] = self._get_response_bodies()

        extensions = self.get_extensions()
        if extensions:
            operation.update(sanitize_specification_extensions(extensions))

        callbacks = self._get_callbacks()
        if callbacks:
            operation['callbacks'] = callbacks

        return operation

    def is_excluded(self) -> bool:
        """ override this for custom behaviour """
        return False

    def _is_list_view(self, serializer: Optional[_SerializerType] = None) -> bool:
        """
        partially heuristic approach to determine if a view yields an object or a
        list of objects. used for operationId naming, array building and pagination.
        defaults to False if all introspection fail.
        """
        if serializer is None:
            serializer = self.get_response_serializers()

        if isinstance(serializer, dict) and serializer:
            # extract likely main serializer from @extend_schema override
            serializer = {str(code): s for code, s in serializer.items()}
            serializer = serializer[min(serializer)]

        if is_list_serializer(serializer):
            return True
        if is_basic_type(serializer):
            return False
        if hasattr(self.view, 'action'):
            return self.view.action == 'list'
        # list responses are "usually" only returned by GET
        if self.method != 'GET':
            return False
        if isinstance(self.view, ListModelMixin):
            return True
        # primary key/lookup variable in path is a strong indicator for retrieve
        if isinstance(self.view, GenericAPIView):
            lookup_url_kwarg = self.view.lookup_url_kwarg or self.view.lookup_field
            if lookup_url_kwarg in uritemplate.variables(self.path):
                return False

        return False

    def _is_create_operation(self) -> bool:
        if self.method != 'POST':
            return False
        if getattr(self.view, 'action', None) == 'create':
            return True
        if isinstance(self.view, (ListCreateAPIView, CreateAPIView)):
            return True
        return False

    def get_override_parameters(self) -> List[Union[OpenApiParameter, _SerializerType]]:
        """ override this for custom behaviour """
        return []

    def _process_override_parameters(self, direction='request'):
        result = {}
        for parameter in self.get_override_parameters():
            if isinstance(parameter, OpenApiParameter):
                if parameter.response:
                    continue

                if is_basic_type(parameter.type):
                    schema = build_basic_type(parameter.type)
                elif is_basic_serializer(parameter.type):
                    schema = self.resolve_serializer(parameter.type, direction).ref
                elif isinstance(parameter.type, dict):
                    schema = parameter.type
                else:
                    warn(f'unsupported type for parameter "{parameter.name}". Skipping.')
                    continue

                if parameter.many:
                    if is_basic_type(parameter.type):
                        schema = build_array_type(schema)
                    else:
                        warn(
                            f'parameter "{parameter.name}" has many=True and is not a basic type. '
                            f'many=True only makes sense for basic types. Ignoring.'
                        )

                if parameter.exclude:
                    result[parameter.name, parameter.location] = None
                else:
                    result[parameter.name, parameter.location] = build_parameter_type(
                        name=parameter.name,
                        schema=schema,
                        location=parameter.location,
                        required=parameter.required,
                        description=parameter.description,
                        enum=parameter.enum,
                        pattern=parameter.pattern,
                        deprecated=parameter.deprecated,
                        style=parameter.style,
                        explode=parameter.explode,
                        default=parameter.default,
                        allow_blank=parameter.allow_blank,
                        examples=build_examples_list(parameter.examples),
                        extensions=parameter.extensions,
                    )
            elif is_basic_serializer(parameter):
                # explode serializer into separate parameters. defaults to QUERY location
                parameter = force_instance(parameter)
                mapped = self._map_serializer(parameter, 'request')
                for property_name, property_schema in mapped['properties'].items():
                    try:
                        # be graceful when serializer might be non-DRF (via extension).
                        field = parameter.fields.get(property_name)
                    except Exception:
                        field = None
                    result[property_name, OpenApiParameter.QUERY] = build_parameter_type(
                        name=property_name,
                        schema=property_schema,
                        description=property_schema.pop('description', None),
                        location=OpenApiParameter.QUERY,
                        allow_blank=getattr(field, 'allow_blank', True),
                        required=bool(property_name in mapped.get('required', [])),
                    )
            else:
                warn(f'could not resolve parameter annotation {parameter}. Skipping.')
        return result

    def _get_format_parameters(self) -> List[_SchemaType]:
        parameters = []
        formats = self.map_renderers('format')
        if api_settings.URL_FORMAT_OVERRIDE and len(formats) > 1:
            parameters.append(build_parameter_type(
                name=api_settings.URL_FORMAT_OVERRIDE,
                schema=build_basic_type(OpenApiTypes.STR),  # type: ignore
                location=OpenApiParameter.QUERY,
                enum=formats
            ))
        return parameters

    def _get_parameters(self) -> List[_SchemaType]:
        def dict_helper(parameters):
            return {(p['name'], p['in']): p for p in parameters}

        override_parameters = self._process_override_parameters()
        # remove overridden path parameters beforehand so that there are no irrelevant warnings.
        path_variables = [
            v for v in uritemplate.variables(self.path) if (v, 'path') not in override_parameters
        ]
        parameters = {
            **dict_helper(self._resolve_path_parameters(path_variables)),
            **dict_helper(self._get_filter_parameters()),
            **dict_helper(self._get_pagination_parameters()),
            **dict_helper(self._get_format_parameters()),
        }
        # override/add/remove @extend_schema parameters
        for key, parameter in override_parameters.items():
            if parameter is None:
                # either omit or explicitly remove parameter
                if key in parameters:
                    del parameters[key]
            else:
                parameters[key] = parameter

        # collect independently specified parameter examples from @extend_schema.
        # Append to both discovered and manually specified parameters.
        examples_by_key = defaultdict(list)
        for example in self.get_examples():
            if example.parameter_only:
                examples_by_key[example.parameter_only].append(example)
        for key, examples in examples_by_key.items():
            if key in parameters:
                parameters[key].setdefault('examples', {})
                parameters[key]['examples'].update(build_examples_list(examples))

        if callable(spectacular_settings.SORT_OPERATION_PARAMETERS):
            return sorted(parameters.values(), key=spectacular_settings.SORT_OPERATION_PARAMETERS)
        elif spectacular_settings.SORT_OPERATION_PARAMETERS:
            return sorted(parameters.values(), key=lambda p: p['name'])
        else:
            return list(parameters.values())

    def get_description(self) -> str:  # type: ignore[override]
        """ override this for custom behaviour """
        action_or_method = getattr(self.view, getattr(self.view, 'action', self.method.lower()), None)
        view_doc = get_doc(self.view.__class__)
        action_doc = get_doc(action_or_method)
        return action_doc or view_doc

    def get_summary(self) -> Optional[str]:
        """ override this for custom behaviour """
        return None

    def _get_external_docs(self) -> Optional[Dict[str, str]]:
        external_docs = self.get_external_docs()
        if isinstance(external_docs, str):
            return {'url': external_docs}
        else:
            return external_docs

    def get_external_docs(self) -> Optional[Union[Dict[str, str], str]]:
        """ override this for custom behaviour """
        return None

    def get_auth(self) -> List[_SchemaType]:
        """
        Obtains authentication classes and permissions from view. If authentication
        is known, resolve security requirement for endpoint and security definition for
        the component section.
        For custom authentication subclass ``OpenApiAuthenticationExtension``.
        """
        auths = []

        for authenticator in self.view.get_authenticators():
            if not whitelisted(authenticator, spectacular_settings.AUTHENTICATION_WHITELIST, True):
                continue

            scheme = OpenApiAuthenticationExtension.get_match(authenticator)
            if not scheme:
                warn(
                    f'could not resolve authenticator {authenticator.__class__}. There '
                    f'was no OpenApiAuthenticationExtension registered for that class. '
                    f'Try creating one by subclassing it. Ignoring for now.'
                )
                continue

            security_requirements = scheme.get_security_requirement(self)
            if security_requirements is not None:
                if isinstance(security_requirements, dict):
                    auths.append(security_requirements)
                else:
                    auths.extend(security_requirements)

            if isinstance(scheme.name, str):
                names, definitions = [scheme.name], [scheme.get_security_definition(self)]
            else:
                names, definitions = scheme.name, scheme.get_security_definition(self)  # type: ignore[assignment]

            for name, definition in zip(names, definitions):
                self.registry.register_on_missing(
                    ResolvedComponent(
                        name=name,
                        type=ResolvedComponent.SECURITY_SCHEMA,
                        object=authenticator.__class__,
                        schema=definition
                    )
                )

        if spectacular_settings.SECURITY:
            auths.extend(spectacular_settings.SECURITY)

        perms = [p.__class__ for p in self.view.get_permissions()]
        if permissions.AllowAny in perms:
            auths.append({})
        elif permissions.IsAuthenticatedOrReadOnly in perms and self.method in permissions.SAFE_METHODS:
            auths.append({})
        return auths

    def get_request_serializer(self) -> Optional[_SerializerType]:
        """ override this for custom behaviour """
        return self._get_serializer()

    def get_response_serializers(self) -> Optional[_SerializerType]:
        """ override this for custom behaviour """
        return self._get_serializer()

    def get_tags(self) -> List[str]:
        """ override this for custom behaviour """
        tokenized_path = self._tokenize_path()
        # use first non-parameter path part as tag
        return tokenized_path[:1]

    def get_extensions(self) -> _SchemaType:
        return {}

    def _get_callbacks(self):
        """
        Creates a mocked view for every callback. The given extend_schema decorator then
        specifies the expectations on the receiving end of the callback. Effectively
        simulates a sub-schema from the opposing perspective via a virtual view definition.
        """
        result = {}

        for callback in self.get_callbacks():
            if isinstance(callback.decorator, dict):
                methods = callback.decorator
            else:
                methods = {'post': callback.decorator}

            path_items = {}

            for method, decorator in methods.items():
                # a dict indicates a raw schema; use directly
                if isinstance(decorator, dict):
                    path_items[method.lower()] = decorator
                    continue

                mocked_view = build_mocked_view(
                    method=method,
                    path=callback.path,
                    extend_schema_decorator=decorator,
                    registry=self.registry
                )
                operation = {}

                description = mocked_view.schema.get_description()
                if description:
                    operation['description'] = description

                summary = mocked_view.schema.get_summary()
                if summary:
                    operation['summary'] = summary

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

            result[callback.name] = {callback.path: path_items}

        return result

    def get_callbacks(self) -> List[OpenApiCallback]:
        """ override this for custom behaviour """
        return []

    def get_operation_id(self) -> str:
        """ override this for custom behaviour """
        tokenized_path = self._tokenize_path()
        # replace dashes as they can be problematic later in code generation
        tokenized_path = [t.replace('-', '_') for t in tokenized_path]

        if self.method == 'GET' and self._is_list_view():
            action = 'list'
        else:
            action = self.method_mapping[self.method.lower()]

        if not tokenized_path:
            tokenized_path.append('root')

        if re.search(r'<drf_format_suffix\w*:\w+>', self.path_regex):
            tokenized_path.append('formatted')

        if spectacular_settings.OPERATION_ID_METHOD_POSITION == 'PRE':
            return '_'.join([action] + tokenized_path)
        elif spectacular_settings.OPERATION_ID_METHOD_POSITION == 'POST':
            return '_'.join(tokenized_path + [action])
        else:
            assert False, 'Invalid value for OPERATION_ID_METHOD_POSITION. Allowed: PRE, POST'

    def is_deprecated(self) -> bool:
        """ override this for custom behaviour """
        return False

    def _tokenize_path(self) -> List[str]:
        # remove path prefix
        path = re.sub(
            pattern=self.path_prefix,
            repl='',
            string=self.path,
            flags=re.IGNORECASE
        )
        # remove path variables
        path = re.sub(pattern=r'\{[\w\-]+\}', repl='', string=path)
        # cleanup and tokenize remaining parts.
        tokenized_path = path.rstrip('/').lstrip('/').split('/')
        return [t for t in tokenized_path if t]

    def _resolve_path_parameters(self, variables):
        model = get_view_model(self.view, emit_warnings=False)

        parameters = []
        for variable in variables:
            schema = build_basic_type(OpenApiTypes.STR)
            description = ''

            resolved_parameter = resolve_django_path_parameter(
                self.path_regex, variable, self.map_renderers('format'),
            )
            if not resolved_parameter:
                resolved_parameter = resolve_regex_path_parameter(self.path_regex, variable)

            if resolved_parameter:
                schema = resolved_parameter['schema']
            elif model is None:
                warn(
                    f'could not derive type of path parameter "{variable}" because it '
                    f'is untyped and obtaining queryset from the viewset failed. '
                    f'Consider adding a type to the path (e.g. <int:{variable}>) or annotating '
                    f'the parameter type with @extend_schema. Defaulting to "string".'
                )
            else:
                try:
                    if getattr(self.view, 'lookup_url_kwarg', None) == variable:
                        model_field_name = getattr(self.view, 'lookup_field', variable)
                    elif variable.endswith('_pk'):
                        # Django naturally coins foreign keys *_id. improve chances to match a field
                        model_field_name = f'{variable[:-3]}_id'
                    else:
                        model_field_name = variable
                    model_field = follow_model_field_lookup(model, model_field_name)
                    schema = self._map_model_field(model_field, direction=None)
                    if 'description' not in schema and model_field.primary_key:
                        description = get_pk_description(model, model_field)
                except django_exceptions.FieldError:
                    warn(
                        f'could not derive type of path parameter "{variable}" because model '
                        f'"{model.__module__}.{model.__name__}" contained no such field. Consider '
                        f'annotating parameter with @extend_schema. Defaulting to "string".'
                    )

            parameters.append(build_parameter_type(
                name=variable,
                location=OpenApiParameter.PATH,
                description=description,
                schema=schema
            ))

        return parameters

    def get_filter_backends(self) -> List[Any]:
        """ override this for custom behaviour """
        if not self._is_list_view():
            return []
        return getattr(self.view, 'filter_backends', [])

    def _get_filter_parameters(self):
        parameters = []
        for filter_backend in self.get_filter_backends():
            filter_extension = OpenApiFilterExtension.get_match(filter_backend())
            if filter_extension:
                parameters += filter_extension.get_schema_operation_parameters(self)
            else:
                parameters += filter_backend().get_schema_operation_parameters(self.view)
        return parameters

    def _get_pagination_parameters(self):
        if not self._is_list_view():
            return []

        paginator = self._get_paginator()
        if not paginator:
            return []

        filter_extension = OpenApiFilterExtension.get_match(paginator)
        if filter_extension:
            return filter_extension.get_schema_operation_parameters(self)
        else:
            return paginator.get_schema_operation_parameters(self.view)

    def _map_model_field(self, model_field, direction):
        assert isinstance(model_field, models.Field)
        # to get a fully initialized serializer field we use DRF's own init logic
        try:
            field_cls, field_kwargs = serializers.ModelSerializer().build_field(
                field_name=model_field.name,
                info=get_field_info(model_field.model),
                model_class=model_field.model,
                nested_depth=0,
            )
            field = field_cls(**field_kwargs)
            field.field_name = model_field.name
        except:  # noqa
            field = None

        # For some cases, the DRF init logic either breaks (custom field with internal type) or
        # the resulting field is underspecified with regards to the schema (ReadOnlyField).
        if field and isinstance(field, serializers.PrimaryKeyRelatedField):
            # special case handling only for _resolve_path_parameters() where neither queryset nor
            # parent is set by build_field. patch in queryset as _map_serializer_field requires it
            if not field.queryset:
                field.queryset = get_manager(model_field.related_model).none()
            return self._map_serializer_field(field, direction)
        elif isinstance(field, serializers.ManyRelatedField):
            # special case handling similar to the case above. "parent.parent" on child_relation
            # is None and there is no queryset. patch in as _map_serializer_field requires one.
            if not field.child_relation.queryset:
                field.child_relation.queryset = get_manager(model_field.related_model).none()
            return self._map_serializer_field(field, direction)
        elif field and not isinstance(field, (serializers.ReadOnlyField, serializers.ModelField)):
            return self._map_serializer_field(field, direction)
        elif isinstance(model_field, models.ForeignKey):
            return self._map_model_field(model_field.target_field, direction)
        elif hasattr(models, 'JSONField') and isinstance(model_field, models.JSONField):
            # fix for DRF==3.11 with django>=3.1 as it is not yet represented in the field_mapping
            return build_basic_type(OpenApiTypes.ANY)
        elif isinstance(model_field, models.BinaryField):
            return build_basic_type(OpenApiTypes.BYTE)
        elif hasattr(models, model_field.get_internal_type()):
            # be graceful when the model field is not explicitly mapped to a serializer
            internal_type = getattr(models, model_field.get_internal_type())
            field_cls = serializers.ModelSerializer.serializer_field_mapping.get(internal_type)
            if not field_cls:
                warn(
                    f'model field "{model_field.get_internal_type()}" has no mapping in '
                    f'ModelSerializer. It may be a deprecated field. Defaulting to "string"'
                )
                return build_basic_type(OpenApiTypes.STR)
            return self._map_serializer_field(field_cls(), direction)
        else:
            error(
                f'could not resolve model field "{model_field}". Failed to resolve through '
                f'serializer_field_mapping, get_internal_type(), or any override mechanism. '
                f'Defaulting to "string"'
            )
            return build_basic_type(OpenApiTypes.STR)

    def _map_serializer_field(self, field, direction, bypass_extensions=False):
        meta = self._get_serializer_field_meta(field, direction)

        if has_override(field, 'field'):
            override = get_override(field, 'field')
            if is_basic_type(override):
                schema = build_basic_type(override)
                if schema is None:
                    return None
            elif is_higher_order_type_hint(override):
                schema = resolve_type_hint(override)
            elif isinstance(override, dict):
                schema = override
            else:
                schema = self._map_serializer_field(force_instance(override), direction)

            field_component_name = get_override(field, 'field_component_name')
            if field_component_name:
                component = ResolvedComponent(
                    name=field_component_name,
                    type=ResolvedComponent.SCHEMA,
                    schema=schema,
                    object=field,
                )
                self.registry.register_on_missing(component)
                return append_meta(component.ref, meta)
            else:
                return append_meta(schema, meta)

        serializer_field_extension = OpenApiSerializerFieldExtension.get_match(field)
        if serializer_field_extension and not bypass_extensions:
            schema = serializer_field_extension.map_serializer_field(self, direction)
            if schema is None:
                return None
            elif serializer_field_extension.get_name():
                component = ResolvedComponent(
                    name=serializer_field_extension.get_name(),
                    type=ResolvedComponent.SCHEMA,
                    schema=schema,
                    object=field,
                )
                self.registry.register_on_missing(component)
                return append_meta(component.ref, meta)
            else:
                return append_meta(schema, meta)

        # nested serializer with many=True gets automatically replaced with ListSerializer
        if is_list_serializer(field):
            schema = self._unwrap_list_serializer(field, direction)
            return append_meta(schema, meta) if schema else None

        # nested serializer
        if is_serializer(field):
            component = self.resolve_serializer(field, direction)
            return append_meta(component.ref, meta) if component else None

        # Related fields.
        if isinstance(field, serializers.ManyRelatedField):
            schema = self._map_serializer_field(field.child_relation, direction)
            # remove hand-over initkwargs applying only to outer scope
            schema.pop('readOnly', None)
            # similarly from outer scope, default value is invalid inside 'items'
            schema.pop('default', None)
            if meta.get('description') == schema.get('description'):
                schema.pop('description', None)
            return append_meta(build_array_type(schema), meta)

        if isinstance(field, (serializers.PrimaryKeyRelatedField, serializers.SlugRelatedField)):
            # SlugRelatedField is essentially a non-pk version of PrimaryKeyRelatedField.
            is_slug = isinstance(field, serializers.SlugRelatedField)
            # read_only fields do not have a Manager by design. go around and get field
            # from parent. also avoid calling Manager. __bool__ as it might be customized
            # to hit the database.
            if not is_slug and getattr(field, 'pk_field') is not None:
                schema = self._map_serializer_field(field.pk_field, direction)
                return append_meta(schema, meta)
            elif getattr(field, 'queryset', None) is not None:
                if is_slug:
                    model = field.queryset.model
                    source = [field.slug_field]
                    model_field = follow_field_source(model, source, default=models.TextField())
                else:
                    model_field = field.queryset.model._meta.pk
            else:
                if isinstance(field.parent, serializers.ManyRelatedField):
                    model = field.parent.parent.Meta.model
                    source = field.parent.source.split('.')
                elif hasattr(field.parent, 'Meta') and hasattr(field.parent.Meta, 'model'):
                    model = field.parent.Meta.model
                    source = field.source.split('.')
                else:
                    warn(
                        f'Could not derive type for under-specified {field.__class__.__name__} '
                        f'"{field.field_name}". The serializer has no associated model (Meta class) '
                        f'and this particular field has no type without a model association. Consider '
                        f'changing the field or adding a Meta class. Defaulting to string.'
                    )
                    return append_meta(build_basic_type(OpenApiTypes.STR), meta)

                if is_slug:
                    source.append(field.slug_field)

                # estimates the relating model field and jumps to its target model PK field.
                # also differentiate as source can be direct (pk) or relation field (model).
                # be graceful and default to string.
                model_field = follow_field_source(model, source, default=models.TextField())

            # Special case: SlugRelatedField also allows to point to a callable @property.
            if callable(model_field):
                schema = self._map_response_type_hint(model_field)
            elif isinstance(model_field, models.Field):
                schema = self._map_model_field(model_field, direction)
            else:
                assert False, f'Field "{field.field_name}" must point to either a property or a model field.'

            # primary keys are usually non-editable (readOnly=True) and map_model_field correctly
            # signals that attribute. however this does not apply in the context of relations.
            schema.pop('readOnly', None)
            return append_meta(schema, meta)

        if isinstance(field, serializers.StringRelatedField):
            return append_meta(build_basic_type(OpenApiTypes.STR), meta)

        if isinstance(field, serializers.HyperlinkedIdentityField):
            return append_meta(build_basic_type(OpenApiTypes.URI), meta)

        if isinstance(field, serializers.HyperlinkedRelatedField):
            return append_meta(build_basic_type(OpenApiTypes.URI), meta)

        if isinstance(field, serializers.MultipleChoiceField):
            return append_meta(build_array_type(build_choice_field(field)), meta)

        if isinstance(field, serializers.ChoiceField):
            schema = build_choice_field(field)
            if 'description' in meta and 'description' in schema:
                meta['description'] = meta['description'] + '\n\n' + schema.pop('description')
            return append_meta(schema, meta)

        if isinstance(field, serializers.ListField):
            if isinstance(field.child, _UnvalidatedField):
                return append_meta(build_array_type(build_basic_type(OpenApiTypes.ANY)), meta)
            elif is_basic_serializer(field.child):
                component = self.resolve_serializer(field.child, direction)
                return append_meta(build_array_type(component.ref), meta) if component else None
            else:
                schema = self._map_serializer_field(field.child, direction)
                self._insert_field_validators(field.child, schema)
                # remove automatically attached but redundant title
                if is_trivial_string_variation(field.field_name, schema.get('title')):
                    schema.pop('title', None)
                return append_meta(build_array_type(schema), meta)

        # DateField and DateTimeField type is string
        if isinstance(field, serializers.DateField):
            return append_meta(build_basic_type(OpenApiTypes.DATE), meta)

        if isinstance(field, serializers.DateTimeField):
            return append_meta(build_basic_type(OpenApiTypes.DATETIME), meta)

        if isinstance(field, serializers.TimeField):
            return append_meta(build_basic_type(OpenApiTypes.TIME), meta)

        if isinstance(field, serializers.EmailField):
            return append_meta(build_basic_type(OpenApiTypes.EMAIL), meta)

        if isinstance(field, serializers.URLField):
            return append_meta(build_basic_type(OpenApiTypes.URI), meta)

        if isinstance(field, serializers.UUIDField):
            return append_meta(build_basic_type(OpenApiTypes.UUID), meta)

        if isinstance(field, serializers.DurationField):
            return append_meta(build_basic_type(OpenApiTypes.STR), meta)

        if isinstance(field, serializers.IPAddressField):
            # TODO this might be a DRF bug. protocol is not propagated to serializer although it
            #  should have been. results in always 'both' (thus no format)
            if 'ipv4' == field.protocol.lower():
                schema = build_basic_type(OpenApiTypes.IP4)
            elif 'ipv6' == field.protocol.lower():
                schema = build_basic_type(OpenApiTypes.IP6)
            else:
                schema = build_basic_type(OpenApiTypes.STR)
            return append_meta(schema, meta)

        # DecimalField has multipleOf based on decimal_places
        if isinstance(field, serializers.DecimalField):
            if getattr(field, 'coerce_to_string', api_settings.COERCE_DECIMAL_TO_STRING):
                content = {**build_basic_type(OpenApiTypes.STR), 'format': 'decimal'}
                if field.max_whole_digits is not None:
                    content['pattern'] = (
                        r'^-?0?' if field.max_whole_digits == 0 else fr'^-?\d{{0,{field.max_whole_digits}}}'
                    )
                    sep = get_format("DECIMAL_SEPARATOR") if field.localize else "."
                    content['pattern'] += fr'(?:\{sep}\d{{0,{field.decimal_places}}})?$'
            else:
                content = build_basic_type(OpenApiTypes.DECIMAL)
                if field.max_whole_digits is not None:
                    value = 10 ** field.max_whole_digits
                    content.update({
                        'maximum': value,
                        'minimum': -value,
                        'exclusiveMaximum': True,
                        'exclusiveMinimum': True,
                    })
                self._insert_min_max(field, content)
            return append_meta(content, meta)

        if isinstance(field, serializers.FloatField):
            content = build_basic_type(OpenApiTypes.DOUBLE)
            self._insert_min_max(field, content)
            return append_meta(content, meta)

        if isinstance(field, serializers.IntegerField):
            content = build_basic_type(OpenApiTypes.INT)
            self._insert_min_max(field, content)
            # Use int64 for format if value outside the 32-bit signed integer range [-2,147,483,648 to 2,147,483,647].
            if not all(-2147483648 <= int(content.get(key, 0)) <= 2147483647 for key in ('maximum', 'minimum')):
                content['format'] = 'int64'
            return append_meta(content, meta)

        if isinstance(field, serializers.FileField):
            if spectacular_settings.COMPONENT_SPLIT_REQUEST and direction == 'request':
                content = build_basic_type(OpenApiTypes.BINARY)
            else:
                use_url = getattr(field, 'use_url', api_settings.UPLOADED_FILES_USE_URL)
                content = build_basic_type(OpenApiTypes.URI if use_url else OpenApiTypes.STR)
            return append_meta(content, meta)

        if isinstance(field, serializers.SerializerMethodField):
            method = getattr(field.parent, field.method_name, None)
            if method is None:
                error(
                    f'SerializerMethodField "{field.field_name}" is missing required method '
                    f'"{field.method_name}". defaulting to "string".'
                )
                return append_meta(build_basic_type(OpenApiTypes.STR), meta)

            return append_meta(self._map_response_type_hint(method), meta)

        # NullBooleanField was removed in 3.14. Since 3.12.0 NullBooleanField was a subclass of BooleanField
        if hasattr(serializers, "NullBooleanField"):
            boolean_field_classes = (serializers.BooleanField, serializers.NullBooleanField)
        else:
            boolean_field_classes = (serializers.BooleanField,)

        if isinstance(field, boolean_field_classes):
            return append_meta(build_basic_type(OpenApiTypes.BOOL), meta)

        if isinstance(field, serializers.JSONField):
            return append_meta(build_basic_type(OpenApiTypes.ANY), meta)

        if isinstance(field, (serializers.DictField, serializers.HStoreField)):
            content = build_basic_type(OpenApiTypes.OBJECT)
            if not isinstance(field.child, _UnvalidatedField):
                content['additionalProperties'] = self._map_serializer_field(field.child, direction)
                self._insert_field_validators(field.child, content['additionalProperties'])
            return append_meta(content, meta)

        if isinstance(field, serializers.CharField):
            return append_meta(build_basic_type(OpenApiTypes.STR), meta)

        if isinstance(field, serializers.ReadOnlyField):
            # when field is nested inside a ListSerializer, the Meta class is 2 steps removed
            if is_list_serializer(field.parent):
                model = getattr(getattr(field.parent.parent, 'Meta', None), 'model', None)
                source = field.parent.source_attrs
            else:
                model = getattr(getattr(field.parent, 'Meta', None), 'model', None)
                source = field.source_attrs

            if model is None:
                warn(
                    f'Could not derive type for ReadOnlyField "{field.field_name}" because the '
                    f'serializer class has no associated model (Meta class). Consider using some '
                    f'other field like CharField(read_only=True) instead. defaulting to string.'
                )
                return append_meta(build_basic_type(OpenApiTypes.STR), meta)

            target = follow_field_source(model, source)
            if (
                hasattr(target, "_partialmethod")
                and target._partialmethod.func.__name__ == '_get_FIELD_display'
            ):
                # the only way to detect an uninitialized partial method
                # this is a convenience method for model choice fields and is mostly a string
                schema = build_basic_type(str)
            elif (
                hasattr(target, "__partialmethod__")
                and target.__partialmethod__.func.__name__ == '_get_FIELD_display'
            ):
                # same as above but specifically for python >= 3.13
                schema = build_basic_type(str)
            elif callable(target):
                schema = self._map_response_type_hint(target)
            elif isinstance(target, models.Field):
                schema = self._map_model_field(target, direction)
            else:
                assert False, f'ReadOnlyField target "{field}" must be property or model field'
            return append_meta(schema, meta)

        # DRF was not able to match the model field to an explicit SerializerField and therefore
        # used its generic fallback serializer field that simply wraps the model field.
        if isinstance(field, serializers.ModelField):
            schema = self._map_model_field(field.model_field, direction)
            return append_meta(schema, meta)

        warn(f'could not resolve serializer field "{field}". Defaulting to "string"')
        return append_meta(build_basic_type(OpenApiTypes.STR), meta)

    def _insert_min_max(self, field: Any, content: _SchemaType) -> None:
        if field.max_value is not None:
            content['maximum'] = field.max_value
            if 'exclusiveMaximum' in content:
                del content['exclusiveMaximum']
        if field.min_value is not None:
            content['minimum'] = field.min_value
            if 'exclusiveMinimum' in content:
                del content['exclusiveMinimum']

    def _map_serializer(self, serializer, direction, bypass_extensions=False):
        serializer = force_instance(serializer)
        serializer_extension = OpenApiSerializerExtension.get_match(serializer)

        if serializer_extension and not bypass_extensions:
            schema = serializer_extension.map_serializer(self, direction)
        else:
            schema = self._map_basic_serializer(serializer, direction)

        extensions = get_override(serializer, 'extensions', {})
        if extensions:
            schema.update(sanitize_specification_extensions(extensions))

        return self._postprocess_serializer_schema(schema, serializer, direction)

    def _postprocess_serializer_schema(self, schema, serializer, direction):
        """
        postprocess generated schema for component splitting, if enabled.
        does only apply to direct component schemas and not intermediate schemas
        like components composed of sub-component via e.g. oneOf.
        """
        if not spectacular_settings.COMPONENT_SPLIT_REQUEST:
            return schema

        properties = schema.get('properties', [])
        required = schema.get('required', [])

        for prop_name in list(properties):
            if direction == 'request' and properties[prop_name].get('readOnly'):
                del schema['properties'][prop_name]
                if prop_name in required:
                    required.remove(prop_name)
            if direction == 'response' and properties[prop_name].get('writeOnly'):
                del schema['properties'][prop_name]
                if prop_name in required:
                    required.remove(prop_name)

        # remove empty listing as it violates schema specification
        if 'required' in schema and not required:
            del schema['required']
        return schema

    def _get_serializer_field_meta(self, field, direction):
        if not isinstance(field, serializers.Field):
            return {}

        meta = {}
        if field.read_only:
            meta['readOnly'] = True
        if field.write_only:
            meta['writeOnly'] = True
        if field.allow_null:
            # this will be converted later in case of OAS 3.1
            meta['nullable'] = True
        if isinstance(field, serializers.CharField) and not field.allow_blank:
            # blank check only applies to inbound requests
            if spectacular_settings.COMPONENT_SPLIT_REQUEST:
                if direction == 'request':
                    meta['minLength'] = 1
            elif spectacular_settings.ENFORCE_NON_BLANK_FIELDS:
                if not field.read_only:
                    meta['minLength'] = 1
        if field.default is not None and field.default != empty and not callable(field.default):
            if isinstance(
                field,
                (
                    serializers.ModelField,
                    serializers.SerializerMethodField,
                    serializers.PrimaryKeyRelatedField,
                    serializers.SlugRelatedField,
                ),
            ):
                # Skip coercion for lack of a better solution. These are special in that they require
                # a model instance or object (which we don't have) instead of a plain value.
                default = field.default
            else:
                try:
                    # gracefully attempt to transform value or just use as plain on error
                    default = field.to_representation(field.default)
                except:  # noqa: E722
                    default = field.default
            if isinstance(default, set):
                default = list(default)
            meta['default'] = default
        if field.label and not is_trivial_string_variation(field.label, field.field_name):
            meta['title'] = str(field.label)
        if field.help_text:
            meta['description'] = str(field.help_text)
        return meta

    def _map_basic_serializer(self, serializer, direction):
        assert_basic_serializer(serializer)
        serializer = force_instance(serializer)
        # serializers provided through @extend_schema will not receive the mock context
        # via _get_serializer(). Establish behavioral symmetry for those use-cases.
        if not serializer.context:
            serializer.context.update(build_serializer_context(self.view))
        required = set()
        properties = {}

        for field in serializer.fields.values():
            if isinstance(field, serializers.HiddenField):
                continue
            if field.field_name in get_override(serializer, 'exclude_fields', []):
                continue

            schema = self._map_serializer_field(field, direction)
            # skip field if there is no schema for the direction
            if schema is None:
                continue

            add_to_required = (
                field.required
                or (schema.get('readOnly') and not spectacular_settings.COMPONENT_NO_READ_ONLY_REQUIRED)
            )
            if add_to_required:
                required.add(field.field_name)

            self._insert_field_validators(field, schema)

            if field.field_name in get_override(serializer, 'deprecate_fields', []):
                schema['deprecated'] = True

            properties[field.field_name] = safe_ref(schema)

        if is_patched_serializer(serializer, direction):
            required = []

        return build_object_type(
            properties=properties,
            required=required,
            description=get_override(serializer, 'description', get_doc(serializer.__class__)),
        )

    def _insert_field_validators(self, field, schema):
        schema_type = schema.get('type')

        # OAS 3.1 special case - extract the main type
        if isinstance(schema_type, list):
            schema_type = [t for t in schema_type if t != 'null'][0]

        def update_constraint(schema, key, function, value, *, exclusive=False):
            if callable(value):
                value = value()
            current_value = schema.get(key)
            if current_value is not None:
                new_value = function(current_value, value)
            else:
                new_value = value
            schema[key] = new_value
            if key in ('maximum', 'minimum'):
                exclusive_key = f'exclusive{key.title()}'
                if exclusive:
                    if new_value != current_value:
                        schema[exclusive_key] = True
                elif exclusive_key in schema:
                    del schema[exclusive_key]

        for v in field.validators:
            if schema_type == 'string':
                if isinstance(v, validators.EmailValidator):
                    if 'format' not in schema:
                        schema['format'] = 'email'
                elif isinstance(v, validators.URLValidator):
                    if 'format' not in schema:
                        schema['format'] = 'uri'
                elif isinstance(v, validators.RegexValidator):
                    if 'pattern' not in schema:
                        pattern = v.regex.pattern.encode('ascii', 'backslashreplace').decode()
                        pattern = pattern.replace(r'\x', r'\u00')  # unify escaping
                        pattern = pattern.replace(r'\Z', '$').replace(r'\A', '^')  # ECMA anchors
                        schema['pattern'] = pattern
                elif isinstance(v, validators.MaxLengthValidator):
                    update_constraint(schema, 'maxLength', min, v.limit_value)
                elif isinstance(v, validators.MinLengthValidator):
                    update_constraint(schema, 'minLength', max, v.limit_value)
                elif isinstance(v, validators.FileExtensionValidator) and v.allowed_extensions:
                    if 'pattern' not in schema:
                        schema['pattern'] = '(?:%s)$' % '|'.join([re.escape(extn) for extn in v.allowed_extensions])
            elif schema_type in ('integer', 'number'):
                if isinstance(v, validators.MaxValueValidator):
                    update_constraint(schema, 'maximum', min, v.limit_value)
                elif isinstance(v, validators.MinValueValidator):
                    update_constraint(schema, 'minimum', max, v.limit_value)
                elif isinstance(v, validators.DecimalValidator) and v.max_digits:
                    value = 10 ** (v.max_digits - (v.decimal_places or 0))
                    update_constraint(schema, 'maximum', min, value, exclusive=True)
                    update_constraint(schema, 'minimum', max, -value, exclusive=True)
            elif schema_type == 'array':
                if isinstance(v, validators.MaxLengthValidator):
                    update_constraint(schema, 'maxItems', min, v.limit_value)
                elif isinstance(v, validators.MinLengthValidator):
                    update_constraint(schema, 'minItems', max, v.limit_value)
            elif schema_type == 'object':
                if isinstance(v, validators.MaxLengthValidator):
                    update_constraint(schema, 'maxProperties', min, v.limit_value)
                elif isinstance(v, validators.MinLengthValidator):
                    update_constraint(schema, 'minProperties', max, v.limit_value)

    def _map_response_type_hint(self, method):
        hint = get_override(method, 'field') or get_type_hints(method).get('return')

        if is_serializer(hint) or is_field(hint):
            return self._map_serializer_field(force_instance(hint), 'response')
        if isinstance(hint, dict):
            return hint

        try:
            schema = resolve_type_hint(hint)
        except UnableToProceedError:
            warn(
                f'unable to resolve type hint for function "{method.__name__}". Consider '
                f'using a type hint or @extend_schema_field. Defaulting to string.'
            )
            return build_basic_type(OpenApiTypes.STR)

        description = get_doc(
            method.func if isinstance(method, functools.partial) else method
        )
        if description:
            schema['description'] = description

        return schema

    def _get_paginator(self):
        pagination_class = getattr(self.view, 'pagination_class', None)
        if pagination_class:
            return pagination_class()
        return None

    def get_paginated_name(self, serializer_name: str) -> str:
        return f'Paginated{serializer_name}List'

    def map_parsers(self) -> List[Any]:
        return list(dict.fromkeys([
            p.media_type for p in self.view.get_parsers()
            if whitelisted(p, spectacular_settings.PARSER_WHITELIST)
        ]))

    def map_renderers(self, attribute: str) -> List[Any]:
        assert attribute in ['media_type', 'format']

        # Either use whitelist or default back to old behavior by excluding BrowsableAPIRenderer
        def use_renderer(r):
            if spectacular_settings.RENDERER_WHITELIST is not None:
                return whitelisted(r, spectacular_settings.RENDERER_WHITELIST)
            else:
                return not isinstance(r, renderers.BrowsableAPIRenderer)

        return list(dict.fromkeys([
            getattr(r, attribute).split(';')[0]
            for r in self.view.get_renderers()
            if use_renderer(r) and hasattr(r, attribute)
        ]))

    def _get_serializer(self):
        view = self.view
        context = build_serializer_context(view)
        try:
            if isinstance(view, GenericAPIView):
                # try to circumvent queryset issues with calling get_serializer. if view has NOT
                # overridden get_serializer, its safe to use get_serializer_class.
                if view.__class__.get_serializer == GenericAPIView.get_serializer:
                    return view.get_serializer_class()(context=context)
                return view.get_serializer(context=context)
            elif isinstance(view, APIView):
                # APIView does not implement the required interface, but be lenient and make
                # good guesses before giving up and emitting a warning.
                if callable(getattr(view, 'get_serializer', None)):
                    return view.get_serializer(context=context)
                elif callable(getattr(view, 'get_serializer_class', None)):
                    return view.get_serializer_class()(context=context)
                elif hasattr(view, 'serializer_class'):
                    return view.serializer_class
                else:
                    error(
                        'unable to guess serializer. This is graceful fallback handling for APIViews. '
                        'Consider using GenericAPIView as view base class, if view is under your control. '
                        'Either way you may want to add a serializer_class (or method). Ignoring view for now.'
                    )
            else:
                error('Encountered unknown view base class. Please report this issue. Ignoring for now')
        except Exception as exc:
            error(
                f'exception raised while getting serializer. Hint: '
                f'Is get_serializer_class() returning None or is get_queryset() not working without '
                f'a request? Ignoring the view for now. (Exception: {exc})'
            )

    def get_examples(self) -> List[OpenApiExample]:
        """ override this for custom behaviour """
        return []

    def _get_examples(self, serializer, direction, media_type, status_code=None, extras=None):
        """ Handles examples for request/response. purposefully ignores parameter examples """

        # don't let the parameter examples influence the serializer example retrieval
        examples = [e for e in self.get_examples() if not e.parameter_only]

        # Examples from Serializers via @extend_schema_serializer are only considered, if
        # there were no higher priority examples directly from view annotation.
        if not examples:
            if is_list_serializer(serializer):
                examples = get_override(serializer.child, 'examples', [])
            elif is_serializer(serializer):
                examples = get_override(serializer, 'examples', [])

        # additional examples provided via OpenApiResponse/Request are merged with the other methods
        extras = extras or []

        filtered_examples = []
        for example in examples + extras:
            if direction == 'request' and example.response_only:
                continue
            if direction == 'response' and example.request_only:
                continue
            # default to 'application/json' unless nested in OpenApiResponse, in which case inherit
            if not example.media_type:
                example_media_type = media_type if (example in extras) else 'application/json'
            else:
                example_media_type = example.media_type
            if media_type and media_type != example_media_type:
                continue
            # default to [200, 201] unless nested in OpenApiResponse, in which case inherit
            if not example.status_codes:
                example_status_codes = (status_code,) if (example in extras) else ('200', '201')
            else:
                example_status_codes = tuple(map(str, example.status_codes))
            if status_code and status_code not in example_status_codes:
                continue

            if (
                self._is_list_view(serializer)
                and get_override(serializer, 'many') is not False
                and (
                    direction == 'request' or '200' <= status_code < '300'
                    or spectacular_settings.ENABLE_LIST_MECHANICS_ON_NON_2XX
                )
            ):
                # contain modification to this context so "listing" is not propagated elsewhere
                example = copy.copy(example)
                example.value = build_listed_example_value(example.value, self._get_paginator(), direction)

            filtered_examples.append(example)

        return build_examples_list(filtered_examples)

    def _get_request_body(self, direction='request'):
        # only unsafe methods can have a body
        if self.method not in ('PUT', 'PATCH', 'POST'):
            return None

        request_serializer = self.get_request_serializer()
        request_body_required = True
        content = []

        # either implicit media-types via available parsers or manual list via decoration
        if isinstance(request_serializer, dict):
            media_types_iter = request_serializer.items()
        else:
            media_types_iter = zip(self.map_parsers(), itertools.repeat(request_serializer))

        for media_type, serializer in media_types_iter:
            if isinstance(serializer, OpenApiRequest):
                serializer, examples, encoding = serializer.request, serializer.examples, serializer.encoding
            else:
                encoding, examples = None, None

            if (
                encoding
                and media_type != "application/x-www-form-urlencoded"
                and not media_type.startswith('multipart')
            ):
                warn(
                    'Encodings object on media types other than "application/x-www-form-urlencoded" '
                    'or "multipart/*" have undefined behavior.'
                )

            examples = self._get_examples(serializer, direction, media_type, None, examples)
            schema, partial_request_body_required = self._get_request_for_media_type(serializer, direction)

            if schema is not None:
                content.append((media_type, schema, examples, encoding))
                request_body_required &= partial_request_body_required

        if not content:
            return None

        request_body = {
            'content': {
                media_type: build_media_type_object(schema, examples, encoding)
                for media_type, schema, examples, encoding in content
            }
        }
        if request_body_required:
            request_body['required'] = request_body_required
        return request_body

    def _get_request_for_media_type(self, serializer, direction='request'):
        serializer = force_instance(serializer)

        if is_list_serializer(serializer):
            schema = self._unwrap_list_serializer(serializer, direction)
            request_body_required = bool(schema)
        elif is_serializer(serializer):
            if self.method == 'PATCH':
                # we simulate what DRF is doing: the entry serializer is set to partial
                # for PATCH requests. serializer instances received via extend_schema
                # may be reused; prevent race conditions by modifying a copy.
                serializer = copy.copy(serializer)
                serializer.partial = True
            component = self.resolve_serializer(serializer, direction)
            if not component:
                # serializer is empty so skip content enumeration
                return None, False
            schema = component.ref
            # request body is only required if any required property is not read-only
            readonly_props = [
                p for p, s in component.schema.get('properties', {}).items() if s.get('readOnly')
            ]
            required_props = component.schema.get('required', [])
            request_body_required = any(req not in readonly_props for req in required_props)
        elif is_basic_type(serializer):
            schema = build_basic_type(serializer)
            request_body_required = False
        elif is_higher_order_type_hint(serializer):
            schema = resolve_type_hint(serializer)
            request_body_required = False
        elif isinstance(serializer, dict):
            # bypass processing and use given schema directly
            schema = serializer
            request_body_required = False
        else:
            warn(
                f'could not resolve request body for {self.method} {self.path}. Defaulting to generic '
                'free-form object. (Maybe annotate a Serializer class?)'
            )
            schema = build_generic_type()
            schema['description'] = 'Unspecified request body'
            request_body_required = False
        return schema, request_body_required

    def _get_response_bodies(self, direction: Direction = 'response') -> _SchemaType:
        response_serializers = self.get_response_serializers()

        if (
            is_serializer(response_serializers)
            or is_basic_type(response_serializers)
            or is_higher_order_type_hint(response_serializers)
            or isinstance(response_serializers, OpenApiResponse)
        ):
            if self.method == 'DELETE':
                return {'204': {'description': _('No response body')}}
            if self._is_create_operation():
                return {'201': self._get_response_for_code(response_serializers, '201', direction=direction)}
            return {'200': self._get_response_for_code(response_serializers, '200', direction=direction)}
        elif isinstance(response_serializers, dict):
            # custom handling for overriding default return codes with @extend_schema
            responses = {}
            for code, serializer in response_serializers.items():
                if isinstance(code, tuple):
                    code, media_types = str(code[0]), code[1:]
                else:
                    code, media_types = str(code), None
                content_response = self._get_response_for_code(serializer, code, media_types, direction)
                if code in responses:
                    responses[code]['content'].update(content_response['content'])
                else:
                    responses[code] = content_response
            return responses
        else:
            warn(
                f'could not resolve "{response_serializers}" for {self.method} {self.path}. '
                f'Expected either a serializer or some supported override mechanism. '
                f'Defaulting to generic free-form object.'
            )
            schema = build_basic_type(OpenApiTypes.OBJECT)
            schema['description'] = _('Unspecified response body')  # type: ignore
            return {'200': self._get_response_for_code(schema, '200', direction=direction)}

    def _unwrap_list_serializer(self, serializer, direction: Direction) -> Optional[_SchemaType]:
        if is_field(serializer):
            return self._map_serializer_field(serializer, direction)
        elif is_basic_serializer(serializer):
            component = self.resolve_serializer(serializer, direction)
            return component.ref if component else None
        elif is_list_serializer(serializer):
            result = self._unwrap_list_serializer(serializer.child, direction)
            return build_array_type(result) if result else None
        else:
            assert False, 'Serializer is of unknown type.'

    def _get_response_for_code(self, serializer, status_code, media_types=None, direction='response'):
        if isinstance(serializer, OpenApiResponse):
            serializer, description, examples = (
                serializer.response, serializer.description, serializer.examples
            )
        else:
            description, examples = '', []

        serializer = force_instance(serializer)
        headers = self._get_response_headers_for_code(status_code, direction)
        headers = {'headers': headers} if headers else {}

        if not serializer:
            return {**headers, 'description': description or _('No response body')}
        elif is_list_serializer(serializer):
            schema = self._unwrap_list_serializer(serializer.child, direction)
            if not schema:
                return {**headers, 'description': description or _('No response body')}
        elif is_serializer(serializer):
            component = self.resolve_serializer(serializer, direction)
            if not component:
                return {**headers, 'description': description or _('No response body')}
            schema = component.ref
        elif is_basic_type(serializer):
            schema = build_basic_type(serializer)
        elif is_higher_order_type_hint(serializer):
            schema = resolve_type_hint(serializer)
        elif isinstance(serializer, dict):
            # bypass processing and use given schema directly
            schema = serializer
            # prevent invalid dict case in _is_list_view() as this not a status_code dict.
            serializer = None
        else:
            warn(
                f'could not resolve "{serializer}" for {self.method} {self.path}. Expected either '
                f'a serializer or some supported override mechanism. Defaulting to '
                f'generic free-form object.'
            )
            schema = build_basic_type(OpenApiTypes.OBJECT)
            schema['description'] = _('Unspecified response body')

        if (
            self._is_list_view(serializer)
            and get_override(serializer, 'many') is not False
            and ('200' <= status_code < '300' or spectacular_settings.ENABLE_LIST_MECHANICS_ON_NON_2XX)
        ):
            # In case of a non-default ListSerializer, check for matching extension and
            # bypass regular list wrapping by delegating handling to extension.
            if (
                is_list_serializer_customized(serializer)
                and OpenApiSerializerExtension.get_match(get_list_serializer(serializer))
            ):
                schema = self._map_serializer(get_list_serializer(serializer), direction)
            else:
                schema = build_array_type(schema)

            paginator = self._get_paginator()

            if (
                paginator
                and is_serializer(serializer)
                and (not is_list_serializer(serializer) or is_serializer(serializer.child))
            ):
                component = ResolvedComponent(
                    name=self.get_paginated_name(self._get_serializer_name(serializer, 'response')),
                    type=ResolvedComponent.SCHEMA,
                    schema=paginator.get_paginated_response_schema(schema),
                    object=self.get_serializer_identity(
                        serializer.child if is_list_serializer(serializer) else serializer, 'response'
                    )
                )
                self.registry.register_on_missing(component)
                schema = component.ref
            elif paginator:
                schema = paginator.get_paginated_response_schema(schema)

        if not media_types:
            media_types = self.map_renderers('media_type')

        media_types = modify_media_types_for_versioning(self.view, media_types)

        return {
            **headers,
            'content': {
                media_type: build_media_type_object(
                    schema,
                    self._get_examples(serializer, direction, media_type, status_code, examples)
                )
                for media_type in media_types
            },
            'description': description
        }

    def _get_response_headers_for_code(self, status_code, direction='response') -> _SchemaType:
        result = {}
        for parameter in self.get_override_parameters():
            if not isinstance(parameter, OpenApiParameter):
                continue
            if not parameter.response:
                continue
            if (
                isinstance(parameter.response, list)
                and status_code not in [str(code) for code in parameter.response]
            ):
                continue

            if is_basic_type(parameter.type):
                schema = build_basic_type(parameter.type)
            elif is_serializer(parameter.type):
                schema = self.resolve_serializer(parameter.type, direction).ref
            else:
                schema = parameter.type  # type: ignore

            if not schema:
                warn(f'response parameter {parameter.name} requires non-empty schema')
                continue

            if parameter.location not in [OpenApiParameter.HEADER, OpenApiParameter.COOKIE]:
                warn(f'incompatible location type ignored for response parameter {parameter.name}')

            parameter_type = build_parameter_type(
                name=parameter.name,
                schema=schema,
                location=parameter.location,
                required=parameter.required,
                description=parameter.description,
                enum=parameter.enum,
                pattern=parameter.pattern,
                deprecated=parameter.deprecated,
                style=parameter.style,
                explode=parameter.explode,
                default=parameter.default,
                allow_blank=parameter.allow_blank,
                examples=build_examples_list(parameter.examples),
                extensions=parameter.extensions,
            )
            del parameter_type['name']
            del parameter_type['in']
            result[parameter.name] = parameter_type

        return result

    def get_serializer_identity(self, serializer, direction: Direction) -> Any:
        serializer_extension = OpenApiSerializerExtension.get_match(serializer)
        if serializer_extension:
            identity = serializer_extension.get_identity(self, direction)
            if identity is not None:
                return identity

        return serializer

    def get_serializer_name(self, serializer: serializers.Serializer, direction: Direction) -> str:
        """ override this for custom behaviour """
        return serializer.__class__.__name__

    def _get_serializer_name(self, serializer, direction, bypass_extensions=False) -> str:
        serializer_extension = OpenApiSerializerExtension.get_match(serializer)
        if serializer_extension and not bypass_extensions:
            custom_name = serializer_extension.get_name(**filter_supported_arguments(
                serializer_extension.get_name, auto_schema=self, direction=direction
            ))
        else:
            custom_name = None

        if custom_name:
            name = custom_name
        elif has_override(serializer, 'component_name'):
            name = get_override(serializer, 'component_name')
        elif getattr(getattr(serializer, 'Meta', None), 'ref_name', None) is not None:
            # local override mechanisms. for compatibility with drf-yasg we support meta ref_name,
            # though we do not support the serializer inlining feature.
            # https://drf-yasg.readthedocs.io/en/stable/custom_spec.html#serializer-meta-nested-class
            name = serializer.Meta.ref_name
        elif is_list_serializer(serializer):
            return self._get_serializer_name(serializer.child, direction)
        else:
            name = self.get_serializer_name(serializer, direction)

        assert name

        if name.endswith('Serializer'):
            name = name[:-10]

        if is_patched_serializer(serializer, direction):
            name = 'Patched' + name

        if direction == 'request' and spectacular_settings.COMPONENT_SPLIT_REQUEST:
            name = name + 'Request'

        if not re.match(r'^[\w.-]+$', name):
            warn(
                f'Component name "{name}" contains illegal characters. Only "A-Z a-z 0-9 - . _" '
                f'are allowed. Furthermore, "-" and "." are discoursed due to potential tooling '
                f'issues. This likely leads to an invalid schema.'
            )

        return name

    def resolve_serializer(
            self, serializer: _SerializerType, direction: Direction, bypass_extensions=False
    ) -> ResolvedComponent:
        assert_basic_serializer(serializer)
        serializer = force_instance(serializer)

        with add_trace_message(serializer.__class__):
            component = ResolvedComponent(
                name=self._get_serializer_name(serializer, direction, bypass_extensions),
                type=ResolvedComponent.SCHEMA,
                object=self.get_serializer_identity(serializer, direction),
            )
            if component in self.registry:
                return self.registry[component]  # return component with schema

            self.registry.register(component)
            component.schema = self._map_serializer(serializer, direction, bypass_extensions)

            discard_component = (
                # components with empty schemas serve no purpose
                not component.schema
                # concrete component without properties are likely only transactional so discard
                or (
                    component.schema.get('type') == 'object'
                    and not component.schema.get('properties')
                    and 'additionalProperties' not in component.schema
                )
            )

            if discard_component:
                del self.registry[component]
                return ResolvedComponent(None, None)  # sentinel
            return component
