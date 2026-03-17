import warnings
from collections import OrderedDict
from typing import Type  # noqa: F401

from django.db.models import Model  # noqa: F401

import graphene
from graphene.relay import Connection, Node
from graphene.types.objecttype import ObjectType, ObjectTypeOptions
from graphene.types.utils import yank_fields_from_attrs

from .converter import convert_django_field_with_choices
from .registry import Registry, get_global_registry
from .settings import graphene_settings
from .utils import (
    DJANGO_FILTER_INSTALLED,
    camelize,
    get_model_fields,
    is_valid_django_model,
)

ALL_FIELDS = "__all__"


def construct_fields(
    model, registry, only_fields, exclude_fields, convert_choices_to_enum=None
):
    _model_fields = get_model_fields(model)

    fields = OrderedDict()
    for name, field in _model_fields:
        is_not_in_only = (
            only_fields is not None
            and only_fields != ALL_FIELDS
            and name not in only_fields
        )
        # is_already_created = name in options.fields
        is_excluded = (
            exclude_fields is not None and name in exclude_fields
        )  # or is_already_created
        # https://docs.djangoproject.com/en/1.10/ref/models/fields/#django.db.models.ForeignKey.related_query_name
        is_no_backref = str(name).endswith("+")
        if is_not_in_only or is_excluded or is_no_backref:
            # We skip this field if we specify only_fields and is not
            # in there. Or when we exclude this field in exclude_fields.
            # Or when there is no back reference.
            continue

        _convert_choices_to_enum = convert_choices_to_enum
        if isinstance(_convert_choices_to_enum, list):
            # then `convert_choices_to_enum` is a list of field names to convert
            if name in _convert_choices_to_enum:
                _convert_choices_to_enum = True
            else:
                _convert_choices_to_enum = False

        converted = convert_django_field_with_choices(
            field, registry, convert_choices_to_enum=_convert_choices_to_enum
        )
        fields[name] = converted

    return fields


def validate_fields(type_, model, fields, only_fields, exclude_fields):
    # Validate the given fields against the model's fields and custom fields
    all_field_names = set(fields.keys())
    only_fields = only_fields if only_fields is not ALL_FIELDS else ()
    for name in only_fields or ():
        if name in all_field_names:
            continue

        if hasattr(model, name):
            warnings.warn(
                (
                    'Field name "{field_name}" matches an attribute on Django model "{app_label}.{object_name}" '
                    "but it's not a model field so Graphene cannot determine what type it should be. "
                    'Either define the type of the field on DjangoObjectType "{type_}" or remove it from the "fields" list.'
                ).format(
                    field_name=name,
                    app_label=model._meta.app_label,
                    object_name=model._meta.object_name,
                    type_=type_,
                )
            )

        else:
            warnings.warn(
                (
                    'Field name "{field_name}" doesn\'t exist on Django model "{app_label}.{object_name}". '
                    'Consider removing the field from the "fields" list of DjangoObjectType "{type_}" because it has no effect.'
                ).format(
                    field_name=name,
                    app_label=model._meta.app_label,
                    object_name=model._meta.object_name,
                    type_=type_,
                )
            )

    # Validate exclude fields
    for name in exclude_fields or ():
        if name in all_field_names:
            # Field is a custom field
            warnings.warn(
                f'Excluding the custom field "{name}" on DjangoObjectType "{type_}" has no effect. '
                'Either remove the custom field or remove the field from the "exclude" list.'
            )
        else:
            if not hasattr(model, name):
                warnings.warn(
                    (
                        'Django model "{app_label}.{object_name}" does not have a field or attribute named "{field_name}". '
                        'Consider removing the field from the "exclude" list of DjangoObjectType "{type_}" because it has no effect'
                    ).format(
                        field_name=name,
                        app_label=model._meta.app_label,
                        object_name=model._meta.object_name,
                        type_=type_,
                    )
                )


class DjangoObjectTypeOptions(ObjectTypeOptions):
    model = None  # type: Type[Model]
    registry = None  # type: Registry
    connection = None  # type: Type[Connection]

    filter_fields = ()
    filterset_class = None


class DjangoObjectType(ObjectType):
    @classmethod
    def __init_subclass_with_meta__(
        cls,
        model=None,
        registry=None,
        skip_registry=False,
        only_fields=None,  # deprecated in favour of `fields`
        fields=None,
        exclude_fields=None,  # deprecated in favour of `exclude`
        exclude=None,
        filter_fields=None,
        filterset_class=None,
        connection=None,
        connection_class=None,
        use_connection=None,
        interfaces=(),
        convert_choices_to_enum=None,
        _meta=None,
        **options,
    ):
        assert is_valid_django_model(model), (
            'You need to pass a valid Django Model in {}.Meta, received "{}".'
        ).format(cls.__name__, model)

        if not registry:
            registry = get_global_registry()

        assert isinstance(registry, Registry), (
            f"The attribute registry in {cls.__name__} needs to be an instance of "
            f'Registry, received "{registry}".'
        )

        if filter_fields and filterset_class:
            raise Exception("Can't set both filter_fields and filterset_class")

        if not DJANGO_FILTER_INSTALLED and (filter_fields or filterset_class):
            raise Exception(
                "Can only set filter_fields or filterset_class if "
                "Django-Filter is installed"
            )

        assert not (fields and exclude), (
            "Cannot set both 'fields' and 'exclude' options on "
            f"DjangoObjectType {cls.__name__}."
        )

        # Alias only_fields -> fields
        if only_fields and fields:
            raise Exception("Can't set both only_fields and fields")
        if only_fields:
            warnings.warn(
                "Defining `only_fields` is deprecated in favour of `fields`.",
                DeprecationWarning,
                stacklevel=2,
            )
            fields = only_fields
        if fields and fields != ALL_FIELDS and not isinstance(fields, (list, tuple)):
            raise TypeError(
                'The `fields` option must be a list or tuple or "__all__". '
                "Got %s." % type(fields).__name__
            )

        # Alias exclude_fields -> exclude
        if exclude_fields and exclude:
            raise Exception("Can't set both exclude_fields and exclude")
        if exclude_fields:
            warnings.warn(
                "Defining `exclude_fields` is deprecated in favour of `exclude`.",
                DeprecationWarning,
                stacklevel=2,
            )
            exclude = exclude_fields
        if exclude and not isinstance(exclude, (list, tuple)):
            raise TypeError(
                "The `exclude` option must be a list or tuple. Got %s."
                % type(exclude).__name__
            )

        if fields is None and exclude is None:
            warnings.warn(
                "Creating a DjangoObjectType without either the `fields` "
                "or the `exclude` option is deprecated. Add an explicit `fields "
                f"= '__all__'` option on DjangoObjectType {cls.__name__} to use all "
                "fields",
                DeprecationWarning,
                stacklevel=2,
            )

        django_fields = yank_fields_from_attrs(
            construct_fields(model, registry, fields, exclude, convert_choices_to_enum),
            _as=graphene.Field,
        )

        if use_connection is None and interfaces:
            use_connection = any(
                issubclass(interface, Node) for interface in interfaces
            )

        if use_connection and not connection:
            # We create the connection automatically
            if not connection_class:
                connection_class = Connection

            connection = connection_class.create_type(
                "{}Connection".format(options.get("name") or cls.__name__), node=cls
            )

        if connection is not None:
            assert issubclass(
                connection, Connection
            ), f"The connection must be a Connection. Received {connection.__name__}"

        if not _meta:
            _meta = DjangoObjectTypeOptions(cls)

        _meta.model = model
        _meta.registry = registry
        _meta.filter_fields = filter_fields
        _meta.filterset_class = filterset_class
        _meta.fields = django_fields
        _meta.connection = connection
        _meta.convert_choices_to_enum = convert_choices_to_enum

        super().__init_subclass_with_meta__(
            _meta=_meta, interfaces=interfaces, **options
        )

        # Validate fields
        validate_fields(cls, model, _meta.fields, fields, exclude)

        if not skip_registry:
            registry.register(cls)

    def resolve_id(self, info):
        return self.pk

    @classmethod
    def is_type_of(cls, root, info):
        if isinstance(root, cls):
            return True
        if not is_valid_django_model(root.__class__):
            raise Exception(f'Received incompatible instance "{root}".')

        if cls._meta.model._meta.proxy:
            model = root._meta.model
        else:
            model = root._meta.model._meta.concrete_model

        return model == cls._meta.model

    @classmethod
    def get_queryset(cls, queryset, info):
        return queryset

    @classmethod
    def get_node(cls, info, id):
        queryset = cls.get_queryset(cls._meta.model.objects, info)
        try:
            return queryset.get(pk=id)
        except cls._meta.model.DoesNotExist:
            return None


class ErrorType(ObjectType):
    field = graphene.String(required=True)
    messages = graphene.List(graphene.NonNull(graphene.String), required=True)

    @classmethod
    def from_errors(cls, errors):
        data = camelize(errors) if graphene_settings.CAMELCASE_ERRORS else errors
        return [cls(field=key, messages=value) for key, value in data.items()]
