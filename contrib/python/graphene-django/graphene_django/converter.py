import inspect
from functools import partial, singledispatch, wraps

from django.db import models
from django.utils.encoding import force_str
from django.utils.functional import Promise
from django.utils.module_loading import import_string
from graphql import GraphQLError

from graphene import (
    ID,
    UUID,
    Boolean,
    Date,
    DateTime,
    Decimal,
    Dynamic,
    Enum,
    Field,
    Float,
    Int,
    List,
    NonNull,
    String,
    Time,
)
from graphene.types.json import JSONString
from graphene.types.resolver import get_default_resolver
from graphene.types.scalars import BigInt
from graphene.utils.str_converters import to_camel_case

try:
    from graphql import assert_name
except ImportError:
    # Support for older versions of graphql
    from graphql import assert_valid_name as assert_name
from graphql.pyutils import register_description

from .compat import ArrayField, HStoreField, RangeField, normalize_choices
from .fields import DjangoConnectionField, DjangoListField
from .settings import graphene_settings
from .utils.str_converters import to_const


class BlankValueField(Field):
    def wrap_resolve(self, parent_resolver):
        resolver = self.resolver or parent_resolver

        # create custom resolver
        def blank_field_wrapper(func):
            @wraps(func)
            def wrapped_resolver(*args, **kwargs):
                return_value = func(*args, **kwargs)
                if return_value == "":
                    return None
                return return_value

            return wrapped_resolver

        return blank_field_wrapper(resolver)


class EnumValueField(BlankValueField):
    def wrap_resolve(self, parent_resolver):
        resolver = super().wrap_resolve(parent_resolver)

        # create custom resolver
        def enum_field_wrapper(func):
            @wraps(func)
            def wrapped_resolver(*args, **kwargs):
                return_value = func(*args, **kwargs)
                if isinstance(return_value, models.Choices):
                    return_value = return_value.value
                return return_value

            return wrapped_resolver

        return enum_field_wrapper(resolver)


def convert_choice_name(name):
    name = to_const(force_str(name))
    try:
        assert_name(name)
    except GraphQLError:
        name = "A_%s" % name
    return name


def get_choices(choices):
    converted_names = []
    choices = normalize_choices(choices)
    for value, help_text in choices:
        if isinstance(help_text, (tuple, list)):
            yield from get_choices(help_text)
        else:
            name = convert_choice_name(value)
            while name in converted_names:
                name += "_" + str(len(converted_names))
            converted_names.append(name)
            description = str(
                help_text
            )  # TODO: translatable description: https://github.com/graphql-python/graphql-core-next/issues/58
            yield name, value, description


def convert_choices_to_named_enum_with_descriptions(name, choices):
    choices = list(get_choices(choices))
    named_choices = [(c[0], c[1]) for c in choices]
    named_choices_descriptions = {c[0]: c[2] for c in choices}

    class EnumWithDescriptionsType:
        @property
        def description(self):
            return str(named_choices_descriptions[self.name])

    return_type = Enum(
        name,
        list(named_choices),
        type=EnumWithDescriptionsType,
        description="An enumeration.",  # Temporary fix until https://github.com/graphql-python/graphene/pull/1502 is merged
    )
    return return_type


def generate_enum_name(django_model_meta, field):
    if graphene_settings.DJANGO_CHOICE_FIELD_ENUM_CUSTOM_NAME:
        # Try and import custom function
        custom_func = import_string(
            graphene_settings.DJANGO_CHOICE_FIELD_ENUM_CUSTOM_NAME
        )
        name = custom_func(field)
    elif graphene_settings.DJANGO_CHOICE_FIELD_ENUM_V2_NAMING is True:
        name = to_camel_case(f"{django_model_meta.object_name}_{field.name}")
    else:
        name = "{app_label}{object_name}{field_name}Choices".format(
            app_label=to_camel_case(django_model_meta.app_label.title()),
            object_name=django_model_meta.object_name,
            field_name=to_camel_case(field.name.title()),
        )
    return name


def convert_choice_field_to_enum(field, name=None):
    if name is None:
        name = generate_enum_name(field.model._meta, field)
    choices = field.choices
    return convert_choices_to_named_enum_with_descriptions(name, choices)


def convert_django_field_with_choices(
    field, registry=None, convert_choices_to_enum=None
):
    if registry is not None:
        converted = registry.get_converted_field(field)
        if converted:
            return converted
    choices = getattr(field, "choices", None)
    if convert_choices_to_enum is None:
        convert_choices_to_enum = bool(
            graphene_settings.DJANGO_CHOICE_FIELD_ENUM_CONVERT
        )
    if choices and convert_choices_to_enum:
        EnumCls = convert_choice_field_to_enum(field)
        required = not (field.blank or field.null)

        converted = EnumCls(
            description=get_django_field_description(field), required=required
        ).mount_as(EnumValueField)
    else:
        converted = convert_django_field(field, registry)
    if registry is not None:
        registry.register_converted_field(field, converted)
    return converted


def get_django_field_description(field):
    return str(field.help_text) if field.help_text else None


@singledispatch
def convert_django_field(field, registry=None):
    raise Exception(
        f"Don't know how to convert the Django field {field} ({field.__class__})"
    )


@convert_django_field.register(models.CharField)
@convert_django_field.register(models.TextField)
@convert_django_field.register(models.EmailField)
@convert_django_field.register(models.SlugField)
@convert_django_field.register(models.URLField)
@convert_django_field.register(models.GenericIPAddressField)
@convert_django_field.register(models.FileField)
@convert_django_field.register(models.FilePathField)
def convert_field_to_string(field, registry=None):
    return String(
        description=get_django_field_description(field), required=not field.null
    )


@convert_django_field.register(models.AutoField)
@convert_django_field.register(models.BigAutoField)
@convert_django_field.register(models.SmallAutoField)
def convert_field_to_id(field, registry=None):
    return ID(description=get_django_field_description(field), required=not field.null)


@convert_django_field.register(models.UUIDField)
def convert_field_to_uuid(field, registry=None):
    return UUID(
        description=get_django_field_description(field), required=not field.null
    )


@convert_django_field.register(models.BigIntegerField)
def convert_big_int_field(field, registry=None):
    return BigInt(description=field.help_text, required=not field.null)


@convert_django_field.register(models.PositiveIntegerField)
@convert_django_field.register(models.PositiveSmallIntegerField)
@convert_django_field.register(models.SmallIntegerField)
@convert_django_field.register(models.IntegerField)
def convert_field_to_int(field, registry=None):
    return Int(description=get_django_field_description(field), required=not field.null)


@convert_django_field.register(models.NullBooleanField)
@convert_django_field.register(models.BooleanField)
def convert_field_to_boolean(field, registry=None):
    return Boolean(
        description=get_django_field_description(field), required=not field.null
    )


@convert_django_field.register(models.DecimalField)
def convert_field_to_decimal(field, registry=None):
    return Decimal(
        description=get_django_field_description(field), required=not field.null
    )


@convert_django_field.register(models.FloatField)
@convert_django_field.register(models.DurationField)
def convert_field_to_float(field, registry=None):
    return Float(
        description=get_django_field_description(field), required=not field.null
    )


@convert_django_field.register(models.DateTimeField)
def convert_datetime_to_string(field, registry=None):
    return DateTime(
        description=get_django_field_description(field), required=not field.null
    )


@convert_django_field.register(models.DateField)
def convert_date_to_string(field, registry=None):
    return Date(
        description=get_django_field_description(field), required=not field.null
    )


@convert_django_field.register(models.TimeField)
def convert_time_to_string(field, registry=None):
    return Time(
        description=get_django_field_description(field), required=not field.null
    )


@convert_django_field.register(models.OneToOneRel)
def convert_onetoone_field_to_djangomodel(field, registry=None):
    from graphene.utils.str_converters import to_snake_case

    from .types import DjangoObjectType

    model = field.related_model

    def dynamic_type():
        _type = registry.get_type_for_model(model)
        if not _type:
            return

        class CustomField(Field):
            def wrap_resolve(self, parent_resolver):
                """
                Implements a custom resolver which goes through the `get_node` method to ensure that
                it goes through the `get_queryset` method of the DjangoObjectType.
                """
                resolver = super().wrap_resolve(parent_resolver)

                # If `get_queryset` was not overridden in the DjangoObjectType
                # or if we explicitly bypass the `get_queryset` method,
                # we can just return the default resolver.
                if (
                    _type.get_queryset.__func__
                    is DjangoObjectType.get_queryset.__func__
                    or getattr(resolver, "_bypass_get_queryset", False)
                ):
                    return resolver

                def custom_resolver(root, info, **args):
                    # Note: this function is used to resolve 1:1 relation fields

                    is_resolver_awaitable = inspect.iscoroutinefunction(resolver)

                    if is_resolver_awaitable:
                        fk_obj = resolver(root, info, **args)
                        # In case the resolver is a custom awaitable resolver that overwrites
                        # the default Django resolver
                        return fk_obj

                    field_name = to_snake_case(info.field_name)
                    reversed_field_name = root.__class__._meta.get_field(
                        field_name
                    ).remote_field.name
                    try:
                        return _type.get_queryset(
                            _type._meta.model.objects.filter(
                                **{reversed_field_name: root.pk}
                            ),
                            info,
                        ).get()
                    except _type._meta.model.DoesNotExist:
                        return None

                return custom_resolver

        return CustomField(
            _type,
            required=not field.null,
        )

    return Dynamic(dynamic_type)


@convert_django_field.register(models.ManyToManyField)
@convert_django_field.register(models.ManyToManyRel)
@convert_django_field.register(models.ManyToOneRel)
def convert_field_to_list_or_connection(field, registry=None):
    model = field.related_model

    def dynamic_type():
        _type = registry.get_type_for_model(model)
        if not _type:
            return

        if isinstance(field, models.ManyToManyField):
            description = get_django_field_description(field)
        else:
            description = get_django_field_description(field.field)

        # If there is a connection, we should transform the field
        # into a DjangoConnectionField
        if _type._meta.connection:
            # Use a DjangoFilterConnectionField if there are
            # defined filter_fields or a filterset_class in the
            # DjangoObjectType Meta
            if _type._meta.filter_fields or _type._meta.filterset_class:
                from .filter.fields import DjangoFilterConnectionField

                return DjangoFilterConnectionField(
                    _type, required=True, description=description
                )

            return DjangoConnectionField(_type, required=True, description=description)

        return DjangoListField(
            _type,
            required=True,  # A Set is always returned, never None.
            description=description,
        )

    return Dynamic(dynamic_type)


@convert_django_field.register(models.OneToOneField)
@convert_django_field.register(models.ForeignKey)
def convert_field_to_djangomodel(field, registry=None):
    from graphene.utils.str_converters import to_snake_case

    from .types import DjangoObjectType

    model = field.related_model

    def dynamic_type():
        _type = registry.get_type_for_model(model)
        if not _type:
            return

        class CustomField(Field):
            def wrap_resolve(self, parent_resolver):
                """
                Implements a custom resolver which goes through the `get_node` method to ensure that
                it goes through the `get_queryset` method of the DjangoObjectType.
                """
                resolver = super().wrap_resolve(parent_resolver)

                # If `get_queryset` was not overridden in the DjangoObjectType
                # or if we explicitly bypass the `get_queryset` method,
                # we can just return the default resolver.
                if (
                    _type.get_queryset.__func__
                    is DjangoObjectType.get_queryset.__func__
                    or getattr(resolver, "_bypass_get_queryset", False)
                ):
                    return resolver

                def custom_resolver(root, info, **args):
                    # Note: this function is used to resolve FK or 1:1 fields
                    #   it does not differentiate between custom-resolved fields
                    #   and default resolved fields.

                    # because this is a django foreign key or one-to-one field, the primary-key for
                    # this node can be accessed from the root node.
                    # ex: article.reporter_id

                    # get the name of the id field from the root's model
                    field_name = to_snake_case(info.field_name)
                    db_field_key = root.__class__._meta.get_field(field_name).attname
                    if hasattr(root, db_field_key):
                        # get the object's primary-key from root
                        object_pk = getattr(root, db_field_key)
                    else:
                        return None

                    is_resolver_awaitable = inspect.iscoroutinefunction(resolver)

                    if is_resolver_awaitable:
                        fk_obj = resolver(root, info, **args)
                        # In case the resolver is a custom awaitable resolver that overwrites
                        # the default Django resolver
                        return fk_obj

                    instance_from_get_node = _type.get_node(info, object_pk)

                    if instance_from_get_node is None:
                        # no instance to return
                        return
                    elif (
                        isinstance(resolver, partial)
                        and resolver.func is get_default_resolver()
                    ):
                        return instance_from_get_node
                    elif resolver is not get_default_resolver():
                        # Default resolver is overridden
                        # For optimization, add the instance to the resolver
                        setattr(root, field_name, instance_from_get_node)
                        # Explanation:
                        #   previously, _type.get_node` is called which results in at least one hit to the database.
                        #   But, if we did not pass the instance to the root, calling the resolver will result in
                        #   another call to get the instance which results in at least two database queries in total
                        #   to resolve this node only.
                        #   That's why the value of the object is set in the root so when the object is accessed
                        #   in the resolver (root.field_name) it does not access the database unless queried explicitly.
                        fk_obj = resolver(root, info, **args)
                        return fk_obj
                    else:
                        return instance_from_get_node

                return custom_resolver

        return CustomField(
            _type,
            description=get_django_field_description(field),
            required=not field.null,
        )

    return Dynamic(dynamic_type)


@convert_django_field.register(ArrayField)
def convert_postgres_array_to_list(field, registry=None):
    inner_type = convert_django_field(field.base_field)
    if not isinstance(inner_type, (List, NonNull)):
        inner_type = (
            NonNull(type(inner_type))
            if inner_type.kwargs["required"]
            else type(inner_type)
        )
    return List(
        inner_type,
        description=get_django_field_description(field),
        required=not field.null,
    )


@convert_django_field.register(HStoreField)
@convert_django_field.register(models.JSONField)
def convert_json_field_to_string(field, registry=None):
    return JSONString(
        description=get_django_field_description(field), required=not field.null
    )


@convert_django_field.register(RangeField)
def convert_postgres_range_to_string(field, registry=None):
    inner_type = convert_django_field(field.base_field)
    if not isinstance(inner_type, (List, NonNull)):
        inner_type = (
            NonNull(type(inner_type))
            if inner_type.kwargs["required"]
            else type(inner_type)
        )
    return List(
        inner_type,
        description=get_django_field_description(field),
        required=not field.null,
    )


# Register Django lazy()-wrapped values as GraphQL description/help_text.
# This is needed for using lazy translations, see https://github.com/graphql-python/graphql-core-next/issues/58.
register_description(Promise)
