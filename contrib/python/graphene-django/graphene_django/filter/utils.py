from django import forms
from django_filters.utils import get_model_field

import graphene

from ..forms import GlobalIDFormField, GlobalIDMultipleChoiceField
from .filters import ListFilter, RangeFilter, TypedFilter
from .filterset import custom_filterset_factory, setup_filterset


def get_field_type(registry, model, field_name):
    """
    Try to get a model field corresponding Graphql type from the DjangoObjectType.
    """
    object_type = registry.get_type_for_model(model)
    if object_type:
        object_type_field = object_type._meta.fields.get(field_name)
        if object_type_field:
            field_type = object_type_field.type
            if isinstance(field_type, graphene.NonNull):
                field_type = field_type.of_type
            return field_type
    return None


def get_filtering_args_from_filterset(filterset_class, type):
    """
    Inspect a FilterSet and produce the arguments to pass to a Graphene Field.
    These arguments will be available to filter against in the GraphQL API.
    """
    from ..forms.converter import convert_form_field

    args = {}
    model = filterset_class._meta.model
    registry = type._meta.registry
    for name, filter_field in filterset_class.base_filters.items():
        filter_type = filter_field.lookup_expr
        required = filter_field.extra.get("required", False)
        field_type = None
        form_field = None

        if (
            isinstance(filter_field, TypedFilter)
            and filter_field.input_type is not None
        ):
            # First check if the filter input type has been explicitly given
            field_type = filter_field.input_type
        else:
            if name not in filterset_class.declared_filters or isinstance(
                filter_field, TypedFilter
            ):
                # Get the filter field for filters that are no explicitly declared.
                if filter_type == "isnull":
                    field_type = graphene.Boolean
                else:
                    model_field = get_model_field(model, filter_field.field_name)

                    # Get the form field either from:
                    #  1. the formfield corresponding to the model field
                    #  2. the field defined on filter
                    if hasattr(model_field, "formfield"):
                        form_field = model_field.formfield(required=required)
                    if not form_field:
                        form_field = filter_field.field

                    # First try to get the matching field type from the GraphQL DjangoObjectType
                    if model_field:
                        if (
                            isinstance(form_field, forms.ModelChoiceField)
                            or isinstance(form_field, forms.ModelMultipleChoiceField)
                            or isinstance(form_field, GlobalIDMultipleChoiceField)
                            or isinstance(form_field, GlobalIDFormField)
                        ):
                            # Foreign key have dynamic types and filtering on a foreign key actually means filtering on its ID.
                            field_type = get_field_type(
                                registry, model_field.related_model, "id"
                            )
                        else:
                            field_type = get_field_type(
                                registry, model_field.model, model_field.name
                            )

            if not field_type:
                # Fallback on converting the form field either because:
                #  - it's an explicitly declared filters
                #  - we did not manage to get the type from the model type
                form_field = form_field or filter_field.field
                field_type = convert_form_field(form_field).get_type()

            if isinstance(filter_field, ListFilter) or isinstance(
                filter_field, RangeFilter
            ):
                # Replace InFilter/RangeFilter filters (`in`, `range`) argument type to be a list of
                # the same type as the field. See comments in `replace_csv_filters` method for more details.
                field_type = graphene.List(field_type)

        args[name] = graphene.Argument(
            field_type,
            description=filter_field.label,
            required=required,
        )

    return args


def get_filterset_class(filterset_class, **meta):
    """
    Get the class to be used as the FilterSet.
    """
    if filterset_class:
        # If were given a FilterSet class, then set it up.
        graphene_filterset_class = setup_filterset(filterset_class)
    else:
        # Otherwise create one.
        graphene_filterset_class = custom_filterset_factory(**meta)

    replace_csv_filters(graphene_filterset_class)
    return graphene_filterset_class


def replace_csv_filters(filterset_class):
    """
    Replace the "in" and "range" filters (that are not explicitly declared)
    to not be BaseCSVFilter (BaseInFilter, BaseRangeFilter) objects anymore
    but our custom InFilter/RangeFilter filter class that use the input
    value as filter argument on the queryset.

    This is because those BaseCSVFilter are expecting a string as input with
    comma separated values.
    But with GraphQl we can actually have a list as input and have a proper
    type verification of each value in the list.

    See issue https://github.com/graphql-python/graphene-django/issues/1068.
    """
    for name, filter_field in list(filterset_class.base_filters.items()):
        # Do not touch any declared filters
        if name in filterset_class.declared_filters:
            continue

        filter_type = filter_field.lookup_expr
        if filter_type == "in":
            filterset_class.base_filters[name] = ListFilter(
                field_name=filter_field.field_name,
                lookup_expr=filter_field.lookup_expr,
                label=filter_field.label,
                method=filter_field.method,
                exclude=filter_field.exclude,
                **filter_field.extra,
            )
        elif filter_type == "range":
            filterset_class.base_filters[name] = RangeFilter(
                field_name=filter_field.field_name,
                lookup_expr=filter_field.lookup_expr,
                label=filter_field.label,
                method=filter_field.method,
                exclude=filter_field.exclude,
                **filter_field.extra,
            )
