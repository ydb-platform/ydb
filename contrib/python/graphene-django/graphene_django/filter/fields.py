from collections import OrderedDict
from functools import partial

from django.core.exceptions import ValidationError

from graphene.types.argument import to_arguments
from graphene.types.enum import EnumType
from graphene.utils.str_converters import to_snake_case

from ..fields import DjangoConnectionField
from .utils import get_filtering_args_from_filterset, get_filterset_class


def convert_enum(data):
    """
    Check if the data is a enum option (or potentially nested list of enum option)
    and convert it to its value.

    This method is used to pre-process the data for the filters as they can take an
    graphene.Enum as argument, but filters (from django_filters) expect a simple value.
    """
    if isinstance(data, list):
        return [convert_enum(item) for item in data]
    if isinstance(type(data), EnumType):
        return data.value
    else:
        return data


class DjangoFilterConnectionField(DjangoConnectionField):
    def __init__(
        self,
        type_,
        fields=None,
        order_by=None,
        extra_filter_meta=None,
        filterset_class=None,
        *args,
        **kwargs,
    ):
        self._fields = fields
        self._provided_filterset_class = filterset_class
        self._filterset_class = None
        self._filtering_args = None
        self._extra_filter_meta = extra_filter_meta
        self._base_args = None
        super().__init__(type_, *args, **kwargs)

    @property
    def args(self):
        return to_arguments(self._base_args or OrderedDict(), self.filtering_args)

    @args.setter
    def args(self, args):
        self._base_args = args

    @property
    def filterset_class(self):
        if not self._filterset_class:
            fields = self._fields or self.node_type._meta.filter_fields
            meta = {"model": self.model, "fields": fields}
            if self._extra_filter_meta:
                meta.update(self._extra_filter_meta)

            filterset_class = (
                self._provided_filterset_class or self.node_type._meta.filterset_class
            )
            self._filterset_class = get_filterset_class(filterset_class, **meta)

        return self._filterset_class

    @property
    def filtering_args(self):
        if not self._filtering_args:
            self._filtering_args = get_filtering_args_from_filterset(
                self.filterset_class, self.node_type
            )
        return self._filtering_args

    @classmethod
    def resolve_queryset(
        cls, connection, iterable, info, args, filtering_args, filterset_class
    ):
        def filter_kwargs():
            kwargs = {}
            for k, v in args.items():
                if k in filtering_args:
                    if k == "order_by" and v is not None:
                        v = to_snake_case(v)
                    kwargs[k] = convert_enum(v)
            return kwargs

        qs = super().resolve_queryset(connection, iterable, info, args)

        filterset = filterset_class(
            data=filter_kwargs(), queryset=qs, request=info.context
        )
        if filterset.is_valid():
            return filterset.qs
        raise ValidationError(filterset.form.errors.as_json())

    def get_queryset_resolver(self):
        return partial(
            self.resolve_queryset,
            filterset_class=self.filterset_class,
            filtering_args=self.filtering_args,
        )
