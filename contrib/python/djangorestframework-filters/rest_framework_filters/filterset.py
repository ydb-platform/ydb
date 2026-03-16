import copy
from collections import OrderedDict

from django.db.models.constants import LOOKUP_SEP
from django_filters import filterset, rest_framework
from django_filters.utils import get_model_field

from . import filters, utils


def related(filterset, filter_name):
    # Return a related filter_name, using the filterset relationship if present.
    if not filterset.relationship:
        return filter_name
    return LOOKUP_SEP.join([filterset.relationship, filter_name])


class FilterSetMetaclass(filterset.FilterSetMetaclass):
    def __new__(cls, name, bases, attrs):
        attrs['auto_filters'] = cls.get_auto_filters(bases, attrs)

        new_class = super().__new__(cls, name, bases, attrs)

        new_class.related_filters = OrderedDict([
            (name, f) for name, f in new_class.declared_filters.items()
            if isinstance(f, filters.BaseRelatedFilter)])

        # See: :meth:`rest_framework_filters.filters.RelatedFilter.bind`
        for f in new_class.related_filters.values():
            f.bind_filterset(new_class)

        # Only expand when model is defined. Model may be undefined for mixins.
        if new_class._meta.model is not None:
            for name, f in new_class.auto_filters.items():
                expanded = cls.expand_auto_filter(new_class, name, f)
                new_class.base_filters.update(expanded)

            for name, f in new_class.related_filters.items():
                expanded = cls.expand_auto_filter(new_class, name, f)
                new_class.base_filters.update(expanded)

        return new_class

    @classmethod
    def get_auto_filters(cls, bases, attrs):
        # Auto filters are specially handled since they aren't an actual filter
        # subclass and aren't handled by the declared filter machinery. Note
        # that this is a nearly identical copy of `get_declared_filters`.
        auto_filters = [
            (filter_name, attrs.pop(filter_name))
            for filter_name, obj in list(attrs.items())
            if isinstance(obj, filters.AutoFilter)
        ]

        # Default the `filter.field_name` to the attribute name on the filterset
        for filter_name, f in auto_filters:
            if getattr(f, 'field_name', None) is None:
                f.field_name = filter_name

        auto_filters.sort(key=lambda x: x[1].creation_counter)

        # merge auto filters from base classes
        for base in reversed(bases):
            if hasattr(base, 'auto_filters'):
                auto_filters = [
                    (name, f) for name, f
                    in base.auto_filters.items()
                    if name not in attrs
                ] + auto_filters

        return OrderedDict(auto_filters)

    @classmethod
    def expand_auto_filter(cls, new_class, filter_name, f):
        """Resolve an ``AutoFilter`` into its per-lookup filters.

        This method name is slightly inaccurate since it handles both
        :class:`rest_framework_filters.filters.AutoFilter` and
        :class:`rest_framework_filters.filters.BaseRelatedFilter`, as well as
        their subclasses, which all support per-lookup filter generation.

        Args:
            new_class: The ``FilterSet`` class to generate filters for.
            filter_name: The attribute name of the filter on the ``FilterSet``.
            f: The filter instance.

        Returns:
            A named map of generated filter objects.
        """
        expanded = OrderedDict()

        # get reference to opts/declared filters so originals aren't modified
        orig_meta, orig_declared = new_class._meta, new_class.declared_filters
        new_class._meta = copy.deepcopy(new_class._meta)
        new_class.declared_filters = {}

        # Use meta.fields to generate auto filters
        new_class._meta.fields = {f.field_name: f.lookups or []}
        for gen_name, gen_f in new_class.get_filters().items():
            # get_filters() generates param names from the model field name, so
            # replace the field name with the param name from the filerset
            gen_name = gen_name.replace(f.field_name, filter_name, 1)

            # do not overwrite declared filters
            if gen_name not in orig_declared:
                expanded[gen_name] = gen_f

        # restore reference to opts/declared filters
        new_class._meta, new_class.declared_filters = orig_meta, orig_declared

        return expanded


class SubsetDisabledMixin:
    """Disable filter subsetting (see: :meth:`FilterSet.disable_subset`)."""

    @classmethod
    def get_filter_subset(cls, params, rel=None):
        return cls.base_filters


class FilterSet(rest_framework.FilterSet, metaclass=FilterSetMetaclass):

    def __init__(self, data=None, queryset=None, *, relationship=None, **kwargs):
        self.base_filters = self.get_filter_subset(data or {}, relationship)

        super().__init__(data, queryset, **kwargs)

        self.relationship = relationship
        self.related_filtersets = self.get_related_filtersets()
        self.filters = self.get_request_filters()

    @classmethod
    def get_fields(cls):
        # Extend the 'Meta.fields' dict syntax to allow '__all__' field lookups.
        fields = super(FilterSet, cls).get_fields()

        for name, lookups in fields.items():
            if lookups == filters.ALL_LOOKUPS:
                field = get_model_field(cls._meta.model, name)
                if field is not None:
                    fields[name] = utils.lookups_for_field(field)
                else:
                    # FilterSet will handle invalid name
                    fields[name] = []

        return fields

    @classmethod
    def get_filter_subset(cls, params, rel=None):
        """Get the subset of filters that should be initialized by this filterset class.

        A filterset may have a large number of filters, and selecting the subset based on
        the request ``params`` minimizes the cost of initialization by reducing the total
        number of expensive deepcopy operations. See :meth:`.get_param_filter_name()`
        for a better understanding of how the filter names are resolved.

        Args:
            params: The request's query params.
            rel (str, optional): The relationship the ``params`` are resolved against.

        Returns:
            Mapping of the resolved ``{filter names: filter instances}``.
        """
        # Determine names of filters from query params and remove empty values.
        # param names that traverse relations are translated to just the local
        # filter names. eg, `author__username` => `author`. Empty values are
        # removed, as they indicate an unknown field eg, author__foobar__isnull
        filter_names = {cls.get_param_filter_name(param, rel) for param in params}
        filter_names = {f for f in filter_names if f is not None}
        return OrderedDict(
            (k, v) for k, v in cls.base_filters.items() if k in filter_names
        )

    @classmethod
    def disable_subset(cls, *, depth=0):
        """Disable filter subsetting, allowing a form to render the complete filterset.

        Note that this decreases performance and should only be used when rendering a
        form, such as with DRF's browsable API.

        Args:
            depth (int, optional): Disable related filterset subsetting to this depth.
                The form should not render rels beyond this depth.

        Returns:
            This filterset class with subset disabling mixed in.
        """
        if not issubclass(cls, SubsetDisabledMixin):
            cls = type('SubsetDisabled%s' % cls.__name__,
                       (SubsetDisabledMixin, cls), {})

        # recursively disable subset for related filtersets
        if depth > 0:
            # shallow copy to prevent modifying original `base_filters`
            cls.base_filters = cls.base_filters.copy()

            # deepcopy RelateFilter to prevent modifying original `.filterset`
            for name in cls.related_filters:
                f = copy.deepcopy(cls.base_filters[name])
                f.filterset = f.filterset.disable_subset(depth=depth - 1)
                cls.base_filters[name] = f

        return cls

    @classmethod
    def get_param_filter_name(cls, param, rel=None):
        """Resolve a query parameter name into a filter name.

        This is primarily used to resolve query parameters respective to their base or
        related filtersets. e.g.,

        .. code-block:: python

            # regular attribute filters
            >>> FilterSet.get_param_filter_name('email')
            'email'

            # exclusion filters
            >>> FilterSet.get_param_filter_name('email!')
            'email'

            # related filters
            >>> FilterSet.get_param_filter_name('author__email')
            'author'

            # attribute filters based on relationship
            >>> FilterSet.get_param_filter_name('author__email', rel='author')
            'email'

        Args:
            param (str): The query paramater.
            rel (str, optional): The relationship a ``param`` is resolved against.

        Returns:
            The parameter name relative to the relationship.
        """
        # check for empty param
        if not param:
            return param

        # check that param is not the rel.
        if param == rel:
            return None

        # strip the rel prefix from the param name.
        prefix = '%s%s' % (rel or '', LOOKUP_SEP)
        if rel and param.startswith(prefix):
            param = param[len(prefix):]

        # Attempt to match against filters with lookups first. (username__endswith)
        if param in cls.base_filters:
            return param

        # Attempt to match against exclusion filters
        if param[-1] == '!' and param[:-1] in cls.base_filters:
            return param[:-1]

        # Match against relationships. (author__username__endswith).
        # Preference more specific filters. eg, `note__author` over `note`.
        for name in sorted(cls.related_filters, reverse=True):
            # we need to match against '__' to prevent eager matching against
            # like names. eg, note vs note2. Exact matches are handled above.
            if param.startswith("%s%s" % (name, LOOKUP_SEP)):
                return name

    def get_request_filters(self):
        """Build a set of filters based on the request data.

        This currently includes only filter exclusion/negation.

        Returns:
            Mapping of expanded ``{filter names: filter instances}``.
        """
        # build the compiled set of all filters
        requested_filters = OrderedDict()
        for filter_name, f in self.filters.items():
            requested_filters[filter_name] = f

            # exclusion params
            exclude_name = '%s!' % filter_name
            if related(self, exclude_name) in self.data:
                # deepcopy the *base* filter to prevent copying of model & parent
                f_copy = copy.deepcopy(self.base_filters[filter_name])
                f_copy.parent = f.parent
                f_copy.model = f.model
                f_copy.exclude = not f.exclude

                requested_filters[exclude_name] = f_copy

        return requested_filters

    def get_related_filtersets(self):
        """Get the related filterset instances for all related filters.

        Returns:
            Mapping of related ``{filter names: filterset instances}``.
        """
        related_filtersets = OrderedDict()

        for related_name in self.related_filters:
            if related_name not in self.filters:
                continue

            f = self.filters[related_name]
            related_filtersets[related_name] = f.filterset(
                data=self.data,
                queryset=f.get_queryset(self.request),
                relationship=related(self, related_name),
                request=self.request,
                prefix=self.form_prefix,
            )

        return related_filtersets

    def filter_queryset(self, queryset):
        queryset = super(FilterSet, self).filter_queryset(queryset)
        queryset = self.filter_related_filtersets(queryset)
        return queryset

    def filter_related_filtersets(self, queryset):
        """Filter the provided ``queryset`` by the ``related_filtersets``.

        Override this method to change the filtering behavior across relationships.

        Args:
            queryset: The filterset's filtered queryset.

        Returns:
            The ``queryset`` filtered by its related filtersets' querysets.
        """
        for related_name, related_filterset in self.related_filtersets.items():
            # Related filtersets should only be applied if they had data.
            prefix = '%s%s' % (related(self, related_name), LOOKUP_SEP)
            if not any(value.startswith(prefix) for value in self.data):
                continue

            field = self.filters[related_name].field
            to_field_name = getattr(field, 'to_field_name', 'pk') or 'pk'

            field_name = self.filters[related_name].field_name
            lookup_expr = LOOKUP_SEP.join([field_name, 'in'])

            subquery = related_filterset.qs.values(to_field_name)
            queryset = queryset.filter(**{lookup_expr: subquery})

            # handle disinct
            if self.related_filters[related_name].distinct:
                queryset = queryset.distinct()

        return queryset

    def get_form_class(self):
        class Form(super(FilterSet, self).get_form_class()):
            def add_prefix(form, field_name):
                field_name = related(self, field_name)
                return super(Form, form).add_prefix(field_name)

            def clean(form):
                cleaned_data = super(Form, form).clean()

                # when prefixing the errors, use the related filter name,
                # which is relative to the parent filterset, not the root.
                for related_filterset in self.related_filtersets.values():
                    for key, error in related_filterset.form.errors.items():
                        self.form.errors[related(related_filterset, key)] = error

                return cleaned_data
        return Form

    @property
    def form(self):
        from django_filters import compat

        form = super().form
        if compat.is_crispy():
            from crispy_forms.helper import FormHelper

            form.helper = FormHelper(form)
            form.helper.form_tag = False
            form.helper.disable_csrf = True
            form.helper.template_pack = 'bootstrap3'

        return form
