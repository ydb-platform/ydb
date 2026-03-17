import warnings

from django.utils.module_loading import import_string
from django_filters.rest_framework.filters import *  # noqa
from django_filters.rest_framework.filters import (
    ModelChoiceFilter, ModelMultipleChoiceFilter,
)

ALL_LOOKUPS = '__all__'


class AutoFilter:
    """A placeholder class that enables generating multiple per-lookup filters.

    This is a declarative alternative to the ``Meta.fields`` dict syntax, and
    the below are functionally equivalent:

    .. code-block:: python

        class PersonFilter(filters.FilterSet):
            name = AutoFilter(lookups=['exact', 'contains'])

            class Meta:
                model = Person
                fields = []

        class PersonFilter(filters.FilterSet):
            class Meta:
                model = Person
                fields = {'name': ['exact', 'contains']}

    Due to its declarative nature, an ``AutoFilter`` allows for paramater name
    aliasing for its generated filters. e.g.,

    .. code-block:: python

        class BlogFilter(filters.FilterSet):
            title = AutoFilter(field_name='name', lookups=['contains'])

    The above generates a ``title__contains`` filter for the ``name`` field
    intstead of ``name__contains``. This is not possible with ``Meta.fields``,
    since the key must match the model field name.

    Note that ``AutoFilter`` is a filter-like placeholder and is not present in
    the ``FilterSet.filters``. In the above example, ``title`` would not be
    filterable. However, an ``AutoFilter`` is typically replaced by a generated
    ``exact`` filter of the same name, which enables filtering by that param.
    """

    creation_counter = 0

    def __init__(self, field_name=None, *, lookups=None):
        self.field_name = field_name
        self.lookups = lookups or []

        self.creation_counter = AutoFilter.creation_counter
        AutoFilter.creation_counter += 1


class BaseRelatedFilter:

    def __init__(self, filterset, *args, lookups=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.filterset = filterset
        self.lookups = lookups or []

    def bind_filterset(self, filterset):
        """Bind a filterset class to the filter instance.

        This class is used for relative imports. Only the first bound class is used as
        filterset inheritance might otherwise break these relative import paths.

        This is also necessary to allow ``.filterset`` to be resolved during FilterSet
        class creation time, instead of during initialization.

        Args:
            filterset: The filterset to bind
        """
        if not hasattr(self, 'bound_filterset'):
            self.bound_filterset = filterset

    def filterset():
        def fget(self):
            if isinstance(self._filterset, str):
                try:
                    # Assume absolute import path
                    self._filterset = import_string(self._filterset)
                except ImportError:
                    # Fallback to building import path relative to bind class
                    path = '.'.join([self.bound_filterset.__module__, self._filterset])
                    self._filterset = import_string(path)
            return self._filterset

        def fset(self, value):
            self._filterset = value

        return locals()
    filterset = property(**filterset())

    def get_queryset(self, request):
        queryset = super(BaseRelatedFilter, self).get_queryset(request)
        assert queryset is not None, \
            "Expected `.get_queryset()` for related filter '%s.%s' to " \
            "return a `QuerySet`, but got `None`." \
            % (self.parent.__class__.__name__, self.field_name)
        return queryset


class RelatedFilter(BaseRelatedFilter, ModelChoiceFilter):
    """A ``ModelChoiceFilter`` that enables filtering across relationships.

    Take the following example:

        class ManagerFilter(filters.FilterSet):
            class Meta:
                model = Manager
                fields = {'name': ['exact', 'in', 'startswith']}

        class DepartmentFilter(filters.FilterSet):
            manager = RelatedFilter(ManagerFilter, queryset=managers)

            class Meta:
                model = Department
                fields = {'name': ['exact', 'in', 'startswith']}

    In the above, the ``DepartmentFilter`` can traverse the ``manager``
    relationship with the ``__`` lookup seperator, accessing the filters of the
    ``ManagerFilter`` class. For example, the above would enable calls like:

        /api/managers?name=john%20doe
        /api/departments?manager__name=john%20doe

    Related filters function similarly to auto filters in that they can generate
    per-lookup filters. However, unlike auto filters, related filters are
    functional and not just placeholders. They will not be replaced by a
    generated ``exact`` filter.

    Attributes:
        filterset: The ``FilterSet`` that is traversed by this relationship.
            May be a class, an absolute import path, or the name of a class
            located in the same module as the origin filterset.
        lookups: A list of lookups to generate per-lookup filters for. This
            functions similarly to the ``AutoFilter.lookups`` argument.
    """


class RelatedMultipleFilter(BaseRelatedFilter, ModelMultipleChoiceFilter):
    """A ``ModelMultipleChoiceFilter`` variant of ``RelatedFilter``."""


class AllLookupsFilter(AutoFilter):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, lookups=ALL_LOOKUPS, **kwargs)
        warnings.warn(
            "`AllLookupsFilter()` has been deprecated in favor of "
            "`AutoFilter(lookups='__all__')`.",
            DeprecationWarning,
            stacklevel=2,
        )
