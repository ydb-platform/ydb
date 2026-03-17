from django_filters.filters import FilterMethod

from .typed_filter import TypedFilter


class ListFilterMethod(FilterMethod):
    def __call__(self, qs, value):
        if value is None:
            return qs
        return self.method(qs, self.f.field_name, value)


class ListFilter(TypedFilter):
    """
    Filter that takes a list of value as input.
    It is for example used for `__in` filters.
    """

    @TypedFilter.method.setter
    def method(self, value):
        """
        Override method setter so that in case a custom `method` is provided
        (see documentation https://django-filter.readthedocs.io/en/stable/ref/filters.html#method),
        it doesn't fall back to checking if the value is in `EMPTY_VALUES` (from the `__call__` method
        of the `FilterMethod` class) and instead use our ListFilterMethod that consider empty lists as values.

        Indeed when providing a `method` the `filter` method below is overridden and replaced by `FilterMethod(self)`
        which means that the validation of the empty value is made by the `FilterMethod.__call__` method instead.
        """
        TypedFilter.method.fset(self, value)
        if value is not None:
            self.filter = ListFilterMethod(self)

    def filter(self, qs, value):
        """
        Override the default filter class to check first whether the list is
        empty or not.
        This needs to be done as in this case we expect to get an empty output
        (if not an exclude filter) but django_filter consider an empty list
        to be an empty input value (see `EMPTY_VALUES`) meaning that
        the filter does not need to be applied (hence returning the original
        queryset).
        """
        if value is not None and len(value) == 0:
            if self.exclude:
                return qs
            else:
                return qs.none()
        else:
            return super().filter(qs, value)
