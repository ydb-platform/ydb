from django_filters.constants import EMPTY_VALUES
from django_filters.filters import FilterMethod

from .typed_filter import TypedFilter


class ArrayFilterMethod(FilterMethod):
    def __call__(self, qs, value):
        if value is None:
            return qs
        return self.method(qs, self.f.field_name, value)


class ArrayFilter(TypedFilter):
    """
    Filter made for PostgreSQL ArrayField.
    """

    @TypedFilter.method.setter
    def method(self, value):
        """
        Override method setter so that in case a custom `method` is provided
        (see documentation https://django-filter.readthedocs.io/en/stable/ref/filters.html#method),
        it doesn't fall back to checking if the value is in `EMPTY_VALUES` (from the `__call__` method
        of the `FilterMethod` class) and instead use our ArrayFilterMethod that consider empty lists as values.

        Indeed when providing a `method` the `filter` method below is overridden and replaced by `FilterMethod(self)`
        which means that the validation of the empty value is made by the `FilterMethod.__call__` method instead.
        """
        TypedFilter.method.fset(self, value)
        if value is not None:
            self.filter = ArrayFilterMethod(self)

    def filter(self, qs, value):
        """
        Override the default filter class to check first whether the list is
        empty or not.
        This needs to be done as in this case we expect to get the filter applied with
        an empty list since it's a valid value but django_filter consider an empty list
        to be an empty input value (see `EMPTY_VALUES`) meaning that
        the filter does not need to be applied (hence returning the original
        queryset).
        """
        if value in EMPTY_VALUES and value != []:
            return qs
        if self.distinct:
            qs = qs.distinct()
        lookup = f"{self.field_name}__{self.lookup_expr}"
        qs = self.get_method(qs)(**{lookup: value})
        return qs
